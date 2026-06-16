"""
BTC Data Collector - Chainlink Only
- Chainlink BTC/USD on Polygon (primary, no API key)
- Chainlink BTC/USD on Ethereum (fallback)
- CoinGecko REST (final fallback, free)
- Builds synthetic candles from price polling
- Calculates all technical indicators
"""
import asyncio
import aiohttp
import time
import logging
from collections import deque
from typing import Optional, List, Dict, Tuple, Deque
import statistics
import math

logger = logging.getLogger(__name__)


class RingBuffer:
    """Fixed-size circular buffer for price data"""
    def __init__(self, maxlen: int):
        self._buf: Deque = deque(maxlen=maxlen)

    def append(self, val): self._buf.append(val)
    def __len__(self):     return len(self._buf)
    def __iter__(self):    return iter(self._buf)
    def to_list(self):     return list(self._buf)

    @property
    def last(self):  return self._buf[-1] if self._buf else None
    @property
    def mean(self):  return statistics.mean(self._buf) if self._buf else 0
    @property
    def stdev(self): return statistics.stdev(self._buf) if len(self._buf) > 1 else 0


class ChainlinkFeed:
    """
    Fetches BTC/USD from Chainlink on-chain oracles.
    No API key required. Uses free public RPC endpoints.
    """

    # Chainlink BTC/USD on Polygon Mainnet
    POLYGON_RPC      = "https://polygon-rpc.com"
    BTC_USD_POLYGON  = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

    # Chainlink BTC/USD on Ethereum Mainnet
    ETH_RPC          = "https://ethereum.publicnode.com"
    BTC_USD_ETH      = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88"

    # latestAnswer() selector
    LATEST_ANSWER    = "0x50d25bcd"

    def __init__(self):
        self.price:       float = 0.0
        self.last_update: float = 0.0
        self.source:      str   = "none"

    async def get_price(self) -> float:
        """Try Polygon → Ethereum → CoinGecko"""

        # 1. Polygon Chainlink
        price = await self._call(self.POLYGON_RPC, self.BTC_USD_POLYGON)
        if price > 1000:
            self._set(price, "chainlink_polygon")
            return price

        # 2. Ethereum Chainlink
        price = await self._call(self.ETH_RPC, self.BTC_USD_ETH)
        if price > 1000:
            self._set(price, "chainlink_ethereum")
            return price

        # 3. CoinGecko free API
        price = await self._coingecko()
        if price > 1000:
            self._set(price, "coingecko")
            return price

        # Return last known price if all fail
        logger.warning("All price sources failed — using last known price")
        return self.price

    def _set(self, price: float, source: str):
        self.price       = price
        self.last_update = time.time()
        self.source      = source
        logger.debug(f"Price: ${price:,.2f} ({source})")

    async def _call(self, rpc: str, contract: str) -> float:
        """Call latestAnswer() on a Chainlink contract"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "method":  "eth_call",
                "params":  [{"to": contract, "data": self.LATEST_ANSWER}, "latest"],
                "id":      1,
            }
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    rpc, json=payload,
                    timeout=aiohttp.ClientTimeout(total=6)
                ) as resp:
                    data = await resp.json()

            result = data.get("result", "0x0")
            if not result or result in ("0x", "0x0"):
                return 0.0

            price = int(result, 16) / 1e8   # 8 decimals
            return price if 1000 < price < 10_000_000 else 0.0

        except Exception as e:
            logger.debug(f"Chainlink call failed ({rpc}): {e}")
            return 0.0

    async def _coingecko(self) -> float:
        """CoinGecko simple price — free, no key"""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    "https://api.coingecko.com/api/v3/simple/price",
                    params={"ids": "bitcoin", "vs_currencies": "usd"},
                    timeout=aiohttp.ClientTimeout(total=6),
                ) as resp:
                    data = await resp.json()
            return float(data["bitcoin"]["usd"])
        except Exception as e:
            logger.debug(f"CoinGecko failed: {e}")
            return 0.0


class BTCDataFeed:
    """
    BTC data feed using Chainlink as sole price source.
    Polls price every 30 seconds and builds 1-minute OHLCV candles.
    Calculates full technical indicator suite.
    """

    POLL_INTERVAL = 30   # seconds between Chainlink polls

    def __init__(self, db=None):
        self.db        = db
        self.chainlink = ChainlinkFeed()

        self.current_price: float = 0.0
        self.last_update:   float = 0.0
        self.price_source:  str   = "none"

        # Candle data
        self.candle_1m:  Dict        = {}
        self.candles_1m: Deque[Dict] = deque(maxlen=500)

        # Price buffers
        self.closes_1m  = RingBuffer(500)
        self.highs_1m   = RingBuffer(500)
        self.lows_1m    = RingBuffer(500)
        self.volumes_1m = RingBuffer(500)

        # Order flow (approximated from price direction)
        self.buy_volume_1m:  float = 0.0
        self.sell_volume_1m: float = 0.0

        # Indicators cache
        self._indicators:            Dict  = {}
        self._last_indicator_update: float = 0.0

        # Dummy order book (no exchange)
        self.bid_price: float = 0.0
        self.ask_price: float = 0.0
        self.spread:    float = 0.0

        self._running  = False
        self._poll_task: Optional[asyncio.Task] = None

    # ─────────────────────────────────────────────────────────────────────
    # Bootstrap
    # ─────────────────────────────────────────────────────────────────────
    async def load_history(self):
        """
        Get current price from Chainlink then seed 120 synthetic candles
        so indicators have enough history to initialize.
        """
        logger.info("Loading BTC price from Chainlink...")

        price = await self.chainlink.get_price()
        if price <= 0:
            price = 67000.0   # hard fallback if everything is down
            logger.warning(f"Using hardcoded fallback price: ${price:,.2f}")

        self.current_price = price
        self.price_source  = self.chainlink.source
        logger.info(f"BTC price: ${price:,.2f} (source: {self.chainlink.source})")

        # Seed 120 synthetic candles (2 hours of history)
        self._seed_candles(price, count=120)
        self._update_indicators()

        logger.info(f"Seeded {len(self.candles_1m)} candles. Ready.")

    def _seed_candles(self, base_price: float, count: int = 120):
        """Build synthetic flat-line candles for indicator warmup"""
        now = int(time.time())
        for i in range(count):
            ts    = now - (count - i) * 60
            # tiny noise so stdev doesn't hit zero
            noise = base_price * 0.0001 * math.sin(i * 0.5)
            c = {
                "timestamp": ts,
                "open":   base_price + noise,
                "high":   base_price + abs(noise) * 2,
                "low":    base_price - abs(noise) * 2,
                "close":  base_price,
                "volume": 5.0,
            }
            self.candles_1m.append(c)
            self.closes_1m.append(c["close"])
            self.highs_1m.append(c["high"])
            self.lows_1m.append(c["low"])
            self.volumes_1m.append(c["volume"])
            if self.db:
                self.db.upsert_candle(
                    ts, c["open"], c["high"],
                    c["low"], c["close"], c["volume"])

    # ─────────────────────────────────────────────────────────────────────
    # Polling loop (replaces WebSocket)
    # ─────────────────────────────────────────────────────────────────────
    async def start_streaming(self):
        self._running   = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info(f"Chainlink polling started (every {self.POLL_INTERVAL}s)")

    async def stop(self):
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()

    async def _poll_loop(self):
        """Poll Chainlink every 30 seconds and build candles"""
        while self._running:
            try:
                price = await self.chainlink.get_price()
                if price > 0:
                    self.current_price = price
                    self.price_source  = self.chainlink.source
                    self.last_update   = time.time()

                    # Update order book approximation
                    self.bid_price = price * 0.9998
                    self.ask_price = price * 1.0002
                    self.spread    = self.ask_price - self.bid_price

                    # Build candle tick
                    self._update_candle(price, volume=1.0)

                    logger.info(
                        f"BTC ${price:,.2f} | "
                        f"source={self.chainlink.source} | "
                        f"candles={len(self.candles_1m)}"
                    )

            except Exception as e:
                logger.error(f"Poll error: {e}")

            await asyncio.sleep(self.POLL_INTERVAL)

    def _update_candle(self, price: float, volume: float = 1.0):
        """Update current 1-minute candle with new price"""
        ts        = int(time.time())
        minute_ts = ts - (ts % 60)

        # Approximate buy/sell from price direction
        prev_price = self.candle_1m.get("close", price)
        if price >= prev_price:
            self.buy_volume_1m  += volume
        else:
            self.sell_volume_1m += volume

        if not self.candle_1m or self.candle_1m.get("timestamp") != minute_ts:
            # Close old candle
            if self.candle_1m:
                closed = dict(self.candle_1m)
                self.candles_1m.append(closed)
                self.closes_1m.append(closed["close"])
                self.highs_1m.append(closed["high"])
                self.lows_1m.append(closed["low"])
                self.volumes_1m.append(closed["volume"])
                if self.db:
                    self.db.upsert_candle(
                        closed["timestamp"], closed["open"], closed["high"],
                        closed["low"], closed["close"], closed["volume"])
                self._update_indicators()
                self.buy_volume_1m  = 0.0
                self.sell_volume_1m = 0.0

            # Open new candle
            self.candle_1m = {
                "timestamp": minute_ts,
                "open":   price, "high": price,
                "low":    price, "close": price,
                "volume": volume,
            }
        else:
            self.candle_1m["high"]    = max(self.candle_1m["high"], price)
            self.candle_1m["low"]     = min(self.candle_1m["low"],  price)
            self.candle_1m["close"]   = price
            self.candle_1m["volume"] += volume

    # ─────────────────────────────────────────────────────────────────────
    # Technical Indicators
    # ─────────────────────────────────────────────────────────────────────
    def _update_indicators(self):
        closes = self.closes_1m.to_list()
        highs  = self.highs_1m.to_list()
        lows   = self.lows_1m.to_list()
        vols   = self.volumes_1m.to_list()
        n      = len(closes)

        if n < 10:
            return

        ind   = {}
        price = closes[-1]

        # RSI
        ind["rsi_14"] = self._rsi(closes, 14) if n >= 15 else 50.0
        ind["rsi_7"]  = self._rsi(closes, 7)  if n >= 8  else 50.0

        # MACD
        if n >= 27:
            macd_line, signal_line, histogram = self._macd(closes, 12, 26, 9)
            ind["macd"]        = macd_line
            ind["macd_signal"] = signal_line
            ind["macd_hist"]   = histogram
            ind["macd_cross"]  = "bullish" if histogram > 0 else "bearish"
        else:
            ind["macd"] = ind["macd_signal"] = ind["macd_hist"] = 0.0
            ind["macd_cross"] = "neutral"

        # Bollinger Bands
        period = min(20, n)
        bb_mid, bb_upper, bb_lower = self._bollinger(closes, period, 2.0)
        ind["bb_mid"]    = bb_mid
        ind["bb_upper"]  = bb_upper
        ind["bb_lower"]  = bb_lower
        ind["bb_pct"]    = ((price - bb_lower) / (bb_upper - bb_lower)
                            if bb_upper != bb_lower else 0.5)
        ind["bb_squeeze"] = ((bb_upper - bb_lower) / bb_mid < 0.02
                             if bb_mid > 0 else False)

        # ATR
        ind["atr_14"] = self._atr(highs, lows, closes, min(14, n - 1))
        ind["atr_pct"] = ind["atr_14"] / price if price > 0 else 0

        # EMAs
        ind["ema_9"]   = self._ema(closes, min(9,   n))
        ind["ema_21"]  = self._ema(closes, min(21,  n))
        ind["ema_50"]  = self._ema(closes, min(50,  n))
        ind["ema_200"] = self._ema(closes, min(200, n))
        ind["uptrend"]   = (price > ind["ema_9"] > ind["ema_21"]
                            and n >= 21)
        ind["downtrend"] = (price < ind["ema_9"] < ind["ema_21"]
                            and n >= 21)

        # Volume
        if n >= 5:
            vol_mean       = statistics.mean(vols[-min(20, n):])
            ind["vol_ratio"] = vols[-1] / vol_mean if vol_mean else 1.0
            ind["vol_spike"] = ind["vol_ratio"] > 2.0

        # Momentum (safe window checks)
        ind["roc_5"]  = (closes[-1] / closes[-6]  - 1) * 100 if n >= 6  else 0.0
        ind["roc_10"] = (closes[-1] / closes[-11] - 1) * 100 if n >= 11 else 0.0
        ind["roc_30"] = (closes[-1] / closes[-31] - 1) * 100 if n >= 31 else 0.0

        # Stochastic
        if n >= 14:
            h14 = max(highs[-14:])
            l14 = min(lows[-14:])
            ind["stoch_k"] = ((price - l14) / (h14 - l14) * 100
                              if h14 != l14 else 50.0)
        else:
            ind["stoch_k"] = 50.0

        # OBV
        ind["obv_trend"] = self._obv_trend(closes, vols)

        # Hourly volatility — FIXED: use index range, not negative indices
        vol_window = min(60, n)
        if vol_window >= 10:
            start = n - vol_window
            hourly_returns = []
            for i in range(start + 1, n):
                if closes[i - 1] > 0 and closes[i] > 0:
                    try:
                        hourly_returns.append(
                            math.log(closes[i] / closes[i - 1]))
                    except (ValueError, ZeroDivisionError):
                        continue
            if len(hourly_returns) > 1:
                ind["hourly_vol"] = (statistics.stdev(hourly_returns)
                                     * math.sqrt(60))
            else:
                ind["hourly_vol"] = 0.01
        else:
            ind["hourly_vol"] = 0.01

        # Market structure
        if n >= 20:
            swing_highs = self._swing_highs(highs[-20:], 3)
            swing_lows  = self._swing_lows(lows[-20:],   3)
            ind["higher_highs"] = (len(swing_highs) >= 2 and
                                   swing_highs[-1] > swing_highs[-2])
            ind["lower_lows"]   = (len(swing_lows)  >= 2 and
                                   swing_lows[-1]  < swing_lows[-2])
        else:
            ind["higher_highs"] = False
            ind["lower_lows"]   = False

        # Order flow (approximated)
        total = self.buy_volume_1m + self.sell_volume_1m
        ind["delta_ratio"] = (
            (self.buy_volume_1m - self.sell_volume_1m) / total
            if total > 0 else 0.0
        )

        # Metadata
        ind["price"]            = price
        ind["price_source"]     = self.price_source
        ind["chainlink_source"] = self.chainlink.source
        ind["timestamp"]        = int(time.time())

        self._indicators            = ind
        self._last_indicator_update = time.time()

    def get_indicators(self) -> Dict:
        if time.time() - self._last_indicator_update > 120:
            self._update_indicators()
        return dict(self._indicators)

    def get_price(self) -> float:
        return self.current_price

    # ─────────────────────────────────────────────────────────────────────
    # Indicator math helpers
    # ─────────────────────────────────────────────────────────────────────
    @staticmethod
    def _ema(series: List[float], period: int) -> float:
        if not series or period <= 0:
            return series[-1] if series else 0.0
        period = min(period, len(series))
        k      = 2.0 / (period + 1)
        ema    = sum(series[:period]) / period
        for p in series[period:]:
            ema = p * k + ema * (1 - k)
        return ema

    @staticmethod
    def _rsi(closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100.0
        return 100 - (100 / (1 + avg_gain / avg_loss))

    def _macd(self, closes: List[float], fast: int, slow: int,
              signal: int) -> Tuple[float, float, float]:
        ema_fast  = self._ema(closes, fast)
        ema_slow  = self._ema(closes, slow)
        macd_line = ema_fast - ema_slow
        macd_series = []
        for i in range(signal, len(closes) + 1):
            ef = self._ema(closes[:i], fast)
            es = self._ema(closes[:i], slow)
            macd_series.append(ef - es)
        signal_line = (self._ema(macd_series, signal)
                       if len(macd_series) >= signal else macd_line)
        return macd_line, signal_line, macd_line - signal_line

    @staticmethod
    def _bollinger(closes: List[float], period: int,
                   std_mult: float) -> Tuple[float, float, float]:
        if len(closes) < 2:
            p = closes[-1] if closes else 0
            return p, p, p
        window = closes[-period:]
        mid    = statistics.mean(window)
        std    = statistics.stdev(window) if len(window) > 1 else 0
        return mid, mid + std_mult * std, mid - std_mult * std

    @staticmethod
    def _atr(highs: List[float], lows: List[float],
             closes: List[float], period: int) -> float:
        if len(closes) < 2 or period <= 0:
            return 0.0
        trs = []
        for i in range(1, len(closes)):
            hl  = highs[i]  - lows[i]
            hpc = abs(highs[i]  - closes[i - 1])
            lpc = abs(lows[i]   - closes[i - 1])
            trs.append(max(hl, hpc, lpc))
        return statistics.mean(trs[-period:]) if trs else 0.0

    @staticmethod
    def _obv_trend(closes: List[float], volumes: List[float]) -> str:
        if len(closes) < 5:
            return "neutral"
        obv, series = 0, [0]
        for i in range(1, min(len(closes), len(volumes))):
            if   closes[i] > closes[i - 1]: obv += volumes[i]
            elif closes[i] < closes[i - 1]: obv -= volumes[i]
            series.append(obv)
        if len(series) >= 5:
            if series[-1] > series[-5]: return "bullish"
            if series[-1] < series[-5]: return "bearish"
        return "neutral"

    @staticmethod
    def _swing_highs(highs: List[float], window: int) -> List[float]:
        return [highs[i] for i in range(window, len(highs) - window)
                if highs[i] == max(highs[i - window:i + window + 1])]

    @staticmethod
    def _swing_lows(lows: List[float], window: int) -> List[float]:
        return [lows[i] for i in range(window, len(lows) - window)
                if lows[i] == min(lows[i - window:i + window + 1])]


# ─────────────────────────────────────────────────────────────────────────
class FundingRateCollector:
    """
    Funding rate via Bybit public API (not geo-blocked like Binance).
    Falls back to neutral values gracefully.
    """

    def __init__(self):
        self.funding_rate:     float = 0.0
        self.open_interest:    float = 0.0
        self.long_short_ratio: float = 1.0
        self.last_update:      float = 0.0

    async def update(self):
        try:
            async with aiohttp.ClientSession() as s:
                # Bybit funding rate (public, no key needed)
                async with s.get(
                    "https://api.bybit.com/v5/market/tickers",
                    params={"category": "linear", "symbol": "BTCUSDT"},
                    timeout=aiohttp.ClientTimeout(total=6),
                ) as resp:
                    data = await resp.json()
                    items = (data.get("result", {})
                                 .get("list", []))
                    if items:
                        self.funding_rate = float(
                            items[0].get("fundingRate", 0))
                        self.open_interest = float(
                            items[0].get("openInterest", 0))

                self.last_update = time.time()
        except Exception as e:
            logger.debug(f"Funding rate fetch failed: {e}")

    def get_bias(self) -> float:
        funding_bias = -math.tanh(self.funding_rate * 100)
        ls_bias      = math.tanh(
            math.log(max(self.long_short_ratio, 0.01)) * 0.5)
        return (funding_bias + ls_bias) / 2


# ─────────────────────────────────────────────────────────────────────────
class SentimentCollector:
    """Fear & Greed index + optional CryptoPanic news sentiment"""

    def __init__(self, cryptopanic_key: str = ""):
        self.fear_greed:       float     = 50.0
        self.news_sentiment:   float     = 0.0
        self.cryptopanic_key:  str       = cryptopanic_key
        self.last_update:      float     = 0.0
        self.recent_headlines: List[str] = []

    async def update(self):
        try:
            async with aiohttp.ClientSession() as s:
                # Fear & Greed (always free)
                async with s.get(
                    "https://api.alternative.me/fng/?limit=1",
                    timeout=aiohttp.ClientTimeout(total=6),
                ) as resp:
                    data = await resp.json()
                    if data.get("data"):
                        self.fear_greed = float(data["data"][0]["value"])

                # CryptoPanic (optional)
                if self.cryptopanic_key:
                    url = (
                        f"https://cryptopanic.com/api/v1/posts/"
                        f"?auth_token={self.cryptopanic_key}"
                        f"&currencies=BTC&public=true&kind=news&limit=10"
                    )
                    async with s.get(
                        url, timeout=aiohttp.ClientTimeout(total=6)
                    ) as resp:
                        data = await resp.json()
                        sentiments, self.recent_headlines = [], []
                        for item in data.get("results", []):
                            votes = item.get("votes", {})
                            pos   = votes.get("positive", 0)
                            neg   = votes.get("negative", 0)
                            self.recent_headlines.append(
                                item.get("title", ""))
                            if pos + neg > 0:
                                sentiments.append(
                                    (pos - neg) / (pos + neg))
                        self.news_sentiment = (
                            statistics.mean(sentiments)
                            if sentiments else 0.0)

                self.last_update = time.time()
        except Exception as e:
            logger.debug(f"Sentiment fetch failed: {e}")

    def get_score(self) -> float:
        fg_score = (self.fear_greed - 50) / 50
        combined = fg_score * 0.4 + self.news_sentiment * 0.6
        return max(-1.0, min(1.0, combined))
