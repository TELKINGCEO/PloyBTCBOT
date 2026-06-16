"""
BTC Data Collector
- Chainlink on-chain BTC/USD price (primary, no API key needed)
- Binance WebSocket streaming (real-time ticks, best effort)
- Binance REST for historical candles (with full error handling)
- CoinGecko as final fallback
- Aggregates to 1m candles
- Calculates all technical indicators
"""
import asyncio
import aiohttp
import json
import time
import logging
from collections import deque
from datetime import datetime, timezone
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
    Fetches BTC/USD price directly from Chainlink on-chain oracle.
    No API key required. Uses public Polygon RPC.
    Updates approximately every 27 seconds on Polygon.
    """

    # Chainlink BTC/USD feed on Polygon Mainnet
    POLYGON_RPC      = "https://polygon-rpc.com"
    POLYGON_RPC_ALT  = "https://rpc-mainnet.matic.network"
    BTC_USD_CONTRACT = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

    # Ethereum Mainnet fallback
    ETH_RPC          = "https://ethereum.publicnode.com"
    BTC_USD_ETH      = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88"

    # latestAnswer() function selector
    LATEST_ANSWER_SIG = "0x50d25bcd"
    # latestRoundData() function selector (more info)
    LATEST_ROUND_SIG  = "0xfeaf968c"

    def __init__(self):
        self.price:       float = 0.0
        self.last_update: float = 0.0
        self.source:      str   = "none"

    async def get_price(self) -> float:
        """
        Try Polygon first, then Ethereum, then CoinGecko.
        Returns BTC/USD price or 0.0 on total failure.
        """
        # 1. Try Chainlink on Polygon
        price = await self._call_contract(
            self.POLYGON_RPC, self.BTC_USD_CONTRACT)
        if price > 1000:
            self.price       = price
            self.last_update = time.time()
            self.source      = "chainlink_polygon"
            logger.debug(f"Chainlink (Polygon): ${price:,.2f}")
            return price

        # 2. Try Chainlink on Ethereum
        price = await self._call_contract(
            self.ETH_RPC, self.BTC_USD_ETH)
        if price > 1000:
            self.price       = price
            self.last_update = time.time()
            self.source      = "chainlink_ethereum"
            logger.debug(f"Chainlink (Ethereum): ${price:,.2f}")
            return price

        # 3. Try CoinGecko (free, no key)
        price = await self._coingecko_price()
        if price > 1000:
            self.price       = price
            self.last_update = time.time()
            self.source      = "coingecko"
            logger.debug(f"CoinGecko fallback: ${price:,.2f}")
            return price

        logger.warning("All price sources failed")
        return self.price  # Return last known price

    async def _call_contract(self, rpc_url: str, contract: str) -> float:
        """Call latestAnswer() on a Chainlink aggregator contract"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "method":  "eth_call",
                "params":  [{
                    "to":   contract,
                    "data": self.LATEST_ANSWER_SIG,
                }, "latest"],
                "id": 1,
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    rpc_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()

            result = data.get("result", "0x0")
            if not result or result == "0x" or result == "0x0":
                return 0.0

            # Chainlink BTC/USD uses 8 decimal places
            raw   = int(result, 16)
            price = raw / 1e8
            return price if 1000 < price < 10_000_000 else 0.0

        except Exception as e:
            logger.debug(f"Contract call failed ({rpc_url}): {e}")
            return 0.0

    async def _coingecko_price(self) -> float:
        """CoinGecko free API — no key needed"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.coingecko.com/api/v3/simple/price",
                    params={"ids": "bitcoin", "vs_currencies": "usd"},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
            return float(data["bitcoin"]["usd"])
        except Exception as e:
            logger.debug(f"CoinGecko failed: {e}")
            return 0.0


class BTCDataFeed:
    """
    Real-time BTC data feed with full indicator suite.

    Price priority:
      1. Binance WebSocket (real-time ticks, <100ms latency)
      2. Chainlink on-chain oracle (updates ~27s, no API key)
      3. CoinGecko REST (free fallback)

    Historical candles:
      1. Binance REST (500 x 1m candles)
      2. Synthetic candles built from Chainlink price if Binance fails
    """

    WS_URL   = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    REST_URL = "https://api.binance.com/api/v3"

    def __init__(self, db=None):
        self.db        = db
        self.chainlink = ChainlinkFeed()

        self.current_price: float = 0.0
        self.last_update:   float = 0.0
        self.price_source:  str   = "none"

        # Candle data
        self.candle_1m:  Dict        = {}
        self.candles_1m: Deque[Dict] = deque(maxlen=500)
        self.candles_5m: Deque[Dict] = deque(maxlen=200)
        self.candles_1h: Deque[Dict] = deque(maxlen=100)

        # Price buffers for indicator calculation
        self.closes_1m  = RingBuffer(500)
        self.highs_1m   = RingBuffer(500)
        self.lows_1m    = RingBuffer(500)
        self.volumes_1m = RingBuffer(500)

        # Trade flow
        self.buy_volume_1m:  float = 0.0
        self.sell_volume_1m: float = 0.0
        self.recent_trades:  Deque = deque(maxlen=1000)

        # Indicators cache
        self._indicators:             Dict  = {}
        self._last_indicator_update:  float = 0.0

        # Order book
        self.bid_price: float = 0.0
        self.ask_price: float = 0.0
        self.spread:    float = 0.0

        self._running  = False
        self._ws_task: Optional[asyncio.Task] = None
        self._cl_task: Optional[asyncio.Task] = None  # Chainlink polling task

    # ─────────────────────────────────────────────────────────────────────
    # Bootstrap: load historical candles
    # ─────────────────────────────────────────────────────────────────────
    async def load_history(self):
        """
        Load last 500 1-minute candles.
        Tries Binance first; if that fails, seeds with Chainlink current price.
        """
        logger.info("Loading BTC history...")

        # Always get current price from Chainlink first
        cl_price = await self.chainlink.get_price()
        if cl_price > 0:
            self.current_price = cl_price
            self.price_source  = self.chainlink.source
            logger.info(f"Chainlink price: ${cl_price:,.2f} "
                        f"(source: {self.chainlink.source})")

        # Try Binance for historical candles
        loaded = await self._load_binance_history()

        if not loaded:
            logger.warning("Binance history unavailable — seeding from Chainlink price")
            await self._seed_from_chainlink(cl_price)

        self._update_indicators()
        logger.info(f"History ready: {len(self.candles_1m)} candles, "
                    f"BTC @ ${self.current_price:,.2f}")

    async def _load_binance_history(self) -> bool:
        """Returns True if candles loaded successfully"""
        try:
            async with aiohttp.ClientSession() as session:
                url    = f"{self.REST_URL}/klines"
                params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 500}

                async with session.get(
                    url, params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    klines = await resp.json()

                # Binance returns a list of lists for success,
                # or a dict like {"code": -1121, "msg": "..."} for errors
                if isinstance(klines, dict):
                    logger.warning(f"Binance API error: {klines.get('msg', klines)}")
                    return False

                if not isinstance(klines, list) or len(klines) == 0:
                    logger.warning("Binance returned empty candle list")
                    return False

                count = 0
                for k in klines:
                    try:
                        if not isinstance(k, list) or len(k) < 6:
                            continue
                        ts     = int(k[0]) // 1000
                        candle = {
                            "timestamp": ts,
                            "open":      float(k[1]),
                            "high":      float(k[2]),
                            "low":       float(k[3]),
                            "close":     float(k[4]),
                            "volume":    float(k[5]),
                        }
                        self.candles_1m.append(candle)
                        self.closes_1m.append(candle["close"])
                        self.highs_1m.append(candle["high"])
                        self.lows_1m.append(candle["low"])
                        self.volumes_1m.append(candle["volume"])
                        if self.db:
                            self.db.upsert_candle(
                                ts, candle["open"], candle["high"],
                                candle["low"], candle["close"], candle["volume"])
                        count += 1
                    except (ValueError, TypeError, IndexError) as e:
                        logger.debug(f"Skipping bad candle: {k} — {e}")
                        continue

                if count > 0:
                    # Update current price from latest Binance candle
                    # (more precise than Chainlink for indicators)
                    last_close = float(klines[-1][4])
                    if last_close > 0:
                        self.current_price = last_close
                        self.price_source  = "binance"

                    # Load order book
                    try:
                        async with session.get(
                            f"{self.REST_URL}/depth",
                            params={"symbol": "BTCUSDT", "limit": 5},
                            timeout=aiohttp.ClientTimeout(total=5),
                        ) as resp:
                            book = await resp.json()
                            if isinstance(book, dict) and book.get("bids"):
                                self.bid_price = float(book["bids"][0][0])
                                self.ask_price = float(book["asks"][0][0])
                                self.spread    = self.ask_price - self.bid_price
                    except Exception:
                        pass

                    logger.info(f"Binance: loaded {count} candles")
                    return True

                return False

        except Exception as e:
            logger.warning(f"Binance history load failed: {e}")
            return False

    async def _seed_from_chainlink(self, current_price: float):
        """
        Build synthetic candles from Chainlink price when Binance is unavailable.
        Creates 60 candles of fake flat-line history so indicators can initialize.
        """
        if current_price <= 0:
            current_price = 50000.0  # absolute last resort

        logger.info(f"Seeding {60} synthetic candles @ ${current_price:,.2f}")
        now = int(time.time())

        for i in range(60):
            ts = now - (60 - i) * 60
            # Add tiny random noise so indicators don't divide by zero
            noise  = current_price * 0.0002 * (0.5 - (i % 3) * 0.1)
            candle = {
                "timestamp": ts,
                "open":      current_price + noise,
                "high":      current_price + abs(noise) * 1.5,
                "low":       current_price - abs(noise) * 1.5,
                "close":     current_price,
                "volume":    10.0,
            }
            self.candles_1m.append(candle)
            self.closes_1m.append(candle["close"])
            self.highs_1m.append(candle["high"])
            self.lows_1m.append(candle["low"])
            self.volumes_1m.append(candle["volume"])

        self.current_price = current_price
        self.price_source  = "chainlink_synthetic"

    # ─────────────────────────────────────────────────────────────────────
    # Streaming
    # ─────────────────────────────────────────────────────────────────────
    async def start_streaming(self):
        self._running  = True
        self._ws_task  = asyncio.create_task(self._stream_loop())
        self._cl_task  = asyncio.create_task(self._chainlink_poll_loop())
        logger.info("BTC streams started (Binance WS + Chainlink polling)")

    async def stop(self):
        self._running = False
        for task in (self._ws_task, self._cl_task):
            if task:
                task.cancel()

    async def _stream_loop(self):
        """Binance WebSocket — primary real-time feed"""
        backoff = 1
        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        self.WS_URL, heartbeat=20
                    ) as ws:
                        logger.info("Binance WebSocket connected")
                        backoff = 1
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                await self._process_trade(data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                logger.warning(f"Binance WS error: {e}. Retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _chainlink_poll_loop(self):
        """
        Poll Chainlink every 30 seconds.
        Acts as primary price source when Binance WS is down,
        and as cross-validation when WS is up.
        """
        while self._running:
            try:
                price = await self.chainlink.get_price()
                if price > 0:
                    ws_age = time.time() - self.last_update

                    # Use Chainlink as primary if WS is stale (>60s)
                    if ws_age > 60:
                        self.current_price = price
                        self.price_source  = self.chainlink.source
                        logger.info(
                            f"Chainlink price update: ${price:,.2f} "
                            f"(WS stale {ws_age:.0f}s)"
                        )
                        # Inject as synthetic tick to keep candles alive
                        await self._inject_chainlink_tick(price)
                    else:
                        # Cross-validate: warn if >2% divergence
                        if self.current_price > 0:
                            diff = abs(price - self.current_price) / self.current_price
                            if diff > 0.02:
                                logger.warning(
                                    f"Price divergence: Binance=${self.current_price:,.2f} "
                                    f"Chainlink=${price:,.2f} ({diff*100:.1f}%)"
                                )
            except Exception as e:
                logger.debug(f"Chainlink poll error: {e}")

            await asyncio.sleep(30)

    async def _inject_chainlink_tick(self, price: float):
        """Inject a Chainlink price as a synthetic trade tick"""
        ts = int(time.time())
        fake_trade = {
            "p": str(price),
            "q": "0.001",
            "T": ts * 1000,
            "m": False,
        }
        await self._process_trade(fake_trade)

    async def _process_trade(self, trade: Dict):
        """Process a single trade tick (Binance or synthetic)"""
        try:
            price  = float(trade["p"])
            qty    = float(trade["q"])
            ts     = int(trade["T"]) // 1000
            is_buy = not trade.get("m", True)
        except (KeyError, ValueError, TypeError):
            return

        self.current_price = price
        self.last_update   = time.time()

        if is_buy:
            self.buy_volume_1m  += qty
        else:
            self.sell_volume_1m += qty

        self.recent_trades.append({
            "price": price, "qty": qty, "ts": ts,
            "side": "buy" if is_buy else "sell",
        })

        # Build 1m candle
        minute_ts = ts - (ts % 60)
        if not self.candle_1m or self.candle_1m.get("timestamp") != minute_ts:
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

            self.candle_1m = {
                "timestamp": minute_ts,
                "open":   price, "high": price,
                "low":    price, "close": price,
                "volume": qty,
            }
        else:
            self.candle_1m["high"]    = max(self.candle_1m["high"], price)
            self.candle_1m["low"]     = min(self.candle_1m["low"],  price)
            self.candle_1m["close"]   = price
            self.candle_1m["volume"] += qty

    # ─────────────────────────────────────────────────────────────────────
    # Technical Indicators
    # ─────────────────────────────────────────────────────────────────────
    def _update_indicators(self):
        closes = self.closes_1m.to_list()
        highs  = self.highs_1m.to_list()
        lows   = self.lows_1m.to_list()
        vols   = self.volumes_1m.to_list()

        if len(closes) < 30:
            return

        ind = {}

        # RSI
        ind["rsi_14"] = self._rsi(closes, 14)
        ind["rsi_7"]  = self._rsi(closes, 7)

        # MACD
        macd_line, signal_line, histogram = self._macd(closes, 12, 26, 9)
        ind["macd"]        = macd_line
        ind["macd_signal"] = signal_line
        ind["macd_hist"]   = histogram
        ind["macd_cross"]  = "bullish" if histogram > 0 else "bearish"

        # Bollinger Bands
        bb_mid, bb_upper, bb_lower = self._bollinger(closes, 20, 2.0)
        price = closes[-1]
        ind["bb_mid"]    = bb_mid
        ind["bb_upper"]  = bb_upper
        ind["bb_lower"]  = bb_lower
        ind["bb_pct"]    = ((price - bb_lower) / (bb_upper - bb_lower)
                            if bb_upper != bb_lower else 0.5)
        ind["bb_squeeze"] = (bb_upper - bb_lower) / bb_mid < 0.02 if bb_mid else False

        # ATR
        ind["atr_14"] = self._atr(highs, lows, closes, 14)
        ind["atr_pct"] = ind["atr_14"] / price if price else 0

        # EMAs
        ind["ema_9"]   = self._ema(closes, 9)
        ind["ema_21"]  = self._ema(closes, 21)
        ind["ema_50"]  = self._ema(closes, 50)
        ind["ema_200"] = self._ema(closes, 200)
        ind["uptrend"]   = price > ind["ema_9"] > ind["ema_21"] > ind["ema_50"]
        ind["downtrend"] = price < ind["ema_9"] < ind["ema_21"] < ind["ema_50"]

        # Volume
        if len(vols) >= 20:
            vol_mean       = statistics.mean(vols[-20:])
            ind["vol_ratio"] = vols[-1] / vol_mean if vol_mean else 1.0
            ind["vol_spike"] = ind["vol_ratio"] > 2.0

        # Momentum
        ind["roc_5"]  = (closes[-1] / closes[-6]  - 1) * 100 if len(closes) >= 6  else 0
        ind["roc_10"] = (closes[-1] / closes[-11] - 1) * 100 if len(closes) >= 11 else 0
        ind["roc_30"] = (closes[-1] / closes[-31] - 1) * 100 if len(closes) >= 31 else 0

        # Stochastic
        if len(highs) >= 14:
            h14 = max(highs[-14:])
            l14 = min(lows[-14:])
            ind["stoch_k"] = ((price - l14) / (h14 - l14) * 100
                              if h14 != l14 else 50)

        # OBV
        ind["obv_trend"] = self._obv_trend(closes, vols)

        # Hourly volatility
        if len(closes) >= 60:
            hourly_returns = [
                math.log(closes[i] / closes[i-1])
                for i in range(-60, 0)
                if closes[i-1] > 0
            ]
            if len(hourly_returns) > 1:
                ind["hourly_vol"] = statistics.stdev(hourly_returns) * math.sqrt(60)

        # Market structure
        if len(closes) >= 20:
            swing_highs = self._swing_highs(highs[-20:], 3)
            swing_lows  = self._swing_lows(lows[-20:],   3)
            ind["higher_highs"] = (len(swing_highs) >= 2 and
                                   swing_highs[-1] > swing_highs[-2])
            ind["lower_lows"]   = (len(swing_lows)  >= 2 and
                                   swing_lows[-1]  < swing_lows[-2])

        # Delta / order flow
        total = self.buy_volume_1m + self.sell_volume_1m
        ind["delta_ratio"] = (
            (self.buy_volume_1m - self.sell_volume_1m) / total
            if total > 0 else 0.0
        )

        # Chainlink metadata
        ind["chainlink_price"]  = self.chainlink.price
        ind["chainlink_source"] = self.chainlink.source
        ind["price_source"]     = self.price_source

        ind["price"]     = price
        ind["timestamp"] = int(time.time())

        self._indicators              = ind
        self._last_indicator_update   = time.time()

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
        if len(series) < period:
            return series[-1] if series else 0
        k   = 2.0 / (period + 1)
        ema = sum(series[:period]) / period
        for price in series[period:]:
            ema = price * k + ema * (1 - k)
        return ema

    @staticmethod
    def _rsi(closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
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
        if len(closes) < period:
            p = closes[-1] if closes else 0
            return p, p, p
        window = closes[-period:]
        mid    = statistics.mean(window)
        std    = statistics.stdev(window) if len(window) > 1 else 0
        return mid, mid + std_mult * std, mid - std_mult * std

    @staticmethod
    def _atr(highs: List[float], lows: List[float],
             closes: List[float], period: int) -> float:
        if len(closes) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(closes)):
            hl  = highs[i]  - lows[i]
            hpc = abs(highs[i]  - closes[i-1])
            lpc = abs(lows[i]   - closes[i-1])
            trs.append(max(hl, hpc, lpc))
        return statistics.mean(trs[-period:]) if trs else 0.0

    @staticmethod
    def _obv_trend(closes: List[float], volumes: List[float]) -> str:
        if len(closes) < 10:
            return "neutral"
        obv, obv_series = 0, [0]
        for i in range(1, min(len(closes), len(volumes))):
            if   closes[i] > closes[i-1]: obv += volumes[i]
            elif closes[i] < closes[i-1]: obv -= volumes[i]
            obv_series.append(obv)
        if len(obv_series) >= 5:
            recent = obv_series[-5:]
            if recent[-1] > recent[0]: return "bullish"
            if recent[-1] < recent[0]: return "bearish"
        return "neutral"

    @staticmethod
    def _swing_highs(highs: List[float], window: int) -> List[float]:
        return [highs[i] for i in range(window, len(highs) - window)
                if highs[i] == max(highs[i-window:i+window+1])]

    @staticmethod
    def _swing_lows(lows: List[float], window: int) -> List[float]:
        return [lows[i] for i in range(window, len(lows) - window)
                if lows[i] == min(lows[i-window:i+window+1])]


# ─────────────────────────────────────────────────────────────────────────
class FundingRateCollector:
    """Fetches BTC perpetual funding rates and open interest"""

    BINANCE_URL = "https://fapi.binance.com/fapi/v1"

    def __init__(self):
        self.funding_rate:    float = 0.0
        self.open_interest:   float = 0.0
        self.long_short_ratio: float = 1.0
        self.last_update:     float = 0.0

    async def update(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.BINANCE_URL}/fundingRate",
                    params={"symbol": "BTCUSDT"},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        self.funding_rate = float(
                            data[-1].get("fundingRate", 0))

                async with session.get(
                    f"{self.BINANCE_URL}/openInterest",
                    params={"symbol": "BTCUSDT"},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                    if isinstance(data, dict):
                        self.open_interest = float(
                            data.get("openInterest", 0))

                async with session.get(
                    f"{self.BINANCE_URL}/globalLongShortAccountRatio",
                    params={"symbol": "BTCUSDT", "period": "1h", "limit": 1},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        self.long_short_ratio = float(
                            data[-1].get("longShortRatio", 1.0))

                self.last_update = time.time()
        except Exception as e:
            logger.debug(f"Funding rate fetch failed: {e}")

    def get_bias(self) -> float:
        funding_bias = -math.tanh(self.funding_rate * 100)
        ls_bias      = math.tanh(math.log(max(self.long_short_ratio, 0.01)) * 0.5)
        return (funding_bias + ls_bias) / 2


# ─────────────────────────────────────────────────────────────────────────
class SentimentCollector:
    """Fear & Greed + CryptoPanic news sentiment"""

    def __init__(self, cryptopanic_key: str = ""):
        self.fear_greed:       float = 50.0
        self.news_sentiment:   float = 0.0
        self.cryptopanic_key         = cryptopanic_key
        self.last_update:      float = 0.0
        self.recent_headlines: List[str] = []

    async def update(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.alternative.me/fng/?limit=1",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                    if data.get("data"):
                        self.fear_greed = float(data["data"][0]["value"])

                if self.cryptopanic_key:
                    url = (
                        f"https://cryptopanic.com/api/v1/posts/"
                        f"?auth_token={self.cryptopanic_key}"
                        f"&currencies=BTC&public=true&kind=news&limit=10"
                    )
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        data = await resp.json()
                        sentiments = []
                        self.recent_headlines = []
                        for item in data.get("results", []):
                            votes = item.get("votes", {})
                            pos   = votes.get("positive", 0)
                            neg   = votes.get("negative", 0)
                            self.recent_headlines.append(item.get("title", ""))
                            if pos + neg > 0:
                                sentiments.append((pos - neg) / (pos + neg))
                        self.news_sentiment = (statistics.mean(sentiments)
                                               if sentiments else 0.0)

                self.last_update = time.time()
        except Exception as e:
            logger.debug(f"Sentiment fetch failed: {e}")

    def get_score(self) -> float:
        fg_score = (self.fear_greed - 50) / 50
        combined = fg_score * 0.4 + self.news_sentiment * 0.6
        return max(-1.0, min(1.0, combined))
