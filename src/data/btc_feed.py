"""
BTC Data Collector
- WebSocket streaming from Binance
- REST fallback for historical data
- Aggregates to 1m, 5m, 1h candles
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
    def __len__(self): return len(self._buf)
    def __iter__(self): return iter(self._buf)
    def to_list(self): return list(self._buf)
    
    @property
    def last(self): return self._buf[-1] if self._buf else None
    @property
    def mean(self): return statistics.mean(self._buf) if self._buf else 0
    @property
    def stdev(self): return statistics.stdev(self._buf) if len(self._buf) > 1 else 0


class BTCDataFeed:
    """
    Real-time BTC data feed with full indicator suite.
    Subscribes to Binance trade stream, builds OHLCV candles.
    """
    
    WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    REST_URL = "https://api.binance.com/api/v3"
    
    def __init__(self, db=None):
        self.db = db
        self.current_price: float = 0.0
        self.last_update: float = 0.0
        
        # Candle data
        self.candle_1m: Dict = {}
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
        
        # Indicators cache (updated every new candle)
        self._indicators: Dict = {}
        self._last_indicator_update: float = 0.0
        
        # Order book snapshot
        self.bid_price: float = 0.0
        self.ask_price: float = 0.0
        self.spread: float = 0.0
        
        self._running = False
        self._ws_task: Optional[asyncio.Task] = None
    
    # ─────────────────────────────────────────────────────────────────────
    # Bootstrap: load historical data from Binance REST
    # ─────────────────────────────────────────────────────────────────────
    async def load_history(self):
        """Load last 500 1-minute candles from Binance"""
        logger.info("Loading historical candles from Binance...")
        async with aiohttp.ClientSession() as session:
            url = f"{self.REST_URL}/klines"
            params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 500}
            async with session.get(url, params=params) as resp:
                klines = await resp.json()
            
            for k in klines:
                ts   = int(k[0]) // 1000
                candle = {
                    "timestamp": ts,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low":  float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                }
                self.candles_1m.append(candle)
                self.closes_1m.append(candle["close"])
                self.highs_1m.append(candle["high"])
                self.lows_1m.append(candle["low"])
                self.volumes_1m.append(candle["volume"])
                if self.db:
                    self.db.upsert_candle(ts, candle["open"], candle["high"],
                                          candle["low"], candle["close"], candle["volume"])
            
            self.current_price = float(klines[-1][4]) if klines else 0.0
            logger.info(f"Loaded {len(klines)} candles. BTC @ ${self.current_price:,.2f}")
            
            # Also load order book
            async with session.get(f"{self.REST_URL}/depth",
                                    params={"symbol":"BTCUSDT","limit":5}) as resp:
                book = await resp.json()
                if book.get("bids") and book.get("asks"):
                    self.bid_price = float(book["bids"][0][0])
                    self.ask_price = float(book["asks"][0][0])
                    self.spread    = self.ask_price - self.bid_price
        
        self._update_indicators()

    # ─────────────────────────────────────────────────────────────────────
    # WebSocket streaming
    # ─────────────────────────────────────────────────────────────────────
    async def start_streaming(self):
        self._running = True
        self._ws_task = asyncio.create_task(self._stream_loop())
        logger.info("BTC stream started")
    
    async def stop(self):
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
    
    async def _stream_loop(self):
        backoff = 1
        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.WS_URL, heartbeat=20) as ws:
                        logger.info("WebSocket connected to Binance")
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
                logger.warning(f"WS error: {e}. Reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
    
    async def _process_trade(self, trade: Dict):
        """Process a single trade tick"""
        price  = float(trade["p"])
        qty    = float(trade["q"])
        ts     = int(trade["T"]) // 1000
        is_buy = not trade.get("m", True)  # m=True means market sell
        
        self.current_price = price
        self.last_update   = time.time()
        
        # Track buy/sell volume
        if is_buy:
            self.buy_volume_1m  += qty
        else:
            self.sell_volume_1m += qty
        
        self.recent_trades.append({
            "price": price, "qty": qty, "ts": ts, "side": "buy" if is_buy else "sell"
        })
        
        # Build 1m candle
        minute_ts = ts - (ts % 60)
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
                    self.db.upsert_candle(closed["timestamp"], closed["open"],
                                          closed["high"], closed["low"],
                                          closed["close"], closed["volume"])
                self._update_indicators()
                self.buy_volume_1m  = 0.0
                self.sell_volume_1m = 0.0
            # Open new candle
            self.candle_1m = {
                "timestamp": minute_ts,
                "open": price, "high": price, "low": price,
                "close": price, "volume": qty
            }
        else:
            self.candle_1m["high"]   = max(self.candle_1m["high"], price)
            self.candle_1m["low"]    = min(self.candle_1m["low"],  price)
            self.candle_1m["close"]  = price
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
        
        # ── RSI (14) ──────────────────────────────────────────────────────
        ind["rsi_14"] = self._rsi(closes, 14)
        ind["rsi_7"]  = self._rsi(closes, 7)
        
        # ── MACD ──────────────────────────────────────────────────────────
        macd_line, signal_line, histogram = self._macd(closes, 12, 26, 9)
        ind["macd"]         = macd_line
        ind["macd_signal"]  = signal_line
        ind["macd_hist"]    = histogram
        ind["macd_cross"]   = "bullish" if histogram > 0 else "bearish"
        
        # ── Bollinger Bands ───────────────────────────────────────────────
        bb_mid, bb_upper, bb_lower = self._bollinger(closes, 20, 2.0)
        ind["bb_mid"]   = bb_mid
        ind["bb_upper"] = bb_upper
        ind["bb_lower"] = bb_lower
        price = closes[-1]
        ind["bb_pct"] = (price - bb_lower) / (bb_upper - bb_lower) if bb_upper != bb_lower else 0.5
        ind["bb_squeeze"] = (bb_upper - bb_lower) / bb_mid < 0.02  # Tight bands
        
        # ── ATR (14) ──────────────────────────────────────────────────────
        ind["atr_14"] = self._atr(highs, lows, closes, 14)
        ind["atr_pct"] = ind["atr_14"] / price if price else 0
        
        # ── EMA stack ─────────────────────────────────────────────────────
        ind["ema_9"]   = self._ema(closes, 9)
        ind["ema_21"]  = self._ema(closes, 21)
        ind["ema_50"]  = self._ema(closes, 50)
        ind["ema_200"] = self._ema(closes, 200)
        
        # Trend alignment
        ind["uptrend"] = (price > ind["ema_9"] > ind["ema_21"] > ind["ema_50"])
        ind["downtrend"] = (price < ind["ema_9"] < ind["ema_21"] < ind["ema_50"])
        
        # ── Volume analysis ───────────────────────────────────────────────
        if len(vols) >= 20:
            vol_mean = statistics.mean(vols[-20:])
            ind["vol_ratio"]  = vols[-1] / vol_mean if vol_mean else 1.0
            ind["vol_spike"]  = ind["vol_ratio"] > 2.0
        
        # ── Momentum ──────────────────────────────────────────────────────
        ind["roc_5"]  = (closes[-1] / closes[-6]  - 1) * 100 if len(closes) >= 6  else 0
        ind["roc_10"] = (closes[-1] / closes[-11] - 1) * 100 if len(closes) >= 11 else 0
        ind["roc_30"] = (closes[-1] / closes[-31] - 1) * 100 if len(closes) >= 31 else 0
        
        # ── Stochastic ────────────────────────────────────────────────────
        if len(highs) >= 14:
            h14 = max(highs[-14:])
            l14 = min(lows[-14:])
            ind["stoch_k"] = ((price - l14) / (h14 - l14) * 100) if h14 != l14 else 50
        
        # ── OBV trend ─────────────────────────────────────────────────────
        ind["obv_trend"] = self._obv_trend(closes, vols)
        
        # ── Hourly volatility ─────────────────────────────────────────────
        if len(closes) >= 60:
            hourly_returns = [math.log(closes[i] / closes[i-1])
                              for i in range(-60, 0) if closes[i-1] > 0]
            if len(hourly_returns) > 1:
                ind["hourly_vol"] = statistics.stdev(hourly_returns) * math.sqrt(60)
        
        # ── Market structure ──────────────────────────────────────────────
        if len(closes) >= 20:
            swing_highs = self._swing_highs(highs[-20:], 3)
            swing_lows  = self._swing_lows(lows[-20:], 3)
            ind["higher_highs"] = len(swing_highs) >= 2 and swing_highs[-1] > swing_highs[-2]
            ind["lower_lows"]   = len(swing_lows)  >= 2 and swing_lows[-1]  < swing_lows[-2]
        
        # ── Delta / Order flow ────────────────────────────────────────────
        if self.buy_volume_1m + self.sell_volume_1m > 0:
            total = self.buy_volume_1m + self.sell_volume_1m
            ind["delta_ratio"] = (self.buy_volume_1m - self.sell_volume_1m) / total
        else:
            ind["delta_ratio"] = 0.0
        
        ind["price"] = price
        ind["timestamp"] = int(time.time())
        
        self._indicators = ind
        self._last_indicator_update = time.time()

    def get_indicators(self) -> Dict:
        """Get current indicator snapshot"""
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
        k = 2.0 / (period + 1)
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
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _macd(self, closes: List[float], fast: int, slow: int, signal: int
              ) -> Tuple[float, float, float]:
        ema_fast   = self._ema(closes, fast)
        ema_slow   = self._ema(closes, slow)
        macd_line  = ema_fast - ema_slow
        
        # Need macd history for signal line; approximate
        macd_series = []
        for i in range(signal, len(closes) + 1):
            ef = self._ema(closes[:i], fast)
            es = self._ema(closes[:i], slow)
            macd_series.append(ef - es)
        
        if len(macd_series) >= signal:
            signal_line = self._ema(macd_series, signal)
        else:
            signal_line = macd_line
        
        return macd_line, signal_line, macd_line - signal_line

    @staticmethod
    def _bollinger(closes: List[float], period: int, std_mult: float
                   ) -> Tuple[float, float, float]:
        if len(closes) < period:
            p = closes[-1] if closes else 0
            return p, p, p
        window = closes[-period:]
        mid   = statistics.mean(window)
        std   = statistics.stdev(window) if len(window) > 1 else 0
        return mid, mid + std_mult * std, mid - std_mult * std

    @staticmethod
    def _atr(highs: List[float], lows: List[float], closes: List[float],
             period: int) -> float:
        if len(closes) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(closes)):
            hl  = highs[i] - lows[i]
            hpc = abs(highs[i] - closes[i-1])
            lpc = abs(lows[i] - closes[i-1])
            trs.append(max(hl, hpc, lpc))
        return statistics.mean(trs[-period:]) if trs else 0.0

    @staticmethod
    def _obv_trend(closes: List[float], volumes: List[float]) -> str:
        if len(closes) < 10:
            return "neutral"
        obv = 0
        obv_series = [0]
        for i in range(1, min(len(closes), len(volumes))):
            if closes[i] > closes[i-1]:
                obv += volumes[i]
            elif closes[i] < closes[i-1]:
                obv -= volumes[i]
            obv_series.append(obv)
        if len(obv_series) >= 5:
            recent = obv_series[-5:]
            if recent[-1] > recent[0]: return "bullish"
            if recent[-1] < recent[0]: return "bearish"
        return "neutral"

    @staticmethod
    def _swing_highs(highs: List[float], window: int) -> List[float]:
        result = []
        for i in range(window, len(highs) - window):
            if highs[i] == max(highs[i-window:i+window+1]):
                result.append(highs[i])
        return result

    @staticmethod
    def _swing_lows(lows: List[float], window: int) -> List[float]:
        result = []
        for i in range(window, len(lows) - window):
            if lows[i] == min(lows[i-window:i+window+1]):
                result.append(lows[i])
        return result


class FundingRateCollector:
    """Fetches BTC perpetual funding rates and open interest"""
    
    BYBIT_URL = "https://api.bybit.com/v5/market"
    BINANCE_URL = "https://fapi.binance.com/fapi/v1"
    
    def __init__(self):
        self.funding_rate: float = 0.0
        self.open_interest: float = 0.0
        self.long_short_ratio: float = 1.0
        self.last_update: float = 0.0
    
    async def update(self):
        try:
            async with aiohttp.ClientSession() as session:
                # Binance funding rate
                async with session.get(
                    f"{self.BINANCE_URL}/fundingRate",
                    params={"symbol": "BTCUSDT"}
                ) as resp:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        self.funding_rate = float(data[-1].get("fundingRate", 0))
                
                # Open interest
                async with session.get(
                    f"{self.BINANCE_URL}/openInterest",
                    params={"symbol": "BTCUSDT"}
                ) as resp:
                    data = await resp.json()
                    self.open_interest = float(data.get("openInterest", 0))
                
                # Long/short ratio
                async with session.get(
                    f"{self.BINANCE_URL}/globalLongShortAccountRatio",
                    params={"symbol": "BTCUSDT", "period": "1h", "limit": 1}
                ) as resp:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        self.long_short_ratio = float(
                            data[-1].get("longShortRatio", 1.0))
                
                self.last_update = time.time()
        except Exception as e:
            logger.debug(f"Funding rate fetch failed: {e}")
    
    def get_bias(self) -> float:
        """Return -1..+1 derived from funding + L/S ratio"""
        funding_bias = -math.tanh(self.funding_rate * 100)  # Positive funding → overbought
        ls_bias = math.tanh(math.log(max(self.long_short_ratio, 0.01)) * 0.5)
        return (funding_bias + ls_bias) / 2


class SentimentCollector:
    """Fear & Greed + news sentiment"""
    
    def __init__(self, cryptopanic_key: str = ""):
        self.fear_greed: float = 50.0   # 0-100
        self.news_sentiment: float = 0.0  # -1 to 1
        self.cryptopanic_key = cryptopanic_key
        self.last_update: float = 0.0
        self.recent_headlines: List[str] = []
    
    async def update(self):
        try:
            async with aiohttp.ClientSession() as session:
                # Fear & Greed
                async with session.get("https://api.alternative.me/fng/?limit=1") as resp:
                    data = await resp.json()
                    if data.get("data"):
                        self.fear_greed = float(data["data"][0]["value"])
                
                # CryptoPanic (optional)
                if self.cryptopanic_key:
                    url = (f"https://cryptopanic.com/api/v1/posts/"
                           f"?auth_token={self.cryptopanic_key}"
                           f"&currencies=BTC&public=true&kind=news&limit=10")
                    async with session.get(url) as resp:
                        data = await resp.json()
                        results = data.get("results", [])
                        sentiments = []
                        self.recent_headlines = []
                        for item in results:
                            votes = item.get("votes", {})
                            pos = votes.get("positive", 0)
                            neg = votes.get("negative", 0)
                            self.recent_headlines.append(item.get("title", ""))
                            if pos + neg > 0:
                                sentiments.append((pos - neg) / (pos + neg))
                        self.news_sentiment = (statistics.mean(sentiments)
                                               if sentiments else 0.0)
                
                self.last_update = time.time()
        except Exception as e:
            logger.debug(f"Sentiment fetch failed: {e}")
    
    def get_score(self) -> float:
        """Return -1..+1 composite sentiment"""
        # Fear & Greed: 0-100 → -1..1 (extreme fear is bullish contrarian)
        fg_score = (self.fear_greed - 50) / 50
        combined = fg_score * 0.4 + self.news_sentiment * 0.6
        return max(-1.0, min(1.0, combined))
