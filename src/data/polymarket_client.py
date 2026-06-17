"""
Polymarket CLOB Client
- Discover active BTC hourly markets
- Stream live order book prices
- Execute buy/sell orders (YES/NO shares)
- Manage open positions
"""
import asyncio
import aiohttp
import json
import time
import hmac
import hashlib
import base64
import logging
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple

logger = logging.getLogger(__name__)

class PolymarketClient:
    """
    Thin async wrapper around Polymarket CLOB + Gamma APIs.
    Uses API key auth (not on-chain signing for simplicity;
    on-chain signing can be added for full production).
    """
    
    GAMMA_URL = "https://gamma-api.polymarket.com"
    CLOB_URL  = "https://clob.polymarket.com"
    
    def __init__(self, api_key: str = "", secret: str = "",
                 passphrase: str = "", private_key: str = ""):
        self.api_key    = api_key
        self.secret     = secret
        self.passphrase = passphrase
        self.private_key = private_key
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Cached market data
        self._markets_cache: List[Dict] = []
        self._cache_ts: float = 0.0
        self._cache_ttl: float = 60.0   # Refresh every 60s
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session
    
    def _auth_headers(self, method: str, path: str, body: str = "") -> Dict:
        """Generate CLOB API auth headers"""
        if not self.api_key:
            return {}
        ts = str(int(time.time() * 1000))
        message = ts + method.upper() + path + (body or "")
        sig = hmac.new(
            base64.b64decode(self.secret),
            message.encode("utf-8"),
            hashlib.sha256
        ).digest()
        signature = base64.b64encode(sig).decode("utf-8")
        return {
            "POLY_ADDRESS":    self.api_key,
            "POLY_SIGNATURE":  signature,
            "POLY_TIMESTAMP":  ts,
            "POLY_PASSPHRASE": self.passphrase,
        }
    
    async def close(self):
        if self._session:
            await self._session.close()
    
    # ─────────────────────────────────────────────────────────────────────
    # Market discovery
    # ─────────────────────────────────────────────────────────────────────
    async def get_btc_hourly_markets(self, force_refresh: bool = False) -> List[Dict]:
        """
        Returns ONLY:
          - Bitcoin 15-minute markets  e.g. "Bitcoin Up or Down - June 17, 2:00AM-2:15AM ET"
          - Bitcoin 1-hour markets     e.g. "Bitcoin above $67,000 on June 17?"
        All other markets are ignored.
        """
        now = time.time()
        if (not force_refresh and self._markets_cache
                and (now - self._cache_ts) < self._cache_ttl):
            return self._markets_cache

        session  = await self._get_session()
        all_raw  = []

        # ── Fetch from multiple endpoints ─────────────────────────────────
        fetch_configs = [
            # Bitcoin tag slug
            {"active": "true", "closed": "false",
             "limit": "200", "tag_slug": "bitcoin"},
            # Crypto category
            {"active": "true", "closed": "false",
             "limit": "200", "category": "crypto"},
            # All active (fallback)
            {"active": "true", "closed": "false", "limit": "500"},
        ]

        for params in fetch_configs:
            try:
                async with session.get(
                    f"{self.GAMMA_URL}/markets",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw  = (data if isinstance(data, list)
                                else data.get("markets", []))
                        all_raw.extend(raw)
            except Exception as e:
                logger.debug(f"Fetch failed ({params}): {e}")

        # Also try events endpoint
        try:
            async with session.get(
                f"{self.GAMMA_URL}/events",
                params={"active": "true", "closed": "false",
                        "limit": "100", "tag_slug": "bitcoin"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data   = await resp.json()
                    events = (data if isinstance(data, list)
                              else data.get("events", []))
                    for event in events:
                        for m in event.get("markets", []):
                            all_raw.append(m)
        except Exception as e:
            logger.debug(f"Events fetch failed: {e}")

        # ── Deduplicate ───────────────────────────────────────────────────
        seen, unique = set(), []
        for m in all_raw:
            mid = (m.get("id") or m.get("conditionId") or "")
            if mid and mid not in seen:
                seen.add(mid)
                unique.append(m)

        # ── Strict filter: only 15min and 1hour BTC markets ──────────────
        markets_15m = []
        markets_1h  = []

        for m in unique:
            q = (m.get("question") or "").lower().strip()

            # Skip if not Bitcoin related
            if "bitcoin" not in q and "btc" not in q:
                continue

            # Skip if already resolved/closed
            if m.get("resolved") or m.get("isResolved"):
                continue

            # ── Detect 15-minute market ───────────────────────────────────
            # Pattern: "Bitcoin Up or Down - June 17, 2:00AM-2:15AM ET"
            # The time range spans exactly 5 or 15 minutes
            is_15min = False
            if "up or down" in q and "bitcoin" in q:
                # Check time range pattern like "2:00am-2:15am"
                time_range = re.findall(
                    r'(\d{1,2}:\d{2}(?:am|pm)?)-(\d{1,2}:\d{2}(?:am|pm)?)',
                    q, re.IGNORECASE)
                if time_range:
                    is_15min = True
                else:
                    # Also match if it just says "up or down" with bitcoin
                    is_15min = True

            # ── Detect 1-hour market ──────────────────────────────────────
            # Pattern: "Bitcoin above $67,000 on June 17?"
            # Pattern: "Will Bitcoin be above $X at Y:00?"
            # Pattern: "Bitcoin above ___ on June 17?"
            is_1hour = False
            if "bitcoin" in q or "btc" in q:
                hour_indicators = [
                    "above" in q and ("on june" in q or "on july" in q or
                                      "on aug" in q or "today" in q or
                                      "tomorrow" in q or re.search(
                                          r'on \w+ \d+', q)),
                    "below" in q and ("on june" in q or "on july" in q or
                                      re.search(r'on \w+ \d+', q)),
                    "will bitcoin" in q and "above" in q,
                    "will bitcoin" in q and "below" in q,
                    "will btc" in q and "above" in q,
                    "will btc" in q and "below" in q,
                    # "Bitcoin above ___ on June 17?"
                    bool(re.search(
                        r'bitcoin\s+(above|below|exceed|hit|reach)',
                        q, re.IGNORECASE)),
                ]
                is_1hour = any(hour_indicators)
                # But exclude if it's a 15min "up or down" market
                if is_15min:
                    is_1hour = False
                # Exclude multi-day/weekly/monthly markets
                long_term = [
                    "2026", "2027", "year", "month", "week",
                    "january", "february", "march", "april", "may",
                    "q1", "q2", "q3", "q4", "annual", "halving",
                    "microstrategy", "etf", "regulation", "sec",
                    "election", "president", "senator",
                ]
                if any(lt in q for lt in long_term):
                    is_1hour = False

            if not is_15min and not is_1hour:
                continue

            # ── Extract end time ──────────────────────────────────────────
            end_ts  = 0
            end_str = (m.get("endDate") or m.get("end_date_iso") or
                       m.get("endDateIso") or m.get("end_date") or
                       m.get("endTime") or "")
            if end_str:
                try:
                    dt     = datetime.fromisoformat(
                        str(end_str).replace("Z", "+00:00"))
                    end_ts = int(dt.timestamp())
                except Exception:
                    pass

            if end_ts == 0:
                end_ts = int(m.get("endTimestamp", 0) or
                             m.get("end_timestamp", 0) or 0)

            time_to_expiry = end_ts - int(now) if end_ts > 0 else 0

            # Skip already expired (more than 5 min ago)
            if end_ts > 0 and time_to_expiry < -300:
                continue

            market_type = "btc_15min" if is_15min else "btc_1hour"

            entry = {
                "id":             (m.get("id") or m.get("conditionId") or ""),
                "condition_id":   m.get("conditionId", ""),
                "question":       m.get("question", ""),
                "outcomes":       json.dumps(m.get("outcomes", ["YES", "NO"])),
                "outcome_prices": json.dumps(self._extract_prices(m)),
                "volume":         float(m.get("volume",    0) or 0),
                "liquidity":      float(m.get("liquidity", 0) or 0),
                "end_time":       end_ts,
                "start_time":     0,
                "market_type":    market_type,
                "time_to_expiry": max(0, time_to_expiry),
                "raw":            m,
            }

            if is_15min:
                markets_15m.append(entry)
            else:
                markets_1h.append(entry)

        # Sort each group by soonest expiry
        markets_15m.sort(key=lambda x: x["end_time"] if x["end_time"] > 0
                         else float("inf"))
        markets_1h.sort(key=lambda x: x["end_time"] if x["end_time"] > 0
                        else float("inf"))

        # Combine: 15min first, then 1hour
        markets = markets_15m + markets_1h

        logger.info(
            f"Found {len(markets)} target BTC markets: "
            f"{len(markets_15m)} x 15min, {len(markets_1h)} x 1hour "
            f"(from {len(unique)} total scanned)"
        )

        for mkt in markets[:10]:
            logger.info(
                f"  [{mkt['market_type']:>10}] "
                f"[{mkt['time_to_expiry']//60:>4}min] "
                f"${mkt['volume']:>8,.0f} | "
                f"{mkt['question'][:60]}"
            )

        self._markets_cache = markets
        self._cache_ts      = now
        return markets
    
    def _extract_prices(self, market: Dict) -> List[float]:
        """Extract YES/NO prices from market data"""
        tokens = market.get("tokens") or []
        if tokens:
            prices = []
            for t in tokens:
                p = t.get("price")
                if p is not None:
                    prices.append(float(p))
            if len(prices) >= 2:
                return prices
        
        # Fallback: extract from outcome prices
        op = market.get("outcomePrices") or []
        if op:
            try:
                return [float(p) for p in op]
            except Exception:
                pass
        return [0.5, 0.5]
    
    # ─────────────────────────────────────────────────────────────────────
    # Order book
    # ─────────────────────────────────────────────────────────────────────
    async def get_orderbook(self, token_id: str) -> Dict:
        """Get CLOB order book for a specific outcome token"""
        session = await self._get_session()
        try:
            async with session.get(
                f"{self.CLOB_URL}/book",
                params={"token_id": token_id},
                headers=self._auth_headers("GET", "/book")
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.debug(f"Order book fetch error: {e}")
        return {"bids": [], "asks": []}
    
    async def get_market_price(self, market_id: str) -> Tuple[float, float]:
        """Return (yes_price, no_price) for a market"""
        session = await self._get_session()
        try:
            async with session.get(
                f"{self.GAMMA_URL}/markets/{market_id}"
            ) as resp:
                if resp.status == 200:
                    m = await resp.json()
                    prices = self._extract_prices(m)
                    if len(prices) >= 2:
                        return prices[0], prices[1]
                    if len(prices) == 1:
                        return prices[0], 1 - prices[0]
        except Exception as e:
            logger.debug(f"Price fetch error for {market_id}: {e}")
        return 0.5, 0.5
    
    # ─────────────────────────────────────────────────────────────────────
    # Order execution
    # ─────────────────────────────────────────────────────────────────────
    async def place_order(self, token_id: str, side: str, size: float,
                          price: float, order_type: str = "GTC") -> Optional[Dict]:
        """
        Place a limit order on the CLOB.
        side: "BUY" or "SELL"
        size: in USDC
        price: 0-1 (share price)
        
        NOTE: In production this requires on-chain signing via the py-clob-client
        library with your private key. For paper trading, this returns a simulated fill.
        """
        
        # ── PAPER TRADING MODE (no real API key) ──────────────────────────
        if not self.api_key or self.api_key == "":
            return self._simulate_fill(token_id, side, size, price)
        
        # ── LIVE TRADING ──────────────────────────────────────────────────
        session = await self._get_session()
        
        shares = size / price if price > 0 else 0
        body = json.dumps({
            "tokenID":   token_id,
            "side":      side.upper(),
            "type":      order_type,
            "size":      str(round(shares, 2)),
            "price":     str(round(price, 4)),
        })
        
        try:
            headers = self._auth_headers("POST", "/order", body)
            async with session.post(
                f"{self.CLOB_URL}/order",
                data=body,
                headers=headers
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("success"):
                    logger.info(f"Order placed: {side} {shares:.2f} shares @ {price:.4f}")
                    return data
                else:
                    logger.error(f"Order failed: {data}")
                    return None
        except Exception as e:
            logger.error(f"Order execution error: {e}")
            return None
    
    def _simulate_fill(self, token_id: str, side: str,
                       size: float, price: float) -> Dict:
        """Paper trading simulation with 0.1% slippage"""
        slippage = 0.001
        fill_price = price * (1 + slippage) if side == "BUY" else price * (1 - slippage)
        fill_price = max(0.01, min(0.99, fill_price))
        shares = size / fill_price
        
        return {
            "orderId":    f"PAPER_{int(time.time() * 1000)}",
            "status":     "FILLED",
            "fillPrice":  fill_price,
            "fillSize":   shares,
            "size_usdc":  size,
            "paper_trade": True,
        }
    
    async def cancel_order(self, order_id: str) -> bool:
        if not self.api_key or order_id.startswith("PAPER_"):
            return True
        session = await self._get_session()
        try:
            headers = self._auth_headers("DELETE", f"/order/{order_id}")
            async with session.delete(
                f"{self.CLOB_URL}/order/{order_id}",
                headers=headers
            ) as resp:
                return resp.status == 200
        except Exception:
            return False
    
    # ─────────────────────────────────────────────────────────────────────
    # Balance
    # ─────────────────────────────────────────────────────────────────────
    async def get_balance(self) -> float:
        """Get USDC balance. Returns 10.0 for paper trading."""
        if not self.api_key:
            return 10.0  # Paper trading starting balance
        session = await self._get_session()
        try:
            headers = self._auth_headers("GET", "/balance")
            async with session.get(
                f"{self.CLOB_URL}/balance",
                headers=headers
            ) as resp:
                data = await resp.json()
                return float(data.get("balance", 0))
        except Exception as e:
            logger.error(f"Balance fetch error: {e}")
            return 0.0
    
    # ─────────────────────────────────────────────────────────────────────
    # Market parsing helpers
    # ─────────────────────────────────────────────────────────────────────
    @staticmethod
    def parse_btc_target(question: str) -> Optional[float]:
        """Extract BTC price target from market question string"""
        import re
        patterns = [
            r'\$([0-9,]+(?:\.[0-9]+)?)',
            r'(\d{2,3},\d{3}(?:\.\d+)?)',
            r'above (\d+)',
            r'below (\d+)',
            r'at (\d+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, question.replace(",", ""))
            if match:
                try:
                    return float(match.group(1).replace(",", ""))
                except Exception:
                    continue
        return None
    
    @staticmethod
    def market_direction(question: str) -> str:
        """Return 'above', 'below', or 'unknown'"""
        q = question.lower()
        if any(w in q for w in ["above", "higher", "exceed", "over", "up"]):
            return "above"
        if any(w in q for w in ["below", "lower", "under", "drop", "fall"]):
            return "below"
        return "above"  # Polymarket default is "will be above"
