"""
Fixed Polymarket Client
Key fixes:
1. Use correct Gamma API tag/category filters for Bitcoin markets
2. Search by "Bitcoin" slug not "btc"  
3. Add CLOB markets endpoint as second source
4. Much broader keyword matching
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

    GAMMA_URL = "https://gamma-api.polymarket.com"
    CLOB_URL  = "https://clob.polymarket.com"

    def __init__(self, api_key: str = "", secret: str = "",
                 passphrase: str = "", private_key: str = ""):
        self.api_key     = api_key
        self.secret      = secret
        self.passphrase  = passphrase
        self.private_key = private_key
        self._session: Optional[aiohttp.ClientSession] = None
        self._markets_cache: List[Dict] = []
        self._cache_ts:  float = 0.0
        self._cache_ttl: float = 60.0

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    def _auth_headers(self, method: str, path: str, body: str = "") -> Dict:
        if not self.api_key:
            return {}
        ts      = str(int(time.time() * 1000))
        message = ts + method.upper() + path + (body or "")
        sig     = hmac.new(
            base64.b64decode(self.secret),
            message.encode("utf-8"),
            hashlib.sha256
        ).digest()
        return {
            "POLY_ADDRESS":    self.api_key,
            "POLY_SIGNATURE":  base64.b64encode(sig).decode("utf-8"),
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
        now = time.time()
        if (not force_refresh and self._markets_cache
                and (now - self._cache_ts) < self._cache_ttl):
            return self._markets_cache

        session = await self._get_session()
        all_raw = []

        # ── Strategy 1: Gamma events endpoint (Bitcoin category) ──────────
        try:
            async with session.get(
                f"{self.GAMMA_URL}/events",
                params={
                    "active":   "true",
                    "closed":   "false",
                    "limit":    "100",
                    "tag_slug": "bitcoin",   # ← correct Polymarket tag
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data   = await resp.json()
                    events = data if isinstance(data, list) else data.get("events", [])
                    # Each event has nested markets
                    for event in events:
                        for m in event.get("markets", []):
                            m["_event_title"] = event.get("title", "")
                            all_raw.append(m)
                    logger.debug(f"Events/bitcoin tag: {len(events)} events")
        except Exception as e:
            logger.debug(f"Events/bitcoin failed: {e}")

        # ── Strategy 2: Gamma markets with tag_slug ───────────────────────
        try:
            async with session.get(
                f"{self.GAMMA_URL}/markets",
                params={
                    "active":   "true",
                    "closed":   "false",
                    "limit":    "100",
                    "tag_slug": "bitcoin",
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw  = data if isinstance(data, list) else data.get("markets", [])
                    all_raw.extend(raw)
                    logger.debug(f"Markets/bitcoin tag: {len(raw)} results")
        except Exception as e:
            logger.debug(f"Markets/bitcoin tag failed: {e}")

        # ── Strategy 3: Gamma markets with category ───────────────────────
        for cat in ["crypto", "bitcoin", "cryptocurrency"]:
            try:
                async with session.get(
                    f"{self.GAMMA_URL}/markets",
                    params={
                        "active":   "true",
                        "closed":   "false",
                        "limit":    "100",
                        "category": cat,
                    },
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw  = (data if isinstance(data, list)
                                else data.get("markets", []))
                        all_raw.extend(raw)
                        logger.debug(f"Markets/{cat}: {len(raw)} results")
            except Exception as e:
                logger.debug(f"Markets/{cat} failed: {e}")

        # ── Strategy 4: CLOB markets endpoint ─────────────────────────────
        try:
            async with session.get(
                f"{self.CLOB_URL}/markets",
                params={"active": "true", "limit": "100"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw  = (data.get("data", []) if isinstance(data, dict)
                            else data)
                    all_raw.extend(raw)
                    logger.debug(f"CLOB markets: {len(raw)} results")
        except Exception as e:
            logger.debug(f"CLOB markets failed: {e}")

        # ── Strategy 5: Fallback — all markets, filter manually ───────────
        if len(all_raw) == 0:
            try:
                async with session.get(
                    f"{self.GAMMA_URL}/markets",
                    params={
                        "active": "true",
                        "closed": "false",
                        "limit":  "200",
                    },
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw  = (data if isinstance(data, list)
                                else data.get("markets", []))
                        all_raw.extend(raw)
                        logger.debug(f"Fallback all markets: {len(raw)}")
            except Exception as e:
                logger.debug(f"Fallback fetch failed: {e}")

        # ── Deduplicate ───────────────────────────────────────────────────
        seen, unique = set(), []
        for m in all_raw:
            mid = (m.get("id") or m.get("conditionId") or
                   m.get("market_slug", ""))
            if mid and mid not in seen:
                seen.add(mid)
                unique.append(m)

        logger.debug(f"Unique markets to filter: {len(unique)}")

        # ── BTC keyword filter — very broad ───────────────────────────────
        BTC_KEYWORDS = [
            "bitcoin", "btc", "xbt",
            "up or down",         # Polymarket short-term format
            "above", "below",     # price markets
        ]

        markets = []
        for m in unique:
            q     = (m.get("question")    or
                     m.get("title")       or
                     m.get("_event_title") or "").lower()
            desc  = (m.get("description") or "").lower()
            tags  = " ".join(
                t.get("slug", t) if isinstance(t, dict) else str(t)
                for t in (m.get("tags") or [])
            ).lower()
            group = (m.get("groupItemTitle") or "").lower()

            # Must contain bitcoin/btc keyword
            is_btc = (
                "bitcoin" in q or "btc" in q or
                "bitcoin" in desc or "btc" in desc or
                "bitcoin" in tags or "btc" in tags or
                "bitcoin" in group or "btc" in group
            )

            if not is_btc:
                continue

            # Skip non-price markets (elections, ETF, regulation etc)
            # Focus on price prediction markets
            skip_keywords = [
                "president", "election", "senator", "congress",
                "etf approval", "regulation", "sec", "war", "microstrategy",
                "flip", "company", "stock", "mstr"
            ]
            if any(sk in q for sk in skip_keywords):
                continue

            # Extract end time
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
                end_ts = int(
                    m.get("endTimestamp", 0) or
                    m.get("end_timestamp", 0) or 0)

            time_to_expiry = end_ts - int(now) if end_ts > 0 else 3600

            # Skip already expired
            if end_ts > 0 and time_to_expiry < -300:
                continue

            markets.append({
                "id":             (m.get("id") or m.get("conditionId") or ""),
                "condition_id":   m.get("conditionId", ""),
                "question":       (m.get("question") or m.get("title") or ""),
                "outcomes":       json.dumps(
                    m.get("outcomes", ["YES", "NO"])),
                "outcome_prices": json.dumps(self._extract_prices(m)),
                "volume":         float(m.get("volume",    0) or 0),
                "liquidity":      float(m.get("liquidity", 0) or 0),
                "end_time":       end_ts,
                "start_time":     0,
                "market_type":    "btc_hourly",
                "time_to_expiry": max(0, time_to_expiry),
                "raw":            m,
            })

        # Sort: soonest expiry first
        markets.sort(key=lambda x: x["end_time"] if x["end_time"] > 0
                     else float("inf"))

        logger.info(
            f"Found {len(markets)} active BTC markets "
            f"(scanned {len(unique)} unique markets)"
        )

        for mkt in markets[:8]:
            logger.info(
                f"  [{mkt['time_to_expiry']//60:>4}min] "
                f"${mkt['volume']:>8,.0f} vol | "
                f"{mkt['question'][:65]}"
            )

        self._markets_cache = markets
        self._cache_ts      = now
        return markets

    def _extract_prices(self, market: Dict) -> List[float]:
        # tokens array
        tokens = market.get("tokens") or []
        if tokens:
            prices = []
            for t in tokens:
                p = t.get("price")
                if p is not None:
                    try:
                        prices.append(float(p))
                    except Exception:
                        pass
            if len(prices) >= 2:
                return prices[:2]

        # outcomePrices
        op = market.get("outcomePrices") or []
        if op:
            try:
                prices = [float(p) for p in op]
                if len(prices) >= 2:
                    return prices[:2]
            except Exception:
                pass

        # bestBid
        bid = market.get("bestBid")
        if bid is not None:
            try:
                yes_p = float(bid)
                return [yes_p, round(1.0 - yes_p, 4)]
            except Exception:
                pass

        return [0.5, 0.5]

    # ─────────────────────────────────────────────────────────────────────
    # Prices + Orders
    # ─────────────────────────────────────────────────────────────────────
    async def get_market_price(self, market_id: str) -> Tuple[float, float]:
        session = await self._get_session()
        try:
            async with session.get(
                f"{self.GAMMA_URL}/markets/{market_id}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status == 200:
                    m      = await resp.json()
                    prices = self._extract_prices(m)
                    if len(prices) >= 2:
                        return prices[0], prices[1]
                    if len(prices) == 1:
                        return prices[0], round(1.0 - prices[0], 4)
        except Exception as e:
            logger.debug(f"Price fetch error {market_id}: {e}")
        return 0.5, 0.5

    async def place_order(self, token_id: str, side: str, size: float,
                          price: float, order_type: str = "GTC") -> Optional[Dict]:
        if not self.api_key:
            return self._simulate_fill(token_id, side, size, price)

        session = await self._get_session()
        shares  = size / price if price > 0 else 0
        body    = json.dumps({
            "tokenID": token_id,
            "side":    side.upper(),
            "type":    order_type,
            "size":    str(round(shares, 2)),
            "price":   str(round(price, 4)),
        })
        try:
            async with session.post(
                f"{self.CLOB_URL}/order",
                data=body,
                headers=self._auth_headers("POST", "/order", body)
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("success"):
                    return data
                logger.error(f"Order failed: {data}")
                return None
        except Exception as e:
            logger.error(f"Order error: {e}")
            return None

    def _simulate_fill(self, token_id: str, side: str,
                       size: float, price: float) -> Dict:
        slip       = 0.001
        fill_price = (price * (1 + slip) if side == "BUY"
                      else price * (1 - slip))
        fill_price = max(0.01, min(0.99, fill_price))
        return {
            "orderId":    f"PAPER_{int(time.time()*1000)}",
            "status":     "FILLED",
            "fillPrice":  fill_price,
            "fillSize":   size / fill_price,
            "size_usdc":  size,
            "paper_trade": True,
        }

    async def cancel_order(self, order_id: str) -> bool:
        if not self.api_key or order_id.startswith("PAPER_"):
            return True
        session = await self._get_session()
        try:
            async with session.delete(
                f"{self.CLOB_URL}/order/{order_id}",
                headers=self._auth_headers("DELETE", f"/order/{order_id}")
            ) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def get_balance(self) -> float:
        if not self.api_key:
            return 10.0
        session = await self._get_session()
        try:
            async with session.get(
                f"{self.CLOB_URL}/balance",
                headers=self._auth_headers("GET", "/balance")
            ) as resp:
                data = await resp.json()
                return float(data.get("balance", 0))
        except Exception as e:
            logger.error(f"Balance error: {e}")
            return 0.0

    # ─────────────────────────────────────────────────────────────────────
    # Parsing helpers
    # ─────────────────────────────────────────────────────────────────────
    @staticmethod
    def parse_btc_target(question: str) -> Optional[float]:
        patterns = [
            r'\$([0-9,]+(?:\.[0-9]+)?)',
            r'(\d{2,3},\d{3}(?:\.[0-9]+)?)',
            r'above\s+\$?([0-9,]+)',
            r'below\s+\$?([0-9,]+)',
            r'([0-9]{5,6}(?:\.[0-9]+)?)',
        ]
        clean = question.replace(",", "")
        for pattern in patterns:
            match = re.search(pattern, clean, re.IGNORECASE)
            if match:
                try:
                    val = float(match.group(1).replace(",", ""))
                    if 10_000 < val < 1_000_000:
                        return val
                except Exception:
                    continue
        return None

    @staticmethod
    def market_direction(question: str) -> str:
        q = question.lower()
        if any(w in q for w in ["above", "higher", "exceed", "over",
                                  "up", "rise", "rally", "surpass"]):
            return "above"
        if any(w in q for w in ["below", "lower", "under", "drop",
                                  "fall", "decline", "crash"]):
            return "below"
        return "above"
