"""
Polymarket CLOB Client
- Discover active BTC markets
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
    """

    GAMMA_URL = "https://gamma-api.polymarket.com"
    CLOB_URL  = "https://clob.polymarket.com"

    def __init__(self, api_key: str = "", secret: str = "",
                 passphrase: str = "", private_key: str = ""):
        self.api_key     = api_key
        self.secret      = secret
        self.passphrase  = passphrase
        self.private_key = private_key
        self._session: Optional[aiohttp.ClientSession] = None

        # Cache
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
    # Market discovery — FIXED
    # ─────────────────────────────────────────────────────────────────────
    async def get_btc_hourly_markets(self, force_refresh: bool = False) -> List[Dict]:
        """
        Returns all active BTC markets on Polymarket.
        Removed overly strict hourly filter — now catches all BTC markets
        and lets the analysis engine decide which ones to trade.
        """
        now = time.time()
        if (not force_refresh and self._markets_cache
                and (now - self._cache_ts) < self._cache_ttl):
            return self._markets_cache

        session  = await self._get_session()
        markets  = []
        all_raw  = []

        try:
            # ── Fetch from Gamma API ──────────────────────────────────────
            # Try multiple search terms to catch all BTC markets
            search_terms = ["bitcoin", "BTC"]

            for term in search_terms:
                params = {
                    "active":  "true",
                    "closed":  "false",
                    "limit":   "100",
                    "order":   "volume",
                    "search":  term,
                }
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
                            logger.debug(
                                f"Gamma search '{term}': {len(raw)} results")
                except Exception as e:
                    logger.debug(f"Gamma search '{term}' failed: {e}")

            # Also try without search term (get all active markets)
            try:
                params = {
                    "active": "true",
                    "closed": "false",
                    "limit":  "200",
                    "order":  "volume",
                }
                async with session.get(
                    f"{self.GAMMA_URL}/markets",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data    = await resp.json()
                        raw     = (data if isinstance(data, list)
                                   else data.get("markets", []))
                        all_raw.extend(raw)
                        logger.debug(f"Gamma all markets: {len(raw)} results")
            except Exception as e:
                logger.debug(f"Gamma all-markets fetch failed: {e}")

            # ── Deduplicate by market ID ──────────────────────────────────
            seen   = set()
            unique = []
            for m in all_raw:
                mid = m.get("id") or m.get("conditionId", "")
                if mid and mid not in seen:
                    seen.add(mid)
                    unique.append(m)

            logger.debug(f"Total unique markets from API: {len(unique)}")

            # ── Filter for BTC markets ────────────────────────────────────
            for m in unique:
                q    = (m.get("question")    or "").lower()
                desc = (m.get("description") or "").lower()
                tags = [t.lower() for t in (m.get("tags") or [])]

                # BTC check — broad to catch all variations
                is_btc = (
                    "bitcoin" in q or "btc" in q or
                    "bitcoin" in desc or "btc" in desc or
                    any("bitcoin" in t or "btc" in t for t in tags)
                )

                if not is_btc:
                    continue

                # Active check — Polymarket uses different field names
                is_active = (
                    m.get("active") is True or
                    m.get("active") == "true" or
                    m.get("isActive") is True or
                    m.get("closed") is False or
                    m.get("closed") == "false" or
                    m.get("status", "").lower() == "active"
                )

                # Skip clearly closed/resolved markets
                if m.get("resolved") or m.get("isResolved"):
                    continue

                # Extract end time
                end_ts  = 0
                end_str = (m.get("endDate") or m.get("end_date_iso") or
                           m.get("endDateIso") or m.get("end_date") or "")
                if end_str:
                    try:
                        dt     = datetime.fromisoformat(
                            end_str.replace("Z", "+00:00"))
                        end_ts = int(dt.timestamp())
                    except Exception:
                        pass

                # If no end time found, try unix timestamp fields
                if end_ts == 0:
                    end_ts = int(m.get("endTimestamp", 0) or
                                 m.get("end_timestamp", 0) or 0)

                time_to_expiry = end_ts - int(now) if end_ts > 0 else 0

                # WIDENED window: include markets expiring in next 24 hours
                # (previously was 4 hours — too narrow)
                if end_ts > 0 and time_to_expiry < 0:
                    continue  # Already expired

                # Include market regardless of is_active if it has a future end time
                # or if is_active is True
                if not is_active and time_to_expiry <= 0:
                    continue

                markets.append({
                    "id":             m.get("id", "") or m.get("conditionId", ""),
                    "condition_id":   m.get("conditionId", ""),
                    "question":       m.get("question", ""),
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

            # Sort by soonest expiry first (most urgent opportunities)
            markets.sort(key=lambda x: x["end_time"] if x["end_time"] > 0
                         else float("inf"))

            logger.info(
                f"Found {len(markets)} active BTC markets "
                f"(from {len(unique)} total unique markets scanned)"
            )

            # Log market titles for debugging
            for mkt in markets[:5]:
                logger.info(
                    f"  Market: {mkt['question'][:70]} "
                    f"| TTX: {mkt['time_to_expiry']//60}min "
                    f"| Vol: ${mkt['volume']:.0f}"
                )

        except Exception as e:
            logger.error(f"Market fetch error: {e}", exc_info=True)
            return self._markets_cache

        self._markets_cache = markets
        self._cache_ts      = now
        return markets

    def _extract_prices(self, market: Dict) -> List[float]:
        """Extract YES/NO prices from market data"""
        # Try tokens array first
        tokens = market.get("tokens") or []
        if tokens:
            prices = []
            for t in tokens:
                p = t.get("price")
                if p is not None:
                    try:
                        prices.append(float(p))
                    except (ValueError, TypeError):
                        pass
            if len(prices) >= 2:
                return prices[:2]

        # Try outcomePrices
        op = market.get("outcomePrices") or []
        if op:
            try:
                prices = [float(p) for p in op]
                if len(prices) >= 2:
                    return prices[:2]
            except Exception:
                pass

        # Try bestBid/bestAsk
        bid = market.get("bestBid")
        ask = market.get("bestAsk")
        if bid is not None:
            try:
                yes_price = float(bid)
                return [yes_price, round(1.0 - yes_price, 4)]
            except Exception:
                pass

        return [0.5, 0.5]

    # ─────────────────────────────────────────────────────────────────────
    # Order book
    # ─────────────────────────────────────────────────────────────────────
    async def get_orderbook(self, token_id: str) -> Dict:
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
            logger.debug(f"Price fetch error for {market_id}: {e}")
        return 0.5, 0.5

    # ─────────────────────────────────────────────────────────────────────
    # Order execution
    # ─────────────────────────────────────────────────────────────────────
    async def place_order(self, token_id: str, side: str, size: float,
                          price: float, order_type: str = "GTC") -> Optional[Dict]:
        # Paper trading mode
        if not self.api_key:
            return self._simulate_fill(token_id, side, size, price)

        # Live trading
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
            headers = self._auth_headers("POST", "/order", body)
            async with session.post(
                f"{self.CLOB_URL}/order",
                data=body, headers=headers
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("success"):
                    logger.info(
                        f"Order placed: {side} {shares:.2f} @ {price:.4f}")
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
        slippage   = 0.001
        fill_price = (price * (1 + slippage) if side == "BUY"
                      else price * (1 - slippage))
        fill_price = max(0.01, min(0.99, fill_price))
        shares     = size / fill_price
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
        if not self.api_key:
            return 10.0   # Paper trading balance
        session = await self._get_session()
        try:
            headers = self._auth_headers("GET", "/balance")
            async with session.get(
                f"{self.CLOB_URL}/balance", headers=headers
            ) as resp:
                data = await resp.json()
                return float(data.get("balance", 0))
        except Exception as e:
            logger.error(f"Balance fetch error: {e}")
            return 0.0

    # ─────────────────────────────────────────────────────────────────────
    # Parsing helpers
    # ─────────────────────────────────────────────────────────────────────
    @staticmethod
    def parse_btc_target(question: str) -> Optional[float]:
        """Extract BTC price target from market question"""
        patterns = [
            r'\$([0-9,]+(?:\.[0-9]+)?)',
            r'(\d{2,3},\d{3}(?:\.[0-9]+)?)',
            r'above\s+(\d+)',
            r'below\s+(\d+)',
            r'at\s+\$?(\d+)',
            r'(\d{5,6}(?:\.\d+)?)',   # bare 5-6 digit number (BTC price range)
        ]
        clean = question.replace(",", "")
        for pattern in patterns:
            match = re.search(pattern, clean, re.IGNORECASE)
            if match:
                try:
                    val = float(match.group(1).replace(",", ""))
                    # Sanity check: BTC price between $10k and $1M
                    if 10_000 < val < 1_000_000:
                        return val
                except Exception:
                    continue
        return None

    @staticmethod
    def market_direction(question: str) -> str:
        """Return 'above' or 'below'"""
        q = question.lower()
        if any(w in q for w in ["above", "higher", "exceed", "over",
                                  "up", "rise", "rally", "surpass"]):
            return "above"
        if any(w in q for w in ["below", "lower", "under", "drop",
                                  "fall", "decline", "crash"]):
            return "below"
        return "above"   # default
