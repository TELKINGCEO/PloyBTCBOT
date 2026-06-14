"""
Dashboard WebSocket Server
Broadcasts bot state to the browser dashboard in real-time.
"""
import asyncio
import json
import logging
import time
from typing import Set, Any

logger = logging.getLogger(__name__)

try:
    import websockets
    HAS_WS = True
except ImportError:
    HAS_WS = False


class DashboardServer:
    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self._clients: Set = set()
        self._server = None

    async def start(self):
        if not HAS_WS:
            logger.warning("websockets not installed — dashboard disabled")
            return
        self._server = await websockets.serve(
            self._handler, self.host, self.port
        )
        logger.info(f"Dashboard WS server on ws://{self.host}:{self.port}")

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handler(self, ws, path="/"):
        self._clients.add(ws)
        logger.info(f"Dashboard client connected ({len(self._clients)} total)")
        try:
            async for _ in ws:
                pass  # dashboard is receive-only
        except Exception:
            pass
        finally:
            self._clients.discard(ws)

    async def broadcast(self, msg: dict):
        if not self._clients:
            return
        data = json.dumps(msg)
        dead = set()
        for client in self._clients:
            try:
                await client.send(data)
            except Exception:
                dead.add(client)
        self._clients -= dead

    # ── Typed broadcast helpers ──────────────────────────────────────────
    async def send_balance(self, balance: float):
        await self.broadcast({"type": "balance", "value": balance,
                               "ts": int(time.time())})

    async def send_indicators(self, indicators: dict):
        await self.broadcast({"type": "indicators", "payload": indicators,
                               "ts": int(time.time())})

    async def send_signals(self, signals: dict):
        await self.broadcast({"type": "signals", "payload": signals,
                               "ts": int(time.time())})

    async def send_markets(self, markets: list):
        # Trim to essential fields for bandwidth
        trimmed = [{
            "id":           m.get("id"),
            "question":     m.get("question", "")[:100],
            "ev":           m.get("ev", 0),
            "edge":         m.get("edge", 0),
            "confidence":   m.get("confidence", 0),
            "time_to_expiry": m.get("time_to_expiry", 0),
            "actionable":   m.get("actionable", False),
        } for m in markets[:20]]
        await self.broadcast({"type": "markets", "payload": trimmed})

    async def send_trade_opened(self, trade: dict):
        await self.broadcast({"type": "trade_opened", "trade": trade,
                               "ts": int(time.time())})

    async def send_trade_closed(self, trade: dict):
        await self.broadcast({"type": "trade_closed", "trade": trade,
                               "ts": int(time.time())})

    async def send_log(self, message: str, level: str = "info"):
        await self.broadcast({"type": "log", "message": message,
                               "level": level, "ts": int(time.time())})

    async def send_full_state(self, state: dict):
        await self.broadcast({"type": "state", "payload": state,
                               "ts": int(time.time())})
