"""
Execution Engine
- Converts TradeSignals into live Polymarket orders
- Monitors open positions for exit conditions
- Handles partial fills, slippage, and timeouts
"""
import asyncio
import uuid
import time
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ExecutionEngine:
    """
    Bridges the analysis + risk layers to the Polymarket API.
    Runs in an async loop alongside the data feed.
    """

    def __init__(self, polymarket_client, risk_manager, db, config):
        self.pm   = polymarket_client
        self.risk = risk_manager
        self.db   = db
        self.cfg  = config
        self.tg   = None  # Injected from main.py
        self._open_orders: Dict[str, Dict] = {}   # trade_uuid → order metadata

    # ─────────────────────────────────────────────────────────────────────
    # Enter a position
    # ─────────────────────────────────────────────────────────────────────
    async def enter_position(self, signal, size_result) -> Optional[str]:
        """
        Place a buy order for the signaled outcome.
        Returns trade_uuid on success, None on failure.
        """
        trade_uuid = str(uuid.uuid4())

        # Determine token ID for the outcome
        token_id = self._get_token_id(signal.market_id, signal.outcome,
                                       signal.btc_target, signal.direction)

        logger.info(
            f"ENTER [{signal.outcome}] {signal.question[:60]} | "
            f"${size_result.size_usdc:.2f} @ {signal.entry_price:.4f} | "
            f"Edge={signal.edge*100:+.1f}% EV={signal.ev*100:+.1f}¢"
        )

        # Place order
        fill = await self.pm.place_order(
            token_id = token_id,
            side     = "BUY",
            size     = size_result.size_usdc,
            price    = signal.entry_price,
        )

        if not fill:
            logger.warning(f"Order failed for {signal.market_id}")
            return None

        fill_price  = float(fill.get("fillPrice", signal.entry_price))
        fill_size   = float(fill.get("fillSize",  size_result.size_usdc / fill_price))
        actual_cost = fill_price * fill_size

        # Save to DB
        pred_id = self.db.save_prediction({
            "market_id":      signal.market_id,
            "timestamp":      int(time.time()),
            "predicted_prob": signal.predicted_prob,
            "market_prob":    signal.market_prob,
            "edge":           signal.edge,
            "ev":             signal.ev,
            "confidence":     signal.confidence,
            "strategy":       signal.strategy,
            "signals":        json.dumps([
                {"strategy": s.strategy, "probability": s.probability,
                 "confidence": s.confidence, "rationale": s.rationale}
                for s in signal.signals
            ]),
            "btc_price": signal.btc_price,
        })

        trade = {
            "trade_uuid":          trade_uuid,
            "market_id":           signal.market_id,
            "outcome":             signal.outcome,
            "direction":           "BUY",
            "size_usdc":           round(actual_cost, 4),
            "shares":              round(fill_size, 4),
            "entry_price":         round(fill_price, 6),
            "entry_time":          int(time.time()),
            "status":              "OPEN",
            "strategy":            signal.strategy,
            "prediction_id":       pred_id,
            "polymarket_order_id": fill.get("orderId", ""),
        }

        trade_id = self.db.open_trade(trade)
        self.risk.on_trade_open(trade_uuid, actual_cost, signal.market_id, signal.strategy)

        # Cache for monitoring
        self._open_orders[trade_uuid] = {
            **trade,
            "db_id":       trade_id,
            "signal":      signal,
            "token_id":    token_id,
            "question":    signal.question,
            "expiry_time": int(time.time()) + signal.time_to_expiry,
        }

        self.db.log("INFO", "EXECUTION", f"Opened {signal.outcome} on {signal.market_id}",
                    {"trade_uuid": trade_uuid, "size": actual_cost, "price": fill_price})

        return trade_uuid

    # ─────────────────────────────────────────────────────────────────────
    # Exit a position
    # ─────────────────────────────────────────────────────────────────────
    async def exit_position(self, trade_uuid: str, reason: str,
                             current_price: Optional[float] = None) -> bool:
        """Close an open position by selling shares back."""
        order = self._open_orders.get(trade_uuid)
        if not order:
            logger.warning(f"No cached order found: {trade_uuid}")
            return False

        shares      = order.get("shares", 0)
        entry_price = order.get("entry_price", 0)

        # Get current market price
        if current_price is None:
            yes_p, no_p = await self.pm.get_market_price(order["market_id"])
            current_price = yes_p if order["outcome"] == "YES" else no_p

        current_price = max(0.01, min(0.99, current_price))

        # Sell
        token_id = order.get("token_id", "")
        fill = await self.pm.place_order(
            token_id = token_id,
            side     = "SELL",
            size     = shares * current_price,
            price    = current_price,
        )

        exit_price = float(fill.get("fillPrice", current_price)) if fill else current_price

        # P&L
        pnl     = (exit_price - entry_price) * shares
        pnl_pct = (exit_price - entry_price) / entry_price if entry_price else 0

        logger.info(
            f"EXIT [{reason}] {order['question'][:50]} | "
            f"{exit_price:.4f} (entry={entry_price:.4f}) | "
            f"PnL=${pnl:+.4f} ({pnl_pct*100:+.1f}%)"
        )

        # Update DB
        self.db.close_trade(trade_uuid, exit_price, pnl, pnl_pct, reason)

        # Send Telegram alert
        if hasattr(self, 'tg') and self.tg:
            asyncio.create_task(self.tg.trade_closed({
                "question":    order.get("question", ""),
                "outcome":     order.get("outcome"),
                "pnl":         pnl,
                "pnl_pct":     pnl_pct,
                "exit_reason": reason,
                "strategy":    order.get("strategy"),
            }))

        self.risk.on_trade_close(trade_uuid, pnl)

        # Remove from cache
        self._open_orders.pop(trade_uuid, None)

        self.db.log("INFO", "EXECUTION",
                    f"Closed {trade_uuid} [{reason}] PnL=${pnl:+.4f}",
                    {"exit_price": exit_price, "pnl": pnl, "reason": reason})
        return True

    # ─────────────────────────────────────────────────────────────────────
    # Monitor loop
    # ─────────────────────────────────────────────────────────────────────
    async def monitor_positions(self, analysis_engine, btc_vol_pct: float = 0.0):
        """
        Check all open positions for exit conditions.
        Should be called every ~30 seconds.
        """
        if not self._open_orders:
            return

        now      = int(time.time())
        to_close = []

        for trade_uuid, order in list(self._open_orders.items()):
            try:
                market_id  = order["market_id"]
                outcome    = order["outcome"]
                entry_time = order.get("entry_time", now)
                expiry     = order.get("expiry_time", now + 3600)

                # Fetch live market price
                yes_p, no_p = await self.pm.get_market_price(market_id)
                current_price = yes_p if outcome == "YES" else no_p

                # Get updated model probability
                signal = order.get("signal")
                if signal:
                    updated = analysis_engine.analyze_market(
                        signal.__dict__ if hasattr(signal, '__dict__') else {
                            "id":             market_id,
                            "question":       order.get("question", ""),
                            "outcome_prices": json.dumps([yes_p, no_p]),
                            "time_to_expiry": max(0, expiry - now),
                        }
                    )
                    model_prob = updated.predicted_prob if updated else (
                        yes_p if outcome == "YES" else no_p
                    )
                else:
                    model_prob = current_price

                # Ask risk manager
                db_trade = {
                    "entry_price": order.get("entry_price", 0.5),
                    "outcome":     outcome,
                    "entry_time":  entry_time,
                }
                should_exit, exit_reason = self.risk.check_exit_conditions(
                    db_trade, current_price, model_prob
                )

                # Market expired?
                if now >= expiry - 60:
                    should_exit = True
                    exit_reason = "MARKET_EXPIRY"

                if should_exit:
                    to_close.append((trade_uuid, exit_reason, current_price))

            except Exception as e:
                logger.error(f"Monitor error for {trade_uuid}: {e}")

        # Execute closures
        for trade_uuid, reason, price in to_close:
            await self.exit_position(trade_uuid, reason, price)

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────
    def _get_token_id(self, market_id: str, outcome: str,
                      btc_target: float, direction: str) -> str:
        """
        In production: fetch the token ID from the market's CLOB entry.
        Token IDs are the ERC1155 token addresses for YES/NO shares.
        For paper trading we use a synthetic ID.
        """
        return f"{market_id}_{outcome}"

    def get_open_positions_summary(self) -> List[Dict]:
        """Return summary of all open positions for dashboard"""
        summary = []
        now = time.time()
        for uuid_, order in self._open_orders.items():
            entry = order.get("entry_price", 0)
            summary.append({
                "trade_uuid":   uuid_,
                "market_id":    order.get("market_id"),
                "question":     order.get("question", "")[:80],
                "outcome":      order.get("outcome"),
                "strategy":     order.get("strategy"),
                "size_usdc":    order.get("size_usdc"),
                "shares":       order.get("shares"),
                "entry_price":  entry,
                "hold_minutes": round((now - order.get("entry_time", now)) / 60, 1),
                "expiry_in":    round((order.get("expiry_time", now) - now) / 60, 1),
            })
        return summary
