"""
Main Bot - Async orchestration loop
Coordinates: data feed → analysis → risk → execution → monitoring
"""
import asyncio
import time
import json
import logging
import sys
import os
from datetime import datetime, timezone

# ── Path setup ────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.config import TRADING, API, DATABASE, MODEL
from src.utils.database import Database
from src.utils.telegram_bot import TelegramBot
from src.data.btc_feed import BTCDataFeed, FundingRateCollector, SentimentCollector
from src.data.polymarket_client import PolymarketClient
from src.analysis.engine import AnalysisEngine, MarketScanner
from src.risk.risk_manager import RiskManager
from src.execution.executor import ExecutionEngine

# REPLACE WITH THIS:
os.makedirs("logs", exist_ok=True)
os.makedirs("db", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(DATABASE.LOG_FILE, mode="a"),
    ]
)
logger = logging.getLogger("BOT")


class PolymarketBTCBot:
    """
    Main bot controller.

    Loop cadence:
      - Every 1m  : update BTC indicators (via streaming)
      - Every 2m  : scan for new market opportunities
      - Every 30s : monitor open positions for exit
      - Every 5m  : update sentiment + funding
      - Every 1h  : snapshot portfolio, check progress
    """

    def __init__(self):
        self.db        = Database(DATABASE.DB_PATH)
        self.feed      = BTCDataFeed(db=self.db)
        self.funding   = FundingRateCollector()
        self.sentiment = SentimentCollector(API.CRYPTOPANIC_API_KEY)
        self.pm        = PolymarketClient(
            api_key     = API.POLYMARKET_API_KEY,
            secret      = API.POLYMARKET_SECRET,
            passphrase  = API.POLYMARKET_PASSPHRASE,
            private_key = API.POLYMARKET_PRIVATE_KEY,
        )
        self.risk     = RiskManager(TRADING, self.db)
        self.engine   = AnalysisEngine(TRADING, self.feed, self.funding, self.sentiment)
        self.scanner  = MarketScanner(self.engine, TRADING)
        self.executor = ExecutionEngine(self.pm, self.risk, self.db, TRADING)
        self.tg       = TelegramBot(API.TELEGRAM_BOT_TOKEN, API.TELEGRAM_CHAT_ID)
        self.executor.tg = self.tg

        self.start_time  = datetime.utcnow()
        self.cycle_count = 0
        self._running    = False

        self._analyzed_markets: set = set()

    # ─────────────────────────────────────────────────────────────────────
    # Bootstrap
    # ─────────────────────────────────────────────────────────────────────
    async def start(self):
        logger.info("=" * 60)
        logger.info("  Polymarket BTC Trading Bot Starting")
        logger.info(f"  Target: ${TRADING.INITIAL_BANKROLL:.2f} → ${TRADING.TARGET_BANKROLL:.0f} "
                    f"in {TRADING.TARGET_DAYS} days")
        logger.info("=" * 60)

        await self.feed.load_history()
        await self.feed.start_streaming()

        await asyncio.gather(
            self.funding.update(),
            self.sentiment.update(),
        )

        self._running = True

        balance = await self.pm.get_balance()
        if balance > 0:
            self.risk.update_bankroll(balance)
            logger.info(f"Wallet balance: ${balance:.4f}")

        logger.info("Bot initialized. Starting main loop...")
        await self.tg.startup(balance=self.risk.bankroll)
        await self._main_loop()

    async def stop(self):
        self._running = False
        await self.feed.stop()
        await self.pm.close()
        logger.info("Bot stopped.")

    # ─────────────────────────────────────────────────────────────────────
    # Main loop
    # ─────────────────────────────────────────────────────────────────────
    async def _main_loop(self):
        last_scan           = 0.0
        last_monitor        = 0.0
        last_sentiment      = 0.0
        last_snapshot       = 0.0
        last_market_refresh = 0.0

        while self._running:
            now = time.time()
            self.cycle_count += 1

            try:
                if now - last_sentiment > 300:
                    await asyncio.gather(
                        self.funding.update(),
                        self.sentiment.update(),
                    )
                    last_sentiment = now

                if now - last_market_refresh > 90:
                    markets = await self.pm.get_btc_hourly_markets(force_refresh=True)
                    for m in markets:
                        self.db.upsert_market(m)
                    last_market_refresh = now
                    logger.info(f"Markets refreshed: {len(markets)} active BTC markets")

                if now - last_monitor > 30:
                    btc_vol = self._get_btc_vol()
                    await self.executor.monitor_positions(self.engine, btc_vol)
                    last_monitor = now

                if now - last_scan > 120:
                    await self._scan_and_trade()
                    last_scan = now

                if now - last_snapshot > 3600:
                    await self._snapshot_portfolio()
                    last_snapshot = now

                await asyncio.sleep(5)

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received. Shutting down...")
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                await asyncio.sleep(10)

    # ─────────────────────────────────────────────────────────────────────
    # Scan + Trade
    # ─────────────────────────────────────────────────────────────────────
    async def _scan_and_trade(self):
        if self.risk.is_halted:
            logger.warning(f"Trading halted: {self.risk.halt_reason}")
            await self.tg.circuit_breaker(self.risk.halt_reason)
            return

        markets = self.db.get_active_markets()
        if not markets:
            logger.debug("No active markets in DB")
            return

        fresh = await self.pm.get_btc_hourly_markets()

        market_map = {m["id"]: m for m in markets}
        for m in fresh:
            market_map[m["id"]] = m

        all_markets = list(market_map.values())
        signals     = self.scanner.scan(all_markets)

        if not signals:
            logger.debug("No actionable signals this cycle")
            return

        logger.info(f"Found {len(signals)} signal(s) this cycle")

        btc_vol = self._get_btc_vol()

        for sig in signals:
            open_ids = {t["market_id"] for t in self.db.get_open_trades()}
            if sig.market_id in open_ids:
                continue

            size_result = self.risk.check_trade(sig, btc_vol)

            if not size_result.allowed:
                logger.debug(f"Risk check blocked: {size_result.reason}")
                continue

            logger.info(
                f"SIGNAL: {sig.strategy} | {sig.outcome} on {sig.question[:60]}\n"
                f"        Edge={sig.edge*100:+.1f}% EV={sig.ev*100:+.1f}¢ "
                f"Conf={sig.confidence*100:.0f}% Kelly={sig.kelly_fraction*100:.1f}% "
                f"Size=${size_result.size_usdc:.2f}\n"
                f"        {sig.rationale.split(chr(10))[0]}"
            )

            trade_uuid = await self.executor.enter_position(sig, size_result)
            if trade_uuid:
                logger.info(f"✅ Trade opened: {trade_uuid}")
                await self.tg.trade_opened({
                    "question":    sig.question,
                    "outcome":     sig.outcome,
                    "size_usdc":   size_result.size_usdc,
                    "entry_price": sig.entry_price,
                    "strategy":    sig.strategy,
                    "ev":          sig.ev,
                })
            else:
                logger.warning("Trade execution failed")

            await asyncio.sleep(2)

    # ─────────────────────────────────────────────────────────────────────
    # Portfolio snapshot
    # ─────────────────────────────────────────────────────────────────────
    async def _snapshot_portfolio(self):
        state     = self.risk.get_state()
        stats     = self.db.get_trade_stats()
        btc_price = self.feed.get_price()

        n        = stats.get("total", 0)
        wins     = stats.get("wins", 0)
        win_rate = wins / n if n else 0

        snap = {
            "timestamp":            int(time.time()),
            "total_balance":        state["bankroll"],
            "available_cash":       state["available_cash"],
            "open_positions_value": state["open_exposure"],
            "daily_pnl":            state["daily_pnl"],
            "total_pnl":            state["total_pnl"],
            "total_pnl_pct":        state["total_pnl_pct"],
            "win_rate":             win_rate,
            "sharpe_ratio":         0.0,
            "max_drawdown":         state["drawdown_pct"] / 100,
            "open_trades":          state["open_positions"],
            "btc_price":            btc_price,
        }
        self.db.save_snapshot(snap)

        days = (datetime.utcnow() - self.start_time).days
        logger.info(self.risk.progress_summary(days))

        await self.tg.daily_summary({
            "balance":      snap["total_balance"],
            "daily_pnl":    snap["daily_pnl"],
            "win_rate":     snap["win_rate"],
            "total_trades": stats.get("total", 0),
            "max_drawdown": snap["max_drawdown"],
            "goal_pct":     (snap["total_balance"] / TRADING.TARGET_BANKROLL) * 100,
        })

    def _get_btc_vol(self) -> float:
        ind = self.feed.get_indicators()
        return ind.get("hourly_vol", 0.01)

    # ─────────────────────────────────────────────────────────────────────
    # Status for dashboard
    # ─────────────────────────────────────────────────────────────────────
    def get_status(self) -> dict:
        state = self.risk.get_state()
        stats = self.db.get_trade_stats()
        ind   = self.feed.get_indicators()
        days  = max(1, (datetime.utcnow() - self.start_time).days)

        n            = stats.get("total", 0)
        wins         = stats.get("wins", 0)
        gross_profit = stats.get("gross_profit", 0) or 0
        gross_loss   = stats.get("gross_loss", 0) or 0

        return {
            "bot": {
                "uptime_hours": round((time.time() - self.start_time.timestamp()) / 3600, 1),
                "cycle_count":  self.cycle_count,
                "is_halted":    state["is_halted"],
                "halt_reason":  state["halt_reason"],
            },
            "portfolio": {
                "balance":       state["bankroll"],
                "available":     state["available_cash"],
                "exposure":      state["open_exposure"],
                "peak":          state["peak_bankroll"],
                "total_pnl":     state["total_pnl"],
                "total_pnl_pct": state["total_pnl_pct"],
                "daily_pnl":     state["daily_pnl"],
                "daily_pnl_pct": state["daily_pnl_pct"],
                "drawdown_pct":  state["drawdown_pct"],
            },
            "performance": {
                "total_trades":  n,
                "win_rate":      round(wins / n * 100, 1) if n else 0,
                "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss else 999,
                "avg_pnl_pct":   round(stats.get("avg_pnl_pct", 0) or 0, 4),
                "best_trade":    round(stats.get("best_trade", 0) or 0, 4),
                "worst_trade":   round(stats.get("worst_trade", 0) or 0, 4),
            },
            "market": {
                "btc_price":    ind.get("price", 0),
                "btc_rsi":      ind.get("rsi_14", 50),
                "btc_vol":      round(ind.get("hourly_vol", 0) * 100, 3),
                "sentiment":    round(self.sentiment.get_score(), 3),
                "fear_greed":   self.sentiment.fear_greed,
                "funding_rate": round(self.funding.funding_rate * 100, 5),
            },
            "open_positions": self.executor.get_open_positions_summary(),
            "progress": {
                "days_elapsed":  days,
                "days_left":     TRADING.TARGET_DAYS - days,
                "target":        TRADING.TARGET_BANKROLL,
                "pct_to_goal":   round(
                    (state["bankroll"] / TRADING.TARGET_BANKROLL) * 100, 1
                ),
                "required_daily_return": round(
                    self.risk.required_daily_return(days), 1
                ),
            },
        }


# ─────────────────────────────────────────────────────────────────────────
# Entry points
# ─────────────────────────────────────────────────────────────────────────
async def run_bot():
    bot = PolymarketBTCBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.stop()


async def run_backtest():
    """Quick backtest using local DB candles"""
    from src.backtesting.backtest import Backtester

    db      = Database(DATABASE.DB_PATH)
    candles = db.get_candles(limit=5000)

    if len(candles) < 120:
        print("Not enough candle data. Run: python main.py seed")
        return

    print(f"Running backtest on {len(candles)} candles...")
    bt     = Backtester(TRADING, db)
    result = bt.run(candles, start_idx=60, initial_capital=TRADING.INITIAL_BANKROLL)
    print(result.summary())

    db.log("INFO", "BACKTEST", f"Backtest complete: {result.total_return*100:.1f}% return",
           {"run_id": result.run_id, "trades": result.total_trades})


async def seed_history():
    """Pre-seed the DB with 30 days of 1m candles from Binance"""
    import aiohttp
    db = Database(DATABASE.DB_PATH)
    print("Downloading 30d of 1m candles from Binance...")
    async with aiohttp.ClientSession() as session:
        end_time = int(time.time() * 1000)
        total    = 0
        for _ in range(30):
            params = {
                "symbol": "BTCUSDT", "interval": "1m",
                "limit": "1000", "endTime": str(end_time)
            }
            async with session.get(
                "https://api.binance.com/api/v3/klines", params=params
            ) as resp:
                klines = await resp.json()
            if not klines:
                break
            for k in klines:
                ts = int(k[0]) // 1000
                db.upsert_candle(ts, float(k[1]), float(k[2]),
                                 float(k[3]), float(k[4]), float(k[5]))
            end_time = int(klines[0][0]) - 1
            total   += len(klines)
            print(f"  Downloaded {total} candles...")
            await asyncio.sleep(0.5)
    print(f"Done. {total} candles saved.")


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"

    if cmd == "run":
        asyncio.run(run_bot())
    elif cmd == "backtest":
        asyncio.run(run_backtest())
    elif cmd == "seed":
        asyncio.run(seed_history())
    else:
        print("Usage: python main.py [run|backtest|seed]")
