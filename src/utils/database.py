"""
Database Schema - SQLite with full trading history
"""
import sqlite3
import json
import os
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)

SCHEMA = """
-- BTC Price History (1-minute candles)
CREATE TABLE IF NOT EXISTS btc_prices (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   INTEGER NOT NULL,
    open        REAL NOT NULL,
    high        REAL NOT NULL,
    low         REAL NOT NULL,
    close       REAL NOT NULL,
    volume      REAL NOT NULL,
    UNIQUE(timestamp)
);
CREATE INDEX IF NOT EXISTS idx_btc_ts ON btc_prices(timestamp DESC);

-- Polymarket Markets
CREATE TABLE IF NOT EXISTS markets (
    id              TEXT PRIMARY KEY,
    question        TEXT NOT NULL,
    condition_id    TEXT,
    outcomes        TEXT NOT NULL,        -- JSON array
    outcome_prices  TEXT,                 -- JSON array, current
    volume          REAL DEFAULT 0,
    liquidity       REAL DEFAULT 0,
    start_time      INTEGER,
    end_time        INTEGER,
    resolved        INTEGER DEFAULT 0,
    resolution      TEXT,
    market_type     TEXT DEFAULT 'btc_hourly',
    created_at      INTEGER DEFAULT (strftime('%s','now')),
    updated_at      INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_markets_end ON markets(end_time);
CREATE INDEX IF NOT EXISTS idx_markets_resolved ON markets(resolved);

-- Model Predictions
CREATE TABLE IF NOT EXISTS predictions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id       TEXT NOT NULL,
    timestamp       INTEGER NOT NULL,
    predicted_prob  REAL NOT NULL,        -- Our model probability
    market_prob     REAL NOT NULL,        -- Implied from market price
    edge            REAL NOT NULL,        -- predicted - market
    ev              REAL NOT NULL,        -- Expected value
    confidence      REAL NOT NULL,        -- Model confidence 0-1
    strategy        TEXT NOT NULL,        -- Which strategy fired
    signals         TEXT,                 -- JSON feature dump
    btc_price       REAL,
    FOREIGN KEY(market_id) REFERENCES markets(id)
);
CREATE INDEX IF NOT EXISTS idx_pred_market ON predictions(market_id, timestamp DESC);

-- Trades / Orders
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_uuid      TEXT UNIQUE NOT NULL,
    market_id       TEXT NOT NULL,
    outcome         TEXT NOT NULL,        -- "YES" or "NO"
    direction       TEXT NOT NULL,        -- "BUY" or "SELL"
    size_usdc       REAL NOT NULL,        -- Dollar size
    shares          REAL NOT NULL,        -- Number of shares
    entry_price     REAL NOT NULL,        -- Price per share (0-1)
    exit_price      REAL,
    entry_time      INTEGER NOT NULL,
    exit_time       INTEGER,
    status          TEXT DEFAULT 'OPEN',  -- OPEN, CLOSED, CANCELLED
    pnl             REAL,                 -- Realized P&L
    pnl_pct         REAL,
    exit_reason     TEXT,                 -- PROFIT_TARGET, STOP_LOSS, EXPIRED, MANUAL
    strategy        TEXT NOT NULL,
    prediction_id   INTEGER,
    polymarket_order_id TEXT,
    FOREIGN KEY(market_id) REFERENCES markets(id),
    FOREIGN KEY(prediction_id) REFERENCES predictions(id)
);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id);
CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(entry_time DESC);

-- Portfolio Snapshots (hourly)
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       INTEGER NOT NULL,
    total_balance   REAL NOT NULL,
    available_cash  REAL NOT NULL,
    open_positions_value REAL NOT NULL,
    daily_pnl       REAL NOT NULL,
    total_pnl       REAL NOT NULL,
    total_pnl_pct   REAL NOT NULL,
    win_rate        REAL,
    sharpe_ratio    REAL,
    max_drawdown    REAL,
    open_trades     INTEGER DEFAULT 0,
    btc_price       REAL,
    UNIQUE(timestamp)
);

-- Market Signals
CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       INTEGER NOT NULL,
    signal_type     TEXT NOT NULL,        -- RSI, MACD, BB, MOMENTUM, etc.
    value           REAL NOT NULL,
    direction       TEXT,                 -- BULLISH, BEARISH, NEUTRAL
    strength        REAL,                 -- 0-1
    btc_price       REAL
);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(timestamp DESC);

-- News / Sentiment Events
CREATE TABLE IF NOT EXISTS news_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       INTEGER NOT NULL,
    source          TEXT,
    headline        TEXT NOT NULL,
    sentiment_score REAL,                 -- -1 to 1
    impact_score    REAL,                 -- 0 to 1 estimated market impact
    url             TEXT,
    processed       INTEGER DEFAULT 0
);

-- Bot Run Logs
CREATE TABLE IF NOT EXISTS bot_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    level           TEXT NOT NULL,
    component       TEXT NOT NULL,
    message         TEXT NOT NULL,
    data            TEXT                  -- JSON extra data
);
CREATE INDEX IF NOT EXISTS idx_logs_ts ON bot_logs(timestamp DESC);

-- Backtest Results
CREATE TABLE IF NOT EXISTS backtest_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT UNIQUE NOT NULL,
    start_date      TEXT NOT NULL,
    end_date        TEXT NOT NULL,
    initial_capital REAL NOT NULL,
    final_capital   REAL NOT NULL,
    total_return    REAL NOT NULL,
    annualized_return REAL,
    sharpe_ratio    REAL,
    sortino_ratio   REAL,
    max_drawdown    REAL,
    win_rate        REAL,
    profit_factor   REAL,
    total_trades    INTEGER,
    winning_trades  INTEGER,
    avg_trade_pnl   REAL,
    avg_hold_hours  REAL,
    parameters      TEXT,                 -- JSON config snapshot
    created_at      INTEGER DEFAULT (strftime('%s','now'))
);
"""

class Database:
    def __init__(self, db_path: str = "./db/trading_bot.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.executescript(SCHEMA)
        logger.info(f"Database initialized: {self.db_path}")

    # ── BTC Prices ─────────────────────────────────────────────────────────
    def upsert_candle(self, ts: int, o: float, h: float, l: float, c: float, v: float):
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO btc_prices(timestamp,open,high,low,close,volume)
                   VALUES(?,?,?,?,?,?)""",
                (ts, o, h, l, c, v)
            )

    def get_candles(self, limit: int = 200, since: int = 0) -> List[Dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT * FROM btc_prices WHERE timestamp > ?
                   ORDER BY timestamp DESC LIMIT ?""",
                (since, limit)
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    # ── Markets ────────────────────────────────────────────────────────────
    def upsert_market(self, market: Dict):
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO markets
                   (id,question,condition_id,outcomes,outcome_prices,volume,
                    liquidity,start_time,end_time,market_type,updated_at)
                   VALUES(:id,:question,:condition_id,:outcomes,:outcome_prices,
                          :volume,:liquidity,:start_time,:end_time,:market_type,
                          strftime('%s','now'))""",
                market
            )

    def get_active_markets(self) -> List[Dict]:
        now = int(datetime.utcnow().timestamp())
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT * FROM markets WHERE resolved=0 AND end_time > ?
                   ORDER BY end_time ASC""",
                (now,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Predictions ────────────────────────────────────────────────────────
    def save_prediction(self, pred: Dict) -> int:
        with self._get_conn() as conn:
            cursor = conn.execute(
                """INSERT INTO predictions
                   (market_id,timestamp,predicted_prob,market_prob,edge,ev,
                    confidence,strategy,signals,btc_price)
                   VALUES(:market_id,:timestamp,:predicted_prob,:market_prob,
                          :edge,:ev,:confidence,:strategy,:signals,:btc_price)""",
                pred
            )
        return cursor.lastrowid

    # ── Trades ─────────────────────────────────────────────────────────────
    def open_trade(self, trade: Dict) -> int:
        with self._get_conn() as conn:
            cursor = conn.execute(
                """INSERT INTO trades
                   (trade_uuid,market_id,outcome,direction,size_usdc,shares,
                    entry_price,entry_time,status,strategy,prediction_id,
                    polymarket_order_id)
                   VALUES(:trade_uuid,:market_id,:outcome,:direction,:size_usdc,
                          :shares,:entry_price,:entry_time,:status,:strategy,
                          :prediction_id,:polymarket_order_id)""",
                trade
            )
        return cursor.lastrowid

    def close_trade(self, trade_uuid: str, exit_price: float,
                    pnl: float, pnl_pct: float, reason: str):
        exit_time = int(datetime.utcnow().timestamp())
        with self._get_conn() as conn:
            conn.execute(
                """UPDATE trades SET exit_price=?,pnl=?,pnl_pct=?,
                   exit_time=?,status='CLOSED',exit_reason=?
                   WHERE trade_uuid=?""",
                (exit_price, pnl, pnl_pct, exit_time, reason, trade_uuid)
            )

    def get_open_trades(self) -> List[Dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_trade_stats(self) -> Dict:
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
                    SUM(pnl) as total_pnl,
                    AVG(pnl_pct) as avg_pnl_pct,
                    MAX(pnl) as best_trade,
                    MIN(pnl) as worst_trade,
                    SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as gross_profit,
                    SUM(CASE WHEN pnl < 0 THEN ABS(pnl) ELSE 0 END) as gross_loss
                FROM trades WHERE status='CLOSED'
            """).fetchone()
        return dict(row) if row else {}

    # ── Portfolio ──────────────────────────────────────────────────────────
    def save_snapshot(self, snap: Dict):
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO portfolio_snapshots
                   (timestamp,total_balance,available_cash,open_positions_value,
                    daily_pnl,total_pnl,total_pnl_pct,win_rate,sharpe_ratio,
                    max_drawdown,open_trades,btc_price)
                   VALUES(:timestamp,:total_balance,:available_cash,
                          :open_positions_value,:daily_pnl,:total_pnl,
                          :total_pnl_pct,:win_rate,:sharpe_ratio,:max_drawdown,
                          :open_trades,:btc_price)""",
                snap
            )

    def get_snapshots(self, limit: int = 168) -> List[Dict]:  # 1 week hourly
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT * FROM portfolio_snapshots
                   ORDER BY timestamp DESC LIMIT ?""",
                (limit,)
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    # ── Logging ────────────────────────────────────────────────────────────
    def log(self, level: str, component: str, message: str, data: Any = None):
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO bot_logs(level,component,message,data) VALUES(?,?,?,?)",
                (level, component, message, json.dumps(data) if data else None)
            )

    def get_recent_logs(self, limit: int = 100) -> List[Dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT * FROM bot_logs ORDER BY timestamp DESC LIMIT ?""",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
