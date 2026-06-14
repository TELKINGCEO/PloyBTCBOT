"""
Risk Management
- Dynamic Kelly position sizing
- Max drawdown circuit breaker
- Concurrent position limits
- Abnormal market detection
- Daily loss limits
"""
import time
import math
import logging
import statistics
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class PositionSizeResult:
    allowed:        bool
    size_usdc:      float
    reason:         str
    kelly_fraction: float
    risk_pct:       float


@dataclass
class RiskState:
    bankroll:           float
    peak_bankroll:      float
    daily_start_balance:float
    daily_pnl:          float
    open_positions:     int
    open_exposure_usdc: float
    is_halted:          bool
    halt_reason:        str
    last_reset_date:    str


class RiskManager:
    """
    Central risk controller. Call check_trade() before every entry.
    Call update() after every fill / price update.
    """

    def __init__(self, config, db):
        self.cfg = config
        self.db  = db

        # State
        self.bankroll:            float = config.INITIAL_BANKROLL
        self.peak_bankroll:       float = config.INITIAL_BANKROLL
        self.daily_start_balance: float = config.INITIAL_BANKROLL
        self.daily_pnl:           float = 0.0
        self.open_trades:         Dict[str, Dict] = {}   # uuid → trade info
        self.open_exposure:       float = 0.0
        self.is_halted:           bool  = False
        self.halt_reason:         str   = ""
        self.last_day:            str   = datetime.utcnow().strftime("%Y-%m-%d")

        # Circuit breaker history
        self._recent_losses:      List[float] = []
        self._vol_readings:       List[float] = []

    # ─────────────────────────────────────────────────────────────────────
    # Pre-trade check
    # ─────────────────────────────────────────────────────────────────────
    def check_trade(self, signal, btc_vol_pct: float = 0.0) -> PositionSizeResult:
        """
        Returns PositionSizeResult. If allowed=False, do not trade.
        signal: TradeSignal from analysis engine
        """
        # ── Daily date reset ──────────────────────────────────────────────
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if today != self.last_day:
            self.daily_start_balance = self.bankroll
            self.daily_pnl           = 0.0
            self.last_day            = today
            logger.info(f"New day: daily balance reset to ${self.bankroll:.2f}")

        # ── Hard halt check ───────────────────────────────────────────────
        if self.is_halted:
            return PositionSizeResult(False, 0, f"HALTED: {self.halt_reason}", 0, 0)

        # ── Market halt: extreme vol ───────────────────────────────────────
        if btc_vol_pct > self.cfg.VOLATILITY_PAUSE_THRESHOLD:
            return PositionSizeResult(
                False, 0,
                f"BTC volatility too high ({btc_vol_pct*100:.1f}% > "
                f"{self.cfg.VOLATILITY_PAUSE_THRESHOLD*100:.0f}%)", 0, 0
            )

        # ── Daily loss limit ──────────────────────────────────────────────
        daily_loss_pct = self.daily_pnl / self.daily_start_balance if self.daily_start_balance else 0
        if daily_loss_pct < -self.cfg.MAX_DAILY_LOSS_PCT:
            self._halt(f"Daily loss limit hit: {daily_loss_pct*100:.1f}%")
            return PositionSizeResult(False, 0, self.halt_reason, 0, 0)

        # ── Max drawdown ──────────────────────────────────────────────────
        drawdown = (self.peak_bankroll - self.bankroll) / self.peak_bankroll if self.peak_bankroll else 0
        if drawdown > self.cfg.MAX_DRAWDOWN_PCT:
            self._halt(f"Max drawdown hit: {drawdown*100:.1f}%")
            return PositionSizeResult(False, 0, self.halt_reason, 0, 0)

        # ── Max concurrent positions ──────────────────────────────────────
        if len(self.open_trades) >= self.cfg.MAX_CONCURRENT_POSITIONS:
            return PositionSizeResult(
                False, 0,
                f"Max concurrent positions ({self.cfg.MAX_CONCURRENT_POSITIONS})", 0, 0
            )

        # ── Available capital ─────────────────────────────────────────────
        available = self.bankroll - self.open_exposure
        if available < self.cfg.MIN_TRADE_SIZE:
            return PositionSizeResult(False, 0, "Insufficient free capital", 0, 0)

        # ── Position size: fractional Kelly ───────────────────────────────
        kelly_pct = signal.kelly_fraction   # Already fractional Kelly
        max_pct   = self.cfg.MAX_SINGLE_TRADE_PCT

        # Dynamic adjustment: reduce after losses
        loss_adj = self._loss_adjustment()
        kelly_pct = kelly_pct * loss_adj

        risk_pct  = min(kelly_pct, max_pct)
        size_usdc = self.bankroll * risk_pct

        # Floor / ceiling
        size_usdc = max(self.cfg.MIN_TRADE_SIZE, min(size_usdc, available * 0.95))
        size_usdc = min(size_usdc, available * max_pct)
        size_usdc = round(size_usdc, 2)

        if size_usdc < self.cfg.MIN_TRADE_SIZE:
            return PositionSizeResult(False, 0, "Calculated size below minimum", 0, 0)

        actual_pct = size_usdc / self.bankroll if self.bankroll else 0

        return PositionSizeResult(
            allowed        = True,
            size_usdc      = size_usdc,
            reason         = "OK",
            kelly_fraction = signal.kelly_fraction,
            risk_pct       = actual_pct,
        )

    # ─────────────────────────────────────────────────────────────────────
    # State updates
    # ─────────────────────────────────────────────────────────────────────
    def on_trade_open(self, trade_uuid: str, size_usdc: float,
                      market_id: str, strategy: str):
        self.open_trades[trade_uuid] = {
            "market_id": market_id,
            "size_usdc": size_usdc,
            "strategy":  strategy,
            "open_time": time.time(),
        }
        self.open_exposure += size_usdc
        logger.info(f"Position opened: {trade_uuid} ${size_usdc:.2f} | "
                    f"Exposure={self.open_exposure:.2f}/{self.bankroll:.2f}")

    def on_trade_close(self, trade_uuid: str, pnl: float):
        if trade_uuid in self.open_trades:
            trade = self.open_trades.pop(trade_uuid)
            self.open_exposure = max(0, self.open_exposure - trade["size_usdc"])

        self.bankroll  += pnl
        self.daily_pnl += pnl
        self.peak_bankroll = max(self.peak_bankroll, self.bankroll)

        self._recent_losses.append(pnl)
        if len(self._recent_losses) > 20:
            self._recent_losses.pop(0)

        status = "WIN" if pnl >= 0 else "LOSS"
        logger.info(f"Trade closed [{status}]: PnL=${pnl:+.3f} | "
                    f"Bankroll=${self.bankroll:.2f} | DailyPnL=${self.daily_pnl:+.3f}")

    def update_bankroll(self, new_balance: float):
        """Called when we get a fresh balance from the API"""
        self.bankroll      = new_balance
        self.peak_bankroll = max(self.peak_bankroll, new_balance)

    # ─────────────────────────────────────────────────────────────────────
    # Position monitoring - check existing trades
    # ─────────────────────────────────────────────────────────────────────
    def check_exit_conditions(self, trade: Dict, current_price: float,
                               model_prob: float) -> Tuple[bool, str]:
        """
        Check if an open trade should be exited.
        Returns (should_exit, reason)
        """
        entry_price = trade.get("entry_price", 0.5)
        outcome     = trade.get("outcome", "YES")

        if entry_price <= 0:
            return False, ""

        # Current value of our position
        pnl_pct = (current_price - entry_price) / entry_price

        # Profit target
        if pnl_pct >= self.cfg.PROFIT_TARGET_PCT:
            return True, "PROFIT_TARGET"

        # Stop loss
        if pnl_pct <= -self.cfg.STOP_LOSS_PCT:
            return True, "STOP_LOSS"

        # Model probability collapsed
        if outcome == "YES" and model_prob < 0.25:
            return True, "MODEL_EXIT"
        if outcome == "NO" and model_prob > 0.75:
            return True, "MODEL_EXIT"

        # Time-based exit: < 5 minutes left, take profits if positive
        open_time  = trade.get("entry_time", time.time())
        hold_secs  = time.time() - open_time
        if hold_secs > self.cfg.MAX_HOLD_HOURS * 3600:
            return True, "TIME_EXIT"

        return False, ""

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────
    def _halt(self, reason: str):
        self.is_halted   = True
        self.halt_reason = reason
        logger.error(f"🛑 TRADING HALTED: {reason}")

    def resume(self):
        self.is_halted   = False
        self.halt_reason = ""
        logger.info("Trading resumed")

    def _loss_adjustment(self) -> float:
        """Reduce position size after consecutive losses (anti-martingale)"""
        if len(self._recent_losses) < 3:
            return 1.0
        last_5 = self._recent_losses[-5:]
        losses = sum(1 for p in last_5 if p < 0)
        if losses >= 4:
            return 0.5    # 50% reduction after 4 of 5 losses
        if losses >= 3:
            return 0.70
        if losses >= 2:
            return 0.85
        return 1.0

    def get_state(self) -> Dict:
        drawdown = (
            (self.peak_bankroll - self.bankroll) / self.peak_bankroll
            if self.peak_bankroll > 0 else 0
        )
        daily_pnl_pct = (
            self.daily_pnl / self.daily_start_balance
            if self.daily_start_balance > 0 else 0
        )
        return {
            "bankroll":            round(self.bankroll, 4),
            "peak_bankroll":       round(self.peak_bankroll, 4),
            "available_cash":      round(self.bankroll - self.open_exposure, 4),
            "open_exposure":       round(self.open_exposure, 4),
            "open_positions":      len(self.open_trades),
            "daily_pnl":           round(self.daily_pnl, 4),
            "daily_pnl_pct":       round(daily_pnl_pct * 100, 2),
            "drawdown_pct":        round(drawdown * 100, 2),
            "is_halted":           self.is_halted,
            "halt_reason":         self.halt_reason,
            "total_pnl":           round(self.bankroll - self.cfg.INITIAL_BANKROLL, 4),
            "total_pnl_pct":       round(
                (self.bankroll / self.cfg.INITIAL_BANKROLL - 1) * 100, 2
            ),
        }

    def required_daily_return(self, days_elapsed: int) -> float:
        """How much % daily return do we need to hit $1000 from current balance?"""
        days_left = max(1, self.cfg.TARGET_DAYS - days_elapsed)
        if self.bankroll <= 0:
            return 0.0
        required = (self.cfg.TARGET_BANKROLL / self.bankroll) ** (1 / days_left) - 1
        return required * 100

    def progress_summary(self, days_elapsed: int) -> str:
        state    = self.get_state()
        req      = self.required_daily_return(days_elapsed)
        multiple = self.bankroll / self.cfg.INITIAL_BANKROLL
        target_x = self.cfg.TARGET_BANKROLL / self.cfg.INITIAL_BANKROLL
        pct_done = math.log(multiple + 1e-9) / math.log(target_x + 1e-9) * 100

        return (
            f"Day {days_elapsed}/{self.cfg.TARGET_DAYS} | "
            f"${self.bankroll:.2f} ({multiple:.1f}x) | "
            f"{pct_done:.0f}% to goal | "
            f"Need {req:.1f}%/day | "
            f"PnL today: ${state['daily_pnl']:+.2f}"
        )
