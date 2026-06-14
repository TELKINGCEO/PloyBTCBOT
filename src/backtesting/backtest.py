"""
Backtesting Engine
- Replays historical BTC price data against simulated Polymarket markets
- Evaluates all 8 strategies
- Reports: Sharpe, Sortino, max drawdown, win rate, profit factor, expectancy
"""
import math
import json
import uuid
import time
import logging
import statistics
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class BacktestTrade:
    trade_id:       str
    market_question:str
    outcome:        str
    entry_time:     datetime
    exit_time:      Optional[datetime]
    entry_price:    float
    exit_price:     float
    size_usdc:      float
    shares:         float
    pnl:            float
    pnl_pct:        float
    strategy:       str
    edge:           float
    confidence:     float
    exit_reason:    str


@dataclass
class BacktestResult:
    run_id:             str
    start_date:         str
    end_date:           str
    initial_capital:    float
    final_capital:      float
    total_return:       float
    annualized_return:  float
    sharpe_ratio:       float
    sortino_ratio:      float
    calmar_ratio:       float
    max_drawdown:       float
    max_drawdown_days:  int
    win_rate:           float
    profit_factor:      float
    total_trades:       int
    winning_trades:     int
    losing_trades:      int
    avg_win:            float
    avg_loss:           float
    avg_hold_hours:     float
    best_trade:         float
    worst_trade:        float
    expectancy:         float
    trades:             List[BacktestTrade] = field(default_factory=list)
    equity_curve:       List[Tuple[datetime, float]] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            "=" * 60,
            f"BACKTEST RESULTS: {self.start_date} → {self.end_date}",
            "=" * 60,
            f"  Initial Capital  : ${self.initial_capital:.2f}",
            f"  Final Capital    : ${self.final_capital:.2f}",
            f"  Total Return     : {self.total_return*100:+.1f}%",
            f"  Ann. Return      : {self.annualized_return*100:+.1f}%",
            "",
            f"  Sharpe Ratio     : {self.sharpe_ratio:.3f}",
            f"  Sortino Ratio    : {self.sortino_ratio:.3f}",
            f"  Calmar Ratio     : {self.calmar_ratio:.3f}",
            f"  Max Drawdown     : {self.max_drawdown*100:.1f}%",
            "",
            f"  Win Rate         : {self.win_rate*100:.1f}%",
            f"  Profit Factor    : {self.profit_factor:.3f}",
            f"  Expectancy       : ${self.expectancy:.4f}/trade",
            f"  Total Trades     : {self.total_trades}",
            f"  Avg Hold         : {self.avg_hold_hours:.2f}h",
            f"  Best Trade       : ${self.best_trade:+.4f}",
            f"  Worst Trade      : ${self.worst_trade:+.4f}",
            "=" * 60,
        ]
        return "\n".join(lines)


class BacktestMarketGenerator:
    """
    Generate synthetic Polymarket BTC-hourly markets from historical candles.
    For each 1h candle we create ~3 markets (at current ±0.5%, ±1%, ±2%).
    """

    def __init__(self, offset_pcts: List[float] = None):
        self.offsets = offset_pcts or [-0.02, -0.01, -0.005, 0.005, 0.01, 0.02]

    def generate_markets(self, candle: Dict, expiry_ts: int) -> List[Dict]:
        """Generate hypothetical markets for a given 1h candle start."""
        close = candle["close"]
        markets = []
        for offset in self.offsets:
            target = close * (1 + offset)
            direction = "above" if offset > 0 else "below"
            question = (f"Will BTC be {'above' if offset > 0 else 'below'} "
                        f"${target:,.0f} at {datetime.utcfromtimestamp(expiry_ts).strftime('%H:%M UTC')}?")

            # Simulate naive market price using 50% + adjustment
            # (real market would have real implied prob)
            naive_prob_yes = 0.50 + offset * 5   # crude approximation
            naive_prob_yes = max(0.10, min(0.90, naive_prob_yes))
            naive_prob_no  = 1 - naive_prob_yes

            markets.append({
                "id":              f"bt_{candle['timestamp']}_{offset:.3f}",
                "question":        question,
                "condition_id":    "",
                "outcomes":        json.dumps(["YES", "NO"]),
                "outcome_prices":  json.dumps([naive_prob_yes, naive_prob_no]),
                "volume":          1000.0,
                "liquidity":       500.0,
                "start_time":      candle["timestamp"],
                "end_time":        expiry_ts,
                "market_type":     "btc_hourly",
                "time_to_expiry":  expiry_ts - candle["timestamp"],
                "_target":         target,
                "_direction":      direction,
                "_start_price":    close,
                "_open":           candle["open"],
                "_high":           candle["high"],
                "_low":            candle["low"],
                "_close":          candle["close"],
            })
        return markets


class Backtester:
    """Full walk-forward backtest engine"""

    def __init__(self, config, db=None):
        self.cfg       = config
        self.db        = db
        self.generator = BacktestMarketGenerator()

    def run(self, candles: List[Dict], start_idx: int = 60,
            initial_capital: float = 10.0) -> BacktestResult:
        """
        Walk-forward backtest over historical candles.
        candles: sorted ascending 1-minute OHLCV dicts
        start_idx: warm-up period for indicators
        """
        from src.data.btc_feed import BTCDataFeed
        from src.analysis.engine import AnalysisEngine, MarketScanner

        run_id  = str(uuid.uuid4())[:8]
        capital = initial_capital
        peak    = initial_capital
        trades: List[BacktestTrade] = []
        equity: List[Tuple[datetime, float]] = [(
            datetime.utcfromtimestamp(candles[start_idx]["timestamp"]), capital
        )]

        # Build a mock data feed (static, no WebSocket)
        feed = _MockDataFeed(candles[:start_idx])

        # Mock sentiment and funding (neutral for backtest)
        class NullCollector:
            def get_score(self): return 0.0
            def get_bias(self): return 0.0
            fear_greed = 50
            funding_rate = 0.0

        engine  = AnalysisEngine(self.cfg, feed, NullCollector(), NullCollector())
        scanner = MarketScanner(engine, self.cfg)

        open_trades: List[Dict] = []
        max_dd = 0.0
        max_dd_start = capital

        # Step through hourly boundaries
        hour_boundary = None
        hourly_candles = [c for c in candles if (c["timestamp"] % 3600) == 0]

        logger.info(f"Backtest: {len(hourly_candles)} hours, "
                    f"${initial_capital:.2f} starting capital")

        for i, h_candle in enumerate(hourly_candles[1:], 1):
            ts    = h_candle["timestamp"]
            price = h_candle["open"]   # Use open price of hour as entry

            # Update mock feed with candles up to this point
            candles_so_far = [c for c in candles if c["timestamp"] < ts]
            if len(candles_so_far) < 30:
                continue
            feed._update(candles_so_far)

            # ── Resolve expired trades ─────────────────────────────────────
            resolved = []
            for ot in open_trades:
                if ts >= ot["expiry_ts"]:
                    final_price = h_candle["close"]
                    target      = ot["_target"]
                    direction   = ot["_direction"]
                    outcome     = ot["outcome"]

                    # Did the market resolve YES?
                    if direction == "above":
                        resolved_yes = final_price > target
                    else:
                        resolved_yes = final_price < target

                    won = (outcome == "YES" and resolved_yes) or \
                          (outcome == "NO"  and not resolved_yes)

                    exit_price = 1.0 if won else 0.01   # Binary resolution
                    pnl        = (exit_price - ot["entry_price"]) * ot["shares"]
                    pnl_pct    = (exit_price - ot["entry_price"]) / ot["entry_price"]

                    capital += pnl
                    peak     = max(peak, capital)
                    dd       = (peak - capital) / peak if peak > 0 else 0
                    max_dd   = max(max_dd, dd)

                    trades.append(BacktestTrade(
                        trade_id        = ot["trade_id"],
                        market_question = ot["question"],
                        outcome         = outcome,
                        entry_time      = datetime.utcfromtimestamp(ot["entry_ts"]),
                        exit_time       = datetime.utcfromtimestamp(ts),
                        entry_price     = ot["entry_price"],
                        exit_price      = exit_price,
                        size_usdc       = ot["size_usdc"],
                        shares          = ot["shares"],
                        pnl             = pnl,
                        pnl_pct         = pnl_pct,
                        strategy        = ot["strategy"],
                        edge            = ot["edge"],
                        confidence      = ot["confidence"],
                        exit_reason     = "RESOLUTION",
                    ))
                    resolved.append(ot)

            for r in resolved:
                open_trades.remove(r)

            # ── Check early exits (stop loss / profit target) ──────────────
            still_open = []
            for ot in open_trades:
                curr_p = ot["entry_price"]   # simplified: no live repricing in backtest
                pnl_pct = (curr_p - ot["entry_price"]) / ot["entry_price"]
                if pnl_pct >= self.cfg.PROFIT_TARGET_PCT:
                    exit_p  = min(0.99, ot["entry_price"] * (1 + self.cfg.PROFIT_TARGET_PCT))
                    pnl     = (exit_p - ot["entry_price"]) * ot["shares"]
                    capital += pnl
                    peak     = max(peak, capital)
                    trades.append(BacktestTrade(
                        trade_id=ot["trade_id"], market_question=ot["question"],
                        outcome=ot["outcome"], entry_time=datetime.utcfromtimestamp(ot["entry_ts"]),
                        exit_time=datetime.utcfromtimestamp(ts), entry_price=ot["entry_price"],
                        exit_price=exit_p, size_usdc=ot["size_usdc"], shares=ot["shares"],
                        pnl=pnl, pnl_pct=self.cfg.PROFIT_TARGET_PCT,
                        strategy=ot["strategy"], edge=ot["edge"], confidence=ot["confidence"],
                        exit_reason="PROFIT_TARGET",
                    ))
                else:
                    still_open.append(ot)
            open_trades = still_open

            # ── Max drawdown safety ───────────────────────────────────────
            if capital < initial_capital * (1 - self.cfg.MAX_DRAWDOWN_PCT):
                logger.warning(f"[BT] Max drawdown hit at hour {i}. Stopping.")
                break

            # ── Scan for new trades ───────────────────────────────────────
            if len(open_trades) >= self.cfg.MAX_CONCURRENT_POSITIONS:
                equity.append((datetime.utcfromtimestamp(ts), capital))
                continue

            prev_candle = hourly_candles[i - 1]
            markets     = self.generator.generate_markets(
                h_candle, ts + 3600
            )

            sigs = scanner.scan(markets)
            for sig in sigs[:2]:   # Max 2 new trades per hour
                if len(open_trades) >= self.cfg.MAX_CONCURRENT_POSITIONS:
                    break
                if capital < self.cfg.MIN_TRADE_SIZE:
                    break

                # Kelly size
                kelly_pct  = sig.kelly_fraction
                size_usdc  = min(capital * kelly_pct, capital * self.cfg.MAX_SINGLE_TRADE_PCT)
                size_usdc  = max(self.cfg.MIN_TRADE_SIZE, size_usdc)
                size_usdc  = min(size_usdc, capital * 0.95)
                size_usdc  = round(size_usdc, 4)

                if size_usdc <= 0 or size_usdc > capital:
                    continue

                entry_price = sig.entry_price
                shares      = size_usdc / entry_price
                capital    -= size_usdc

                open_trades.append({
                    "trade_id":   str(uuid.uuid4())[:8],
                    "question":   sig.question,
                    "outcome":    sig.outcome,
                    "entry_price": entry_price,
                    "shares":     shares,
                    "size_usdc":  size_usdc,
                    "strategy":   sig.strategy,
                    "edge":       sig.edge,
                    "confidence": sig.confidence,
                    "entry_ts":   ts,
                    "expiry_ts":  ts + int(sig.time_to_expiry),
                    "_target":    sig.btc_target,
                    "_direction": sig.direction,
                })

            equity.append((datetime.utcfromtimestamp(ts), capital))

        # Force-close any remaining open trades at last price
        for ot in open_trades:
            exit_p = 0.5   # uncertain
            pnl    = (exit_p - ot["entry_price"]) * ot["shares"]
            capital += pnl + ot["size_usdc"]
            trades.append(BacktestTrade(
                trade_id=ot["trade_id"], market_question=ot["question"],
                outcome=ot["outcome"], entry_time=datetime.utcfromtimestamp(ot["entry_ts"]),
                exit_time=datetime.utcfromtimestamp(hourly_candles[-1]["timestamp"]),
                entry_price=ot["entry_price"], exit_price=exit_p,
                size_usdc=ot["size_usdc"], shares=ot["shares"],
                pnl=pnl, pnl_pct=(exit_p-ot["entry_price"])/ot["entry_price"],
                strategy=ot["strategy"], edge=ot["edge"], confidence=ot["confidence"],
                exit_reason="FORCE_CLOSE",
            ))

        return self._compute_metrics(run_id, candles, initial_capital,
                                     capital, trades, equity, max_dd)

    def _compute_metrics(self, run_id, candles, initial, final,
                         trades, equity, max_dd) -> BacktestResult:
        n = len(trades)
        winners = [t for t in trades if t.pnl > 0]
        losers  = [t for t in trades if t.pnl <= 0]

        win_rate      = len(winners) / n if n else 0
        gross_profit  = sum(t.pnl for t in winners)
        gross_loss    = abs(sum(t.pnl for t in losers))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 999.0
        expectancy    = sum(t.pnl for t in trades) / n if n else 0

        # Returns series for Sharpe / Sortino
        pnls = [t.pnl for t in trades]
        if len(pnls) > 1:
            avg_r  = statistics.mean(pnls)
            std_r  = statistics.stdev(pnls)
            sharpe = (avg_r / std_r * math.sqrt(252)) if std_r > 0 else 0
            neg_r  = [p for p in pnls if p < 0]
            down_std = statistics.stdev(neg_r) if len(neg_r) > 1 else std_r
            sortino = (avg_r / down_std * math.sqrt(252)) if down_std > 0 else 0
        else:
            sharpe = sortino = 0.0

        total_return = (final / initial - 1) if initial > 0 else 0
        hours = len(candles) / 60 if candles else 720
        years = hours / 8760
        ann_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        calmar = ann_return / max_dd if max_dd > 0 else 0

        hold_hours = []
        for t in trades:
            if t.exit_time and t.entry_time:
                h = (t.exit_time - t.entry_time).total_seconds() / 3600
                hold_hours.append(h)

        start_dt = datetime.utcfromtimestamp(candles[60]["timestamp"]).strftime("%Y-%m-%d")
        end_dt   = datetime.utcfromtimestamp(candles[-1]["timestamp"]).strftime("%Y-%m-%d")

        return BacktestResult(
            run_id             = run_id,
            start_date         = start_dt,
            end_date           = end_dt,
            initial_capital    = initial,
            final_capital      = round(final, 4),
            total_return       = total_return,
            annualized_return  = ann_return,
            sharpe_ratio       = round(sharpe, 3),
            sortino_ratio      = round(sortino, 3),
            calmar_ratio       = round(calmar, 3),
            max_drawdown       = max_dd,
            max_drawdown_days  = 0,
            win_rate           = win_rate,
            profit_factor      = round(profit_factor, 3),
            total_trades       = n,
            winning_trades     = len(winners),
            losing_trades      = len(losers),
            avg_win            = statistics.mean([t.pnl for t in winners]) if winners else 0,
            avg_loss           = statistics.mean([t.pnl for t in losers]) if losers else 0,
            avg_hold_hours     = statistics.mean(hold_hours) if hold_hours else 0,
            best_trade         = max((t.pnl for t in trades), default=0),
            worst_trade        = min((t.pnl for t in trades), default=0),
            expectancy         = expectancy,
            trades             = trades,
            equity_curve       = equity,
        )


class _MockDataFeed:
    """Minimal feed mock for backtesting (no WebSocket needed)"""
    def __init__(self, candles: List[Dict]):
        self._candles = list(candles)
        self._indicators: Dict = {}
        if candles:
            self._rebuild()

    def _update(self, candles: List[Dict]):
        self._candles = list(candles)
        self._rebuild()

    def _rebuild(self):
        if not self._candles:
            return
        closes = [c["close"] for c in self._candles]
        self._indicators = {
            "price":      closes[-1],
            "rsi_14":     self._rsi(closes, 14),
            "roc_5":      (closes[-1]/closes[-6]-1)*100 if len(closes)>=6 else 0,
            "roc_10":     (closes[-1]/closes[-11]-1)*100 if len(closes)>=11 else 0,
            "ema_9":      self._ema(closes, 9),
            "ema_21":     self._ema(closes, 21),
            "ema_50":     self._ema(closes, 50),
            "bb_pct":     0.5,
            "macd_hist":  0.0,
            "atr_pct":    0.008,
            "hourly_vol": 0.010,
            "stoch_k":    50.0,
            "delta_ratio":0.0,
            "vol_ratio":  1.0,
            "vol_spike":  False,
            "obv_trend":  "neutral",
            "uptrend":    False,
            "downtrend":  False,
            "higher_highs": False,
            "lower_lows":   False,
            "bb_squeeze":   False,
        }
        if len(closes) >= 20:
            import statistics as st
            window = closes[-20:]
            mid    = st.mean(window)
            std    = st.stdev(window) if len(window)>1 else 0
            upper  = mid + 2*std
            lower  = mid - 2*std
            self._indicators["bb_pct"] = (
                (closes[-1]-lower)/(upper-lower) if upper!=lower else 0.5
            )

    def get_indicators(self) -> Dict: return dict(self._indicators)
    def get_price(self) -> float:     return self._indicators.get("price", 0)

    @staticmethod
    def _ema(series, p):
        if len(series)<p: return series[-1] if series else 0
        k=2/(p+1); e=sum(series[:p])/p
        for v in series[p:]: e=v*k+e*(1-k)
        return e

    @staticmethod
    def _rsi(closes, p=14):
        if len(closes)<p+1: return 50.0
        gs,ls=[],[]
        for i in range(1,len(closes)):
            d=closes[i]-closes[i-1]; gs.append(max(d,0)); ls.append(max(-d,0))
        ag=sum(gs[-p:])/p; al=sum(ls[-p:])/p
        return 100-(100/(1+ag/al)) if al else 100.0
