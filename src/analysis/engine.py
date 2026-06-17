"""
Analysis Engine - Multi-Strategy Probability Estimator
Combines 8 strategies into ensemble probability prediction.
Compares vs market-implied probability to find edge.
"""
import math
import time
import json
import logging
import statistics
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    strategy:    str
    direction:   str        # "YES" or "NO"
    probability: float      # 0-1 model probability for YES outcome
    confidence:  float      # 0-1
    ev:          float      # raw EV per dollar
    rationale:   str
    weight:      float = 1.0


@dataclass
class TradeSignal:
    market_id:        str
    question:         str
    outcome:          str           # "YES" or "NO"
    entry_price:      float         # market price for chosen outcome
    predicted_prob:   float         # our model's probability
    market_prob:      float         # market-implied
    edge:             float         # predicted_prob - market_prob
    ev:               float         # expected value per dollar risked
    confidence:       float
    strategy:         str           # primary strategy label
    signals:          List[Signal]
    btc_price:        float
    btc_target:       float
    direction:        str           # "above" or "below"
    time_to_expiry:   float         # seconds
    rationale:        str
    kelly_fraction:   float = 0.0


class AnalysisEngine:
    """
    For each market:
      1. Parse the BTC price target and direction
      2. Run all 8 strategy models
      3. Ensemble-weight the probabilities
      4. Compare to market price → compute edge + EV
      5. Return TradeSignal if edge exceeds threshold
    """

    def __init__(self, config, data_feed, funding_collector, sentiment_collector):
        self.cfg         = config
        self.feed        = data_feed
        self.funding     = funding_collector
        self.sentiment   = sentiment_collector

    # ─────────────────────────────────────────────────────────────────────
    # Main entry point
    # ─────────────────────────────────────────────────────────────────────
    def analyze_market(self, market: Dict) -> Optional[TradeSignal]:
        """Analyze a single Polymarket and return a TradeSignal if edge exists."""
        from src.data.polymarket_client import PolymarketClient

        question   = market.get("question", "")
        btc_target = PolymarketClient.parse_btc_target(question)
        direction  = PolymarketClient.market_direction(question)

        if not btc_target:
            return None

        ind            = self.feed.get_indicators()
        btc_price      = self.feed.get_price() or ind.get("price", 0)
        if btc_price == 0:
            return None

        time_to_expiry = market.get("time_to_expiry", 3600)
        hours_left     = max(time_to_expiry / 3600, 0.01)

        # ── Run all strategy models ───────────────────────────────────────
        signals: List[Signal] = []
        signals.append(self._momentum_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._mean_reversion_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._volatility_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._trend_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._order_flow_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._sentiment_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._funding_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._statistical_model(ind, btc_price, btc_target, direction, hours_left))

        # ── Ensemble ──────────────────────────────────────────────────────
        total_weight = sum(s.weight * s.confidence for s in signals)
        if total_weight == 0:
            return None

        ensemble_yes_prob = sum(
            s.probability * s.weight * s.confidence for s in signals
        ) / total_weight

        ensemble_yes_prob = max(0.02, min(0.98, ensemble_yes_prob))

        # ── Compare to market price ───────────────────────────────────────
        try:
            prices = json.loads(market.get("outcome_prices", "[0.5, 0.5]"))
            yes_price = float(prices[0]) if prices else 0.5
            no_price  = float(prices[1]) if len(prices) > 1 else (1 - yes_price)
        except Exception:
            yes_price, no_price = 0.5, 0.5

        # Clamp to valid range
        yes_price = max(0.01, min(0.99, yes_price))
        no_price  = max(0.01, min(0.99, no_price))

        # Decide: trade YES or NO?
        model_no_prob = 1 - ensemble_yes_prob

        yes_edge = ensemble_yes_prob - yes_price
        no_edge  = model_no_prob - no_price

        if abs(yes_edge) >= abs(no_edge):
            outcome       = "YES"
            entry_price   = yes_price
            model_prob    = ensemble_yes_prob
            market_prob   = yes_price
            edge          = yes_edge
        else:
            outcome       = "NO"
            entry_price   = no_price
            model_prob    = model_no_prob
            market_prob   = no_price
            edge          = no_edge

        # ── EV calculation ────────────────────────────────────────────────
        # EV = (model_prob * profit_if_win) - ((1-model_prob) * cost_if_loss)
        # For binary outcome: win 1-entry_price per share, lose entry_price per share
        win_payout  = (1.0 - entry_price) / entry_price   # return on invested
        ev          = model_prob * win_payout - (1 - model_prob)

        # ── Confidence (agreement among models) ───────────────────────────
        probs         = [s.probability if outcome == "YES" else (1 - s.probability)
                         for s in signals]
        avg_prob      = statistics.mean(probs)
        std_prob      = statistics.stdev(probs) if len(probs) > 1 else 0.5
        agreement     = max(0.0, 1.0 - std_prob * 3)   # low spread = high agreement
        avg_conf      = statistics.mean(s.confidence for s in signals)
        confidence    = (agreement * 0.5 + avg_conf * 0.5)

        # ── Kelly fraction ────────────────────────────────────────────────
        # f* = (p * b - q) / b  where b = win_payout, p = model_prob, q = 1-p
        if win_payout > 0:
            kelly_full = (model_prob * win_payout - (1 - model_prob)) / win_payout
        else:
            kelly_full = 0.0
        kelly_fraction = max(0.0, kelly_full * self.cfg.KELLY_FRACTION)
        kelly_fraction = min(kelly_fraction, self.cfg.MAX_KELLY_BET)

        # ── Primary strategy label ────────────────────────────────────────
        primary = max(signals, key=lambda s: s.confidence * abs(
            s.probability - 0.5 if outcome == "YES" else (1 - s.probability) - 0.5
        ))

        rationale = self._build_rationale(
            signals, outcome, edge, ev, confidence, btc_price, btc_target,
            direction, hours_left, yes_price, no_price
        )

        return TradeSignal(
            market_id       = market["id"],
            question        = question,
            outcome         = outcome,
            entry_price     = entry_price,
            predicted_prob  = model_prob,
            market_prob     = market_prob,
            edge            = edge,
            ev              = ev,
            confidence      = confidence,
            strategy        = primary.strategy,
            signals         = signals,
            btc_price       = btc_price,
            btc_target      = btc_target,
            direction       = direction,
            time_to_expiry  = time_to_expiry,
            rationale       = rationale,
            kelly_fraction  = kelly_fraction,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Strategy Models
    # ─────────────────────────────────────────────────────────────────────

    def _momentum_model(self, ind: Dict, price: float, target: float,
                        direction: str, hours: float) -> Signal:
        """Momentum: follow short-term price direction"""
        roc5  = ind.get("roc_5", 0)
        roc10 = ind.get("roc_10", 0)
        rsi   = ind.get("rsi_14", 50)
        macd_hist = ind.get("macd_hist", 0)

        # Composite momentum score -1..+1
        mom_score = (
            math.tanh(roc5 / 0.5) * 0.4 +
            math.tanh(roc10 / 1.0) * 0.3 +
            (rsi - 50) / 50 * 0.2 +
            math.tanh(macd_hist / 50) * 0.1
        )

        # Convert to probability of BTC moving toward target
        pct_to_target = (target - price) / price * 100
        if direction == "above":
            needed_move = pct_to_target   # positive = need to go up
        else:
            needed_move = -pct_to_target  # direction flip

        # Momentum helps if it aligns with needed move
        base_prob = self._distance_prob(price, target, direction, hours)
        momentum_adjustment = mom_score * 0.08 * (1 / max(hours, 0.5))
        if needed_move > 0:   # need upward move
            yes_prob = base_prob + momentum_adjustment
        else:
            yes_prob = base_prob - momentum_adjustment

        yes_prob   = max(0.05, min(0.95, yes_prob))
        confidence = min(0.9, abs(mom_score) * 1.2 + 0.3)

        return Signal(
            strategy    = "MOMENTUM",
            direction   = "YES" if mom_score > 0 else "NO",
            probability = yes_prob,
            confidence  = confidence,
            ev          = yes_prob - 0.5,
            rationale   = f"ROC5={roc5:.2f}% RSI={rsi:.1f} MACD_hist={macd_hist:.1f}",
            weight      = 0.25,
        )

    def _mean_reversion_model(self, ind: Dict, price: float, target: float,
                               direction: str, hours: float) -> Signal:
        """Mean reversion: extreme moves tend to reverse"""
        rsi      = ind.get("rsi_14", 50)
        bb_pct   = ind.get("bb_pct", 0.5)
        stoch_k  = ind.get("stoch_k", 50)

        # Overbought → more likely to fall; Oversold → more likely to rise
        rsi_score    = (50 - rsi) / 50          # +1 if oversold (RSI=0), -1 if overbought
        bb_score     = (0.5 - bb_pct)           # +1 if at lower band, -1 at upper
        stoch_score  = (50 - stoch_k) / 50

        reversion_bias = rsi_score * 0.4 + bb_score * 0.35 + stoch_score * 0.25

        base_prob   = self._distance_prob(price, target, direction, hours)
        pct_away    = abs(target - price) / price

        # Mean reversion is more relevant when target is close
        reversion_factor = reversion_bias * 0.06 * max(0, 1 - pct_away * 5)
        if direction == "above":
            yes_prob = base_prob + reversion_factor
        else:
            yes_prob = base_prob - reversion_factor

        yes_prob   = max(0.05, min(0.95, yes_prob))

        extreme_condition = (rsi < 25 or rsi > 75 or bb_pct < 0.1 or bb_pct > 0.9)
        confidence = 0.65 if extreme_condition else 0.40

        return Signal(
            strategy    = "MEAN_REVERSION",
            direction   = "YES" if reversion_bias > 0 else "NO",
            probability = yes_prob,
            confidence  = confidence,
            ev          = yes_prob - 0.5,
            rationale   = f"RSI={rsi:.1f} BB%={bb_pct:.2f} Stoch={stoch_k:.1f}",
            weight      = 0.20,
        )

    def _volatility_model(self, ind: Dict, price: float, target: float,
                           direction: str, hours: float) -> Signal:
        """Volatility: use realized vol to price probability via normal dist"""
        atr_pct     = ind.get("atr_pct", 0.005)
        hourly_vol  = ind.get("hourly_vol", atr_pct * math.sqrt(60))
        bb_squeeze  = ind.get("bb_squeeze", False)

        # Scale vol to hours remaining
        vol_for_period = hourly_vol * math.sqrt(max(hours, 0.05))

        # Z-score of target from current price
        log_return_needed = math.log(target / price) if price > 0 else 0
        z_score = log_return_needed / vol_for_period if vol_for_period > 0 else 0

        # Normal CDF probability
        if direction == "above":
            yes_prob = 1 - self._norm_cdf(z_score)
        else:
            yes_prob = self._norm_cdf(z_score)

        # Squeeze: expect breakout soon → higher vol → wider confidence intervals
        if bb_squeeze:
            yes_prob = yes_prob * 0.9 + 0.5 * 0.1   # pull toward 50%

        yes_prob   = max(0.05, min(0.95, yes_prob))
        confidence = 0.80   # Statistical model → higher inherent confidence

        return Signal(
            strategy    = "VOLATILITY",
            direction   = "YES" if yes_prob > 0.5 else "NO",
            probability = yes_prob,
            confidence  = confidence,
            ev          = yes_prob - 0.5,
            rationale   = (f"ATR={atr_pct*100:.2f}% vol_period={vol_for_period*100:.2f}% "
                           f"z={z_score:.2f} squeeze={bb_squeeze}"),
            weight      = 0.20,
        )

    def _trend_model(self, ind: Dict, price: float, target: float,
                     direction: str, hours: float) -> Signal:
        """Trend following: EMA alignment + market structure"""
        uptrend   = ind.get("uptrend", False)
        downtrend = ind.get("downtrend", False)
        hh        = ind.get("higher_highs", False)
        ll        = ind.get("lower_lows", False)
        obv       = ind.get("obv_trend", "neutral")
        ema9      = ind.get("ema_9", price)
        ema21     = ind.get("ema_21", price)

        trend_score = 0.0
        if uptrend:     trend_score += 0.5
        if hh:          trend_score += 0.2
        if obv == "bullish": trend_score += 0.15
        if ema9 > ema21: trend_score += 0.15

        if downtrend:   trend_score -= 0.5
        if ll:          trend_score -= 0.2
        if obv == "bearish": trend_score -= 0.15
        if ema9 < ema21: trend_score -= 0.15

        trend_score = max(-1.0, min(1.0, trend_score))

        base_prob = self._distance_prob(price, target, direction, hours)
        trend_adj = trend_score * 0.07

        if direction == "above":
            yes_prob = base_prob + trend_adj
        else:
            yes_prob = base_prob - trend_adj

        yes_prob   = max(0.05, min(0.95, yes_prob))
        confidence = 0.45 + abs(trend_score) * 0.35

        return Signal(
            strategy    = "TREND_FOLLOWING",
            direction   = "YES" if trend_score > 0 else "NO",
            probability = yes_prob,
            confidence  = confidence,
            ev          = yes_prob - 0.5,
            rationale   = (f"uptrend={uptrend} downtrend={downtrend} "
                           f"HH={hh} LL={ll} OBV={obv}"),
            weight      = 0.15,
        )

    def _order_flow_model(self, ind: Dict, price: float, target: float,
                          direction: str, hours: float) -> Signal:
        """Order flow: buy/sell delta and volume spikes"""
        delta_ratio = ind.get("delta_ratio", 0.0)   # -1..+1
        vol_ratio   = ind.get("vol_ratio", 1.0)
        vol_spike   = ind.get("vol_spike", False)

        # Strong buy delta → bullish
        flow_score = math.tanh(delta_ratio * 2.0)

        # Volume spike amplifies signal
        if vol_spike:
            flow_score *= 1.3
        flow_score = max(-1.0, min(1.0, flow_score))

        base_prob = self._distance_prob(price, target, direction, hours)
        flow_adj  = flow_score * 0.06

        if direction == "above":
            yes_prob = base_prob + flow_adj
        else:
            yes_prob = base_prob - flow_adj

        yes_prob   = max(0.05, min(0.95, yes_prob))
        confidence = 0.40 + abs(flow_score) * 0.30 + (0.15 if vol_spike else 0)

        return Signal(
            strategy    = "ORDER_FLOW",
            direction   = "YES" if flow_score > 0 else "NO",
            probability = yes_prob,
            confidence  = confidence,
            ev          = yes_prob - 0.5,
            rationale   = (f"delta={delta_ratio:.3f} vol_ratio={vol_ratio:.2f} "
                           f"spike={vol_spike}"),
            weight      = 0.10,
        )

    def _sentiment_model(self, ind: Dict, price: float, target: float,
                         direction: str, hours: float) -> Signal:
        """News sentiment + Fear & Greed"""
        sent_score = self.sentiment.get_score() if self.sentiment else 0.0
        fear_greed = getattr(self.sentiment, "fear_greed", 50)

        base_prob = self._distance_prob(price, target, direction, hours)
        sent_adj  = sent_score * 0.05

        if direction == "above":
            yes_prob = base_prob + sent_adj
        else:
            yes_prob = base_prob - sent_adj

        yes_prob   = max(0.05, min(0.95, yes_prob))
        confidence = 0.35 + abs(sent_score) * 0.25

        return Signal(
            strategy    = "SENTIMENT",
            direction   = "YES" if sent_score > 0 else "NO",
            probability = yes_prob,
            confidence  = confidence,
            ev          = yes_prob - 0.5,
            rationale   = f"sentiment={sent_score:.3f} F&G={fear_greed:.0f}",
            weight      = 0.05,
        )

    def _funding_model(self, ind: Dict, price: float, target: float,
                       direction: str, hours: float) -> Signal:
        """Funding rates + OI as contrarian signal"""
        funding_bias = self.funding.get_bias() if self.funding else 0.0
        funding_rate = getattr(self.funding, "funding_rate", 0.0)

        base_prob = self._distance_prob(price, target, direction, hours)
        # Contrarian: very positive funding → crowded long → potential reversal
        funding_adj = -funding_bias * 0.04

        if direction == "above":
            yes_prob = base_prob + funding_adj
        else:
            yes_prob = base_prob - funding_adj

        yes_prob   = max(0.05, min(0.95, yes_prob))
        confidence = 0.35 + abs(funding_bias) * 0.30

        return Signal(
            strategy    = "FUNDING_RATE",
            direction   = "YES" if funding_adj > 0 else "NO",
            probability = yes_prob,
            confidence  = confidence,
            ev          = yes_prob - 0.5,
            rationale   = f"funding={funding_rate*100:.4f}% bias={funding_bias:.3f}",
            weight      = 0.03,
        )

    def _statistical_model(self, ind: Dict, price: float, target: float,
                            direction: str, hours: float) -> Signal:
        """Pure statistical: normal distribution over hourly returns"""
        yes_prob = self._distance_prob(price, target, direction, hours)

        return Signal(
            strategy    = "STATISTICAL",
            direction   = "YES" if yes_prob > 0.5 else "NO",
            probability = yes_prob,
            confidence  = 0.75,
            ev          = yes_prob - 0.5,
            rationale   = (f"price={price:.0f} target={target:.0f} "
                           f"hours={hours:.2f}"),
            weight      = 0.02,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────

    def _distance_prob(self, price: float, target: float,
                       direction: str, hours: float) -> float:
        """
        Base probability from lognormal model.
        BTC hourly vol ≈ 0.8-1.5% (annualized ~50-80%)
        """
        hourly_vol = 0.010   # 1% per hour baseline
        ind = self.feed.get_indicators()
        measured_vol = ind.get("hourly_vol", None)
        if measured_vol:
            hourly_vol = max(0.003, min(0.08, measured_vol))

        vol_period = hourly_vol * math.sqrt(max(hours, 0.05))
        log_ret    = math.log(target / price) if price > 0 and target > 0 else 0

        if direction == "above":
            z = log_ret / vol_period if vol_period > 0 else 0
            prob = 1 - self._norm_cdf(z)
        else:
            z = log_ret / vol_period if vol_period > 0 else 0
            prob = self._norm_cdf(z)

        return max(0.03, min(0.97, prob))

    @staticmethod
    def _norm_cdf(x: float) -> float:
        """Approximation of standard normal CDF"""
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    def _build_rationale(self, signals: List[Signal], outcome: str,
                         edge: float, ev: float, confidence: float,
                         price: float, target: float, direction: str,
                         hours: float, yes_price: float, no_price: float) -> str:
        pct = (target - price) / price * 100
        lines = [
            f"BTC @ ${price:,.2f} | Target ${target:,.2f} ({pct:+.2f}%) "
            f"| {direction.upper()} | {hours:.1f}h left",
            f"Trade {outcome} @ {yes_price if outcome=='YES' else no_price:.3f} | "
            f"Edge={edge*100:+.1f}% | EV={ev*100:+.1f}¢/$ | Confidence={confidence*100:.0f}%",
            "Strategy signals:",
        ]
        for s in sorted(signals, key=lambda x: x.confidence, reverse=True):
            marker = "✓" if (s.direction == outcome) else "✗"
            lines.append(f"  {marker} [{s.strategy:16s}] prob={s.probability:.3f} "
                         f"conf={s.confidence:.2f} | {s.rationale}")
        return "\n".join(lines)


class MarketScanner:
    """Scans all active markets and returns ranked trade opportunities"""

    def __init__(self, analysis_engine: AnalysisEngine, config):
        self.engine = analysis_engine
        self.cfg    = config

    def scan(self, markets: List[Dict]) -> List[TradeSignal]:
        """Return list of actionable signals, sorted by EV × confidence"""
        candidates = []
        for market in markets:
            try:
                sig = self.engine.analyze_market(market)
                if sig is None:
                    continue

                # Apply filters
                if sig.edge    < self.cfg.MIN_EDGE_PCT:    continue
                if sig.ev      < self.cfg.MIN_EV:          continue
                if sig.confidence < self.cfg.MIN_CONFIDENCE: continue
                if sig.time_to_expiry < 300:               continue  # <5 min
                if sig.kelly_fraction <= 0:                 continue

                candidates.append(sig)
            except Exception as e:
                logger.debug(f"Analysis error for {market.get('id')}: {e}")

        # Rank by composite score
        candidates.sort(
            key=lambda s: s.ev * s.confidence * min(s.edge / 0.05, 2.0),
            reverse=True
        )
        return candidates
