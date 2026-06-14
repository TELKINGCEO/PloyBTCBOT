"""
Analysis Engine
- Ensemble probability model for BTC hourly markets
- Multi-strategy signal generation
- Edge and EV calculation
"""
import math
import time
import json
import logging
import statistics
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ProbabilityModel:
    """
    Ensemble model combining momentum, mean-reversion,
    volatility, sentiment, and order-flow signals to estimate
    P(BTC closes above/below target in next hour).
    """

    def __init__(self, model_cfg):
        self.cfg = model_cfg

    def predict(
        self,
        btc_price: float,
        target_price: float,
        direction: str,          # "above" | "below"
        time_to_expiry_sec: float,
        indicators: Dict,
        funding_bias: float,     # -1..1
        sentiment_score: float,  # -1..1
    ) -> Tuple[float, float, str]:
        """
        Returns (probability, confidence, dominant_strategy).
        probability = P(market resolves YES)
        """
        if btc_price <= 0 or target_price <= 0:
            return 0.5, 0.0, "insufficient_data"

        hours_left = time_to_expiry_sec / 3600.0
        pct_gap    = (target_price - btc_price) / btc_price  # + means target above current

        # ── 1. Base probability from log-normal diffusion ─────────────────
        vol = indicators.get("hourly_vol", 0.02)  # hourly vol
        if vol < 0.005:
            vol = 0.015  # floor
        base_prob = self._lognormal_prob(btc_price, target_price,
                                         vol, hours_left, direction)
        signals = {}

        # ── 2. Momentum signal ────────────────────────────────────────────
        roc_5  = indicators.get("roc_5",  0.0)
        roc_10 = indicators.get("roc_10", 0.0)
        rsi    = indicators.get("rsi_14", 50.0)
        macd_h = indicators.get("macd_hist", 0.0)

        momentum_score = (
            math.tanh(roc_5  / 2.0) * 0.40 +
            math.tanh(roc_10 / 2.0) * 0.30 +
            (rsi - 50) / 50.0       * 0.20 +
            math.tanh(macd_h / 100) * 0.10
        )
        signals["momentum"] = momentum_score

        # ── 3. Mean-reversion signal ──────────────────────────────────────
        bb_pct  = indicators.get("bb_pct", 0.5)   # 0=at lower band, 1=at upper
        stoch_k = indicators.get("stoch_k", 50.0)
        # Overbought → expect reversion downward; oversold → upward
        mr_score = -(bb_pct - 0.5) * 2.0 * 0.6 - (stoch_k - 50) / 50.0 * 0.4
        signals["mean_reversion"] = mr_score

        # ── 4. Volatility regime ──────────────────────────────────────────
        atr_pct    = indicators.get("atr_pct", 0.01)
        bb_squeeze = indicators.get("bb_squeeze", False)
        # High vol + gap near = stay directional; squeeze = breakout imminent
        vol_adjustment = 0.0
        if bb_squeeze:
            # Before breakout, stay neutral – boost confidence later
            vol_adjustment = 0.0
        elif atr_pct > 0.015:
            # High vol: momentum signals are more reliable
            vol_adjustment = momentum_score * 0.1
        signals["volatility"] = vol_adjustment

        # ── 5. Order-flow / delta ─────────────────────────────────────────
        delta_ratio = indicators.get("delta_ratio", 0.0)
        obv_trend   = indicators.get("obv_trend", "neutral")
        obv_score   = {"bullish": 0.5, "bearish": -0.5, "neutral": 0.0}[obv_trend]
        flow_score  = delta_ratio * 0.6 + obv_score * 0.4
        signals["order_flow"] = flow_score

        # ── 6. Trend alignment ────────────────────────────────────────────
        uptrend   = indicators.get("uptrend",   False)
        downtrend = indicators.get("downtrend", False)
        hh        = indicators.get("higher_highs", False)
        ll        = indicators.get("lower_lows",   False)
        trend_score = 0.0
        if uptrend and hh:     trend_score =  0.8
        elif downtrend and ll: trend_score = -0.8
        elif uptrend:          trend_score =  0.4
        elif downtrend:        trend_score = -0.4
        signals["trend"] = trend_score

        # ── 7. Sentiment & funding ────────────────────────────────────────
        signals["sentiment"] = sentiment_score * 0.5 + funding_bias * 0.5

        # ── Ensemble: weighted sum → directional adjustment ───────────────
        weights = {
            "momentum":      self.cfg.MOMENTUM_WEIGHT,
            "mean_reversion": self.cfg.MEAN_REVERSION_WEIGHT,
            "volatility":    self.cfg.VOLATILITY_WEIGHT,
            "order_flow":    self.cfg.ORDERFLOW_WEIGHT,
            "trend":         self.cfg.MOMENTUM_WEIGHT * 0.5,
            "sentiment":     self.cfg.SENTIMENT_WEIGHT,
        }
        total_w   = sum(weights.values())
        composite = sum(signals[k] * weights[k] for k in signals) / total_w
        # composite: -1 (strongly bearish) … +1 (strongly bullish)

        # ── Adjust base probability ───────────────────────────────────────
        # Shift prob toward direction implied by composite score
        # Dampened: max ±15% adjustment so model stays calibrated
        direction_sign = 1.0 if direction == "above" else -1.0
        adjustment = math.tanh(composite * direction_sign) * 0.15

        # Time decay: signals less predictive with very little time left
        time_decay = min(1.0, hours_left / 0.5)
        adjustment *= time_decay

        raw_prob = base_prob + adjustment
        final_prob = max(0.02, min(0.98, raw_prob))

        # ── Confidence ────────────────────────────────────────────────────
        # Higher confidence when:
        #   - Signals agree (low stdev)
        #   - Sufficient time (not last 5 minutes)
        #   - Reasonable volatility (not wild)
        signal_vals = list(signals.values())
        signal_std  = statistics.stdev(signal_vals) if len(signal_vals) > 1 else 1.0
        agreement   = max(0.0, 1.0 - signal_std)
        time_conf   = min(1.0, max(0.0, (hours_left - 0.08) / 0.5))
        vol_conf    = 1.0 - min(1.0, atr_pct / 0.03)
        confidence  = (agreement * 0.5 + time_conf * 0.3 + vol_conf * 0.2)
        confidence  = max(0.0, min(1.0, confidence))

        # Dominant strategy
        dominant = max(signals, key=lambda k: abs(signals[k]))

        return final_prob, confidence, dominant

    @staticmethod
    def _lognormal_prob(current: float, target: float, hourly_vol: float,
                        hours: float, direction: str) -> float:
        """
        P(BTC_T > target | BTC_0 = current) under GBM.
        Uses standard Black-Scholes d2 formula.
        """
        if hours <= 0:
            return 1.0 if (direction == "above" and current > target) else (
                   1.0 if (direction == "below" and current < target) else 0.0)

        sigma_t = hourly_vol * math.sqrt(hours)
        # risk-neutral drift ~0 for short horizons
        log_ratio = math.log(current / target)
        d = (log_ratio + 0.5 * sigma_t ** 2) / sigma_t if sigma_t > 0 else 0

        prob_above = _norm_cdf(d)
        return prob_above if direction == "above" else 1.0 - prob_above


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via Abramowitz & Stegun approximation"""
    t = 1.0 / (1.0 + 0.2316419 * abs(x))
    d = 0.3989422820 * math.exp(-0.5 * x * x)
    poly = t * (0.3193815 + t * (-0.3565638 + t * (1.7814779
            + t * (-1.8212560 + t * 1.3302744))))
    prob = 1.0 - d * poly
    return prob if x >= 0 else 1.0 - prob


class EVCalculator:
    """
    Calculates expected value and Kelly position size.
    EV = p_win * (1/price - 1) - p_loss
       = p_win * (1 - price) / price - (1 - p_win)
    
    For prediction markets: buy YES at price P
      If correct: return = (1 - P) / P  (profit per dollar)
      If wrong:   lose   = -1 (lose stake)
    """

    def calculate_ev(self, model_prob: float, market_price: float,
                     direction: str = "YES") -> Dict:
        """
        model_prob  = our estimated P(YES)
        market_price = current YES price on Polymarket (0-1)
        
        Returns dict with edge, ev, kelly_fraction, recommendation
        """
        if market_price <= 0.01 or market_price >= 0.99:
            return {"ev": 0.0, "edge": 0.0, "kelly": 0.0, "trade": False}

        # Buy YES
        yes_ev = model_prob * (1 - market_price) / market_price - (1 - model_prob)

        # Buy NO (equivalent to shorting YES)
        no_price    = 1.0 - market_price
        no_prob     = 1.0 - model_prob
        no_ev       = no_prob * (1 - no_price) / no_price - model_prob

        # Edge = model prob - implied prob
        yes_edge = model_prob - market_price
        no_edge  = no_prob   - no_price

        # Kelly criterion: f* = (p * b - q) / b
        # where b = (1/price - 1) = payout odds
        def kelly(p, price):
            if price <= 0 or price >= 1:
                return 0.0
            b = (1 - price) / price
            q = 1 - p
            f = (p * b - q) / b
            return max(0.0, f)

        yes_kelly = kelly(model_prob, market_price)
        no_kelly  = kelly(no_prob,    no_price)

        # Decide best trade
        if yes_ev > no_ev and yes_ev > 0:
            return {
                "ev":       yes_ev,
                "edge":     yes_edge,
                "kelly":    yes_kelly,
                "trade":    True,
                "side":     "YES",
                "price":    market_price,
                "ev_no":    no_ev,
            }
        elif no_ev > 0:
            return {
                "ev":       no_ev,
                "edge":     no_edge,
                "kelly":    no_kelly,
                "trade":    True,
                "side":     "NO",
                "price":    no_price,
                "ev_no":    yes_ev,
            }
        else:
            return {
                "ev":    max(yes_ev, no_ev),
                "edge":  yes_edge,
                "kelly": 0.0,
                "trade": False,
                "side":  None,
            }


class StrategySelector:
    """
    Routes each market to the best strategy based on current conditions.
    Returns a strategy tag that also feeds back into position sizing.
    """

    STRATEGIES = [
        "momentum",
        "mean_reversion",
        "volatility_breakout",
        "trend_following",
        "scalp",
        "sentiment_driven",
        "mispricing",
    ]

    def select(self, indicators: Dict, market: Dict,
               model_prob: float, market_price: float) -> str:
        bb_squeeze = indicators.get("bb_squeeze", False)
        rsi        = indicators.get("rsi_14", 50)
        roc_5      = indicators.get("roc_5", 0)
        uptrend    = indicators.get("uptrend", False)
        downtrend  = indicators.get("downtrend", False)
        vol_spike  = indicators.get("vol_spike", False)
        edge       = abs(model_prob - market_price)
        tte        = market.get("time_to_expiry", 3600)

        if edge > 0.15:
            return "mispricing"
        if bb_squeeze and vol_spike:
            return "volatility_breakout"
        if (uptrend or downtrend) and abs(roc_5) > 1.0:
            return "trend_following"
        if abs(rsi - 50) > 30:
            return "mean_reversion"
        if abs(roc_5) > 0.5 and tte > 1800:
            return "momentum"
        if tte < 900:
            return "scalp"
        return "momentum"


class MarketAnalyzer:
    """
    Top-level orchestrator. Given a market and current data,
    produces a full analysis record.
    """

    def __init__(self, trading_cfg, model_cfg):
        self.t_cfg    = trading_cfg
        self.model    = ProbabilityModel(model_cfg)
        self.ev_calc  = EVCalculator()
        self.strategy = StrategySelector()

    def analyze(
        self,
        market: Dict,
        btc_price: float,
        indicators: Dict,
        funding_bias: float,
        sentiment_score: float,
    ) -> Optional[Dict]:
        """
        Returns analysis dict or None if market is not tradeable.
        """
        from src.data.polymarket_client import PolymarketClient

        question  = market.get("question", "")
        direction = PolymarketClient.market_direction(question)
        target    = PolymarketClient.parse_btc_target(question)
        tte       = market.get("time_to_expiry", 0)

        if not target or tte <= 0 or btc_price <= 0:
            return None

        # Parse current market prices
        try:
            prices = json.loads(market.get("outcome_prices", "[0.5,0.5]"))
            yes_price = float(prices[0]) if prices else 0.5
        except Exception:
            yes_price = 0.5

        if yes_price <= 0.01 or yes_price >= 0.99:
            return None

        # Model prediction
        model_prob, confidence, dominant_strategy = self.model.predict(
            btc_price=btc_price,
            target_price=target,
            direction=direction,
            time_to_expiry_sec=tte,
            indicators=indicators,
            funding_bias=funding_bias,
            sentiment_score=sentiment_score,
        )

        # EV calculation
        ev_data = self.ev_calc.calculate_ev(model_prob, yes_price, direction)

        # Strategy selection
        strategy = self.strategy.select(
            indicators, market, model_prob, yes_price)

        # Build analysis record
        analysis = {
            "market_id":      market["id"],
            "question":       question,
            "target_price":   target,
            "direction":      direction,
            "btc_price":      btc_price,
            "yes_price":      yes_price,
            "no_price":       1.0 - yes_price,
            "model_prob":     round(model_prob, 4),
            "market_prob":    round(yes_price, 4),
            "edge":           round(ev_data["edge"], 4),
            "ev":             round(ev_data["ev"], 4),
            "kelly":          round(ev_data.get("kelly", 0), 4),
            "confidence":     round(confidence, 4),
            "side":           ev_data.get("side"),
            "trade_signal":   ev_data["trade"],
            "strategy":       strategy,
            "dominant_signal": dominant_strategy,
            "time_to_expiry": tte,
            "timestamp":      int(time.time()),
            "signals_json":   json.dumps({
                k: round(v, 4) if isinstance(v, float) else v
                for k, v in indicators.items()
                if isinstance(v, (int, float, bool))
            }),
        }

        # Apply confidence and min-edge filters
        if (analysis["trade_signal"] and
                confidence >= self.t_cfg.MIN_CONFIDENCE and
                abs(analysis["edge"]) >= self.t_cfg.MIN_EDGE_PCT and
                analysis["ev"] >= self.t_cfg.MIN_EV):
            analysis["actionable"] = True
        else:
            analysis["actionable"] = False
            analysis["skip_reason"] = (
                "low_confidence" if confidence < self.t_cfg.MIN_CONFIDENCE else
                "insufficient_edge" if abs(analysis["edge"]) < self.t_cfg.MIN_EDGE_PCT else
                "low_ev" if analysis["ev"] < self.t_cfg.MIN_EV else
                "no_signal"
            )

        return analysis
