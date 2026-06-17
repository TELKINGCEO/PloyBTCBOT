"""
Analysis Engine - Multi-Strategy Probability Estimator
Handles two market types:
  1. Price-target markets  e.g. "Bitcoin above $67,000 on June 17?"
  2. Directional markets   e.g. "BTC Up or Down 15m", "BTC Up or Down Hourly"
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
    direction:   str        # "UP" or "DOWN" (or "YES"/"NO" for price markets)
    probability: float      # 0-1 probability of UP outcome
    confidence:  float      # 0-1
    ev:          float
    rationale:   str
    weight:      float = 1.0


@dataclass
class TradeSignal:
    market_id:        str
    question:         str
    outcome:          str           # "Up" or "Down" (directional) / "YES"/"NO" (price)
    entry_price:      float
    predicted_prob:   float
    market_prob:      float
    edge:             float
    ev:               float
    confidence:       float
    strategy:         str
    signals:          List[Signal]
    btc_price:        float
    btc_target:       float         # 0.0 for directional markets
    direction:        str           # "up"/"down" or "above"/"below"
    time_to_expiry:   float
    rationale:        str
    kelly_fraction:   float = 0.0


class AnalysisEngine:
    """
    Analyzes both price-target and directional BTC markets.
    For directional markets (Up/Down 15m, Hourly):
      - Uses momentum, trend, mean-reversion, order flow signals
      - Predicts P(BTC closes higher than open) for this candle
      - Compares to market-implied probability to find edge
    """

    def __init__(self, config, data_feed, funding_collector, sentiment_collector):
        self.cfg       = config
        self.feed      = data_feed
        self.funding   = funding_collector
        self.sentiment = sentiment_collector

    # ─────────────────────────────────────────────────────────────────────
    # Main entry point
    # ─────────────────────────────────────────────────────────────────────
    def analyze_market(self, market: Dict) -> Optional[TradeSignal]:
        """Analyze a single market and return TradeSignal if edge exists."""
        from src.data.polymarket_client import PolymarketClient

        question    = market.get("question", "")
        market_type = market.get("market_type", "")

        ind       = self.feed.get_indicators()
        btc_price = self.feed.get_price() or ind.get("price", 0)
        if btc_price == 0:
            return None

        time_to_expiry = market.get("time_to_expiry", 3600)

        # ── Route to correct analysis path ────────────────────────────────
        is_directional = market_type in ("btc_15min", "btc_1hour")

        if is_directional:
            return self._analyze_directional(
                market, question, market_type,
                ind, btc_price, time_to_expiry
            )
        else:
            # Price-target market
            btc_target = PolymarketClient.parse_btc_target(question)
            direction  = PolymarketClient.market_direction(question)
            if not btc_target:
                return None
            return self._analyze_price_target(
                market, question, btc_target, direction,
                ind, btc_price, time_to_expiry
            )

    # ─────────────────────────────────────────────────────────────────────
    # Path 1: Directional markets (Up or Down 15m / Hourly)
    # ─────────────────────────────────────────────────────────────────────
    def _analyze_directional(self, market: Dict, question: str,
                              market_type: str, ind: Dict,
                              btc_price: float,
                              time_to_expiry: float) -> Optional[TradeSignal]:
        """
        Predict P(BTC closes UP) for the current 15min or 1-hour candle.
        Uses pure momentum/trend signals — no price target needed.
        """
        hours_left = max(time_to_expiry / 3600, 0.01)

        # ── Run directional signals ───────────────────────────────────────
        signals: List[Signal] = []
        signals.append(self._dir_momentum(ind, hours_left, market_type))
        signals.append(self._dir_trend(ind, hours_left))
        signals.append(self._dir_mean_reversion(ind, hours_left))
        signals.append(self._dir_order_flow(ind, hours_left))
        signals.append(self._dir_sentiment(ind, hours_left))
        signals.append(self._dir_volatility(ind, hours_left))

        # ── Ensemble ──────────────────────────────────────────────────────
        total_weight = sum(s.weight * s.confidence for s in signals)
        if total_weight == 0:
            return None

        up_prob = sum(
            s.probability * s.weight * s.confidence for s in signals
        ) / total_weight
        up_prob = max(0.03, min(0.97, up_prob))

        # ── Market prices ─────────────────────────────────────────────────
        try:
            prices    = json.loads(market.get("outcome_prices", "[0.5, 0.5]"))
            up_price  = float(prices[0]) if prices else 0.5
            dn_price  = float(prices[1]) if len(prices) > 1 else (1 - up_price)
        except Exception:
            up_price, dn_price = 0.5, 0.5

        up_price = max(0.02, min(0.98, up_price))
        dn_price = max(0.02, min(0.98, dn_price))

        dn_prob  = 1.0 - up_prob

        # ── Best side ─────────────────────────────────────────────────────
        up_edge = up_prob - up_price
        dn_edge = dn_prob - dn_price

        if abs(up_edge) >= abs(dn_edge):
            outcome      = "Up"
            entry_price  = up_price
            model_prob   = up_prob
            market_prob  = up_price
            edge         = up_edge
        else:
            outcome      = "Down"
            entry_price  = dn_price
            model_prob   = dn_prob
            market_prob  = dn_price
            edge         = dn_edge

        # ── EV + Kelly ────────────────────────────────────────────────────
        win_payout     = (1.0 - entry_price) / entry_price
        ev             = model_prob * win_payout - (1 - model_prob)
        kelly_full     = ((model_prob * win_payout - (1 - model_prob))
                          / win_payout) if win_payout > 0 else 0.0
        kelly_fraction = max(0.0, min(
            kelly_full * self.cfg.KELLY_FRACTION,
            self.cfg.MAX_KELLY_BET
        ))

        # ── Confidence ────────────────────────────────────────────────────
        probs      = ([s.probability for s in signals]
                      if outcome == "Up"
                      else [1 - s.probability for s in signals])
        std_prob   = statistics.stdev(probs) if len(probs) > 1 else 0.5
        agreement  = max(0.0, 1.0 - std_prob * 3)
        avg_conf   = statistics.mean(s.confidence for s in signals)
        confidence = agreement * 0.5 + avg_conf * 0.5

        # ── Primary strategy ──────────────────────────────────────────────
        primary = max(signals, key=lambda s: s.confidence * abs(s.probability - 0.5))

        rationale = (
            f"BTC @ ${btc_price:,.2f} | {market_type} | "
            f"{hours_left*60:.0f}min left\n"
            f"Trade {outcome} @ {entry_price:.3f} | "
            f"Edge={edge*100:+.1f}% | EV={ev*100:+.1f}¢/$ | "
            f"Conf={confidence*100:.0f}%\n"
            f"P(Up)={up_prob:.3f} | market Up={up_price:.3f} "
            f"Down={dn_price:.3f}"
        )

        return TradeSignal(
            market_id      = market.get("id", ""),
            question       = question,
            outcome        = outcome,
            entry_price    = entry_price,
            predicted_prob = model_prob,
            market_prob    = market_prob,
            edge           = edge,
            ev             = ev,
            confidence     = confidence,
            strategy       = primary.strategy,
            signals        = signals,
            btc_price      = btc_price,
            btc_target     = 0.0,
            direction      = "up" if outcome == "Up" else "down",
            time_to_expiry = time_to_expiry,
            rationale      = rationale,
            kelly_fraction = kelly_fraction,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Directional signal models
    # ─────────────────────────────────────────────────────────────────────
    def _dir_momentum(self, ind: Dict, hours: float,
                      market_type: str) -> Signal:
        """
        Momentum: recent price direction predicts continuation.
        15m markets: use very short-term momentum (roc_5)
        1h markets:  use medium-term momentum (roc_10)
        """
        roc_5  = ind.get("roc_5",  0.0)
        roc_10 = ind.get("roc_10", 0.0)
        macd_h = ind.get("macd_hist", 0.0)
        rsi    = ind.get("rsi_14", 50.0)

        if market_type == "btc_15min":
            # Short-term: weight roc_5 heavily
            score = (math.tanh(roc_5  * 1.5) * 0.55 +
                     math.tanh(roc_10 * 0.8) * 0.25 +
                     math.tanh(macd_h / 80)  * 0.20)
        else:
            # 1-hour: balanced
            score = (math.tanh(roc_5  * 1.0) * 0.30 +
                     math.tanh(roc_10 * 1.2) * 0.45 +
                     math.tanh(macd_h / 80)  * 0.25)

        # RSI adjustment: extreme RSI weakens momentum
        rsi_penalty = abs(rsi - 50) / 50 * 0.1
        if rsi > 70 and score > 0:   score -= rsi_penalty
        if rsi < 30 and score < 0:   score += rsi_penalty

        up_prob    = 0.5 + score * 0.30
        up_prob    = max(0.05, min(0.95, up_prob))
        confidence = 0.50 + min(abs(score) * 0.40, 0.35)

        return Signal(
            strategy   = "MOMENTUM",
            direction  = "UP" if score > 0 else "DOWN",
            probability = up_prob,
            confidence = confidence,
            ev         = up_prob - 0.5,
            rationale  = (f"roc5={roc_5:+.3f}% roc10={roc_10:+.3f}% "
                          f"macd_h={macd_h:+.2f} rsi={rsi:.1f}"),
            weight     = 0.30,
        )

    def _dir_trend(self, ind: Dict, hours: float) -> Signal:
        """EMA trend alignment: price above/below EMA stack"""
        price    = ind.get("price",    0.0)
        ema_9    = ind.get("ema_9",    price)
        ema_21   = ind.get("ema_21",   price)
        ema_50   = ind.get("ema_50",   price)
        uptrend  = ind.get("uptrend",  False)
        dntred   = ind.get("downtrend", False)
        hh       = ind.get("higher_highs", False)
        ll       = ind.get("lower_lows",   False)

        if uptrend and hh:
            score = 0.75
        elif uptrend:
            score = 0.45
        elif dntred and ll:
            score = -0.75
        elif dntred:
            score = -0.45
        else:
            # Partial trend
            if price > ema_9 > ema_21:
                score = 0.25
            elif price < ema_9 < ema_21:
                score = -0.25
            else:
                score = 0.0

        up_prob    = 0.5 + score * 0.25
        up_prob    = max(0.05, min(0.95, up_prob))
        confidence = 0.45 + abs(score) * 0.35

        return Signal(
            strategy    = "TREND",
            direction   = "UP" if score > 0 else "DOWN",
            probability = up_prob,
            confidence  = confidence,
            ev          = up_prob - 0.5,
            rationale   = (f"uptrend={uptrend} dntrend={dntred} "
                           f"hh={hh} ll={ll} "
                           f"ema9={ema_9:.0f} ema21={ema_21:.0f}"),
            weight      = 0.25,
        )

    def _dir_mean_reversion(self, ind: Dict, hours: float) -> Signal:
        """
        Mean reversion: overbought → expect down, oversold → expect up.
        More relevant for 15m markets.
        """
        rsi     = ind.get("rsi_14",  50.0)
        bb_pct  = ind.get("bb_pct",   0.5)  # 0=lower band, 1=upper band
        stoch_k = ind.get("stoch_k",  50.0)

        # RSI signal: >70 overbought (bearish), <30 oversold (bullish)
        rsi_score   = -(rsi - 50) / 50       # -1..+1 (negative=overbought)

        # BB position: near upper band → bearish, near lower → bullish
        bb_score    = -(bb_pct - 0.5) * 2    # -1..+1

        # Stochastic
        stoch_score = -(stoch_k - 50) / 50

        score = (rsi_score * 0.45 + bb_score * 0.35 + stoch_score * 0.20)
        score = max(-1.0, min(1.0, score))

        # Mean reversion only has real edge at extremes
        if abs(rsi - 50) < 15 and abs(bb_pct - 0.5) < 0.2:
            confidence = 0.25   # neutral zone — low confidence
        else:
            confidence = 0.40 + abs(score) * 0.35

        up_prob = 0.5 + score * 0.20
        up_prob = max(0.05, min(0.95, up_prob))

        return Signal(
            strategy    = "MEAN_REVERSION",
            direction   = "UP" if score > 0 else "DOWN",
            probability = up_prob,
            confidence  = confidence,
            ev          = up_prob - 0.5,
            rationale   = (f"rsi={rsi:.1f} bb_pct={bb_pct:.2f} "
                           f"stoch={stoch_k:.1f} score={score:+.3f}"),
            weight      = 0.20,
        )

    def _dir_order_flow(self, ind: Dict, hours: float) -> Signal:
        """Delta ratio and OBV — buy vs sell pressure"""
        delta   = ind.get("delta_ratio", 0.0)   # -1..+1
        obv     = ind.get("obv_trend", "neutral")
        vol_spike = ind.get("vol_spike", False)

        obv_score = {"bullish": 0.5, "bearish": -0.5, "neutral": 0.0}[obv]
        score     = delta * 0.6 + obv_score * 0.4
        if vol_spike:
            score *= 1.2
        score = max(-1.0, min(1.0, score))

        up_prob    = 0.5 + score * 0.20
        up_prob    = max(0.05, min(0.95, up_prob))
        confidence = 0.35 + abs(score) * 0.30 + (0.10 if vol_spike else 0)

        return Signal(
            strategy    = "ORDER_FLOW",
            direction   = "UP" if score > 0 else "DOWN",
            probability = up_prob,
            confidence  = confidence,
            ev          = up_prob - 0.5,
            rationale   = (f"delta={delta:.3f} obv={obv} "
                           f"vol_spike={vol_spike}"),
            weight      = 0.15,
        )

    def _dir_sentiment(self, ind: Dict, hours: float) -> Signal:
        """Fear & Greed + news sentiment"""
        sent  = self.sentiment.get_score() if self.sentiment else 0.0
        fg    = getattr(self.sentiment, "fear_greed", 50)

        # Contrarian: extreme fear = bullish, extreme greed = bearish
        fg_score   = (fg - 50) / 50        # -1..+1 (positive=greedy=contrarian bear)
        score      = sent * 0.6 - fg_score * 0.4 * 0.3   # sentiment dominant
        score      = max(-1.0, min(1.0, score))

        up_prob    = 0.5 + score * 0.10
        up_prob    = max(0.05, min(0.95, up_prob))
        confidence = 0.30 + abs(sent) * 0.25

        return Signal(
            strategy    = "SENTIMENT",
            direction   = "UP" if score > 0 else "DOWN",
            probability = up_prob,
            confidence  = confidence,
            ev          = up_prob - 0.5,
            rationale   = f"sent={sent:.3f} F&G={fg:.0f}",
            weight      = 0.05,
        )

    def _dir_volatility(self, ind: Dict, hours: float) -> Signal:
        """
        Volatility regime:
        - BB squeeze → imminent breakout (direction uncertain → neutral)
        - High vol + uptrend → continuation up
        - High vol + downtrend → continuation down
        """
        squeeze  = ind.get("bb_squeeze", False)
        atr_pct  = ind.get("atr_pct",    0.01)
        uptrend  = ind.get("uptrend",    False)
        dntred   = ind.get("downtrend",  False)
        hvol     = ind.get("hourly_vol", 0.01)

        if squeeze:
            # Pre-breakout: stay neutral
            score      = 0.0
            confidence = 0.20
        elif atr_pct > 0.015:
            # High vol: trend continuation
            if uptrend:
                score = 0.40
            elif dntred:
                score = -0.40
            else:
                score = 0.0
            confidence = 0.40 + min(atr_pct * 10, 0.25)
        else:
            # Low vol: weak signal
            score      = 0.0
            confidence = 0.25

        up_prob = 0.5 + score * 0.15
        up_prob = max(0.05, min(0.95, up_prob))

        return Signal(
            strategy    = "VOLATILITY",
            direction   = "UP" if score > 0 else "DOWN",
            probability = up_prob,
            confidence  = confidence,
            ev          = up_prob - 0.5,
            rationale   = (f"squeeze={squeeze} atr={atr_pct*100:.2f}% "
                           f"hvol={hvol*100:.2f}%"),
            weight      = 0.05,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Path 2: Price-target markets
    # ─────────────────────────────────────────────────────────────────────
    def _analyze_price_target(self, market: Dict, question: str,
                               btc_target: float, direction: str,
                               ind: Dict, btc_price: float,
                               time_to_expiry: float) -> Optional[TradeSignal]:
        """Original price-target analysis logic"""
        hours_left = max(time_to_expiry / 3600, 0.01)

        signals: List[Signal] = []
        signals.append(self._momentum_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._mean_reversion_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._volatility_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._trend_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._order_flow_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._sentiment_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._funding_model(ind, btc_price, btc_target, direction, hours_left))
        signals.append(self._statistical_model(ind, btc_price, btc_target, direction, hours_left))

        total_weight = sum(s.weight * s.confidence for s in signals)
        if total_weight == 0:
            return None

        ensemble_yes_prob = sum(
            s.probability * s.weight * s.confidence for s in signals
        ) / total_weight
        ensemble_yes_prob = max(0.02, min(0.98, ensemble_yes_prob))

        try:
            prices    = json.loads(market.get("outcome_prices", "[0.5, 0.5]"))
            yes_price = float(prices[0]) if prices else 0.5
            no_price  = float(prices[1]) if len(prices) > 1 else (1 - yes_price)
        except Exception:
            yes_price, no_price = 0.5, 0.5

        yes_price = max(0.01, min(0.99, yes_price))
        no_price  = max(0.01, min(0.99, no_price))

        model_no_prob = 1 - ensemble_yes_prob
        yes_edge = ensemble_yes_prob - yes_price
        no_edge  = model_no_prob    - no_price

        if abs(yes_edge) >= abs(no_edge):
            outcome, entry_price = "YES", yes_price
            model_prob, market_prob, edge = ensemble_yes_prob, yes_price, yes_edge
        else:
            outcome, entry_price = "NO", no_price
            model_prob, market_prob, edge = model_no_prob, no_price, no_edge

        win_payout     = (1.0 - entry_price) / entry_price
        ev             = model_prob * win_payout - (1 - model_prob)
        kelly_full     = ((model_prob * win_payout - (1 - model_prob))
                          / win_payout) if win_payout > 0 else 0.0
        kelly_fraction = max(0.0, min(
            kelly_full * self.cfg.KELLY_FRACTION, self.cfg.MAX_KELLY_BET))

        probs      = [s.probability if outcome == "YES"
                      else (1 - s.probability) for s in signals]
        std_prob   = statistics.stdev(probs) if len(probs) > 1 else 0.5
        agreement  = max(0.0, 1.0 - std_prob * 3)
        avg_conf   = statistics.mean(s.confidence for s in signals)
        confidence = agreement * 0.5 + avg_conf * 0.5

        primary = max(signals, key=lambda s: s.confidence * abs(
            s.probability - 0.5 if outcome == "YES"
            else (1 - s.probability) - 0.5))

        rationale = self._build_rationale(
            signals, outcome, edge, ev, confidence,
            btc_price, btc_target, direction,
            hours_left, yes_price, no_price)

        return TradeSignal(
            market_id      = market.get("id", ""),
            question       = question,
            outcome        = outcome,
            entry_price    = entry_price,
            predicted_prob = model_prob,
            market_prob    = market_prob,
            edge           = edge,
            ev             = ev,
            confidence     = confidence,
            strategy       = primary.strategy,
            signals        = signals,
            btc_price      = btc_price,
            btc_target     = btc_target,
            direction      = direction,
            time_to_expiry = time_to_expiry,
            rationale      = rationale,
            kelly_fraction = kelly_fraction,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Price-target strategy models (unchanged)
    # ─────────────────────────────────────────────────────────────────────
    def _momentum_model(self, ind, price, target, direction, hours):
        roc_5  = ind.get("roc_5",  0.0)
        roc_10 = ind.get("roc_10", 0.0)
        rsi    = ind.get("rsi_14", 50.0)
        macd_h = ind.get("macd_hist", 0.0)
        mom_score = (math.tanh(roc_5 * 2) * 0.4 + math.tanh(roc_10) * 0.35 +
                     (rsi - 50) / 50 * 0.15 + math.tanh(macd_h / 100) * 0.10)
        base_prob = self._distance_prob(price, target, direction, hours)
        adj       = mom_score * 0.08
        yes_prob  = base_prob + adj if direction == "above" else base_prob - adj
        yes_prob  = max(0.05, min(0.95, yes_prob))
        return Signal(
            strategy="MOMENTUM", direction="YES" if mom_score > 0 else "NO",
            probability=yes_prob, confidence=0.50 + abs(mom_score)*0.25,
            ev=yes_prob - 0.5,
            rationale=f"roc5={roc_5:+.3f}% roc10={roc_10:+.3f}% rsi={rsi:.1f}",
            weight=0.25)

    def _mean_reversion_model(self, ind, price, target, direction, hours):
        rsi    = ind.get("rsi_14",  50.0)
        bb_pct = ind.get("bb_pct",   0.5)
        mr     = -(rsi - 50) / 50 * 0.5 - (bb_pct - 0.5) * 0.5
        base   = self._distance_prob(price, target, direction, hours)
        adj    = mr * 0.05
        yes_p  = base + adj if direction == "above" else base - adj
        yes_p  = max(0.05, min(0.95, yes_p))
        return Signal(
            strategy="MEAN_REVERSION", direction="YES" if mr > 0 else "NO",
            probability=yes_p, confidence=0.40 + abs(mr)*0.20,
            ev=yes_p - 0.5,
            rationale=f"rsi={rsi:.1f} bb%={bb_pct:.2f}",
            weight=0.15)

    def _volatility_model(self, ind, price, target, direction, hours):
        atr_pct = ind.get("atr_pct", 0.01)
        squeeze = ind.get("bb_squeeze", False)
        base    = self._distance_prob(price, target, direction, hours)
        if squeeze:
            yes_p = 0.5
            conf  = 0.20
        else:
            yes_p = base
            conf  = 0.40 + min(atr_pct * 5, 0.25)
        yes_p = max(0.05, min(0.95, yes_p))
        return Signal(
            strategy="VOLATILITY", direction="YES" if yes_p > 0.5 else "NO",
            probability=yes_p, confidence=conf,
            ev=yes_p - 0.5,
            rationale=f"atr={atr_pct*100:.2f}% squeeze={squeeze}",
            weight=0.10)

    def _trend_model(self, ind, price, target, direction, hours):
        uptrend = ind.get("uptrend",  False)
        dntred  = ind.get("downtrend", False)
        hh      = ind.get("higher_highs", False)
        ll      = ind.get("lower_lows",   False)
        base    = self._distance_prob(price, target, direction, hours)
        if uptrend and hh:   adj =  0.06
        elif uptrend:        adj =  0.03
        elif dntred and ll:  adj = -0.06
        elif dntred:         adj = -0.03
        else:                adj =  0.0
        yes_p = base + (adj if direction == "above" else -adj)
        yes_p = max(0.05, min(0.95, yes_p))
        conf  = 0.45 + abs(adj) * 3
        return Signal(
            strategy="TREND", direction="YES" if adj > 0 else "NO",
            probability=yes_p, confidence=conf,
            ev=yes_p - 0.5,
            rationale=f"up={uptrend} dn={dntred} hh={hh} ll={ll}",
            weight=0.15)

    def _order_flow_model(self, ind, price, target, direction, hours):
        delta  = ind.get("delta_ratio", 0.0)
        vol_sp = ind.get("vol_spike",  False)
        obv    = ind.get("obv_trend", "neutral")
        score  = math.tanh(delta * 2.0)
        if vol_sp: score *= 1.3
        score  = max(-1.0, min(1.0, score))
        base   = self._distance_prob(price, target, direction, hours)
        adj    = score * 0.06
        yes_p  = base + (adj if direction == "above" else -adj)
        yes_p  = max(0.05, min(0.95, yes_p))
        return Signal(
            strategy="ORDER_FLOW", direction="YES" if score > 0 else "NO",
            probability=yes_p, confidence=0.40 + abs(score)*0.30,
            ev=yes_p - 0.5,
            rationale=f"delta={delta:.3f} vol_spike={vol_sp}",
            weight=0.10)

    def _sentiment_model(self, ind, price, target, direction, hours):
        sent = self.sentiment.get_score() if self.sentiment else 0.0
        fg   = getattr(self.sentiment, "fear_greed", 50)
        base = self._distance_prob(price, target, direction, hours)
        adj  = sent * 0.05
        yes_p = base + (adj if direction == "above" else -adj)
        yes_p = max(0.05, min(0.95, yes_p))
        return Signal(
            strategy="SENTIMENT", direction="YES" if sent > 0 else "NO",
            probability=yes_p, confidence=0.35 + abs(sent)*0.25,
            ev=yes_p - 0.5,
            rationale=f"sent={sent:.3f} F&G={fg:.0f}",
            weight=0.05)

    def _funding_model(self, ind, price, target, direction, hours):
        bias = self.funding.get_bias() if self.funding else 0.0
        rate = getattr(self.funding, "funding_rate", 0.0)
        base = self._distance_prob(price, target, direction, hours)
        adj  = -bias * 0.04
        yes_p = base + (adj if direction == "above" else -adj)
        yes_p = max(0.05, min(0.95, yes_p))
        return Signal(
            strategy="FUNDING_RATE", direction="YES" if adj > 0 else "NO",
            probability=yes_p, confidence=0.35 + abs(bias)*0.30,
            ev=yes_p - 0.5,
            rationale=f"rate={rate*100:.4f}% bias={bias:.3f}",
            weight=0.03)

    def _statistical_model(self, ind, price, target, direction, hours):
        yes_p = self._distance_prob(price, target, direction, hours)
        return Signal(
            strategy="STATISTICAL", direction="YES" if yes_p > 0.5 else "NO",
            probability=yes_p, confidence=0.75,
            ev=yes_p - 0.5,
            rationale=f"price={price:.0f} target={target:.0f} h={hours:.2f}",
            weight=0.02)

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────
    def _distance_prob(self, price, target, direction, hours):
        ind          = self.feed.get_indicators()
        measured_vol = ind.get("hourly_vol", None)
        hourly_vol   = max(0.003, min(0.08, measured_vol)) if measured_vol else 0.010
        vol_period   = hourly_vol * math.sqrt(max(hours, 0.05))
        log_ret      = math.log(target / price) if price > 0 and target > 0 else 0
        z            = log_ret / vol_period if vol_period > 0 else 0
        prob         = (1 - self._norm_cdf(z) if direction == "above"
                        else self._norm_cdf(z))
        return max(0.03, min(0.97, prob))

    @staticmethod
    def _norm_cdf(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    def _build_rationale(self, signals, outcome, edge, ev, confidence,
                         price, target, direction, hours,
                         yes_price, no_price):
        pct   = (target - price) / price * 100
        lines = [
            f"BTC @ ${price:,.2f} | Target ${target:,.2f} ({pct:+.2f}%) "
            f"| {direction.upper()} | {hours:.1f}h left",
            f"Trade {outcome} @ {yes_price if outcome=='YES' else no_price:.3f} | "
            f"Edge={edge*100:+.1f}% | EV={ev*100:+.1f}c/$ | Conf={confidence*100:.0f}%",
        ]
        for s in sorted(signals, key=lambda x: x.confidence, reverse=True):
            m = "+" if s.direction == outcome else "-"
            lines.append(f"  {m} [{s.strategy:16s}] p={s.probability:.3f} "
                         f"c={s.confidence:.2f} | {s.rationale}")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────
class MarketScanner:
    """Scans all active markets and returns ranked trade opportunities"""

    def __init__(self, analysis_engine: AnalysisEngine, config):
        self.engine = analysis_engine
        self.cfg    = config

    def scan(self, markets: List[Dict]) -> List[TradeSignal]:
        candidates = []
        for market in markets:
            try:
                sig = self.engine.analyze_market(market)
                if sig is None:
                    continue
                if sig.edge          < self.cfg.MIN_EDGE_PCT:    continue
                if sig.ev            < self.cfg.MIN_EV:          continue
                if sig.confidence    < self.cfg.MIN_CONFIDENCE:  continue
                if sig.time_to_expiry < 120:                     continue  # <2 min
                if sig.kelly_fraction <= 0:                      continue
                candidates.append(sig)
            except Exception as e:
                logger.debug(f"Analysis error for {market.get('id')}: {e}")

        candidates.sort(
            key=lambda s: s.ev * s.confidence * min(s.edge / 0.05, 2.0),
            reverse=True
        )
        return candidates
