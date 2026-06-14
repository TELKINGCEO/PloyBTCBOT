# Polymarket BTC Trading Bot — Complete System Guide

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     POLYMARKET BTC BOT                              │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │  DATA LAYER  │    │ ANALYSIS     │    │   EXECUTION LAYER    │  │
│  │              │    │ ENGINE       │    │                      │  │
│  │ BTCDataFeed  │───▶│              │───▶│ RiskManager          │  │
│  │ (Binance WS) │    │ 8 Strategies │    │ (Kelly + Drawdown)   │  │
│  │              │    │  - Momentum  │    │                      │  │
│  │ FundingRate  │───▶│  - MeanRev   │───▶│ ExecutionEngine      │  │
│  │ Collector    │    │  - Volatility│    │ (Order placement)    │  │
│  │              │    │  - Trend     │    │                      │  │
│  │ Sentiment    │───▶│  - OrderFlow │    │ Polymarket CLOB API  │  │
│  │ Collector    │    │  - Sentiment │    │ (YES/NO shares)      │  │
│  └──────────────┘    │  - Funding   │    └──────────────────────┘  │
│                      │  - Statistical│                              │
│  ┌──────────────┐    └──────────────┘    ┌──────────────────────┐  │
│  │   DATABASE   │                        │     DASHBOARD         │  │
│  │   (SQLite)   │◀───────────────────────│ (React + Recharts)   │  │
│  │              │                        │                      │  │
│  │ - Candles    │                        │ - Equity curve       │  │
│  │ - Markets    │                        │ - Open positions     │  │
│  │ - Trades     │                        │ - Trade history      │  │
│  │ - Predictions│                        │ - Live signals       │  │
│  │ - Snapshots  │                        │ - Strategy stats     │  │
│  └──────────────┘                        └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Prerequisites
```bash
Python 3.11+
pip install aiohttp python-dotenv websockets
```

### 2. Environment variables
Create `.env` in the project root:
```bash
# Required for live trading (optional for paper trading)
POLYMARKET_PRIVATE_KEY=0x...      # Your wallet private key
POLYMARKET_API_KEY=...            # From clob.polymarket.com
POLYMARKET_SECRET=...
POLYMARKET_PASSPHRASE=...

# Optional (improves signals)
CRYPTOPANIC_API_KEY=...           # Free at cryptopanic.com
```

### 3. Seed historical data
```bash
python main.py seed
```
Downloads ~30,000 1-minute BTC candles from Binance (free, no API key needed).

### 4. Run backtest
```bash
python main.py backtest
```

### 5. Run live (paper trading without API keys)
```bash
python main.py run
```

### 6. Run live (real trading)
Set your API keys in `.env`, then:
```bash
python main.py run
```

---

## Database Schema

7 tables (SQLite WAL mode for concurrent access):

| Table | Purpose |
|-------|---------|
| `btc_prices` | 1m OHLCV candles |
| `markets` | Polymarket market cache |
| `predictions` | Model outputs per market |
| `trades` | All entries/exits with full P&L |
| `portfolio_snapshots` | Hourly balance checkpoints |
| `signals` | Individual technical signals |
| `bot_logs` | Structured event log |

---

## Strategy Details

### 1. VOLATILITY (weight: 20%, confidence: 80%)
Uses lognormal distribution with realized hourly vol (from ATR + rolling std).
Computes z-score of (target - current_price) / vol_for_period → normal CDF probability.
**Best at**: markets near expiry with stable vol.

### 2. MOMENTUM (weight: 25%)
Composite of ROC(5), ROC(10), RSI(14), MACD histogram.
Adjusts base probability by momentum direction × 8% per unit.
**Best at**: trending markets 15-60 min to expiry.

### 3. MEAN_REVERSION (weight: 20%)
RSI extremes, Bollinger Band %, Stochastic crossovers.
**Best at**: markets after spike moves, close to bands.

### 4. TREND_FOLLOWING (weight: 15%)
EMA alignment (9/21/50), higher-highs/lower-lows, OBV.
**Best at**: confirmed trends, not ranging markets.

### 5. ORDER_FLOW (weight: 10%)
Buy/sell delta ratio from trade tick data, volume spikes.
**Best at**: news-driven directional moves.

### 6. SENTIMENT (weight: 5%)
Fear & Greed index + CryptoPanic news sentiment.
**Best at**: extreme fear/greed with contrarian signals.

### 7. FUNDING_RATE (weight: 3%)
Perpetual funding rate as crowding/contrarian signal.
**Best at**: positioning-based reversals.

### 8. STATISTICAL (weight: 2%)
Pure lognormal base probability.

---

## Risk Management Rules

| Rule | Value | Rationale |
|------|-------|-----------|
| Min edge | 3% | Covers bid/ask spread + slippage |
| Min confidence | 55% | Reduces noise trades |
| Max position | 20% of bankroll | Kelly cap |
| Kelly fraction | 25% fractional | Conservative compounding |
| Profit target | 40% per trade | Take profits before reversal |
| Stop loss | 35% per trade | Asymmetric risk |
| Max daily loss | 25% | Prevents ruin in bad days |
| Max drawdown | 40% | Hard stop on consecutive losses |
| Max positions | 5 concurrent | Diversification |
| Vol pause | >15% hourly vol | Protect against flash crashes |

---

## Realistic Expectations: $10 → $1,000

Required daily compound return: **~17.5%/day** — this is **extremely aggressive**.

### Realistic scenarios:
| Scenario | Daily return | 30-day result |
|----------|-------------|---------------|
| Conservative | 2-5% | $18-$43 |
| Moderate | 8-12% | $108-$285 |
| Aggressive | 15-18% | $662-$1,073 |
| Goal | 17.5% | ~$1,000 |

The goal is achievable **only** if:
- You find consistent 5-15% edge markets
- Win rate stays above 58%
- No catastrophic drawdown occurs
- Polymarket has sufficient BTC hourly liquidity

**The math**: at 17.5% daily compounding, a 40% drawdown on day 10 wipes 70% of progress. Risk management is the most important part of this system.

---

## Polymarket API Setup (Live Trading)

### Option A: Paper trading (no setup needed)
Leave API keys blank. The bot simulates fills with 0.1% slippage.

### Option B: Full live trading
1. Connect wallet at polymarket.com
2. Generate API keys at clob.polymarket.com/auth
3. Install py-clob-client for on-chain order signing:
   ```bash
   pip install py-clob-client
   ```
4. Replace `ExecutionEngine.place_order` with `py_clob_client.ClobClient.create_order`

The `py-clob-client` library handles EIP-712 signing required for on-chain orders.

---

## Performance Optimization

### Latency improvements
- Run on a VPS close to Binance (Tokyo/Singapore/Frankfurt)
- Use `uvloop` for faster event loop: `pip install uvloop`
- Pre-compute indicators in C via `ta-lib`: `pip install TA-Lib`

### Signal quality improvements
- Add liquidation heatmaps (Coinglass API)
- Add order book imbalance from Binance depth stream
- Add on-chain metrics (Glassnode: SOPR, NUPL, exchange flows)
- Train a gradient boosting model (XGBoost) on historical signal features

### Database scaling
- Migrate to TimescaleDB or InfluxDB for production
- Use Redis for sub-second indicator caching
- Add pgBouncer if using PostgreSQL

### Multi-market
- Run 3-5 concurrent BTC markets at once to increase trade frequency
- Add ETH markets for diversification (similar config)

---

## Monitoring

### Log files
- `./logs/bot.log` — structured text log
- SQLite `bot_logs` table — queryable event history

### Health checks
```python
# Check bot status via Python
from main import PolymarketBTCBot
# Or query DB directly:
# SELECT * FROM portfolio_snapshots ORDER BY timestamp DESC LIMIT 1;
```

### Alerts
Add Telegram alerts by inserting into `bot.py`:
```python
import httpx
async def telegram_alert(msg):
    await httpx.AsyncClient().post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": msg}
    )
```

---

## Files

```
polymarket_btc_bot/
├── main.py                          # Entry point (run/backtest/seed)
├── requirements.txt
├── config/
│   └── config.py                    # All tuneable parameters
├── src/
│   ├── data/
│   │   ├── btc_feed.py              # Binance WebSocket + indicators
│   │   └── polymarket_client.py     # Polymarket CLOB + Gamma API
│   ├── analysis/
│   │   └── engine.py                # 8-strategy ensemble + MarketScanner
│   ├── risk/
│   │   └── risk_manager.py          # Kelly sizing, drawdown, circuit breakers
│   ├── execution/
│   │   └── executor.py              # Order placement + position monitoring
│   ├── backtesting/
│   │   └── backtest.py              # Walk-forward backtest engine
│   └── utils/
│       └── database.py              # SQLite ORM (7 tables)
├── db/
│   └── trading_bot.db               # Auto-created
└── logs/
    └── bot.log                      # Auto-created
```
