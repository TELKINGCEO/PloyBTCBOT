"""
Polymarket BTC Trading Bot - Configuration
"""
import os
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class TradingConfig:
    # Capital Management
    INITIAL_BANKROLL: float = 10.0
    TARGET_BANKROLL: float = 1000.0
    TARGET_DAYS: int = 30
    MIN_TRADE_SIZE: float = 0.50        # Minimum $0.50 per trade
    MAX_SINGLE_TRADE_PCT: float = 0.20  # Max 20% of bankroll per trade
    MAX_CONCURRENT_POSITIONS: int = 5
    
    # Kelly Criterion
    KELLY_FRACTION: float = 0.25        # Fractional Kelly (conservative)
    MAX_KELLY_BET: float = 0.30         # Hard cap at 30% even if Kelly says more
    
    # EV Thresholds
    MIN_EDGE_PCT: float = 0.015   # Lowered: 1.5% edge minimum          # Minimum 3% edge to enter
    MIN_EV: float = 0.01          # Lowered: 1% EV minimum                # Minimum 2 cents EV per dollar risked
    MIN_CONFIDENCE: float = 0.55        # Minimum 55% model confidence
    
    # Exit Conditions
    PROFIT_TARGET_PCT: float = 0.40     # Take profit at 40% gain
    STOP_LOSS_PCT: float = 0.35         # Stop loss at 35% loss
    MAX_HOLD_HOURS: float = 2.0         # Max hold time for 1h market
    
    # Risk Controls
    MAX_DAILY_LOSS_PCT: float = 0.25    # Stop trading if down 25% in a day
    MAX_DRAWDOWN_PCT: float = 0.40      # Stop trading if down 40% from peak
    VOLATILITY_PAUSE_THRESHOLD: float = 0.15  # BTC hourly vol > 15% → pause
    
    # Compounding
    REINVESTMENT_RATE: float = 0.90     # Reinvest 90% of profits
    RESERVE_PCT: float = 0.10           # Keep 10% as reserve

@dataclass
class APIConfig:
    # Polymarket
    POLYMARKET_API_URL: str = "https://clob.polymarket.com"
    POLYMARKET_GAMMA_URL: str = "https://gamma-api.polymarket.com"
    POLYMARKET_PRIVATE_KEY: str = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    POLYMARKET_API_KEY: str = os.getenv("POLYMARKET_API_KEY", "")
    POLYMARKET_SECRET: str = os.getenv("POLYMARKET_SECRET", "")
    POLYMARKET_PASSPHRASE: str = os.getenv("POLYMARKET_PASSPHRASE", "")
    
    # Price Data
    BINANCE_WS_URL: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    BINANCE_REST_URL: str = "https://api.binance.com/api/v3"
    COINGECKO_URL: str = "https://api.coingecko.com/api/v3"
    
    # News/Sentiment
    CRYPTOPANIC_API_KEY: str = os.getenv("CRYPTOPANIC_API_KEY", "")
    CRYPTOPANIC_URL: str = "https://cryptopanic.com/api/v1"
    NEWSAPI_KEY: str = os.getenv("NEWSAPI_KEY", "")
    
    # Alternative Data
    FEAR_GREED_URL: str = "https://api.alternative.me/fng/"
    GLASSNODE_API_KEY: str = os.getenv("GLASSNODE_API_KEY", "")
    
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID:   str = os.getenv("TELEGRAM_CHAT_ID", "")

@dataclass
class DatabaseConfig:
    DB_PATH: str = "./db/trading_bot.db"
    BACKUP_PATH: str = "./db/backups/"
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "./logs/bot.log"

@dataclass
class ModelConfig:
    # Feature engineering
    RSI_PERIOD: int = 14
    MACD_FAST: int = 12
    MACD_SLOW: int = 26
    MACD_SIGNAL: int = 9
    BB_PERIOD: int = 20
    BB_STD: float = 2.0
    ATR_PERIOD: int = 14
    
    # Lookback windows
    SHORT_WINDOW: int = 5
    MEDIUM_WINDOW: int = 15
    LONG_WINDOW: int = 60
    
    # Model weights for ensemble
    MOMENTUM_WEIGHT: float = 0.25
    MEAN_REVERSION_WEIGHT: float = 0.20
    VOLATILITY_WEIGHT: float = 0.20
    SENTIMENT_WEIGHT: float = 0.15
    ORDERFLOW_WEIGHT: float = 0.20

TRADING = TradingConfig()
API = APIConfig()
DATABASE = DatabaseConfig()
MODEL = ModelConfig()


