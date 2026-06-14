import aiohttp
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token    = token
        self.chat_id  = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.enabled  = bool(token and chat_id)

    async def send(self, message: str):
        if not self.enabled:
            return
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    f"{self.base_url}/sendMessage",
                    json={
                        "chat_id":    self.chat_id,
                        "text":       message,
                        "parse_mode": "HTML",
                    }
                )
        except Exception as e:
            logger.debug(f"Telegram error: {e}")

    async def startup(self, balance: float):
        await self.send(
            f"🤖 <b>BOT STARTED</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💵 Balance: <b>${balance:.2f}</b>\n"
            f"🎯 Target: $1,000 in 30 days\n"
            f"🕐 {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )

    async def trade_opened(self, trade: dict):
        await self.send(
            f"🟡 <b>TRADE OPENED</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📋 {trade.get('question','')[:80]}\n"
            f"📌 Side: <b>{trade.get('outcome')}</b>\n"
            f"💵 Size: <b>${trade.get('size_usdc',0):.2f}</b>\n"
            f"📈 Entry: <b>{trade.get('entry_price',0):.4f}</b>\n"
            f"🎯 Strategy: {trade.get('strategy')}\n"
            f"⚡ EV: {trade.get('ev',0)*100:.1f}¢ per $\n"
            f"🕐 {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )

    async def trade_closed(self, trade: dict):
        pnl   = trade.get('pnl', 0)
        emoji = "✅" if pnl >= 0 else "❌"
        await self.send(
            f"{emoji} <b>TRADE CLOSED</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📋 {trade.get('question','')[:80]}\n"
            f"📌 Side: <b>{trade.get('outcome')}</b>\n"
            f"💰 P&L: <b>${pnl:+.2f} ({trade.get('pnl_pct',0)*100:+.1f}%)</b>\n"
            f"🚪 Reason: {trade.get('exit_reason')}\n"
            f"🎯 Strategy: {trade.get('strategy')}\n"
            f"🕐 {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )

    async def circuit_breaker(self, reason: str):
        await self.send(
            f"🚨 <b>CIRCUIT BREAKER</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⚠️ {reason}\n"
            f"🛑 Trading HALTED\n"
            f"🕐 {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )

    async def daily_summary(self, stats: dict):
        wr = stats.get('win_rate', 0) * 100
        await self.send(
            f"📊 <b>DAILY SUMMARY</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💵 Balance: <b>${stats.get('balance',0):.2f}</b>\n"
            f"💰 Daily P&L: <b>${stats.get('daily_pnl',0):+.2f}</b>\n"
            f"🏆 Win Rate: {wr:.1f}%\n"
            f"📈 Trades: {stats.get('total_trades',0)}\n"
            f"📉 Drawdown: {stats.get('max_drawdown',0)*100:.1f}%\n"
            f"🎯 Goal: {stats.get('goal_pct',0):.1f}% of $1,000"
        )
