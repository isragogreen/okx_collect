import aiohttp
import asyncio
import logging
import traceback
import sys

logger = logging.getLogger(__name__)

class TelegramNotifier:
    def __init__(self, config):
        self.config = config
        self.base_url = f"https://api.telegram.org/bot{self.config.telegram_token}/sendMessage"

    async def send_message(self, message):
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "chat_id": self.config.telegram_chat_id,
                    "text": message,
                    "parse_mode": "Markdown"
                }
                async with session.post(self.base_url, json=payload) as response:
                    if response.status == 200:
                        logger.info("Telegram message sent successfully")
                    else:
                        logger.error(f"Failed to send Telegram message: {await response.text()}")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")

    async def send_exception(self, exc_info):
        exc_type, exc_value, exc_traceback = exc_info
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        message = f"Exception occurred:\n```python\n{''.join(tb_lines)}\n```"
        await self.send_message(message)

def exception_handler(notifier):
    def handler(exc_type, exc_value, exc_traceback):
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        message = f"Exception occurred:\n```python\n{''.join(tb_lines)}\n```"
        logger.critical(message)
    return handler
