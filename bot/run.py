# bot/run.py
"""
Arranca el bot en modo polling (desarrollo).
Para producción usar webhook.py con Flask.

Uso:
    python -m bot.run
"""
import logging
from bot.handlers import bot

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(name)s  %(message)s',
)

if __name__ == "__main__":
    print("🤖 Onoruame Bot arrancando...")
    bot.infinity_polling()
