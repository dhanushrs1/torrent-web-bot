# ==============================================================================
# File: link-scraper-bot/bot/bot_instance.py
# Description: Initializes the Telegram bot application.
# ==============================================================================

from telegram.ext import Application
from core.config import settings
from .handlers import get_handlers

def create_bot_app():
    """ Creates and configures the Telegram bot Application instance. """
    application = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    
    handlers = get_handlers()
    for handler in handlers:
        application.add_handler(handler)
        
    return application
