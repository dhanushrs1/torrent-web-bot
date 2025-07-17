# ==============================================================================
# File: link-scraper-bot/core/logger.py
# Description: Configures the application-wide logger.
# ==============================================================================

import sys
import os
from loguru import logger

def setup_logger():
    """ Configures Loguru logger for the application. """
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(
        "logs/bot.log",
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    logger.info("Logger has been configured.")
