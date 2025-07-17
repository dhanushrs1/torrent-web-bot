# ==============================================================================
# File: link-scraper-bot/core/config.py
# Description: Loads and validates all configuration from the .env file.
# ==============================================================================

import os
from dotenv import load_dotenv
from loguru import logger

# Load environment variables from .env file
load_dotenv()

class Settings:
    """ Class to hold all application settings. """
    def __init__(self):
        logger.info("Loading application settings...")
        self.TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        self.MONGO_DB_URI = os.getenv("MONGO_DB_URI")
        self.ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", 0))
        self.PROXY_URL = os.getenv("PROXY_URL")
        self.TARGET_WEBSITE_URL = os.getenv("TARGET_WEBSITE_URL")

        self._validate()
        logger.info("Settings loaded and validated successfully.")

    def _validate(self):
        """ Ensures that all necessary environment variables are set. """
        required_vars = [
            "TELEGRAM_BOT_TOKEN", "MONGO_DB_URI", "ADMIN_TELEGRAM_ID",
            "PROXY_URL", "TARGET_WEBSITE_URL"
        ]
        for var in required_vars:
            if not getattr(self, var):
                error_msg = f"Missing required environment variable: {var}"
                logger.error(error_msg)
                raise ValueError(error_msg)

# Create a single instance of the settings to be imported across the app
settings = Settings()
