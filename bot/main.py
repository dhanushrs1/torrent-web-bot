# ==============================================================================
# File: link-scraper-bot/bot/main.py
# Description: Main entry point to start the bot and scheduler. (FIXED)
# ==============================================================================

import asyncio
from loguru import logger

from core.logger import setup_logger
from bot.bot_instance import create_bot_app
from scraper.scheduler import setup_scheduler

async def main():
    """ Main asynchronous function to initialize and run the bot and scheduler. """
    setup_logger()
    
    try:
        app = create_bot_app()
        
        # Initialize the application but don't start polling yet
        await app.initialize()
        
        # Setup and start the scheduler
        setup_scheduler(app.bot)

        logger.info("Starting bot polling...")
        # Start the bot
        await app.start()
        # Start polling for updates
        await app.updater.start_polling()

        # Keep the script running
        while True:
            await asyncio.sleep(3600) # Sleep for an hour, or any long duration

    except Exception as e:
        logger.critical(f"A critical error occurred in the main async loop: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except Exception as e:
        logger.critical(f"Failed to start the bot: {e}")
