# ==============================================================================
# File: link-scraper-bot/database/mongo_db.py
# Description: Handles all database interactions with MongoDB.
# ==============================================================================

import os
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, OperationFailure
from loguru import logger
from core.config import settings

# --- Database Class ---
class Database:
    """ A singleton class for all database operations """
    client: AsyncIOMotorClient = None
    db = None

    @staticmethod
    async def initialize():
        """
        Initializes the database connection using settings from the config.
        This method is idempotent and can be called multiple times safely.
        """
        if Database.client and Database.db:
            return

        if not settings.MONGO_URI:
            logger.critical("❌ MONGO_URI is not set in the environment or .env file. Exiting.")
            raise ValueError("MONGO_URI is not configured.")

        logger.info(f"🔌 Connecting to MongoDB database: {settings.MONGO_DB_NAME}")
        try:
            Database.client = AsyncIOMotorClient(settings.MONGO_URI)
            # The ismaster command is cheap and does not require auth.
            await Database.client.admin.command('ismaster')
            Database.db = Database.client[settings.MONGO_DB_NAME]
            logger.success("✅ MongoDB connection successful.")
        except ConnectionFailure as e:
            logger.critical(f"💥 MongoDB connection failed: {e}")
            raise

    @staticmethod
    async def add_processed_post(url: str, content_hash: str, link_count: int):
        """
        Adds or updates a record of a processed post in the database.
        This prevents reprocessing unchanged posts and tracks updates.
        """
        if not Database.db:
            await Database.initialize()

        try:
            await Database.db.processed_posts.update_one(
                {"url": url},
                {
                    "$set": {
                        "content_hash": content_hash,
                        "link_count": link_count,
                        "processed_at": datetime.now()
                    }
                },
                upsert=True
            )
        except OperationFailure as e:
            logger.error(f"DB Error | Could not add or update processed post for {url}: {e}")

    @staticmethod
    async def is_url_processed(url: str) -> bool:
        """
        Checks if a URL has ever been processed by looking it up in the database.
        """
        if not Database.db:
            await Database.initialize()
        try:
            post = await Database.db.processed_posts.find_one({"url": url})
            return bool(post)
        except OperationFailure as e:
            logger.error(f"DB Error | Could not check if URL is processed {url}: {e}")
            return False

    @staticmethod
    async def get_post_hash(url: str) -> str | None:
        """
        Retrieves the stored content hash for a given URL to check for updates.
        """
        if not Database.db:
            await Database.initialize()
        try:
            post = await Database.db.processed_posts.find_one({"url": url})
            return post.get("content_hash") if post else None
        except OperationFailure as e:
            logger.error(f"DB Error | Could not retrieve post hash for {url}: {e}")
            return None

    @staticmethod
    async def was_recently_processed(url: str, hours: int) -> bool:
        """
        Checks if a URL was processed within a given number of hours. This is
        used to prevent re-scraping the same post in rapid succession.
        """
        if not Database.db:
            await Database.initialize()

        try:
            time_threshold = datetime.now() - timedelta(hours=hours)
            post = await Database.db.processed_posts.find_one({
                "url": url,
                "processed_at": {"$gte": time_threshold}
            })
            return bool(post)
        except OperationFailure as e:
            logger.error(f"DB Error | Could not check if URL was recently processed {url}: {e}")
            return False  # Assume not processed on error to be safe

    @staticmethod
    async def get_all_channels() -> list:
        """
        Retrieves a list of all approved channels for message broadcasting.
        """
        if not Database.db:
            await Database.initialize()
        try:
            cursor = Database.db.channels.find({"approved": True})
            return await cursor.to_list(length=None)
        except OperationFailure as e:
            logger.error(f"DB Error | Could not fetch approved channels: {e}")
            return []

    @staticmethod
    async def close_connection():
        """
        Closes the connection to the MongoDB client.
        """
        if Database.client:
            Database.client.close()
            logger.info("MongoDB connection has been closed.")
