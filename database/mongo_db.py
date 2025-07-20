# ==============================================================================
# File: link-scraper-bot/database/mongo_db.py
# Description: Handles all database interactions with MongoDB.
# ==============================================================================

import os
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, OperationFailure
from loguru import logger

# --- Environment Variables ---
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "link-scraper")

# --- Database Class ---
class Database:
    """ A singleton class for all database operations """
    client: AsyncIOMotorClient = None
    db = None

    @staticmethod
    async def initialize():
        """
        Initializes the database connection and collections.
        """
        if Database.client and Database.db:
            return

        if not MONGO_URI:
            logger.critical("❌ MONGO_URI environment variable not set. Exiting.")
            raise ValueError("MONGO_URI is not configured.")

        logger.info(f"🔌 Connecting to MongoDB database: {MONGO_DB_NAME}")
        try:
            Database.client = AsyncIOMotorClient(MONGO_URI)
            # Ping the server to verify connection
            await Database.client.admin.command('ping')
            Database.db = Database.client[MONGO_DB_NAME]
            logger.success("✅ MongoDB connection successful.")
        except ConnectionFailure as e:
            logger.critical(f"💥 MongoDB connection failed: {e}")
            raise

    @staticmethod
    async def add_processed_post(url: str, content_hash: str, link_count: int):
        """
        Adds or updates a processed post in the database.
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
            logger.error(f"Error updating processed post in DB: {e}")

    @staticmethod
    async def is_url_processed(url: str) -> bool:
        """
        Checks if a URL has ever been processed.
        """
        if not Database.db:
            await Database.initialize()
        post = await Database.db.processed_posts.find_one({"url": url})
        return bool(post)

    @staticmethod
    async def get_post_hash(url: str) -> str | None:
        """
        Retrieves the content hash for a given URL.
        """
        if not Database.db:
            await Database.initialize()
        post = await Database.db.processed_posts.find_one({"url": url})
        return post.get("content_hash") if post else None

    @staticmethod
    async def was_recently_processed(url: str, hours: int) -> bool:
        """
        Checks if a URL was processed within the last X hours to avoid duplicates.
        """
        if not Database.db:
            await Database.initialize()

        try:
            # Calculate the time threshold to check against
            time_threshold = datetime.now() - timedelta(hours=hours)

            # Check if a post with the same URL exists and was processed recently
            post = await Database.db.processed_posts.find_one({
                "url": url,
                "processed_at": {"$gte": time_threshold}
            })
            return bool(post)
        except OperationFailure as e:
            logger.error(f"Error checking if URL was recently processed: {e}")
            return False  # Assume not processed on error

    @staticmethod
    async def get_all_channels() -> list:
        """
        Retrieves all approved channel configurations.
        """
        if not Database.db:
            await Database.initialize()
        try:
            cursor = Database.db.channels.find({"approved": True})
            return await cursor.to_list(length=None)
        except OperationFailure as e:
            logger.error(f"Error fetching channels from DB: {e}")
            return []

    @staticmethod
    async def close_connection():
        """
        Closes the database connection.
        """
        if Database.client:
            Database.client.close()
            logger.info("MongoDB connection closed.")
