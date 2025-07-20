# ==============================================================================
# File: link-scraper-bot/database/mongo_db.py
# Description: Handles all interactions with the MongoDB database. (STATS UPDATE)
# ==============================================================================

from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from core.config import settings
from datetime import datetime, timezone

class Database:
    """ Singleton class for MongoDB connection and operations. """
    _client = None
    _settings_collection = "settings"

    @classmethod
    def get_client(cls):
        if cls._client is None:
            logger.info("Initializing MongoDB client...")
            cls._client = AsyncIOMotorClient(settings.MONGO_DB_URI)
        return cls._client

    @classmethod
    async def get_db(cls):
        return cls.get_client().link_scraper_bot

    # --- Channel Methods ---
    @classmethod
    async def add_channel(cls, channel_id: int, channel_name: str):
        db = await cls.get_db()
        is_first_channel = await db.channels.count_documents({}) == 0
        await db.channels.update_one(
            {'channel_id': channel_id},
            {'$set': {'channel_name': channel_name, 'is_main': is_first_channel}},
            upsert=True
        )
        logger.info(f"Added/Updated channel: {channel_name} ({channel_id}). Main: {is_first_channel}")
        return is_first_channel

    @classmethod
    async def get_main_channel(cls):
        db = await cls.get_db()
        return await db.channels.find_one({'is_main': True})

    @classmethod
    async def set_main_channel(cls, channel_id: int):
        db = await cls.get_db()
        await db.channels.update_many({'is_main': True}, {'$set': {'is_main': False}})
        result = await db.channels.update_one({'channel_id': channel_id}, {'$set': {'is_main': True}})
        if result.modified_count:
            logger.info(f"Set channel {channel_id} as the new main channel.")
            return True
        return False

    @classmethod
    async def get_all_channels(cls):
        db = await cls.get_db()
        return await db.channels.find({}).to_list(length=100)

    # --- Post Methods ---
    @classmethod
    async def is_url_processed(cls, post_url: str) -> bool:
        db = await cls.get_db()
        return await db.posts.count_documents({'post_url': post_url}) > 0

    @classmethod
    async def add_processed_post(cls, post_url: str, content_hash: str, link_count: int):
        db = await cls.get_db()
        await db.posts.update_one(
            {'post_url': post_url},
            {
                '$set': {'content_hash': content_hash, 'processed_at': datetime.now(timezone.utc)},
                '$inc': {'link_count': link_count} # Increment link count
            },
            upsert=True
        )
        logger.info(f"Added/Updated post in DB: {post_url}")
        
    @classmethod
    async def get_post_hash(cls, post_url: str) -> str | None:
        db = await cls.get_db()
        post = await db.posts.find_one({'post_url': post_url})
        return post.get('content_hash') if post else None

    @classmethod
    async def get_total_links(cls) -> int:
        """ Calculates the total number of links processed. """
        db = await cls.get_db()
        pipeline = [{"$group": {"_id": None, "total": {"$sum": "$link_count"}}}]
        result = await db.posts.aggregate(pipeline).to_list(length=1)
        return result[0]['total'] if result else 0

    # --- Prefix Setting Methods ---
    @classmethod
    async def _get_settings(cls):
        db = await cls.get_db()
        return await db[cls._settings_collection].find_one({'_id': 'prefix_config'})

    @classmethod
    async def set_prefix(cls, prefix: str):
        db = await cls.get_db()
        await db[cls._settings_collection].update_one(
            {'_id': 'prefix_config'},
            {'$set': {'prefix': prefix}},
            upsert=True
        )

    @classmethod
    async def toggle_prefix(cls, is_enabled: bool):
        db = await cls.get_db()
        await db[cls._settings_collection].update_one(
            {'_id': 'prefix_config'},
            {'$set': {'enabled': is_enabled}},
            upsert=True
        )

    @classmethod
    async def get_prefix(cls) -> str | None:
        settings = await cls._get_settings()
        return settings.get('prefix') if settings else None

    @classmethod
    async def is_prefix_enabled(cls) -> bool:
        settings = await cls._get_settings()
        return settings.get('enabled', False) if settings else False
        

async def was_recently_processed(self, url: str, hours: int = 2) -> bool:
    """Check if URL was processed in the last N hours"""
    recent_time = datetime.utcnow() - timedelta(hours=hours)
    result = await self.processed_posts.find_one({
        "url": url,
        "processed_at": {"$gte": recent_time}
    })
    return result is not None
