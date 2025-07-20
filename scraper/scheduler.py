# ==============================================================================
# File: link-scraper-bot/scraper/scheduler.py
# Description: Manages the scheduled task of checking the website. (DEBUG FIX)
# ==============================================================================

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from telegram import Bot
import re

from .engine import ScraperEngine
from database.mongo_db import Database
from bot.messages import format_and_send_links

def escape_markdown_v2(text: str) -> str:
    text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

async def check_website_job(bot: Bot):
    """ The core job that runs on a schedule. Now with detailed logging. """
    logger.info("--- Starting Automatic Website Check ---")
    
    all_channels = await Database.get_all_channels()
    if not all_channels:
        logger.warning("SCHEDULER: No approved channels configured. Skipping job.")
        return

    scraper = ScraperEngine()
    latest_post_urls = await scraper.find_latest_posts()

    # --- NEW: Log exactly what was found on the main page ---
    if not latest_post_urls:
        logger.warning("SCHEDULER: Found 0 post URLs on the main page. The scraper's `find_latest_posts` method might need an update if this persists.")
        return
    else:
        logger.info(f"SCHEDULER: Found {len(latest_post_urls)} post URLs on the main page.")
    # --- END NEW ---

    for post_url in reversed(latest_post_urls):
        try:
            logger.info(f"SCHEDULER: Processing URL -> {post_url}")
            result = await scraper.scrape_post(post_url)
            
            if not result or not result[0]:
                logger.warning(f"SCHEDULER: No links found for {post_url}, skipping.")
                continue
            
            links, new_hash, quality_tags = result
            
            post_title_fallback = post_url.split('/')[-2].replace('-', ' ').title()
            post_title = links[0]['title'].split(' (')[0] if links else post_title_fallback
            
            is_processed = await Database.is_url_processed(post_url)
            status = ""

            if not is_processed:
                status = "new"
                logger.success(f"SCHEDULER: Found NEW post: {post_url}")
            else:
                old_hash = await Database.get_post_hash(post_url)
                if old_hash != new_hash:
                    status = "updated"
                    logger.success(f"SCHEDULER: Found UPDATED post: {post_url}")
                else:
                    # This is the normal case for posts already processed
                    logger.info(f"SCHEDULER: Post is unchanged. Skipping: {post_url}")
                    continue
            
            if status:
                for channel in all_channels:
                    channel_id = channel['channel_id']
                    logger.info(f"SCHEDULER: Sending links for '{post_title}' to channel ID: {channel_id}")
                    await format_and_send_links(bot, channel_id, post_title, links, status, quality_tags)
                
                await Database.add_processed_post(post_url, new_hash)

        except Exception as e:
            logger.error(f"SCHEDULER: An error occurred while processing post {post_url}: {e}", exc_info=True)

    logger.info("--- Finished Automatic Website Check ---")


def setup_scheduler(bot: Bot):
    """ Initializes and starts the job scheduler. """
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(check_website_job, 'interval', minutes=15, args=[bot], id="main_check_job")
    scheduler.start()
    logger.info("Scheduler started. Website will be checked every 15 minutes.")
