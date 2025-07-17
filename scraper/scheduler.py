# ==============================================================================
# File: link-scraper-bot/scraper/scheduler.py
# Description: Manages the scheduled task of checking the website. (FIXED)
# ==============================================================================

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from telegram import Bot
import re

from .engine import ScraperEngine
from database.mongo_db import Database
from bot.messages import format_and_send_links

def escape_markdown_v2(text: str) -> str:
    """Escapes text for Telegram's MarkdownV2 parse mode."""
    text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

async def check_website_job(bot: Bot):
    """ The core job that runs on a schedule. Now sends to ALL approved channels. """
    logger.info("--- Starting scheduled website check ---")
    
    # Get ALL approved channels instead of just the main one.
    all_channels = await Database.get_all_channels()
    if not all_channels:
        logger.warning("No approved channels configured. Skipping scrape job.")
        return

    scraper = ScraperEngine()
    latest_post_urls = await scraper.find_latest_posts()

    for post_url in reversed(latest_post_urls): # Process oldest first
        try:
            result = await scraper.scrape_post(post_url)
            if not result or not result[0]:
                logger.warning(f"No links found for {post_url}, skipping.")
                continue
            
            links, new_hash = result
            
            # Use the first part of the URL path as a title fallback
            post_title_fallback = post_url.split('/')[-2].replace('-', ' ').title()
            post_title = links[0]['title'].split(' (')[0] if links else post_title_fallback
            
            is_processed = await Database.is_url_processed(post_url)
            status = ""

            if not is_processed:
                status = "new"
                logger.info(f"Found new post: {post_url}")
            else:
                old_hash = await Database.get_post_hash(post_url)
                if old_hash != new_hash:
                    status = "updated"
                    logger.info(f"Found updated post: {post_url}")
                else:
                    logger.info(f"Post {post_url} is unchanged. Skipping.")
                    continue
            
            # If the post is new or updated, loop through all channels and send the links.
            if status:
                for channel in all_channels:
                    channel_id = channel['channel_id']
                    logger.info(f"Sending links for '{post_title}' to channel ID: {channel_id}")
                    await format_and_send_links(bot, channel_id, post_title, links, status)
                
                # Mark the post as processed once it has been sent to all channels.
                await Database.add_processed_post(post_url, new_hash)

        except Exception as e:
            logger.error(f"Error processing post {post_url}: {e}", exc_info=True)

    logger.info("--- Finished scheduled website check ---")


def setup_scheduler(bot: Bot):
    """ Initializes and starts the job scheduler. """
    scheduler = AsyncIOScheduler(timezone="UTC")
    # Schedule the job to run every 15 minutes. Adjust as needed.
    scheduler.add_job(check_website_job, 'interval', minutes=15, args=[bot])
    scheduler.start()
    logger.info("Scheduler started. Website will be checked every 15 minutes.")
