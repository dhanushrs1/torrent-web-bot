# ==============================================================================
# File: link-scraper-bot/scraper/scheduler.py  
# Description: Improved scheduler with date/time filtering and efficiency
# ==============================================================================

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from telegram import Bot
import re
from datetime import datetime, timedelta
from .engine import ScraperEngine
from database.mongo_db import Database
from bot.messages import format_and_send_links

async def check_website_job(bot: Bot):
    """ 
    Optimized job that focuses on recent posts only for maximum efficiency.
    """
    logger.info("🚀 Starting Automatic Website Check")
    start_time = datetime.now()
    
    # Get configured channels
    all_channels = await Database.get_all_channels()
    if not all_channels:
        logger.warning("⚠️ No approved channels configured. Skipping job.")
        return
    
    logger.info(f"📢 Will send updates to {len(all_channels)} channels")
    
    # Initialize scraper with date filtering
    scraper = ScraperEngine()
    
    # Get only recent posts (last 24 hours for frequent checks, 48 hours for safety)
    check_hours = 24  # Adjust based on your check frequency
    latest_post_urls = await scraper.find_latest_posts(max_posts=15, hours_filter=check_hours)
    
    if not latest_post_urls:
        logger.warning(f"📭 No recent posts found in last {check_hours} hours")
        return
    
    logger.info(f"🔍 Processing {len(latest_post_urls)} recent posts")
    
    processed_count = 0
    new_posts_count = 0
    updated_posts_count = 0
    
    for i, post_url in enumerate(latest_post_urls, 1):
        try:
            logger.info(f"🔄 [{i}/{len(latest_post_urls)}] Processing: {post_url}")
            
            # Quick check if URL was recently processed (last 2 hours) to avoid duplicates
            if await Database.was_recently_processed(post_url, hours=2):
                logger.info("⏭️ Recently processed, skipping")
                continue
            
            # Scrape the post
            result = await scraper.scrape_post(post_url)
            
            if not result or not result[0]:
                logger.warning("❌ No download links found, skipping")
                continue
            
            links, new_hash, quality_tags, metadata = result
            processed_count += 1
            
            # Extract post title
            post_title_fallback = post_url.split('/')[-2].replace('-', ' ').title()
            post_title = links[0]['title'].split(' (')[0] if links else post_title_fallback
            
            # Check if post is new or updated
            is_processed = await Database.is_url_processed(post_url)
            status = ""
            
            if not is_processed:
                status = "new"
                new_posts_count += 1
                logger.success(f"🆕 NEW POST: {post_title}")
            else:
                old_hash = await Database.get_post_hash(post_url)
                if old_hash != new_hash:
                    status = "updated"
                    updated_posts_count += 1
                    logger.success(f"🔄 UPDATED POST: {post_title}")
                else:
                    logger.info("✅ Post unchanged, skipping")
                    continue
            
            # Send to all channels if new or updated
            if status:
                send_tasks = []
                for channel in all_channels:
                    channel_id = channel['channel_id']
                    logger.info(f"📤 Sending to channel: {channel_id}")
                    
                    try:
                        await format_and_send_links(
                            bot, channel_id, post_title, links, 
                            status, quality_tags, metadata
                        )
                    except Exception as send_error:
                        logger.error(f"❌ Failed to send to channel {channel_id}: {send_error}")
                        continue
                
                # Update database
                await Database.add_processed_post(post_url, new_hash, len(links))
                logger.success(f"✅ Successfully processed: {post_title}")
                
        except Exception as e:
            logger.error(f"💥 Error processing {post_url}: {e}", exc_info=True)
            continue
    
    # Final summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.success(f"""
🏁 Website Check Complete!
⏱️ Duration: {duration:.1f}s
📊 Stats:
  • Posts checked: {len(latest_post_urls)}
  • Posts processed: {processed_count}  
  • New posts: {new_posts_count}
  • Updated posts: {updated_posts_count}
  • Channels notified: {len(all_channels)}
""")

def setup_scheduler(bot: Bot):
    """ 
    Initialize scheduler with optimized settings.
    """
    scheduler = AsyncIOScheduler(timezone="UTC")
    
    # Check every 10 minutes for faster updates (you can adjust this)
    scheduler.add_job(
        check_website_job, 
        'interval', 
        minutes=10,  # Reduced from 15 to 10 for faster updates
        args=[bot], 
        id="main_check_job",
        max_instances=1  # Prevent overlapping jobs
    )
    
    scheduler.start()
    logger.success("🕒 Scheduler started - checking every 10 minutes for recent posts")
    
    return scheduler
