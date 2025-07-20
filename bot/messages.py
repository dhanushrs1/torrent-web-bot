# ==============================================================================
# File: link-scraper-bot/bot/messages.py
# Description: Handles formatting and sending messages via Telegram. (FINAL FIX)
# ==============================================================================

from telegram import Bot
from telegram.constants import ParseMode
from loguru import logger
import re
import asyncio
import io

from database.mongo_db import Database

def escape_markdown_v2(text: str) -> str:
    text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

async def format_and_send_links(bot: Bot, chat_id: int, post_title: str, links: list, status: str, quality_tags: list = None, metadata: dict = None):
    """
    Formats scraped links and sends them to a Telegram channel.
    Includes status, quality, and metadata tags in the header.
    """
    if not links:
        logger.warning("format_and_send_links called with no links to send.")
        return

    torrent_links = [link for link in links if not link.get('url', '').startswith('magnet:')]
    if not torrent_links:
        logger.info(f"No .torrent files found for post '{post_title}'. Nothing to send.")
        return

    # --- Build the tag string ---
    all_tags = []
    if status == "new":
        all_tags.append("\\#new\\_feed")
    elif status == "updated":
        all_tags.append("\\#updated\\_feed")

    if quality_tags:
        all_tags.extend([escape_markdown_v2(tag) for tag in quality_tags])
    
    # Add metadata tags if they exist
    if metadata:
        if metadata.get('language_tags'):
            # FIX: Escape each language tag to prevent parsing errors
            all_tags.extend([f"\\#{escape_markdown_v2(lang)}" for lang in metadata['language_tags']])
        if metadata.get('file_sizes'):
            # FIX: Escape each file size tag to prevent parsing errors from characters like '.'
            all_tags.extend([f"\\#{escape_markdown_v2(size)}" for size in metadata['file_sizes']])

    tags_string = " ".join(all_tags)
    
    header_message = f"*{escape_markdown_v2(post_title)}*\n\n{tags_string}"
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=header_message,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Failed to send header message to {chat_id}: {e}")
        return

    prefix_enabled = await Database.is_prefix_enabled()
    prefix_text = await Database.get_prefix() if prefix_enabled else ""

    for link in torrent_links:
        try:
            link_title = link.get('title', 'No Title')
            link_url = link.get('url', 'No URL')
            
            final_url = f"{prefix_text} {link_url}" if prefix_enabled and prefix_text else link_url
            message_body = f"_{escape_markdown_v2(link_title)}_\n`{escape_markdown_v2(final_url)}`"

            await bot.send_message(
                chat_id=chat_id,
                text=message_body,
                parse_mode=ParseMode.MARKDOWN_V2,
                disable_web_page_preview=True
            )
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Failed to send link message for '{link.get('title')}' to {chat_id}: {e}", exc_info=True)
