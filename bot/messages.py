# ==============================================================================
# File: link-scraper-bot/bot/messages.py
# Description: Handles formatting and sending messages via Telegram. (PREFIX UPDATE)
# ==============================================================================

from telegram import Bot
from telegram.constants import ParseMode
from loguru import logger
import re
import asyncio
import io

from database.mongo_db import Database

def escape_markdown_v2(text: str) -> str:
    """Escapes text for Telegram's MarkdownV2 parse mode."""
    text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

async def format_and_send_links(bot: Bot, chat_id: int, post_title: str, links: list, status: str):
    """
    Formats scraped links and sends them to a Telegram channel.
    Applies a prefix to the link if the feature is enabled.
    """
    if not links:
        logger.warning("format_and_send_links called with no links to send.")
        return

    torrent_links = [link for link in links if not link.get('url', '').startswith('magnet:')]
    if not torrent_links:
        logger.info(f"No .torrent files found for post '{post_title}'. Nothing to send.")
        return

    tag = "\\#new\\_feed" if status == "new" else "\\#updated\\_feed"
    header_message = f"*{escape_markdown_v2(post_title)}*\n\n{tag}"
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

    # Get prefix settings from the database
    prefix_enabled = await Database.is_prefix_enabled()
    prefix_text = await Database.get_prefix() if prefix_enabled else ""

    for link in torrent_links:
        try:
            link_title = link.get('title', 'No Title')
            link_url = link.get('url', 'No URL')
            
            # Construct the final URL with the prefix if enabled
            final_url = f"{prefix_text} {link_url}" if prefix_enabled and prefix_text else link_url

            # Clean, readable MarkdownV2 format
            message_body = (
                f"*🎬 Title:*\n{escape_markdown_v2(link_title)}\n\n"
                f"*🔗 Link:*\n`{escape_markdown_v2(final_url)}`"
            )

            await bot.send_message(
                chat_id=chat_id,
                text=message_body,
                parse_mode=ParseMode.MARKDOWN_V2,
                disable_web_page_preview=True
            )
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Failed to send link message for '{link.get('title')}' to {chat_id}: {e}", exc_info=True)
