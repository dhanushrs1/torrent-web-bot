# ==============================================================================
# File: link-scraper-bot/bot/prefix_handler.py
# Description: Handles all logic related to the link prefix feature. (NEW FILE)
# ==============================================================================

from telegram import Update
from telegram.ext import ContextTypes
from loguru import logger

from database.mongo_db import Database
from .messages import escape_markdown_v2

async def set_prefix_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Allows the admin to set a custom prefix for links. """
    if not context.args:
        await update.message.reply_text("Usage: /setprefix <your_prefix>\nExample: /setprefix /ql")
        return

    new_prefix = context.args[0]
    await Database.set_prefix(new_prefix)
    logger.info(f"Admin set a new prefix: {new_prefix}")
    await update.message.reply_text(f"✅ Prefix has been set to: `{escape_markdown_v2(new_prefix)}`", parse_mode='MarkdownV2')

async def toggle_prefix_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Allows the admin to enable or disable the prefix feature. """
    if not context.args or context.args[0].lower() not in ['on', 'off']:
        await update.message.reply_text("Usage: /toggleprefix <on|off>")
        return

    is_enabled = context.args[0].lower() == 'on'
    await Database.toggle_prefix(is_enabled)
    status = "ENABLED" if is_enabled else "DISABLED"
    logger.info(f"Admin {status} the prefix feature.")
    await update.message.reply_text(f"✅ Prefix feature has been *{status}*.", parse_mode='MarkdownV2')

async def prefix_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Displays the current status of the prefix feature. """
    is_enabled = await Database.is_prefix_enabled()
    prefix = await Database.get_prefix()
    
    status_text = "ENABLED" if is_enabled else "DISABLED"
    prefix_text = f"`{escape_markdown_v2(prefix)}`" if prefix else "Not set"

    message = (
        f"*Prefix Status*\n"
        f"\\- *Status:* {status_text}\n"
        f"\\- *Current Prefix:* {prefix_text}"
    )
    await update.message.reply_text(message, parse_mode='MarkdownV2')
