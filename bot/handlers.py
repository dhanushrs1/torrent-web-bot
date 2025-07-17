# ==============================================================================
# File: link-scraper-bot/bot/handlers.py
# Description: Defines all command, message, and callback query handlers. (COMPLETE)
# ==============================================================================

from telegram import Update, ChatMember
from telegram.constants import ChatType
from telegram.ext import (
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)
from loguru import logger
import re
import os
import time
import psutil
from tenacity import retry, stop_after_attempt, wait_fixed

from core.config import settings
from database.mongo_db import Database
from .keyboards import (
    get_channel_approval_keyboard, 
    get_channel_management_keyboard, 
    get_start_keyboard,
    get_prefix_settings_keyboard
)
from scraper.engine import ScraperEngine
from .messages import format_and_send_links, escape_markdown_v2

# --- Admin Filter ---
admin_filter = filters.User(user_id=settings.ADMIN_TELEGRAM_ID)

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = get_start_keyboard()

    if user_id == settings.ADMIN_TELEGRAM_ID:
        # Admin-specific welcome message
        text = (
            "👑 *Welcome, Admin*\!\n\n"
            "This is your control panel\. You have access to special commands to manage the bot\.\n\n"
            "`/channels` \- Manage authorized channels\.\n"
            "`/prefixsettings` \- Configure link prefixes\.\n"
            "`/test <url>` \- Scrape a single URL for testing\.\n"
            "`/status` \- Show server and bot status\.\n"
            "`/stats` \- Show bot statistics\.\n"
            "`/log` \- Show recent log lines\.\n"
            "`/help` \- Shows detailed command info\."
        )
    else:
        # Regular user welcome message
        text = (
            "👋 *Welcome to the Link Scraper Bot*\!\n\n"
            "This bot automatically posts new file links into our channels\.\n\n"
            "Check out our channels below to get the latest updates\!"
        )
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode='MarkdownV2')


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Provides a detailed help message for the admin. """
    help_text = (
        "*Admin Commands:*\n"
        "`/test <url>` \- Scrapes a single URL for testing\.\n"
        "`/channels` \- Opens the channel management panel\.\n"
        "`/prefixsettings` \- Opens the prefix management panel\.\n"
        "`/status` \- Shows server and bot status\.\n"
        "`/stats` \- Shows bot statistics\.\n"
        "`/log` \- Shows recent log lines\.\n"
        "`/help` \- Shows this help message\.\n\n"
        "*How to Authorize a New Channel:*\n"
        "1\. Add me to your channel and promote me to an *Administrator*\.\n"
        "2\. Forward *any message* from that channel to me here in this private chat\."
    )
    await update.message.reply_text(help_text, parse_mode='MarkdownV2')

async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Scrapes a single URL for testing purposes. """
    if not context.args:
        await update.message.reply_text("Please provide a URL to test. Usage: /test <url>")
        return
    url = context.args[0]
    await update.message.reply_text(f"Testing scraper on: {escape_markdown_v2(url)}", parse_mode='MarkdownV2')
    scraper = ScraperEngine()
    result = await scraper.scrape_post(url)
    if result and result[0]:
        links, content_hash, quality_tags, meta = result
        # Show advanced info, escape all dynamic values
        await update.message.reply_text(
            escape_markdown_v2(
                f"Scraping successful! Found {len(links)} links.\n"
                f"Content hash: `{content_hash}`\n"
                f"Quality tags: {', '.join(quality_tags) if quality_tags else 'None'}\n"
                f"Languages: {', '.join(meta.get('language_tags', [])) if meta.get('language_tags') else 'None'}\n"
                f"Sizes: {', '.join(str(s) + 'MB' for s in meta.get('file_sizes', [])) if meta.get('file_sizes') else 'None'}\nSending file links now..."
            ),
            parse_mode='MarkdownV2'
        )
        post_title = "Test Scrape Results"
        await format_and_send_links(
            bot=context.bot,
            chat_id=update.effective_chat.id,
            post_title=post_title,
            links=links,
            status="new"
        )
    else:
        await update.message.reply_text("Scraping failed. No links found or an error occurred. Check logs.")

async def channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Displays the channel management panel. """
    channels = await Database.get_all_channels()
    if not channels:
        await update.message.reply_text("No channels have been authorized yet.")
        return
    main_channel = await Database.get_main_channel()
    main_channel_id = main_channel['channel_id'] if main_channel else None
    keyboard = get_channel_management_keyboard(channels, main_channel_id)
    await update.message.reply_text("Authorized Channels:", reply_markup=keyboard)

async def prefix_settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Displays the prefix settings panel. """
    is_enabled = await Database.is_prefix_enabled()
    prefix = await Database.get_prefix()
    keyboard = get_prefix_settings_keyboard(is_enabled, prefix)
    await update.message.reply_text("Manage the link prefix settings below:", reply_markup=keyboard)

# --- Outgoing Message Logging & Retry Decorator ---
START_TIME = time.time()

def log_and_retry(func):
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def wrapper(*args, **kwargs):
        try:
            result = await func(*args, **kwargs)
            logger.info(f"Outgoing message: {func.__name__} args={args} kwargs={kwargs}")
            return result
        except Exception as e:
            logger.error(f"Error in outgoing message: {func.__name__}: {e}")
            raise
    return wrapper

async def global_error_handler(update, context):
    logger.error(f"Global error: {context.error}")
    try:
        await update.message.reply_text("An unexpected error occurred. Please contact the admin.")
    except Exception:
        pass

@log_and_retry
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows server/bot status (uptime, memory, etc)."""
    uptime = time.time() - START_TIME
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent()
    text = (
        f"*Server Status:*\n"
        f"Uptime: `{int(uptime // 60)} min {int(uptime % 60)} sec`\n"
        f"Memory: `{mem.percent}%` used\n"
        f"CPU: `{cpu}%`\n"
        f"Bot PID: `{os.getpid()}`"
    )
    await update.message.reply_text(escape_markdown_v2(text), parse_mode='MarkdownV2')

@log_and_retry
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows bot stats (total links scraped, channels, etc)."""
    channels = await Database.get_all_channels()
    total_channels = len(channels)
    try:
        total_links = await Database.get_total_links()
    except Exception:
        total_links = 'N/A'
    text = (
        f"*Bot Stats:*\n"
        f"Total Channels: `{total_channels}`\n"
        f"Total Links Scraped: `{total_links}`"
    )
    await update.message.reply_text(escape_markdown_v2(text), parse_mode='MarkdownV2')

@log_and_retry
async def log_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows recent log lines to admin."""
    log_path = os.path.join(os.path.dirname(__file__), '../../bot.log')
    log_path = os.path.abspath(log_path)
    if not os.path.exists(log_path):
        await update.message.reply_text("No log file found yet. Logging will start after the first error or event.")
        return
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()[-20:]
        text = "Recent Logs:\n" + "\n".join(lines)
    except Exception as e:
        text = f"Could not read log file: {e}"
    await update.message.reply_text(f"<pre>{text}</pre>", parse_mode='HTML')

# --- Message Handlers ---

@log_and_retry
async def forwarded_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """ Handles forwarded messages from the admin to authorize a channel. """
    forward_origin = update.message.forward_origin
    if not forward_origin or forward_origin.type != ChatType.CHANNEL:
        await update.message.reply_text("Please forward a message directly from the channel you wish to authorize.")
        return
    channel = forward_origin.chat
    logger.info(f"Admin forwarded a message from channel '{channel.title}' ({channel.id}) for authorization check.")
    try:
        bot_member = await context.bot.get_chat_member(chat_id=channel.id, user_id=context.bot.id)
        if bot_member.status == ChatMember.ADMINISTRATOR:
            keyboard = get_channel_approval_keyboard(channel.id)
            await update.message.reply_text(
                f"I am an admin in *{escape_markdown_v2(channel.title)}*\\.\n\nPlease approve or deny my participation in this channel\\.",
                reply_markup=keyboard,
                parse_mode='MarkdownV2'
            )
        else:
            await update.message.reply_text(f"I am not an admin in *{escape_markdown_v2(channel.title)}*\\. Please promote me and forward the message again\\.", parse_mode='MarkdownV2')
    except Exception as e:
        logger.error(f"Failed to check bot status in channel {channel.id}: {e}")
        await update.message.reply_text(f"Could not verify my status in the channel *{escape_markdown_v2(channel.title)}*\\.", parse_mode='MarkdownV2')

async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Handles text messages from the admin, specifically for setting the prefix via reply. """
    # Check if this message is a reply and if it's replying to our specific prompt
    if update.message.reply_to_message and context.user_data.get('awaiting_prefix_message_id') == update.message.reply_to_message.message_id:
        new_prefix = update.message.text
        await Database.set_prefix(new_prefix)
        
        # Clear the flag now that we've received the reply
        context.user_data['awaiting_prefix_message_id'] = None
        logger.info(f"Admin set new prefix via reply: {new_prefix}")
        await update.message.reply_text(f"✅ Prefix has been updated to: `{escape_markdown_v2(new_prefix)}`", parse_mode='MarkdownV2')
        
        # Show the updated settings panel
        await prefix_settings_command(update, context)

# --- Callback Query Handlers ---

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ Handles all button presses from inline keyboards. """
    query = update.callback_query
    await query.answer()
    data = query.data

    # --- Channel Management Callbacks ---
    if data.startswith("approve_channel_"):
        channel_id = int(data.split("_")[2])
        try:
            chat = await context.bot.get_chat(channel_id)
            is_main = await Database.add_channel(channel_id, chat.title)
            main_text = " and set as the MAIN channel" if is_main else ""
            await query.edit_message_text(f"✅ Approved channel *{escape_markdown_v2(chat.title)}*{escape_markdown_v2(main_text)}\\.", parse_mode='MarkdownV2')
            await context.bot.send_message(channel_id, "This channel has been approved for receiving feeds\\.")
        except Exception as e:
            await query.edit_message_text(f"Error approving channel: {e}")

    elif data.startswith("deny_channel_"):
        channel_id = int(data.split("_")[2])
        await query.edit_message_text(f"❌ Denied channel access. I will now leave this channel.")
        try:
            await context.bot.leave_chat(channel_id)
        except Exception as e:
            logger.error(f"Could not leave chat {channel_id}: {e}")

    elif data.startswith("set_main_"):
        channel_id = int(data.split("_")[2])
        success = await Database.set_main_channel(channel_id)
        if success:
            await query.edit_message_text("✅ New main channel has been set.")
        else:
            await query.edit_message_text("❌ Failed to set new main channel.")
        if query.message:
            # Refresh the channel list to show the new main channel
            await channels_command(query.message, context)

    # --- Prefix Settings Callbacks ---
    elif data == "toggle_prefix_status":
        current_status = await Database.is_prefix_enabled()
        new_status = not current_status
        await Database.toggle_prefix(new_status)
        prefix = await Database.get_prefix()
        keyboard = get_prefix_settings_keyboard(new_status, prefix)
        await query.edit_message_text("Manage the link prefix settings below:", reply_markup=keyboard)

    elif data == "set_prefix_prompt":
        # Ask the user to reply to this new message
        prompt_message = await query.message.reply_text("Please **reply to this message** with your new prefix.\nFor example: `/ql` or `prefix:`", parse_mode='Markdown')
        # Store the ID of the prompt message to check against the reply
        context.user_data['awaiting_prefix_message_id'] = prompt_message.message_id
        await query.delete_message() # Clean up the settings panel

    elif data.startswith("info_"):
        # Acknowledge the press on info buttons but do nothing.
        pass

# --- Handler Registration ---
def get_handlers():
    return [
        CommandHandler("start", start_command),
        CommandHandler("help", help_command, filters=admin_filter),
        CommandHandler("test", test_command, filters=admin_filter),
        CommandHandler("channels", channels_command, filters=admin_filter),
        CommandHandler("prefixsettings", prefix_settings_command, filters=admin_filter),
        CommandHandler("status", status_command, filters=admin_filter),
        CommandHandler("stats", stats_command, filters=admin_filter),
        CommandHandler("log", log_command, filters=admin_filter),
        MessageHandler(filters.FORWARDED & filters.ChatType.PRIVATE & admin_filter, forwarded_message_handler),
        MessageHandler(filters.TEXT & filters.ChatType.PRIVATE & admin_filter, text_message_handler),
        CallbackQueryHandler(button_callback_handler)
    ]
