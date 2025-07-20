# ==============================================================================
# File: link-scraper-bot/bot/handlers.py
# Description: Defines all command, message, and callback query handlers. (COMPLETE)
# ==============================================================================

from telegram import Update, ChatMember
from telegram.constants import ChatType, ParseMode
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
from scraper.scheduler import check_website_job

# --- Admin Filter & Start Time ---
admin_filter = filters.User(user_id=settings.ADMIN_TELEGRAM_ID)
START_TIME = time.time()

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = get_start_keyboard()
    if user_id == settings.ADMIN_TELEGRAM_ID:
        text = (
            "👑 *Welcome, Admin*\\!\n\n"
            "This is your control panel\\. You have access to special commands to manage the bot\\.\n\n"
            "`/checknow` \\- Manually trigger the website check\\.\n"
            "`/channels` \\- Manage authorized channels\\.\n"
            "`/prefixsettings` \\- Configure link prefixes\\.\n"
            "`/status` \\- Show server and bot status\\.\n"
            "`/stats` \\- Show bot statistics\\.\n"
            "`/log` \\- Show recent log lines\\.\n"
            "`/help` \\- Shows detailed command info\\."
        )
    else:
        text = (
            "👋 *Welcome to the Link Scraper Bot*\\!\n\n"
            "This bot automatically posts new file links into our channels\\.\n\n"
            "Check out our channels below to get the latest updates\\!"
        )
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "*Admin Commands:*\n"
        "`/checknow` \\- Manually trigger the website check\\.\n"
        "`/test <url>` \\- Scrapes a single URL for testing\\.\n"
        "`/channels` \\- Opens the channel management panel\\.\n"
        "`/prefixsettings` \\- Opens the prefix management panel\\.\n"
        "`/status` \\- Shows server and bot status\\.\n"
        "`/stats` \\- Shows bot statistics\\.\n"
        "`/log` \\- Shows recent log lines\\.\n\n"
        "*How to Authorize a New Channel:*\n"
        "1\\. Add me to your channel and promote me to an *Administrator*\\.\n"
        "2\\. Forward *any message* from that channel to me here in this private chat\\."
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN_V2)

async def check_now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Manually triggering the website check now. Please wait...")
    try:
        await check_website_job(context.bot)
        await update.message.reply_text("✅ Manual check finished. Please check your channels or the bot's logs for the results.")
    except Exception as e:
        logger.error(f"Error during manual check: {e}", exc_info=True)
        await update.message.reply_text(f"❌ An error occurred during the manual check. Please review the logs.")

async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Please provide a URL to test. Usage: /test <url>")
        return
    url = context.args[0]
    await update.message.reply_text(f"Testing scraper on: {escape_markdown_v2(url)}", parse_mode=ParseMode.MARKDOWN_V2)
    scraper = ScraperEngine()
    result = await scraper.scrape_post(url)
    if result and result[0]:
        links, content_hash, quality_tags = result
        await update.message.reply_text(
            f"Scraping successful\\! Found {len(links)} links\\.\n"
            f"*Content hash:* `{escape_markdown_v2(content_hash)}`\n"
            f"*Quality tags:* {escape_markdown_v2(', '.join(quality_tags) if quality_tags else 'None')}",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        post_title = "Test Scrape Results"
        await format_and_send_links(
            bot=context.bot,
            chat_id=update.effective_chat.id,
            post_title=post_title,
            links=links,
            status="new",
            quality_tags=quality_tags
        )
    else:
        await update.message.reply_text("Scraping failed. No links found or an error occurred. Check logs.")

async def channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channels = await Database.get_all_channels()
    if not channels:
        await update.message.reply_text("No channels have been authorized yet.")
        return
    main_channel = await Database.get_main_channel()
    main_channel_id = main_channel['channel_id'] if main_channel else None
    keyboard = get_channel_management_keyboard(channels, main_channel_id)
    await update.message.reply_text("Authorized Channels:", reply_markup=keyboard)

async def prefix_settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_enabled = await Database.is_prefix_enabled()
    prefix = await Database.get_prefix()
    keyboard = get_prefix_settings_keyboard(is_enabled, prefix)
    await update.message.reply_text("Manage the link prefix settings below:", reply_markup=keyboard)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime_seconds = time.time() - START_TIME
    uptime_str = f"{int(uptime_seconds // 3600)}h {int((uptime_seconds % 3600) // 60)}m {int(uptime_seconds % 60)}s"
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent()
    text = (
        "*Server Status:*\n"
        f"Uptime: `{uptime_str}`\n"
        f"Memory: `{mem.percent}%` used\n"
        f"CPU: `{cpu}%`"
    )
    await update.message.reply_text(escape_markdown_v2(text), parse_mode=ParseMode.MARKDOWN_V2)

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total_channels = len(await Database.get_all_channels())
    total_links = await Database.get_total_links()
    text = (
        "*Bot Stats:*\n"
        f"Authorized Channels: `{total_channels}`\n"
        f"Total Links Processed: `{total_links}`"
    )
    await update.message.reply_text(escape_markdown_v2(text), parse_mode=ParseMode.MARKDOWN_V2)

async def log_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_path = "logs/bot.log"
    if not os.path.exists(log_path):
        await update.message.reply_text("No log file found yet.")
        return
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()[-20:]
        text = "```\n--- Last 20 Log Lines ---\n" + "".join(lines) + "\n```"
        if len(text) > 4096:
            text = text[:4090] + "\n...```"
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        await update.message.reply_text(f"Could not read log file: {e}")

async def forwarded_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            await update.message.reply_text(f"I am not an admin in *{escape_markdown_v2(channel.title)}*\\.", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Failed to check bot status in channel {channel.id}: {e}")
        await update.message.reply_text(f"Could not verify my status in the channel *{escape_markdown_v2(channel.title)}*\\.", parse_mode=ParseMode.MARKDOWN_V2)

async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message and context.user_data.get('awaiting_prefix_message_id') == update.message.reply_to_message.message_id:
        new_prefix = update.message.text
        await Database.set_prefix(new_prefix)
        context.user_data['awaiting_prefix_message_id'] = None
        logger.info(f"Admin set new prefix via reply: {new_prefix}")
        await update.message.reply_text(f"✅ Prefix has been updated to: `{escape_markdown_v2(new_prefix)}`", parse_mode=ParseMode.MARKDOWN_V2)
        await prefix_settings_command(update, context)

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("approve_channel_"):
        channel_id = int(data.split("_")[2])
        try:
            chat = await context.bot.get_chat(channel_id)
            is_main = await Database.add_channel(channel_id, chat.title)
            main_text = " and set as the MAIN channel" if is_main else ""
            await query.edit_message_text(f"✅ Approved channel *{escape_markdown_v2(chat.title)}*{escape_markdown_v2(main_text)}\\.", parse_mode=ParseMode.MARKDOWN_V2)
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
            await channels_command(query.message, context)
    elif data == "toggle_prefix_status":
        current_status = await Database.is_prefix_enabled()
        new_status = not current_status
        await Database.toggle_prefix(new_status)
        prefix = await Database.get_prefix()
        keyboard = get_prefix_settings_keyboard(new_status, prefix)
        await query.edit_message_text("Manage the link prefix settings below:", reply_markup=keyboard)
    elif data == "set_prefix_prompt":
        prompt_message = await query.message.reply_text("Please **reply to this message** with your new prefix.", parse_mode='Markdown')
        context.user_data['awaiting_prefix_message_id'] = prompt_message.message_id
        await query.delete_message()
    elif data.startswith("info_"):
        pass

def get_handlers():
    return [
        CommandHandler("start", start_command),
        CommandHandler("help", help_command, filters=admin_filter),
        CommandHandler("test", test_command, filters=admin_filter),
        CommandHandler("channels", channels_command, filters=admin_filter),
        CommandHandler("prefixsettings", prefix_settings_command, filters=admin_filter),
        CommandHandler("checknow", check_now_command, filters=admin_filter),
        CommandHandler("status", status_command, filters=admin_filter),
        CommandHandler("stats", stats_command, filters=admin_filter),
        CommandHandler("log", log_command, filters=admin_filter),
        MessageHandler(filters.FORWARDED & filters.ChatType.PRIVATE & admin_filter, forwarded_message_handler),
        MessageHandler(filters.TEXT & filters.ChatType.PRIVATE & admin_filter, text_message_handler),
        CallbackQueryHandler(button_callback_handler)
    ]
