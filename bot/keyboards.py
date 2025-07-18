# ==============================================================================
# File: link-scraper-bot/bot/keyboards.py
# Description: Creates interactive inline keyboards for the bot. (PREFIX UI UPDATE)
# ==============================================================================

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

def get_start_keyboard():
    """ Creates the keyboard with external channel links for the /start command. """
    keyboard = [
        [
            InlineKeyboardButton("📢 Update Channel", url="https://t.me/filmyspotupdate"),
            InlineKeyboardButton("🎬 Movie Channel", url="https://t.me/+o_VcAI8GRQ8zYzA9")
        ],
        [
             InlineKeyboardButton("🛜 Movie feed Update 🛜", url="https://t.me/+o_VcAI8GRQ8zYzA9")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_prefix_settings_keyboard(is_enabled: bool, prefix: str | None):
    """ Creates the new, advanced UI for managing prefix settings. """
    status_text = "✅ Enabled" if is_enabled else "❌ Disabled"
    prefix_display = f"Current: {prefix}" if prefix else "Not Set"

    keyboard = [
        [
            InlineKeyboardButton(f"Status: {status_text}", callback_data="toggle_prefix_status"),
        ],
        [
            InlineKeyboardButton(f"Prefix: {prefix_display}", callback_data="info_prefix"),
            InlineKeyboardButton("Set Prefix ➡️", callback_data="set_prefix_prompt")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_channel_approval_keyboard(channel_id: int):
    """ Keyboard for the admin to approve or deny a new channel. """
    keyboard = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_channel_{channel_id}"),
            InlineKeyboardButton("❌ Deny", callback_data=f"deny_channel_{channel_id}"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_channel_management_keyboard(channels: list, main_channel_id: int | None):
    """ Creates the new, advanced UI for managing channels. """
    keyboard = []
    for channel in channels:
        channel_id = channel['channel_id']
        channel_name = channel['channel_name']
        button_row = []
        if channel_id == main_channel_id:
            button_text = f"✅ {channel_name} (Main)"
            button_row.append(InlineKeyboardButton(button_text, callback_data=f"info_channel_{channel_id}"))
        else:
            button_text = f"📢 {channel_name}"
            button_row.append(InlineKeyboardButton(button_text, callback_data=f"info_channel_{channel_id}"))
            button_row.append(InlineKeyboardButton("Set as Main ➡️", callback_data=f"set_main_{channel_id}"))
        keyboard.append(button_row)
    return InlineKeyboardMarkup(keyboard)
