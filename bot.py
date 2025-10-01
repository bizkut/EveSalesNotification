import logging
import os
import json
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta, time as dt_time
import asyncio
import telegram
from telegram.error import BadRequest
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
import io
import calendar
from PIL import Image

# Import the new shared utility module
import app_utils
import database
from app_utils import (
    Character, CHARACTERS, load_characters_from_db, setup_database,
    get_bot_state, set_bot_state, get_characters_for_user, get_character_by_id,
    update_character_setting, update_character_notification_setting,
    reset_update_notification_flag, schedule_character_deletion,
    cancel_character_deletion, get_character_deletion_status,
    get_market_orders, get_market_orders_history, get_wallet_balance,
    get_wallet_transactions, get_wallet_journal, get_contracts,
    get_names_from_ids, calculate_cogs_and_update_lots, get_next_run_delay,
    add_processed_orders, get_processed_orders, add_processed_contracts,
    get_processed_contracts, update_contracts_cache, remove_stale_contracts,
    add_wallet_journal_entries_to_db, add_historical_transactions_to_db,
    add_processed_journal_refs, get_ids_from_db,
    get_structure_market_orders, get_region_market_orders, get_station_info,
    get_system_info, get_character_location, get_character_online_status,
    get_character_public_info, get_corporation_info, get_alliance_info,
    get_structure_info, get_constellation_info, get_route,
    get_jump_distance_from_db, save_jump_distance_to_db,
    get_jump_distance, get_cached_chart, save_chart_to_cache,
    get_undercut_statuses, update_undercut_statuses, remove_stale_undercut_statuses,
    get_tracked_market_orders, remove_tracked_market_orders, update_tracked_market_orders,
    seed_data_for_character, get_contracts_from_db, get_full_wallet_journal_from_db,
    get_historical_transactions_from_db, get_last_known_wallet_balance,
    add_purchase_lot, get_character_skills, _create_character_info_image,
    _resolve_location_to_system_id, delete_character,
    get_new_and_updated_character_info, get_characters_to_purge,
    _calculate_overview_data, _format_overview_message,
    backfill_character_journal_history, _prepare_chart_data
)


# Configure logging
log_level_str = os.getenv('LOG_LEVEL', 'WARNING').upper()
log_level = getattr(logging, log_level_str, logging.WARNING)
# Get the root logger
logger = logging.getLogger()
logger.setLevel(log_level)
# Remove any existing handlers to prevent conflicts
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
# Create a new stream handler
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# Add the new handler to the root logger
logger.addHandler(handler)


# --- Telegram Bot Functions ---

async def send_telegram_message(context: ContextTypes.DEFAULT_TYPE, message: str, chat_id: int, reply_markup=None):
    """Sends a message to a specific chat_id."""
    if not chat_id:
        logging.error("No chat_id provided. Cannot send message.")
        return

    try:
        await context.bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown', reply_markup=reply_markup)
        logging.info(f"Sent message to chat_id: {chat_id}.")
    except Exception as e:
        if "bot was blocked by the user" in str(e).lower():
            logging.warning(f"Could not send message to {chat_id}: Bot was blocked by the user.")
            # Future improvement: Disable notifications for this user in the database.
        else:
            logging.error(f"Error sending Telegram message to {chat_id}: {e}")


# --- Main Application Logic ---

async def send_paginated_message(context: ContextTypes.DEFAULT_TYPE, header: str, item_lines: list, footer: str, chat_id: int):
    """
    Sends a potentially long message by splitting the item_lines into chunks,
    ensuring each message respects Telegram's character limit.
    """
    # A safe number of lines per message to avoid hitting the character limit.
    CHUNK_SIZE = 30

    if not item_lines:
        message = header + "\n" + footer
        await send_telegram_message(context, message, chat_id)
        return

    # Send the first chunk with the header
    first_chunk = item_lines[:CHUNK_SIZE]
    message = header + "\n" + "\n".join(first_chunk)

    # If this is the only message, add the footer and send
    if len(item_lines) <= CHUNK_SIZE:
        message += "\n" + footer
        await send_telegram_message(context, message, chat_id)
        return
    else:
        # Send the first part (header + first chunk)
        await send_telegram_message(context, message, chat_id)
        # Give telegram a moment
        await asyncio.sleep(0.5)

    # Send the intermediate chunks
    remaining_lines = item_lines[CHUNK_SIZE:]
    for i in range(0, len(remaining_lines), CHUNK_SIZE):
        chunk = remaining_lines[i:i + CHUNK_SIZE]

        # If this is the last chunk, append the footer
        if (i + CHUNK_SIZE) >= len(remaining_lines):
            message = "\n".join(chunk) + "\n" + footer
        else:
            message = "\n".join(chunk)

        await send_telegram_message(context, message, chat_id)
        await asyncio.sleep(0.5)

async def check_and_handle_pending_deletion(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character) -> bool:
    """
    Checks if a character is pending deletion. If so, sends an informative message and returns True.
    Otherwise, returns False, allowing the original command to proceed.
    """
    # No character object means we can't check, so proceed.
    if not character:
        return False

    deletion_time = get_character_deletion_status(character.id)
    if deletion_time:
        query = update.callback_query
        time_left = deletion_time - datetime.now(timezone.utc)
        minutes_left = max(1, int(time_left.total_seconds() / 60))

        message = (
            f"⚠️ **Action Disabled** ⚠️\n\n"
            f"Character **{character.name}** is scheduled for permanent deletion in approximately **{minutes_left} minutes**.\n\n"
            f"All commands for this character are blocked during this time. To cancel the deletion, please use the 'Add Character' option from the main menu."
        )
        keyboard = [[InlineKeyboardButton("« Back to Main Menu", callback_data="start_command")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if query:
            # Edit the message that triggered the callback
            await query.edit_message_text(text=message, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            # Reply to a direct command
            await update.message.reply_text(text=message, parse_mode='Markdown', reply_markup=reply_markup)
        return True
    return False




async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the main menu or routes to the add character flow for new users."""
    user = update.effective_user
    user_characters = get_characters_for_user(user.id)

    # If the user has no characters, treat it as a request to add one.
    if not user_characters:
        await add_character_command(update, context)
        return

    # Otherwise, show the main menu for existing users.
    welcome_message = f"Welcome, {user.first_name}!"
    message = (
        f"{welcome_message}\n\nYou have {len(user_characters)} character(s) registered. "
        "Please choose an option:"
    )
    keyboard = [
        [
            InlineKeyboardButton("💰 View Balances", callback_data="balance"),
            InlineKeyboardButton("📊 Open Orders", callback_data="open_orders")
        ],
        [
            InlineKeyboardButton("📈 View Sales", callback_data="sales"),
            InlineKeyboardButton("🛒 View Buys", callback_data="buys")
        ],
        [
            InlineKeyboardButton("📝 View Contracts", callback_data="contracts"),
                        InlineKeyboardButton("📊 Request Overview", callback_data="overview")
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
            InlineKeyboardButton("➕ Add Character", callback_data="add_character"),
            InlineKeyboardButton("🗑️ Remove", callback_data="remove")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(text=message, reply_markup=reply_markup)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise e
            logging.info("Message not modified, skipping edit.")
    else:
        await update.message.reply_text(text=message, reply_markup=reply_markup)


async def _generate_and_send_overview(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Handles the interactive flow of generating and sending a overview message."""
    if await check_and_handle_pending_deletion(update, context, character):
        return
    target_chat_id = update.effective_chat.id
    query = update.callback_query
    message_id = None

    if query:
        # If we came from a button, edit that message.
        await query.edit_message_text(text=f"⏳ Generating overview for {character.name}...")
        message_id = query.message.message_id
    else:
        # Otherwise, send a new message (e.g., for scheduled overviews).
        sent_message = await context.bot.send_message(chat_id=target_chat_id, text=f"⏳ Generating overview for {character.name}...")
        message_id = sent_message.message_id

    try:
        # Run the synchronous data calculation in a thread to avoid blocking
        overview_data = await asyncio.to_thread(_calculate_overview_data, character)
        message, reply_markup = _format_overview_message(overview_data, character)

        # Add a contextual back button based on how many characters the user has
        user_characters = get_characters_for_user(update.effective_user.id)
        back_button_callback = "overview" if len(user_characters) > 1 else "start_command"

        new_keyboard = list(reply_markup.inline_keyboard) # Create a mutable copy
        new_keyboard.append([InlineKeyboardButton("« Back", callback_data=back_button_callback)])
        new_reply_markup = InlineKeyboardMarkup(new_keyboard)


        # Edit the placeholder message with the final content
        await context.bot.edit_message_text(
            chat_id=target_chat_id,
            message_id=message_id,
            text=message,
            parse_mode='Markdown',
            reply_markup=new_reply_markup
        )
    except Exception as e:
        logging.error(f"Failed to generate and send overview for {character.name}: {e}", exc_info=True)
        await context.bot.edit_message_text(
            chat_id=target_chat_id,
            message_id=message_id,
            text=f"❌ An error occurred while generating the overview for {character.name}."
        )


async def run_daily_overview_for_character(character: Character, context: ContextTypes.DEFAULT_TYPE):
    """Calculates and sends the daily overview for a single character (for scheduled jobs)."""
    logging.info(f"Running scheduled daily overview for {character.name}...")
    try:
        overview_data = await asyncio.to_thread(_calculate_overview_data, character)
        message, reply_markup = _format_overview_message(overview_data, character)

        # For scheduled overviews, always add a simple back button to the main menu
        new_keyboard = list(reply_markup.inline_keyboard)
        new_keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        new_reply_markup = InlineKeyboardMarkup(new_keyboard)

        await send_telegram_message(context, message, chat_id=character.telegram_user_id, reply_markup=new_reply_markup)
        logging.info(f"Daily overview sent for {character.name}.")
    except Exception as e:
        logging.error(f"Failed to send daily overview for {character.name}: {e}", exc_info=True)


    """Displays the main menu or routes to the add character flow for new users."""
    user = update.effective_user
    user_characters = get_characters_for_user(user.id)

    # If the user has no characters, treat it as a request to add one.
    if not user_characters:
        await add_character_command(update, context)
        return

    # Otherwise, show the main menu for existing users.
    welcome_message = f"Welcome, {user.first_name}!"
    message = (
        f"{welcome_message}\n\nYou have {len(user_characters)} character(s) registered. "
        "Please choose an option:"
    )
    keyboard = [
        [
            InlineKeyboardButton("💰 View Balances", callback_data="balance"),
            InlineKeyboardButton("📊 Open Orders", callback_data="open_orders")
        ],
        [
            InlineKeyboardButton("📈 View Sales", callback_data="sales"),
            InlineKeyboardButton("🛒 View Buys", callback_data="buys")
        ],
        [
            InlineKeyboardButton("📝 View Contracts", callback_data="contracts"),
                        InlineKeyboardButton("📊 Request Overview", callback_data="overview")
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
            InlineKeyboardButton("➕ Add Character", callback_data="add_character"),
            InlineKeyboardButton("🗑️ Remove", callback_data="remove")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(text=message, reply_markup=reply_markup)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise e
            logging.info("Message not modified, skipping edit.")
    else:
        await update.message.reply_text(text=message, reply_markup=reply_markup)


async def add_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Provides a link for the user to add a new EVE Online character.
    Displays a combined welcome/auth message for new users.
    """
    user = update.effective_user
    user_characters = get_characters_for_user(user.id)
    is_new_user = not user_characters

    logging.info(f"Received add_character request from user {user.id} (New User: {is_new_user})")
    webapp_base_url = os.getenv('WEBAPP_URL', 'http://localhost:5000')
    login_url = f"{webapp_base_url}/login?user={user.id}"

    # Determine the message and keyboard based on whether the user is new
    if is_new_user:
        welcome_message = f"Welcome, {user.first_name}!"
        message = (
            f"{welcome_message}\n\n"
            "It looks like you don't have any EVE Online characters added yet. To get started, "
            "please click the button below to authorize one with EVE Online.\n\n"
            "You will be redirected to the official EVE Online login page. After logging in and "
            "authorizing, you can close the browser window.\n\n"
            "It may take a minute or two for the character to be fully registered with the bot."
        )
        keyboard = [[InlineKeyboardButton("➕ Authorize Character", url=login_url)]]
    else: # Existing user
        message = (
            "To add a new character, please click the button below and authorize with EVE Online.\n\n"
            "You will be redirected to the official EVE Online login page. After logging in and "
            "authorizing, you can close the browser window."
        )
        keyboard = [
            [InlineKeyboardButton("Authorize with EVE Online", url=login_url)],
            [InlineKeyboardButton("« Back", callback_data="start_command")]
        ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    sent_message = None

    if update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
        sent_message = update.callback_query.message
    else:
        sent_message = await update.message.reply_text(message, reply_markup=reply_markup)

    # Track the prompt message so it can be deleted upon successful character addition
    if sent_message:
        set_bot_state(f"add_character_prompt_{user.id}", f"{sent_message.chat_id}:{sent_message.message_id}")


async def _show_balance_for_characters(update: Update, context: ContextTypes.DEFAULT_TYPE, characters: list[Character]):
    """Helper function to fetch and display balances for a list of characters."""
    query = update.callback_query
    char_names = ", ".join([c.name for c in characters])

    # For callbacks, let the user know we're working on it
    if query:
        await query.edit_message_text(f"⏳ Fetching balance(s) for {char_names}...")

    message_lines = ["💰 *Wallet Balances* 💰\n"]
    total_balance = 0
    errors = False
    for char in characters:
        # Check deletion status first
        if get_character_deletion_status(char.id):
            message_lines.append(f"• `{char.name}`: `(Pending Deletion)`")
            continue  # Skip to the next character

        balance = get_wallet_balance(char, force_revalidate=True)
        if balance is not None:
            message_lines.append(f"• `{char.name}`: `{balance:,.2f} ISK`")
            total_balance += balance
        else:
            message_lines.append(f"• `{char.name}`: `Error fetching balance`")
            errors = True

    if len(characters) > 1 and not errors:
        message_lines.append(f"\n**Combined Total:** `{total_balance:,.2f} ISK`")

    final_text = "\n".join(message_lines)

    if query:
        # For callbacks, edit the message and add a "Back" button
        keyboard = [[InlineKeyboardButton("« Back", callback_data="start_command")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text=final_text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        # For commands, send a new message
        await update.message.reply_text(
            text=final_text,
            parse_mode='Markdown'
        )


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Fetches and displays wallet balance. Prompts for character selection
    if the user has multiple characters via an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _show_balance_for_characters(update, context, user_characters)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"balance_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("All Characters", callback_data="balance_char_all")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character (or all) to view the balance:"

        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def overview_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Manually triggers the daily overview report. Prompts for character
    selection if the user has multiple characters via an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _generate_and_send_overview(update, context, user_characters[0])
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"overview_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("All Characters", callback_data="overview_char_all")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character (or all) to generate an overview for:"

        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)




def format_isk(value):
    """Formats a number into a human-readable ISK string."""
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}b"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.2f}k"
    return f"{value:.2f}"


def generate_hourly_chart(character_id: int):
    """
    Generates a chart with cumulative profit (area) and hourly sales/fees (bars)
    for the last 24 hours, optimized with daily profit summaries.
    """
    import matplotlib
    matplotlib.use('Agg')  # Use a non-interactive backend
    import matplotlib.pyplot as plt
    character = get_character_by_id(character_id)
    if not character: return None

    now = datetime.now(timezone.utc)
    start_of_period = now - timedelta(days=1)

    accumulated_profit, inventory, events_in_period = _prepare_chart_data(character_id, start_of_period)
    cumulative_profit_over_time = [accumulated_profit]

    hours = [(start_of_period + timedelta(hours=i)) for i in range(24)]
    bar_labels = [h.strftime('%H:00') for h in hours]
    hourly_sales = {label: 0 for label in bar_labels}
    hourly_fees = {label: 0 for label in bar_labels}

    for i in range(24):
        hour_start = start_of_period + timedelta(hours=i)
        hour_end = hour_start + timedelta(hours=1)
        hour_label = hour_start.strftime('%H:00')

        sales_in_hour = [e['data'] for e in events_in_period if e['type'] == 'tx' and not e['data'].get('is_buy') and hour_start <= e['date'] < hour_end]
        fees_in_hour = [e['data'] for e in events_in_period if e['type'] == 'fee' and hour_start <= e['date'] < hour_end]

        hourly_sales[hour_label] = sum(s['quantity'] * s['unit_price'] for s in sales_in_hour)
        hourly_fees[hour_label] = sum(abs(f['amount']) for f in fees_in_hour)

        profit_this_hour = -hourly_fees[hour_label]
        for sale in sales_in_hour:
            sale_value = sale['quantity'] * sale['unit_price']
            cogs = 0
            remaining_to_sell = sale['quantity']
            lots = inventory.get(sale['type_id'], [])
            if lots:
                consumed_count = 0
                for lot in lots:
                    if remaining_to_sell <= 0: break
                    take = min(remaining_to_sell, lot['quantity'])
                    cogs += take * lot['price']
                    remaining_to_sell -= take
                    lot['quantity'] -= take
                    if lot['quantity'] == 0: consumed_count += 1
                inventory[sale['type_id']] = lots[consumed_count:]
            profit_this_hour += sale_value - cogs

        accumulated_profit += profit_this_hour
        cumulative_profit_over_time.append(accumulated_profit)

    if not any(hourly_sales.values()) and not any(hourly_fees.values()): return None

    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(bar_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(hourly_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(hourly_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)
    ax2 = ax.twinx()
    ax2.fill_between(range(25), cumulative_profit_over_time, color="lime", alpha=0.3, zorder=1)
    ax2.plot(range(25), cumulative_profit_over_time, label='Accumulated Profit', color='lime', marker='o', linestyle='-', zorder=3)
    ax.set_title(f'Hourly Performance for {character.name} (Last 24h)', color='white', fontsize=16)
    ax.set_xlabel('Hour (UTC)', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in range(len(bar_labels))], bar_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf

def generate_last_day_chart(character_id: int):
    """
    Generates a chart for the last 24 hours, processing events chronologically to ensure accuracy.
    """
    import matplotlib
    matplotlib.use('Agg')  # Use a non-interactive backend
    import matplotlib.pyplot as plt
    character = get_character_by_id(character_id)
    if not character: return None

    now = datetime.now(timezone.utc)
    start_of_period = now - timedelta(days=1)

    # Get initial inventory state and all events within the period, sorted chronologically.
    inventory, events_in_period = _prepare_chart_data(character_id, start_of_period)

    # If there are no events, there's nothing to chart.
    if not any(e['type'] == 'tx' and not e['data'].get('is_buy') for e in events_in_period) and \
       not any(e['type'] == 'fee' for e in events_in_period):
        return None

    # --- Data Preparation ---
    hour_labels = [(start_of_period + timedelta(hours=i)).strftime('%H:00') for i in range(24)]
    hourly_sales = {label: 0 for label in hour_labels}
    hourly_fees = {label: 0 for label in hour_labels}

    hourly_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    # --- Chronological Event Processing ---
    for i in range(24):
        hour_start = start_of_period + timedelta(hours=i)
        hour_end = hour_start + timedelta(hours=1)
        hour_label = hour_start.strftime('%H:00')

        # Process all events that fall within this hour, in order.
        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < hour_end:
            event = events_in_period[event_idx]
            event_type = event['type']
            data = event['data']

            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else:  # Sale
                    sale_value = data['quantity'] * data['unit_price']
                    hourly_sales[hour_label] += sale_value

                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    accumulated_profit += sale_value - cogs
            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                hourly_fees[hour_label] += fee_amount
                accumulated_profit -= fee_amount

            event_idx += 1

        hourly_cumulative_profit.append(accumulated_profit)

    # --- Plotting ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(hour_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(hourly_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(hourly_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)

    ax2 = ax.twinx()
    # Prepend the starting profit (0) for the line plot
    final_profit_line = [0] + hourly_cumulative_profit
    ax2.plot(range(25), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(25), final_profit_line, color="lime", alpha=0.3, zorder=1)

    ax.set_title(f'Performance for {character.name} (Last 24 Hours)', color='white', fontsize=16)
    ax.set_xlabel('Hour (UTC)', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], hour_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf

def _generate_daily_breakdown_chart(character_id: int, days_to_show: int):
    """Helper to generate charts with a daily breakdown (last 7/30 days)."""
    import matplotlib
    matplotlib.use('Agg')  # Use a non-interactive backend
    import matplotlib.pyplot as plt
    character = get_character_by_id(character_id)
    if not character: return None

    now = datetime.now(timezone.utc)
    start_of_period = (now - timedelta(days=days_to_show-1)).replace(hour=0, minute=0, second=0, microsecond=0)

    inventory, events_in_period = _prepare_chart_data(character_id, start_of_period)

    if not any(e['type'] == 'tx' and not e['data'].get('is_buy') for e in events_in_period) and \
       not any(e['type'] == 'fee' for e in events_in_period):
        return None

    # --- Data Preparation ---
    days = [(start_of_period + timedelta(days=i)) for i in range(days_to_show)]
    label_format = '%d' if days_to_show == 30 else '%m-%d'
    bar_labels = [d.strftime(label_format) for d in days]
    daily_sales = {label: 0 for label in bar_labels}
    daily_fees = {label: 0 for label in bar_labels}

    daily_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    # --- Chronological Event Processing ---
    for day_start in days:
        day_end = day_start + timedelta(days=1)
        day_label = day_start.strftime(label_format)

        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < day_end:
            event = events_in_period[event_idx]
            event_type = event['type']
            data = event['data']

            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else:  # Sale
                    sale_value = data['quantity'] * data['unit_price']
                    daily_sales[day_label] += sale_value
                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    accumulated_profit += sale_value - cogs
            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                daily_fees[day_label] += fee_amount
                accumulated_profit -= fee_amount

            event_idx += 1

        daily_cumulative_profit.append(accumulated_profit)

    # --- Plotting ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(bar_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(daily_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(daily_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)

    ax2 = ax.twinx()
    final_profit_line = [0] + daily_cumulative_profit
    ax2.plot(range(days_to_show + 1), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(days_to_show + 1), final_profit_line, color="lime", alpha=0.3, zorder=1)

    ax.set_title(f'Performance for {character.name} (Last {days_to_show} Days)', color='white', fontsize=16)
    ax.set_xlabel('Date', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], bar_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf

def generate_last_7_days_chart(character_id: int):
    """Generates a chart for the last 7 days."""
    return _generate_daily_breakdown_chart(character_id, 7)

def generate_last_30_days_chart(character_id: int):
    """Generates a chart for the last 30 days."""
    return _generate_daily_breakdown_chart(character_id, 30)

def generate_all_time_chart(character_id: int):
    """Generates a monthly breakdown chart for the character's entire history."""
    import matplotlib
    matplotlib.use('Agg')  # Use a non-interactive backend
    import matplotlib.pyplot as plt
    character = get_character_by_id(character_id)
    if not character: return None

    inventory, events_in_period = _prepare_chart_data(character_id, datetime.min.replace(tzinfo=timezone.utc))
    if not events_in_period: return None

    # --- Data Preparation ---
    start_date = events_in_period[0]['date']
    end_date = datetime.now(timezone.utc)
    months = []
    current_month = start_date.replace(day=1)
    while current_month <= end_date:
        months.append(current_month)
        next_month_val = current_month.month + 1
        next_year_val = current_month.year
        if next_month_val > 12:
            next_month_val = 1
            next_year_val += 1
        current_month = current_month.replace(year=next_year_val, month=next_month_val)

    bar_labels = [m.strftime('%Y-%m') for m in months]
    monthly_sales = {label: 0 for label in bar_labels}
    monthly_fees = {label: 0 for label in bar_labels}
    monthly_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    # --- Chronological Event Processing ---
    for month_start in months:
        month_label = month_start.strftime('%Y-%m')
        next_month_start = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)

        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < next_month_start:
            event = events_in_period[event_idx]
            event_type = event['type']
            data = event['data']

            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else: # Sale
                    sale_value = data['quantity'] * data['unit_price']
                    monthly_sales[month_label] += sale_value
                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    accumulated_profit += sale_value - cogs
            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                monthly_fees[month_label] += fee_amount
                accumulated_profit -= fee_amount

            event_idx += 1

        monthly_cumulative_profit.append(accumulated_profit)

    # --- Plotting ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(bar_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(monthly_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(monthly_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)

    ax2 = ax.twinx()
    final_profit_line = [0] + monthly_cumulative_profit
    ax2.plot(range(len(months) + 1), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(len(months) + 1), final_profit_line, color="lime", alpha=0.3, zorder=1)

    ax.set_title(f'Performance for {character.name} (All Time)', color='white', fontsize=16)
    ax.set_xlabel('Month', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], bar_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf


async def generate_chart_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job to generate and send a chart in the background, with caching."""
    job = context.job
    chat_id = job.chat_id
    character_id = job.data['character_id']
    chart_type = job.data['chart_type']
    generating_message_id = job.data['generating_message_id']

    character = get_character_by_id(character_id)
    if not character:
        await context.bot.edit_message_text(text="Error: Could not find character for this chart.", chat_id=chat_id, message_id=generating_message_id)
        return

    now = datetime.now(timezone.utc)
    chart_key = f"chart:{character_id}:{chart_type}"
    if chart_type == 'lastday':
        chart_key += f":{now.strftime('%Y-%m-%d-%H')}"
    elif chart_type in ['7days', '30days']:
        chart_key += f":{now.strftime('%Y-%m-%d')}"

    is_dirty = get_bot_state(f"chart_cache_dirty_{character_id}") == "true"

    caption_map = {
        'lastday': "Last Day", '7days': "Last 7 Days",
        '30days': "Last 30 Days", 'alltime': "All Time"
    }
    caption = f"{caption_map.get(chart_type, chart_type.capitalize())} chart for {character.name}"
    keyboard = [[InlineKeyboardButton("Back to Overview", callback_data=f"overview_back_{character_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if not (chart_type == 'alltime' and is_dirty):
        cached_chart_data = get_cached_chart(chart_key)
        if cached_chart_data:
            logging.info(f"Using cached chart for key: {chart_key}")
            await context.bot.delete_message(chat_id=chat_id, message_id=generating_message_id)
            await context.bot.send_photo(chat_id=chat_id, photo=io.BytesIO(bytes(cached_chart_data)), caption=caption, reply_markup=reply_markup)
            return

    logging.info(f"Generating new chart for key: {chart_key} (All-Time Dirty: {is_dirty if chart_type == 'alltime' else 'N/A'})")
    chart_buffer = None
    try:
        if chart_type == 'lastday':
            chart_buffer = await asyncio.to_thread(generate_last_day_chart, character_id)
        elif chart_type == '7days':
            chart_buffer = await asyncio.to_thread(generate_last_7_days_chart, character_id)
        elif chart_type == '30days':
            chart_buffer = await asyncio.to_thread(generate_last_30_days_chart, character_id)
        elif chart_type == 'alltime':
            chart_buffer = await asyncio.to_thread(generate_all_time_chart, character_id)
    except Exception as e:
        logging.error(f"Error generating chart for char {character_id}: {e}", exc_info=True)
        await context.bot.edit_message_text(text=f"An error occurred while generating the chart for {character.name}.", chat_id=chat_id, message_id=generating_message_id, reply_markup=reply_markup)
        return

    await context.bot.delete_message(chat_id=chat_id, message_id=generating_message_id)

    if chart_buffer:
        save_chart_to_cache(chart_key, character_id, chart_buffer.getvalue())
        if chart_type == 'alltime':
            set_bot_state(f"chart_cache_dirty_{character_id}", "false")
        chart_buffer.seek(0)
        await context.bot.send_photo(chat_id=chat_id, photo=chart_buffer, caption=caption, reply_markup=reply_markup)
    else:
        await context.bot.send_message(chat_id=chat_id, text=f"Could not generate chart for {character.name}. No data available for the period.", reply_markup=reply_markup)


async def chart_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles callbacks from the chart buttons by scheduling a background job."""
    query = update.callback_query
    await query.answer()

    try:
        parts = query.data.split('_')
        action = parts[0]
        chart_type = parts[1]
        character_id = int(parts[2])

        if action != 'chart':
            return

    except (IndexError, ValueError):
        await query.edit_message_text(text="Invalid chart request.")
        return

    character = get_character_by_id(character_id)
    if not character:
        await query.edit_message_text(text="Error: Could not find character for this chart.")
        return

    # Delete the summary message and send a new "generating" message.
    await query.message.delete()

    caption_map = {
        'lastday': "Last Day",
        '7days': "Last 7 Days",
        '30days': "Last 30 Days",
        'alltime': "All Time"
    }
    chart_name = caption_map.get(chart_type, chart_type.capitalize())

    generating_message = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"⏳ Generating {chart_name} chart for {character.name}. This may take a moment..."
    )

    job_data = {
        'character_id': character_id,
        'chart_type': chart_type,
        'generating_message_id': generating_message.message_id
    }
    context.job_queue.run_once(generate_chart_job, when=1, data=job_data, chat_id=query.message.chat_id)


async def back_to_overview_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the 'Back to Overview' button press by deleting the chart and regenerating the overview."""
    query = update.callback_query
    await query.answer()

    try:
        character_id = int(query.data.split('_')[2])
    except (IndexError, ValueError):
        await context.bot.send_message(chat_id=query.message.chat_id, text="Invalid request.")
        return

    character = get_character_by_id(character_id)
    if not character:
        await context.bot.send_message(chat_id=query.message.chat_id, text="Error: Could not find character.")
        return

    # Delete the chart message, as we cannot edit a media message into a text message.
    await query.message.delete()

    # Regenerate the overview as a new message.
    # We create a new Update object without a callback_query to force sending a new message.
    await _generate_and_send_overview(Update(update.update_id, message=query.message), context, character)


async def _select_character_for_historical_sales(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Asks the user to select a character to view their historical sales."""
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="You have no characters added.")
        await start_command(update, context)
        return

    if len(user_characters) == 1:
        await _display_historical_sales(update, context, character_id=user_characters[0].id, page=0)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"history_list_sale_{char.id}_0")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to view their historical sales:"
        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def _select_character_for_historical_buys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Asks the user to select a character to view their historical buys."""
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="You have no characters added.")
        await start_command(update, context)
        return

    if len(user_characters) == 1:
        await _display_historical_buys(update, context, character_id=user_characters[0].id, page=0)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"history_list_buy_{char.id}_0")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to view their historical buys:"
        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def sales_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays historical sales. Prompts for character selection
    if the user has multiple characters.
    """
    await _select_character_for_historical_sales(update, context)


async def buys_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays historical buys. Prompts for character selection
    if the user has multiple characters.
    """
    await _select_character_for_historical_buys(update, context)


async def contracts_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays outstanding contracts. Prompts for character selection
    if the user has multiple characters.
    """
    await _select_character_for_contracts(update, context)


async def _select_character_for_contracts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Asks the user to select a character to view their contracts."""
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="You have no characters added.")
        await start_command(update, context)
        return

    if len(user_characters) == 1:
        await _display_contracts(update, context, character_id=user_characters[0].id, page=0)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"contracts_list_{char.id}_0")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to view their outstanding contracts:"
        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


def get_contracts_from_db(character_id: int) -> list:
    """Retrieves all cached contracts for a character from the local database."""
    conn = database.get_db_connection()
    contracts = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT contract_data FROM contracts WHERE character_id = %s", (character_id,))
            contracts = [row[0] for row in cursor.fetchall()]
    finally:
        database.release_db_connection(conn)
    return contracts


async def _display_contracts(update: Update, context: ContextTypes.DEFAULT_TYPE, character_id: int, page: int = 0):
    """Fetches and displays a paginated list of outstanding contracts from the local cache."""
    query = update.callback_query
    character = get_character_by_id(character_id)
    if await check_and_handle_pending_deletion(update, context, character):
        return
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    await query.edit_message_text(text=f"⏳ Fetching contracts for {character.name}...")

    # --- Data Fetching & Filtering ---
    all_contracts = get_contracts_from_db(character.id)
    outstanding_contracts = [c for c in all_contracts if c.get('status') == 'outstanding']

    if not outstanding_contracts:
        user_characters = get_characters_for_user(query.from_user.id)
        back_callback = "start_command" # Go back to main menu if no contracts
        keyboard = [[InlineKeyboardButton("« Back", callback_data=back_callback)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text=f"📝 *Outstanding Contracts for {character.name}*\n\nNo outstanding contracts found.",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        return

    # Sort by date, newest first for display
    outstanding_contracts.sort(key=lambda x: datetime.fromisoformat(x['date_issued'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 5
    total_items = len(outstanding_contracts)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_contracts = outstanding_contracts[start_index:end_index]

    # --- Name Resolution ---
    ids_to_resolve = set()
    for c in paginated_contracts:
        ids_to_resolve.add(c['issuer_id'])
        if c.get('assignee_id'):
            ids_to_resolve.add(c['assignee_id'])
        if c.get('start_location_id'):
            ids_to_resolve.add(c['start_location_id'])
        if c.get('end_location_id'):
            ids_to_resolve.add(c['end_location_id'])
    id_to_name = get_names_from_ids(list(ids_to_resolve), character)

    # --- Message Formatting ---
    header = f"📝 *Outstanding Contracts for {character.name}*\n"
    message_lines = []
    for contract in paginated_contracts:
        contract_type = contract['type'].replace('_', ' ').title()
        issuer_name = id_to_name.get(contract['issuer_id'], 'Unknown')

        line = f"*{contract_type}* from `{issuer_name}`"
        if contract.get('title'):
            line += f"\n  *Title:* `{contract['title']}`"

        if contract.get('assignee_id'):
            assignee_name = id_to_name.get(contract['assignee_id'], 'Unknown')
            line += f"\n  *To:* `{assignee_name}`"

        if contract.get('start_location_id'):
            location_name = id_to_name.get(contract['start_location_id'], 'Unknown')
            line += f"\n  *Location:* `{location_name}`"

        if contract.get('reward', 0) > 0:
            line += f"\n  *Reward:* `{contract['reward']:,.2f}` ISK"

        if contract.get('collateral', 0) > 0:
            line += f"\n  *Collateral:* `{contract['collateral']:,.2f}` ISK"

        try:
            expires_dt = datetime.fromisoformat(contract['date_expired'].replace('Z', '+00:00'))
            time_left = expires_dt - datetime.now(timezone.utc)
            if time_left.total_seconds() <= 0:
                expires_str = "Expired"
            else:
                days = time_left.days
                hours = time_left.seconds // 3600
                expires_str = f"{days}d {hours}h"
            line += f"\n  *Expires in:* `{expires_str}`"
        except (ValueError, KeyError):
            pass

        message_lines.append(line)


    # --- Keyboard ---
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"contracts_list_{character_id}_{page - 1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"contracts_list_{character_id}_{page + 1}"))

    if nav_row: keyboard.append(nav_row)
    user_characters = get_characters_for_user(query.from_user.id)
    back_callback = "start_command"
    keyboard.append([InlineKeyboardButton("« Back", callback_data=back_callback)])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # --- Send Message ---
    full_message = header + "\n\n".join(message_lines)
    await query.edit_message_text(text=full_message, parse_mode='Markdown', reply_markup=reply_markup)


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to manage their general settings
    using an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _show_character_settings(update, context, user_characters[0])
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"settings_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to manage their settings:"

        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def _show_notification_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Displays the notification settings menu for a specific character using an InlineKeyboardMarkup."""
    character = get_character_by_id(character.id) # Re-fetch to ensure latest settings
    if not character:
        await context.bot.send_message(update.effective_chat.id, "Error: Could not find this character.")
        return

    back_callback = f"settings_char_{character.id}"

    keyboard = [
        [InlineKeyboardButton(f"Sales Notifications: {'✅ On' if character.enable_sales_notifications else '❌ Off'}", callback_data=f"toggle_sales_{character.id}")],
        [InlineKeyboardButton(f"Buy Notifications: {'✅ On' if character.enable_buy_notifications else '❌ Off'}", callback_data=f"toggle_buys_{character.id}")],
        [InlineKeyboardButton(f"Contract Notifications: {'✅ On' if character.enable_contract_notifications else '❌ Off'}", callback_data=f"toggle_contracts_{character.id}")],
        [InlineKeyboardButton(f"Daily Overview: {'✅ On' if character.enable_daily_overview else '❌ Off'}", callback_data=f"toggle_overview_{character.id}")],
        [InlineKeyboardButton(f"Undercut Notifications: {'✅ On' if character.enable_undercut_notifications else '❌ Off'}", callback_data=f"toggle_undercut_{character.id}")],
        [InlineKeyboardButton("« Back", callback_data=back_callback)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = f"🔔 Notification settings for *{character.name}*:"

    if update.callback_query:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=update.effective_message.message_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


def _build_settings_message_and_keyboard(character: Character):
    """Builds the message text and keyboard for the character settings menu."""
    user_characters = get_characters_for_user(character.telegram_user_id)
    back_callback = "start_command" if len(user_characters) <= 1 else "settings"

    keyboard = [
        [InlineKeyboardButton("ℹ️ Character Info", callback_data=f"character_info_{character.id}")],
        [InlineKeyboardButton("🔔 Notification Settings", callback_data=f"notifications_char_{character.id}")],
        [InlineKeyboardButton(f"Low Wallet Alert: {character.wallet_balance_threshold:,.0f} ISK", callback_data=f"set_wallet_{character.id}")],
        [InlineKeyboardButton("« Back", callback_data=back_callback)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = f"⚙️ General settings for *{character.name}*:"
    return message_text, reply_markup


async def _show_character_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Displays the general settings menu for a specific character using an InlineKeyboardMarkup."""
    if await check_and_handle_pending_deletion(update, context, character):
        return
    character = get_character_by_id(character.id) # Re-fetch for latest data
    if not character:
        await context.bot.send_message(update.effective_chat.id, "Error: Could not find this character.")
        return

    message_text, reply_markup = _build_settings_message_and_keyboard(character)

    if update.callback_query:
        # If the original message was a photo, we can't edit it to be text.
        # So, we delete it and send a new message.
        if update.callback_query.message.photo:
            await update.callback_query.message.delete()
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            try:
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=update.effective_message.message_id,
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
            except BadRequest as e:
                if "message is not modified" in str(e).lower():
                    logging.info("Message not modified, skipping edit in _show_character_settings.")
                else:
                    raise e
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


async def _show_character_info(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Fetches and displays public information for a character."""
    query = update.callback_query
    await query.edit_message_text(f"⏳ Fetching public info for {character.name}...")

    # --- ESI Calls ---
    # Concurrently fetch all required info
    tasks = {
        "public": asyncio.to_thread(get_character_public_info, character.id),
        "online": asyncio.to_thread(get_character_online_status, character)
    }
    results = await asyncio.gather(*tasks.values())
    public_info, online_status = results

    if not public_info:
        await query.edit_message_text(f"❌ Could not fetch public info for {character.name}.")
        return

    # Concurrently fetch corporation and alliance info
    corp_tasks = [asyncio.to_thread(get_corporation_info, public_info['corporation_id'])]
    if 'alliance_id' in public_info:
        corp_tasks.append(asyncio.to_thread(get_alliance_info, public_info['alliance_id']))

    corp_results = await asyncio.gather(*corp_tasks)
    corp_info = corp_results[0]
    alliance_info = corp_results[1] if 'alliance_id' in public_info else None


    # --- Formatting ---
    char_name = public_info.get('name', character.name)
    try:
        birthday = datetime.fromisoformat(public_info['birthday'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
    except (ValueError, KeyError):
        birthday = "Unknown"
    security_status = f"{public_info.get('security_status', 0):.2f}"
    corp_name = corp_info.get('name', 'Unknown Corporation') if corp_info else 'Unknown Corporation'

    caption_lines = [
        f"*{char_name}*",
        f"`Character ID: {character.id}`",
        f"Birthday: {birthday}",
        f"Security Status: {security_status}",
        f"Corporation: {corp_name}",
    ]

    if alliance_info:
        alliance_name = alliance_info.get('name', 'Unknown Alliance')
        caption_lines.append(f"Alliance: {alliance_name}")

    if online_status:
        status_text = "🟢 Online" if online_status.get('online') else "🔴 Offline"
        login_count = online_status.get('logins', 'N/A')
        caption_lines.append(f"Status: {status_text}")
        caption_lines.append(f"Total Logins: {login_count:,}")


    caption = "\n".join(caption_lines)

    # --- Image Composition ---
    image_buffer = await asyncio.to_thread(
        _create_character_info_image,
        character.id,
        public_info['corporation_id'],
        public_info.get('alliance_id')
    )

    # --- Sending Photo ---
    await query.message.delete()
    back_callback = f"settings_char_{character.id}"
    keyboard = [[InlineKeyboardButton("« Back", callback_data=back_callback)]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if image_buffer:
        await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=image_buffer,
            caption=caption,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        # Fallback to text if image creation fails
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=caption,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )


async def remove_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to remove using an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters to remove.")
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        keyboard = [
            [InlineKeyboardButton("YES, REMOVE a character", callback_data=f"remove_confirm_{character.id}")],
            [InlineKeyboardButton("NO, CANCEL", callback_data="start_command")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = (
            f"⚠️ *This is permanent and cannot be undone.* ⚠️\n\n"
            f"Are you sure you want to remove your character **{character.name}** and all their associated data?"
        )
        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        keyboard = [[InlineKeyboardButton(f"Remove {char.name}", callback_data=f"remove_select_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to remove. This action is permanent and will delete all of their data."
        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles text input when the bot is expecting a specific value from the user."""
    text = update.message.text
    next_action_tuple = context.user_data.get('next_action')

    if not next_action_tuple:
        await start_command(update, context)
        return

    action_type, character_id = next_action_tuple
    prompt_message_id = context.user_data.get('prompt_message_id')

    await update.message.delete()

    async def show_settings_again():
        """Helper to edit the prompt message back to the settings menu."""
        if prompt_message_id:
            character = get_character_by_id(character_id)
            message_text, reply_markup = _build_settings_message_and_keyboard(character)
            try:
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=prompt_message_id,
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
            except BadRequest:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
        context.user_data.clear()

    if text.lower() == 'cancel':
        await show_settings_again()
        return

    success = False
    error_message = ""

    if action_type == 'set_wallet_value':
        try:
            new_threshold = int(text.replace(',', '').replace('.', ''))
            update_character_setting(character_id, 'wallet_balance_threshold', new_threshold)
            load_characters_from_db()
            success = True
        except ValueError:
            error_message = "❌ Invalid input. Please enter a valid number. Try again or type `cancel`."

    if success:
        await show_settings_again()
    elif prompt_message_id:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=prompt_message_id,
            text=error_message
        )


def main() -> None:
    """Run the bot."""
    logging.info("Bot starting up...")

    database.initialize_pool()
    setup_database()
    load_characters_from_db()

    application = Application.builder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()

    # --- Add command handlers ---
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    # Start the bot
    application.run_polling()


async def open_orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays a menu to choose between open buy or sell orders."""
    keyboard = [
        [
            InlineKeyboardButton("📈 Open Sale Orders", callback_data="open_orders_sales"),
            InlineKeyboardButton("🛒 Open Buy Orders", callback_data="open_orders_buys")
        ],
        [InlineKeyboardButton("« Back", callback_data="start_command")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(
        text="Please select which open orders you would like to view:",
        reply_markup=reply_markup
    )


async def _display_open_orders(update: Update, context: ContextTypes.DEFAULT_TYPE, character_id: int, is_buy: bool, page: int = 0):
    """Fetches and displays a paginated list of open orders from the local cache."""
    query = update.callback_query
    character = get_character_by_id(character_id)
    if await check_and_handle_pending_deletion(update, context, character):
        return
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    # Let the user know we're working on it
    await query.edit_message_text(text=f"⏳ Fetching open orders for {character.name}...")

    # --- Data Fetching ---
    # Use asyncio.gather to run skill and order fetches concurrently
    # Orders are fetched from the local DB cache, which is updated by the master_orders_poll
    results = await asyncio.gather(
        asyncio.to_thread(get_tracked_market_orders, character.id),
        asyncio.to_thread(get_character_skills, character)
    )
    all_orders, skills_data = results
    if all_orders is None:
        # This case should be rare now, but good to keep as a fallback.
        await query.edit_message_text(text=f"❌ Could not fetch market orders for {character.name}. The database might be unavailable.")
        return

    # --- Order Capacity Calculation (must happen before filtering) ---
    order_capacity_str = ""
    if skills_data and 'skills' in skills_data:
        skill_map = {s['skill_id']: s['active_skill_level'] for s in skills_data['skills']}
        # Skill IDs for market orders:
        # Trade (3443): +4 orders/level
        # Retail (3444): +8 orders/level
        # Wholesale (16596): +16 orders/level
        # Tycoon (18580): +32 orders/level
        trade_level = skill_map.get(3443, 0)
        retail_level = skill_map.get(3444, 0)
        wholesale_level = skill_map.get(16596, 0)
        tycoon_level = skill_map.get(18580, 0)

        # 5 (base) + (Trade * 4) + (Retail * 8) + (Wholesale * 16) + (Tycoon * 32)
        max_orders = 5 + (trade_level * 4) + (retail_level * 8) + (wholesale_level * 16) + (tycoon_level * 32)
        # Use len(all_orders) for the current count, not the filtered count
        order_capacity_str = f"({len(all_orders)} / {max_orders} orders)"

    # Filter for buy or sell orders
    filtered_orders = [order for order in all_orders if bool(order.get('is_buy_order')) == is_buy]
    order_type_str = "Buy" if is_buy else "Sale"

    # --- Message Formatting ---
    header = f"📄 *Open {order_type_str} Orders for {character.name}* {order_capacity_str}\n\n"

    if not filtered_orders:
        # Provide a back button
        keyboard = [[InlineKeyboardButton("« Back", callback_data="open_orders")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        # Combine header and the "no orders" message
        message = header + f"✅ No open {order_type_str.lower()} orders found."
        await query.edit_message_text(
            text=message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Sort orders by issued date, newest first
    filtered_orders.sort(key=lambda x: datetime.fromisoformat(x['issued'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 10
    total_items = len(filtered_orders)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1)) # clamp page number
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_orders = filtered_orders[start_index:end_index]

    # --- Data Resolution (Names, Undercut Status, Character Location) ---
    type_ids_on_page = list(set(order['type_id'] for order in paginated_orders))
    location_ids = [order['location_id'] for order in paginated_orders]

    # Fetch character's current location and all undercut statuses for them
    results = await asyncio.gather(
        asyncio.to_thread(get_character_location, character),
        asyncio.to_thread(get_undercut_statuses, character.id)
    )
    char_location_info, all_undercut_statuses = results
    character_location_id = char_location_info.get('station_id') or char_location_info.get('structure_id') if char_location_info else None

    # Now, gather all the IDs we need to resolve into names
    competitor_location_ids = [
        status.get('competitor_location_id')
        for status in all_undercut_statuses.values()
        if status.get('competitor_location_id')
    ]
    ids_to_resolve = list(set(type_ids_on_page + location_ids + competitor_location_ids))
    id_to_name = await asyncio.to_thread(get_names_from_ids, ids_to_resolve, character)

    message_lines = []
    for order in paginated_orders:
        item_name = id_to_name.get(order['type_id'], f"Type ID {order['type_id']}")
        location_name = id_to_name.get(order['location_id'], f"Location ID {order['location_id']}")
        remaining_vol = order['volume_remain']
        total_vol = order['volume_total']
        price = order['price']

        line = (
            f"*{item_name}*\n"
            f"  `{remaining_vol:,}` of `{total_vol:,}` @ `{price:,.2f}` ISK\n"
            f"  *Location:* `{location_name}`"
        )

        # Add undercut alert from the cached status
        undercut_status = all_undercut_statuses.get(order['order_id'])
        if undercut_status and undercut_status['is_undercut']:
            competitor_price = undercut_status.get('competitor_price', 0.0)
            competitor_location_id = undercut_status.get('competitor_location_id')

            if competitor_price and competitor_location_id:
                competitor_loc_name = id_to_name.get(competitor_location_id, "Unknown Location")
                jumps_str = ""
                if character_location_id:
                    jumps = await get_jump_distance(character_location_id, competitor_location_id, character)
                    if jumps is not None:
                        jumps_str = f" ({jumps}j)"

                order_type_str = "buy" if is_buy else "sell"
                line += f"\n  `> ❗️ Undercut! Best {order_type_str}: {competitor_price:,.2f} in {competitor_loc_name}{jumps_str}`"

        message_lines.append(line)

    # --- Page Summary & Disclaimer ---
    summary_footer = "\n\n---\n_Undercut status is updated periodically._"

    # --- Keyboard ---
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"openorders_list_{character_id}_{str(is_buy).lower()}_{page - 1}"))

    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))

    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"openorders_list_{character_id}_{str(is_buy).lower()}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    # Add a back button to go back to the open orders sub-menu
    back_callback = "open_orders"
    keyboard.append([InlineKeyboardButton("« Back", callback_data=back_callback)])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # --- Send Message ---
    full_message = header + "\n\n".join(message_lines) + summary_footer
    await query.edit_message_text(text=full_message, parse_mode='Markdown', reply_markup=reply_markup)


async def _display_historical_buys(update: Update, context: ContextTypes.DEFAULT_TYPE, character_id: int, page: int = 0):
    """
    Fetches and displays a paginated list of historical buy transactions.
    """
    query = update.callback_query
    character = get_character_by_id(character_id)
    if await check_and_handle_pending_deletion(update, context, character):
        return
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    await query.edit_message_text(text=f"⏳ Fetching historical buys for {character.name}...")

    # --- Data Fetching & Filtering ---
    all_transactions = get_historical_transactions_from_db(character.id)
    buy_transactions = [tx for tx in all_transactions if tx.get('is_buy')]

    if not buy_transactions:
        user_characters = get_characters_for_user(query.from_user.id)
        back_callback = "buys" if len(user_characters) > 1 else "start_command"
        keyboard = [[InlineKeyboardButton("« Back", callback_data=back_callback)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text=f"🧾 *Historical Buys for {character.name}*\n\nNo historical buys found.",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        return

    # Sort by date, newest first for display
    buy_transactions.sort(key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 5
    total_items = len(buy_transactions)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_tx = buy_transactions[start_index:end_index]

    # --- Name Resolution ---
    type_ids = [tx['type_id'] for tx in paginated_tx]
    location_ids = [tx['location_id'] for tx in paginated_tx]
    id_to_name = get_names_from_ids(list(set(type_ids + location_ids)), character)

    # --- Message Formatting ---
    header = f"🧾 *Historical Buys for {character.name}*\n"
    message_lines = []
    for tx in paginated_tx:
        item_name = id_to_name.get(tx['type_id'], f"Type ID {tx['type_id']}")
        location_name = id_to_name.get(tx['location_id'], f"Location ID {tx['location_id']}")
        date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
        total_value = tx['quantity'] * tx['unit_price']

        line = (
            f"*{item_name}*\n"
            f"  *Date:* `{date_str}`\n"
            f"  *Qty:* `{tx['quantity']:,}` @ `{tx['unit_price']:,.2f}`\n"
            f"  *Total Cost:* `{total_value:,.2f}` ISK\n"
            f"  *Location:* `{location_name}`"
        )
        message_lines.append(line)

    # --- Keyboard ---
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"history_list_buy_{character_id}_{page - 1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"history_list_buy_{character_id}_{page + 1}"))

    if nav_row: keyboard.append(nav_row)

    user_characters = get_characters_for_user(query.from_user.id)
    back_callback = "buys" if len(user_characters) > 1 else "start_command"
    keyboard.append([InlineKeyboardButton("« Back", callback_data=back_callback)])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # --- Send Message ---
    full_message = header + "\n".join(message_lines)
    await query.edit_message_text(text=full_message, parse_mode='Markdown', reply_markup=reply_markup)


async def _display_historical_sales(update: Update, context: ContextTypes.DEFAULT_TYPE, character_id: int, page: int = 0):
    """
    Fetches and displays a paginated list of historical sales transactions,
    with detailed profit and loss analysis using FIFO for COGS and
    wallet journal entries for accurate tax and fee calculations.
    """
    query = update.callback_query
    character = get_character_by_id(character_id)
    if await check_and_handle_pending_deletion(update, context, character):
        return
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    await query.edit_message_text(text=f"⏳ Checking data for {character.name}...")

    # --- Data Integrity Check & Backfill ---
    journal_state_key = f"journal_history_backfilled_{character.id}"
    if not get_bot_state(journal_state_key):
        await query.edit_message_text(text=f"⏳ Performing a one-time sync of wallet journal history for {character.name}. This may take a moment...")
        # Run the backfill in a thread to avoid blocking the bot
        backfill_success = await asyncio.to_thread(backfill_character_journal_history, character)
        if not backfill_success:
            await query.edit_message_text(text=f"❌ Failed to sync journal history for {character.name}. Please try again later.")
            return

    await query.edit_message_text(text=f"⏳ Calculating historical sales for {character.name}...")

    # --- Data Fetching & On-Demand Refresh ---
    # First, load what we already have in the database.
    all_transactions = get_historical_transactions_from_db(character.id)
    full_journal = get_full_wallet_journal_from_db(character.id) # This returns entries with datetime objects

    # Now, perform a quick, on-demand refresh of both transactions and journal
    # to prevent race conditions.
    try:
        # 1. Refresh Transactions
        logging.info(f"Performing on-demand transaction refresh for {character.name}...")
        recent_transactions_from_esi = await asyncio.to_thread(get_wallet_transactions, character)
        if recent_transactions_from_esi:
            existing_tx_ids = {tx['transaction_id'] for tx in all_transactions}
            new_transactions = [
                tx for tx in recent_transactions_from_esi
                if tx['transaction_id'] not in existing_tx_ids
            ]
            if new_transactions:
                logging.info(f"On-demand refresh found {len(new_transactions)} new transactions for {character.name}.")
                # Save the raw new transactions to the DB
                await asyncio.to_thread(add_historical_transactions_to_db, character.id, new_transactions)
                # Extend the in-memory list for immediate use. The date is still a string, which is fine
                # because the COGS calculation parses it on the fly.
                all_transactions.extend(new_transactions)

        # 2. Refresh Journal
        logging.info(f"Performing on-demand journal refresh for {character.name}...")
        recent_journal_entries_from_esi = await asyncio.to_thread(get_wallet_journal, character)
        if recent_journal_entries_from_esi:
            existing_journal_ids = {entry['id'] for entry in full_journal}
            new_journal_entries = [
                entry for entry in recent_journal_entries_from_esi
                if entry['id'] not in existing_journal_ids
            ]

            if new_journal_entries:
                logging.info(f"On-demand refresh found {len(new_journal_entries)} new journal entries for {character.name}.")
                # Save the raw new journal entries to the DB
                await asyncio.to_thread(add_wallet_journal_entries_to_db, character.id, new_journal_entries)

                # Before merging, parse the date strings to datetime objects to match the existing list's type
                for entry in new_journal_entries:
                    entry['date'] = datetime.fromisoformat(entry['date'].replace('Z', '+00:00'))

                # Extend the in-memory list and re-sort to ensure chronological order
                full_journal.extend(new_journal_entries)
                full_journal.sort(key=lambda x: x['date'], reverse=True)

    except Exception as e:
        logging.error(f"On-demand data refresh failed for {character.name}: {e}", exc_info=True)
        # Don't block the user, just log the error and proceed with potentially stale data.

    # --- COGS Calculation (In-Memory FIFO Simulation) ---
    inventory = defaultdict(list)
    # Sort all transactions chronologically to build up inventory state correctly
    sorted_transactions = sorted(all_transactions, key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')))
    sale_cogs_data = {} # {transaction_id: cogs_value}

    for tx in sorted_transactions:
        type_id = tx['type_id']
        if tx.get('is_buy'):
            inventory[type_id].append({'quantity': tx['quantity'], 'price': tx['unit_price']})
        else: # It's a sale, calculate its COGS
            cogs = 0
            remaining_to_sell = tx['quantity']
            lots_to_consume_from = inventory.get(type_id, [])
            cogs_calculable = True
            if not lots_to_consume_from:
                cogs_calculable = False
            else:
                lots_consumed_count = 0
                for lot in lots_to_consume_from:
                    if remaining_to_sell <= 0: break
                    quantity_from_lot = min(remaining_to_sell, lot['quantity'])
                    cogs += quantity_from_lot * lot['price']
                    remaining_to_sell -= quantity_from_lot
                    lot['quantity'] -= quantity_from_lot
                    if lot['quantity'] == 0:
                        lots_consumed_count += 1
                inventory[type_id] = lots_to_consume_from[lots_consumed_count:] # Remove consumed lots
                if remaining_to_sell > 0:
                    cogs_calculable = False
            sale_cogs_data[tx['transaction_id']] = cogs if cogs_calculable else None

    # --- Data Filtering and Annotation ---
    # Identify sales from the journal, as this is the source of truth for completed sales.
    sale_journal_entries = [
        entry for entry in full_journal
        if entry.get('ref_type') == 'market_transaction' and entry.get('amount', 0) > 0
    ]
    sale_transaction_ids = {entry['context_id'] for entry in sale_journal_entries}

    # Filter all transactions to get only the ones that correspond to a sale journal entry.
    sales_transactions = [
        tx for tx in all_transactions
        if tx['transaction_id'] in sale_transaction_ids
    ]

    # Create a lookup for market transaction journal entries by their context_id (which is the transaction_id)
    tx_id_to_journal_map = {
        entry['context_id']: entry for entry in full_journal
        if entry.get('ref_type') == 'market_transaction'
    }

    # Group all fee-related journal entries by their exact timestamp for quick lookup
    fee_journal_by_timestamp = defaultdict(list)
    tax_ref_types = {'transaction_tax', 'market_provider_tax'}
    for entry in full_journal:
        if entry['ref_type'] in tax_ref_types:
            # Use the parsed datetime object directly as the key
            fee_journal_by_timestamp[entry['date']].append(entry)


    for sale in sales_transactions:
        sale['cogs'] = sale_cogs_data.get(sale['transaction_id'])
        sale_value = sale['quantity'] * sale['unit_price']

        # Find the main market transaction entry in the journal to get the precise timestamp
        main_journal_entry = tx_id_to_journal_map.get(sale['transaction_id'])
        taxes = 0
        if main_journal_entry:
            # The date from the journal is the source of truth for matching fees
            precise_timestamp = main_journal_entry['date']
            # Find all tax/fee entries that occurred at the exact same second
            related_fees = fee_journal_by_timestamp.get(precise_timestamp, [])
            taxes = sum(abs(fee['amount']) for fee in related_fees)
        else:
            logging.warning(f"Could not find matching journal entry for sale transaction_id {sale['transaction_id']}")


        sale['taxes'] = taxes
        if sale['cogs'] is not None:
            sale['net_profit'] = sale_value - sale['cogs'] - taxes
        else:
            sale['net_profit'] = None

    if not sales_transactions:
        user_characters = get_characters_for_user(query.from_user.id)
        back_callback = "sales" if len(user_characters) > 1 else "start_command"
        keyboard = [[InlineKeyboardButton("« Back", callback_data=back_callback)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text=f"🧾 *Historical Sales for {character.name}*\n\nNo historical sales found.",
            parse_mode='Markdown', reply_markup=reply_markup
        )
        return

    sales_transactions.sort(key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 5
    total_items = len(sales_transactions)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_tx = sales_transactions[start_index:end_index]

    # --- Page-Specific Broker Fee Calculation ---
    page_broker_fees = 0
    if paginated_tx:
        start_date = datetime.fromisoformat(paginated_tx[-1]['date'].replace('Z', '+00:00'))
        end_date = datetime.fromisoformat(paginated_tx[0]['date'].replace('Z', '+00:00'))
        page_broker_fees = sum(
            abs(entry['amount']) for entry in full_journal
            if entry['ref_type'] == 'brokers_fee' and start_date <= entry['date'] <= end_date
        )

    # --- Name Resolution ---
    type_ids = [tx['type_id'] for tx in paginated_tx]
    location_ids = [tx['location_id'] for tx in paginated_tx]
    id_to_name = get_names_from_ids(list(set(type_ids + location_ids)), character)

    # --- Message Formatting ---
    header = f"🧾 *Historical Sales for {character.name}*\n"
    message_lines = []
    for tx in paginated_tx:
        item_name = id_to_name.get(tx['type_id'], f"Type ID {tx['type_id']}")
        date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
        sale_value = tx['quantity'] * tx['unit_price']
        line = (
            f"*{item_name}*\n"
            f"  *Date:* `{date_str}`\n"
            f"  *Qty:* `{tx['quantity']:,}` @ `{tx['unit_price']:,.2f}`\n"
            f"  *Sale Value:* `{sale_value:,.2f}` ISK\n"
        )
        if tx.get('cogs') is not None:
            line += f"  *Cost (FIFO):* `{tx['cogs']:,.2f}` ISK\n"
            line += f"  *Taxes (Journal):* `{tx['taxes']:,.2f}` ISK\n"
            line += f"  *Net Profit:* `{tx['net_profit']:,.2f}` ISK"
        else:
            line += f"  *Net Profit:* `N/A (Missing Purchase History)`"
        message_lines.append(line)

    # --- Page Summary & Footer ---
    footer = (
        f"\n---\n*Broker Fees for this page's period:* `{page_broker_fees:,.2f}` ISK\n"
        f"_(Note: Net Profit includes transaction taxes but not broker fees.)_"
    )

    # --- Keyboard ---
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"history_list_sale_{character_id}_{page - 1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"history_list_sale_{character_id}_{page + 1}"))

    if nav_row: keyboard.append(nav_row)
    user_characters = get_characters_for_user(query.from_user.id)
    back_callback = "sales" if len(user_characters) > 1 else "start_command"
    keyboard.append([InlineKeyboardButton("« Back", callback_data=back_callback)])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # --- Send Message ---
    full_message = header + "\n".join(message_lines) + footer
    await query.edit_message_text(text=full_message, parse_mode='Markdown', reply_markup=reply_markup)


async def _select_character_for_open_orders(update: Update, context: ContextTypes.DEFAULT_TYPE, is_buy: bool):
    """Asks the user to select a character to view their open orders."""
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id
    order_type_str = "buy" if is_buy else "sales"

    if not user_characters:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="You have no characters added.")
        await start_command(update, context)
        return

    if len(user_characters) == 1:
        await _display_open_orders(update, context, character_id=user_characters[0].id, is_buy=is_buy, page=0)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"openorders_list_{char.id}_{str(is_buy).lower()}_0")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="open_orders")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = f"Please select a character to view their open {order_type_str} orders:"
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)


async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """The single, main handler for all callback queries from inline keyboards."""
    query = update.callback_query
    await query.answer()

    # Simple router based on the callback data prefix
    data = query.data
    logging.info(f"Received callback query with data: {data}")

    # --- Main Menu Navigation ---
    if data == "start_command": await start_command(update, context)
    elif data == "balance": await balance_command(update, context)
    elif data == "open_orders": await open_orders_command(update, context)
    elif data == "overview": await overview_command(update, context)
    elif data == "sales": await sales_command(update, context)
    elif data == "buys": await buys_command(update, context)
    elif data == "settings": await settings_command(update, context)
    elif data == "add_character": await add_character_command(update, context)
    elif data == "remove": await remove_character_command(update, context)
    elif data == "contracts": await contracts_command(update, context)

    # --- Open Orders Flow ---
    elif data == "open_orders_sales":
        await _select_character_for_open_orders(update, context, is_buy=False)
    elif data == "open_orders_buys":
        await _select_character_for_open_orders(update, context, is_buy=True)
    elif data.startswith("openorders_list_"):
        _, _, char_id_str, is_buy_str, page_str = data.split('_')
        character_id = int(char_id_str)
        is_buy = is_buy_str == 'true'
        page = int(page_str)
        await _display_open_orders(update, context, character_id, is_buy, page)

    # --- Character Selection for Data Views ---
    elif data.startswith("balance_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        chars_to_query = get_characters_for_user(user_id) if char_id_str == "all" else [get_character_by_id(int(char_id_str))]
        await _show_balance_for_characters(update, context, chars_to_query)

    elif data.startswith("overview_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        if char_id_str == "all":
            # If "All" is selected, delete the menu and send new messages for each overview.
            await query.message.delete()
            chars_to_query = get_characters_for_user(user_id)
            for char in chars_to_query:
                # We pass 'None' for the callback_query part of the update to force new messages
                await _generate_and_send_overview(Update(update.update_id, message=query.message), context, char)
                await asyncio.sleep(1) # Be nice to Telegram's API
        else:
            # If a single character is selected, edit the existing message.
            char_to_query = get_character_by_id(int(char_id_str))
            await _generate_and_send_overview(update, context, char_to_query)

    # --- Historical Transaction Lists (Sales & Buys) ---
    elif data.startswith("history_list_sale_"):
        try:
            _, _, _, char_id_str, page_str = data.split('_')
            character_id = int(char_id_str)
            page = int(page_str)
            await _display_historical_sales(update, context, character_id, page)
        except (ValueError, IndexError) as e:
            logging.error(f"Could not parse history_list_sale callback data: {data}. Error: {e}")
            await query.edit_message_text(text="Error: Invalid callback data.")
    elif data.startswith("history_list_buy_"):
        try:
            _, _, _, char_id_str, page_str = data.split('_')
            character_id = int(char_id_str)
            page = int(page_str)
            await _display_historical_buys(update, context, character_id, page)
        except (ValueError, IndexError) as e:
            logging.error(f"Could not parse history_list_buy callback data: {data}. Error: {e}")
            await query.edit_message_text(text="Error: Invalid callback data.")

    # --- Character Selection for Settings Menus ---
    elif data.startswith("character_info_"):
        char_id = int(data.split('_')[-1])
        character = get_character_by_id(char_id)
        await _show_character_info(update, context, character)

    elif data.startswith("notifications_char_"):
        char_id = int(data.split('_')[-1])
        await _show_notification_settings(update, context, get_character_by_id(char_id))

    elif data.startswith("settings_char_"):
        char_id = int(data.split('_')[-1])
        await _show_character_settings(update, context, get_character_by_id(char_id))

    elif data.startswith("contracts_list_"):
        _, _, char_id_str, page_str = data.split('_')
        character_id = int(char_id_str)
        page = int(page_str)
        await _display_contracts(update, context, character_id, page)

    # --- Toggling Notification Settings ---
    elif data.startswith("toggle_"):
        _, setting, char_id_str = data.split('_')
        char_id = int(char_id_str)
        character = get_character_by_id(char_id)
        current_value = getattr(character, f"enable_{setting}_notifications" if setting != 'overview' else "enable_daily_overview")
        update_character_notification_setting(char_id, setting, not current_value)
        load_characters_from_db() # Reload to get fresh data
        await _show_notification_settings(update, context, get_character_by_id(char_id)) # Refresh the menu

    # --- General Settings Value Input ---
    if data.startswith("set_wallet_"):
        char_id = int(data.split('_')[-1])
        context.user_data['next_action'] = ('set_wallet_value', char_id)
        context.user_data['prompt_message_id'] = query.message.message_id
        await query.edit_message_text("Please enter the new wallet balance threshold (e.g., 100000000 for 100m ISK).\n\nType `cancel` to go back.")

    # --- Character Removal Flow ---
    elif data.startswith("remove_select_"):
        char_id = int(data.split('_')[-1])
        character = get_character_by_id(char_id)
        if not character:
            await query.edit_message_text(text="Error: Character not found.")
            return

        keyboard = [
            [InlineKeyboardButton("YES, REMOVE THIS CHARACTER", callback_data=f"remove_confirm_{character.id}")],
            [InlineKeyboardButton("NO, CANCEL", callback_data="start_command")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = (
            f"⚠️ *This is permanent and cannot be undone.* ⚠️\n\n"
            f"Are you sure you want to remove your character **{character.name}** and all their associated data?"
        )
        await query.edit_message_text(text=message_text, reply_markup=reply_markup, parse_mode='Markdown')

    elif data.startswith("remove_confirm_"):
        char_id = int(data.split('_')[-1])
        character = get_character_by_id(char_id)
        if not character:
            await query.edit_message_text(text="Error: Character not found or already deleted.")
            return

        char_name = character.name

        # Schedule the character for deletion
        schedule_character_deletion(char_id)
        # load_characters_from_db() # DO NOT refresh the list, so monitoring continues for the grace period.

        # Notify the user
        success_message = (
            f"✅ Character **{char_name}** has been removed.\n\n"
            f"If this was a mistake, you can recover the character and all their data by adding them again within the next hour."
        )
        keyboard = [[InlineKeyboardButton("« Back to Main Menu", callback_data="start_command")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            text=success_message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    # --- Charting Callbacks ---
    elif data.startswith("chart_"):
        await chart_callback_handler(update, context)
    elif data.startswith("overview_back_"):
        await back_to_overview_handler(update, context)

    elif data == "noop":
        return # Do nothing, it's just a label


if __name__ == "__main__":
    main()