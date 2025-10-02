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
    update_character_fee_setting,
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
    get_jump_distance,
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


from log_config import setup_logging

# Configure logging
setup_logging()


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

    query = update.callback_query

    # This part handles the "All Characters" case, where there's no query.
    if not query:
        # This case is for scheduled overviews or the "All" button, which always send a new message.
        sent_message = await context.bot.send_message(chat_id=character.telegram_user_id, text=f"⏳ Generating overview for {character.name}...")
        message_id = sent_message.message_id
        user_id = character.telegram_user_id
    else:
        # This is the standard case from a button press.
        if query.message.photo:
            await query.message.delete()
            sent_message = await context.bot.send_message(chat_id=update.effective_chat.id, text=f"⏳ Generating overview for {character.name}...")
            message_id = sent_message.message_id
        else:
            await query.edit_message_text(text=f"⏳ Generating overview for {character.name}...")
            message_id = query.message.message_id
        user_id = query.from_user.id

    # Dispatch the background task via Celery
    generate_overview_task.delay(
        character_id=character.id,
        user_id=user_id,
        chat_id=update.effective_chat.id,
        message_id=message_id
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

        balance = get_wallet_balance(char)
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


from tasks import generate_chart_task, generate_historical_sales_task, generate_overview_task, display_open_orders_task, generate_historical_buys_task, generate_character_info_task, generate_paginated_overview_task

async def chart_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles callbacks from the chart buttons by dispatching a Celery task.
    It now parses optional pagination info to construct the correct "Back" button.
    """
    query = update.callback_query
    await query.answer()

    try:
        parts = query.data.split('_')
        action = parts[0]
        chart_type = parts[1]
        character_id = int(parts[2])
        origin_page = None

        # Check for pagination info, e.g., "chart_lastday_123_page_0"
        if len(parts) > 3 and parts[3] == 'page':
            origin_page = int(parts[4])

        if action != 'chart':
            return
    except (IndexError, ValueError):
        await query.edit_message_text(text="Invalid chart request.")
        return

    character = get_character_by_id(character_id)
    if not character:
        await query.edit_message_text(text="Error: Could not find character for this chart.")
        return

    # Delete the overview message to clean up the chat interface
    await query.message.delete()

    caption_map = {
        'lastday': "Last Day",
        '7days': "Last 7 Days",
        '30days': "Last 30 Days",
        'alltime': "All Time"
    }
    chart_name = caption_map.get(chart_type, chart_type.capitalize())

    # Send a new "generating" message that the Celery task can then update/delete
    generating_message = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"⏳ Generating {chart_name} chart for {character.name}. This may take a moment..."
    )

    # Dispatch the background task via Celery, now with optional origin_page
    generate_chart_task.delay(
        character_id=character_id,
        chart_type=chart_type,
        chat_id=query.message.chat_id,
        generating_message_id=generating_message.message_id,
        origin_page=origin_page
    )


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
        [InlineKeyboardButton(f"Buy Notifications: {'✅ On' if character.enable_buys_notifications else '❌ Off'}", callback_data=f"toggle_buys_{character.id}")],
        [InlineKeyboardButton(f"Contract Notifications: {'✅ On' if character.enable_contracts_notifications else '❌ Off'}", callback_data=f"toggle_contracts_{character.id}")],
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
        [
            InlineKeyboardButton(f"Buy Broker Fee: {character.buy_broker_fee:.2f}%", callback_data=f"set_buy_fee_{character.id}"),
            InlineKeyboardButton(f"Sell Broker Fee: {character.sell_broker_fee:.2f}%", callback_data=f"set_sell_fee_{character.id}")
        ],
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
    """Dispatches a Celery task to fetch and display public information for a character."""
    query = update.callback_query

    # Send a "Fetching..." message, which the task will then delete and replace.
    # It's better to send a new message and delete it than to edit, because we are switching
    # from a text message to a photo message.
    sent_message = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"⏳ Fetching public info for {character.name}..."
    )

    # Delete the original message with the buttons
    await query.message.delete()

    # Dispatch the background task via Celery
    generate_character_info_task.delay(
        character_id=character.id,
        chat_id=query.message.chat_id,
        message_id=sent_message.message_id
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

    elif action_type in ['set_buy_fee_value', 'set_sell_fee_value']:
        try:
            new_fee = float(text.replace(',', '.'))
            if not (0 <= new_fee <= 100):
                raise ValueError("Fee must be between 0 and 100.")

            fee_type = 'buy' if action_type == 'set_buy_fee_value' else 'sell'
            update_character_fee_setting(character_id, fee_type, new_fee)
            load_characters_from_db()
            success = True
        except ValueError:
            error_message = "❌ Invalid input. Please enter a valid percentage (e.g., `3.5`). Try again or type `cancel`."


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

    # Increase the timeout to handle slow network conditions
    application = (
        Application.builder()
        .token(os.getenv("TELEGRAM_BOT_TOKEN"))
        .connect_timeout(30)
        .read_timeout(30)
        .build()
    )

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
    """Dispatches a Celery task to generate and display a paginated list of open orders."""
    query = update.callback_query
    character = get_character_by_id(character_id)
    if await check_and_handle_pending_deletion(update, context, character):
        return
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    # Let the user know we're working on it. The task will edit this message.
    await query.edit_message_text(text=f"⏳ Fetching open orders for {character.name}...")

    # Dispatch the background task via Celery
    display_open_orders_task.delay(
        character_id=character_id,
        user_id=query.from_user.id,
        is_buy=is_buy,
        page=page,
        chat_id=query.message.chat_id,
        message_id=query.message.message_id
    )


async def _display_historical_buys(update: Update, context: ContextTypes.DEFAULT_TYPE, character_id: int, page: int = 0):
    """Dispatches a Celery task to generate and display a paginated list of historical buy transactions."""
    query = update.callback_query
    character = get_character_by_id(character_id)
    if await check_and_handle_pending_deletion(update, context, character):
        return
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    # Let the user know we're working on it. The task will edit this message.
    await query.edit_message_text(text=f"⏳ Fetching historical buys for {character.name}...")

    # Dispatch the background task via Celery
    generate_historical_buys_task.delay(
        character_id=character_id,
        user_id=query.from_user.id,
        chat_id=query.message.chat_id,
        page=page,
        message_id=query.message.message_id
    )


async def _display_historical_sales(update: Update, context: ContextTypes.DEFAULT_TYPE, character_id: int, page: int = 0):
    """
    Dispatches a Celery task to generate and display a paginated list of
    historical sales transactions.
    """
    query = update.callback_query
    character = get_character_by_id(character_id)
    if await check_and_handle_pending_deletion(update, context, character):
        return
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    # Let the user know we're working on it. The task will edit this message.
    await query.edit_message_text(text=f"⏳ Calculating historical sales for {character.name}...")

    # Dispatch the background task via Celery
    generate_historical_sales_task.delay(
        character_id=character_id,
        user_id=query.from_user.id,
        chat_id=query.message.chat_id,
        page=page,
        message_id=query.message.message_id
    )


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
            # If "All" is selected, show the first page of the paginated overview.
            await query.edit_message_text(text="⏳ Loading paginated overview...")
            generate_paginated_overview_task.delay(
                user_id=user_id,
                chat_id=query.message.chat_id,
                message_id=query.message.message_id,
                page=0
            )
        else:
            # If a single character is selected, edit the existing message.
            char_to_query = get_character_by_id(int(char_id_str))
            await _generate_and_send_overview(update, context, char_to_query)

    elif data.startswith("overview_page_"):
        try:
            page = int(data.split('_')[-1])
            user_id = query.from_user.id
            message_id_to_use = query.message.message_id

            # If coming back from a chart (photo), delete it and send a new placeholder message.
            if query.message.photo:
                await query.message.delete()
                sent_message = await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="⏳ Loading overview..."
                )
                message_id_to_use = sent_message.message_id

            generate_paginated_overview_task.delay(
                user_id=user_id,
                chat_id=query.message.chat_id,
                message_id=message_id_to_use,
                page=page
            )
        except (ValueError, IndexError):
            logging.error(f"Could not parse overview_page callback data: {data}")
            await query.edit_message_text(text="Error: Invalid page data.")

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

    elif data.startswith("set_buy_fee_"):
        char_id = int(data.split('_')[-1])
        context.user_data['next_action'] = ('set_buy_fee_value', char_id)
        context.user_data['prompt_message_id'] = query.message.message_id
        await query.edit_message_text("Please enter the new buy broker fee percentage (e.g., `3.0`).\n\nType `cancel` to go back.")

    elif data.startswith("set_sell_fee_"):
        char_id = int(data.split('_')[-1])
        context.user_data['next_action'] = ('set_sell_fee_value', char_id)
        context.user_data['prompt_message_id'] = query.message.message_id
        await query.edit_message_text("Please enter the new sell broker fee percentage (e.g., `3.0`).\n\nType `cancel` to go back.")


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

    elif data == "noop":
        return  # Do nothing, it's just a label


if __name__ == "__main__":
    main()