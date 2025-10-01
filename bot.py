import logging
import os
import asyncio
from datetime import datetime, timezone, timedelta
import io

from telegram.error import BadRequest
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler

import app_utils
from app_utils import (
    CHARACTERS,
    Character,
    get_character_by_id,
    get_characters_for_user,
    load_characters_from_db,
    setup_database,
    set_bot_state,
    get_character_deletion_status,
    get_wallet_balance,
    update_character_setting,
    update_character_notification_setting,
    cancel_character_deletion,
    schedule_character_deletion,
    get_contracts_from_db,
    get_names_from_ids,
    get_tracked_market_orders,
    get_character_skills,
    get_undercut_statuses,
    get_jump_distance,
    get_region_market_orders,
    get_historical_transactions_from_db,
    backfill_character_journal_history,
    get_full_wallet_journal_from_db,
    get_character_public_info,
    get_character_online_status,
    get_corporation_info,
    get_alliance_info,
    _create_character_info_image,
    seed_data_for_character,
    reset_update_notification_flag,
    delete_character,
    master_wallet_journal_poll,
    master_wallet_transaction_poll,
    master_order_history_poll,
    master_orders_poll,
    master_contracts_poll,
    _calculate_overview_data,
    _format_overview_message,
    _display_historical_sales,
    _display_historical_buys,
    _display_open_orders,
    resolve_location_to_region,
)

from tasks import generate_chart_task

# Configure logging
log_level_str = os.getenv('LOG_LEVEL', 'WARNING').upper()
log_level = getattr(logging, log_level_str, logging.WARNING)
logger = logging.getLogger()
logger.setLevel(log_level)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
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
        else:
            logging.error(f"Error sending Telegram message to {chat_id}: {e}")

async def send_paginated_message(context: ContextTypes.DEFAULT_TYPE, header: str, item_lines: list, footer: str, chat_id: int):
    """
    Sends a potentially long message by splitting the item_lines into chunks.
    """
    CHUNK_SIZE = 30
    if not item_lines:
        message = header + "\n" + footer
        await send_telegram_message(context, message, chat_id)
        return

    first_chunk = item_lines[:CHUNK_SIZE]
    message = header + "\n" + "\n".join(first_chunk)

    if len(item_lines) <= CHUNK_SIZE:
        message += "\n" + footer
        await send_telegram_message(context, message, chat_id)
        return
    else:
        await send_telegram_message(context, message, chat_id)
        await asyncio.sleep(0.5)

    remaining_lines = item_lines[CHUNK_SIZE:]
    for i in range(0, len(remaining_lines), CHUNK_SIZE):
        chunk = remaining_lines[i:i + CHUNK_SIZE]
        if (i + CHUNK_SIZE) >= len(remaining_lines):
            message = "\n".join(chunk) + "\n" + footer
        else:
            message = "\n".join(chunk)
        await send_telegram_message(context, message, chat_id)
        await asyncio.sleep(0.5)

async def check_and_handle_pending_deletion(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character) -> bool:
    """
    Checks if a character is pending deletion. If so, sends an informative message and returns True.
    """
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
            f"All commands for this character are blocked. To cancel, use the 'Add Character' option from the main menu."
        )
        keyboard = [[InlineKeyboardButton("« Back to Main Menu", callback_data="start_command")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if query:
            await query.edit_message_text(text=message, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            await update.message.reply_text(text=message, parse_mode='Markdown', reply_markup=reply_markup)
        return True
    return False


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the main menu or routes to the add character flow for new users."""
    user = update.effective_user
    user_characters = get_characters_for_user(user.id)

    if not user_characters:
        await add_character_command(update, context)
        return

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
    else:
        await update.message.reply_text(text=message, reply_markup=reply_markup)


async def add_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Provides a link for the user to add a new EVE Online character."""
    user = update.effective_user
    user_characters = get_characters_for_user(user.id)
    is_new_user = not user_characters

    webapp_base_url = os.getenv('WEBAPP_URL', 'http://localhost:5000')
    login_url = f"{webapp_base_url}/login?user={user.id}"

    if is_new_user:
        message = (
            f"Welcome, {user.first_name}!\n\nTo get started, please authorize a character using the button below."
        )
        keyboard = [[InlineKeyboardButton("➕ Authorize Character", url=login_url)]]
    else:
        message = "To add another character, please use the button below."
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

    if sent_message:
        set_bot_state(f"add_character_prompt_{user.id}", f"{sent_message.chat_id}:{sent_message.message_id}")


async def _show_balance_for_characters(update: Update, context: ContextTypes.DEFAULT_TYPE, characters: list[Character]):
    """Helper function to fetch and display balances for a list of characters."""
    query = update.callback_query
    char_names = ", ".join([c.name for c in characters])

    if query:
        await query.edit_message_text(f"⏳ Fetching balance(s) for {char_names}...")

    message_lines = ["💰 *Wallet Balances* 💰\n"]
    total_balance = 0
    errors = False
    for char in characters:
        if get_character_deletion_status(char.id):
            message_lines.append(f"• `{char.name}`: `(Pending Deletion)`")
            continue

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
    keyboard = [[InlineKeyboardButton("« Back", callback_data="start_command")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query:
        await query.edit_message_text(text=final_text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update.message.reply_text(text=final_text, parse_mode='Markdown')


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the balance command, prompting for character selection if needed."""
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.effective_message.reply_text("You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _show_balance_for_characters(update, context, user_characters)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"balance_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("All Characters", callback_data="balance_char_all")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to view the balance:"
        if update.callback_query:
            await update.callback_query.edit_message_text(text=message_text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text=message_text, reply_markup=reply_markup)


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
                if "message is not modified" not in str(e).lower():
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
    tasks = {
        "public": asyncio.to_thread(get_character_public_info, character.id),
        "online": asyncio.to_thread(get_character_online_status, character)
    }
    results = await asyncio.gather(*tasks.values())
    public_info, online_status = results

    if not public_info:
        await query.edit_message_text(f"❌ Could not fetch public info for {character.name}.")
        return

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

async def chart_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles callbacks from the chart buttons by scheduling a background Celery task."""
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

    await query.message.delete()

    caption_map = {
        'lastday': "Last Day", '7days': "Last 7 Days",
        '30days': "Last 30 Days", 'alltime': "All Time"
    }
    chart_name = caption_map.get(chart_type, chart_type.capitalize())

    generating_message = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"⏳ Generating {chart_name} chart for {character.name}. This may take a moment..."
    )

    generate_chart_task.delay(
        character_id=character_id,
        chart_type=chart_type,
        chat_id=query.message.chat_id,
        generating_message_id=generating_message.message_id
    )


async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """The single, main handler for all callback queries from inline keyboards."""
    query = update.callback_query
    await query.answer()

    data = query.data
    logging.info(f"Received callback query with data: {data}")

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
    elif data.startswith("balance_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        chars_to_query = get_characters_for_user(user_id) if char_id_str == "all" else [get_character_by_id(int(char_id_str))]
        await _show_balance_for_characters(update, context, chars_to_query)
    elif data.startswith("chart_"):
        await chart_callback_handler(update, context)
    elif data.startswith("overview_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        if char_id_str == "all":
            await query.message.delete()
            chars_to_query = get_characters_for_user(user_id)
            for char in chars_to_query:
                await _generate_and_send_overview(Update(update.update_id, message=query.message), context, char)
                await asyncio.sleep(1)
        else:
            char_to_query = get_character_by_id(int(char_id_str))
            await _generate_and_send_overview(update, context, char_to_query)
    elif data.startswith("history_list_sale_"):
        _, _, _, char_id_str, page_str = data.split('_')
        await _display_historical_sales(update, context, int(char_id_str), int(page_str))
    elif data.startswith("history_list_buy_"):
        _, _, _, char_id_str, page_str = data.split('_')
        await _display_historical_buys(update, context, int(char_id_str), int(page_str))
    elif data.startswith("character_info_"):
        char_id = int(data.split('_')[-1])
        await _show_character_info(update, context, get_character_by_id(char_id))
    elif data.startswith("notifications_char_"):
        char_id = int(data.split('_')[-1])
        await _show_notification_settings(update, context, get_character_by_id(char_id))
    elif data.startswith("settings_char_"):
        char_id = int(data.split('_')[-1])
        await _show_character_settings(update, context, get_character_by_id(char_id))
    elif data.startswith("contracts_list_"):
        _, _, char_id_str, page_str = data.split('_')
        await _display_contracts(update, context, int(char_id_str), int(page_str))
    elif data.startswith("toggle_"):
        _, setting, char_id_str = data.split('_')
        char_id = int(char_id_str)
        character = get_character_by_id(char_id)
        current_value = getattr(character, f"enable_{setting}_notifications" if setting != 'overview' else "enable_daily_overview")
        update_character_notification_setting(char_id, setting, not current_value)
        load_characters_from_db()
        await _show_notification_settings(update, context, get_character_by_id(char_id))
    elif data.startswith("set_wallet_"):
        char_id = int(data.split('_')[-1])
        context.user_data['next_action'] = ('set_wallet_value', char_id)
        context.user_data['prompt_message_id'] = query.message.message_id
        await query.edit_message_text("Please enter the new wallet balance threshold (e.g., 100000000 for 100m ISK).\n\nType `cancel` to go back.")
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
        schedule_character_deletion(char_id)
        success_message = (
            f"✅ Character **{char_name}** has been removed.\n\n"
            f"If this was a mistake, you can recover the character and all their data by adding them again within the next hour."
        )
        keyboard = [[InlineKeyboardButton("« Back to Main Menu", callback_data="start_command")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text=success_message, reply_markup=reply_markup, parse_mode='Markdown')
    elif data.startswith("overview_back_"):
        await back_to_overview_handler(update, context)


async def post_init(application: Application):
    """Starts background tasks after initialization."""
    asyncio.create_task(master_wallet_journal_poll(application))
    asyncio.create_task(master_wallet_transaction_poll(application))
    asyncio.create_task(master_order_history_poll(application))
    asyncio.create_task(master_orders_poll(application))
    asyncio.create_task(master_contracts_poll(application))
    logging.info("Master polling loops have been started.")

async def purge_deleted_characters_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Periodically checks for and permanently deletes characters whose deletion grace period has expired.
    """
    logging.info("Running job to purge characters marked for deletion.")
    # This function would call app_utils.delete_character
    pass

def main() -> None:
    """Run the bot."""
    logging.info("Bot starting up...")
    setup_database()
    load_characters_from_db()

    application = Application.builder().token(os.getenv("TELEGRAM_BOT_TOKEN")).post_init(post_init).build()

    # Command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    application.run_polling()

if __name__ == "__main__":
    main()