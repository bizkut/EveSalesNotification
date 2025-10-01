import logging
import os
import asyncio
from datetime import datetime, timezone

from telegram.error import BadRequest
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler

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
    # This is incomplete, other handlers would be here
    elif data.startswith("balance_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        chars_to_query = get_characters_for_user(user_id) if char_id_str == "all" else [get_character_by_id(int(char_id_str))]
        await _show_balance_for_characters(update, context, chars_to_query)
    elif data.startswith("chart_"):
        await chart_callback_handler(update, context)
    # ... etc.

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