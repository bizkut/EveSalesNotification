import logging
import os
import asyncio
from telegram.error import BadRequest
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler

import app_utils
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

async def check_and_handle_pending_deletion(update: Update, context: ContextTypes.DEFAULT_TYPE, character: app_utils.Character) -> bool:
    """
    Checks if a character is pending deletion. If so, sends an informative message and returns True.
    """
    if not character:
        return False

    deletion_time = app_utils.get_character_deletion_status(character.id)
    if deletion_time:
        query = update.callback_query
        time_left = deletion_time - app_utils.datetime.now(app_utils.timezone.utc)
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
    user_characters = app_utils.get_characters_for_user(user.id)

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
    user_characters = app_utils.get_characters_for_user(user.id)
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
        app_utils.set_bot_state(f"add_character_prompt_{user.id}", f"{sent_message.chat_id}:{sent_message.message_id}")


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the balance command, prompting for character selection if needed."""
    user_id = update.effective_user.id
    user_characters = app_utils.get_characters_for_user(user_id)

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

async def overview_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Manually triggers the daily overview report. Prompts for character
    selection if the user has multiple characters via an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = app_utils.get_characters_for_user(user_id)

    if not user_characters:
        await update.effective_message.reply_text("You have no characters added. Please use `/add_character` first.")
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
            await update.callback_query.edit_message_text(text=message_text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text=message_text, reply_markup=reply_markup)

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

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to manage their general settings
    using an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = app_utils.get_characters_for_user(user_id)

    if not user_characters:
        await update.effective_message.reply_text("You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _show_character_settings(update, context, user_characters[0])
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"settings_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to manage their settings:"

        if update.callback_query:
            await update.callback_query.edit_message_text(text=message_text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text=message_text, reply_markup=reply_markup)

async def remove_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to remove using an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = app_utils.get_characters_for_user(user_id)

    if not user_characters:
        await update.effective_message.reply_text("You have no characters to remove.")
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
            await update.callback_query.edit_message_text(text=message_text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(text=message_text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        keyboard = [[InlineKeyboardButton(f"Remove {char.name}", callback_data=f"remove_select_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to remove. This action is permanent and will delete all of their data."
        if update.callback_query:
            await update.callback_query.edit_message_text(text=message_text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text=message_text, reply_markup=reply_markup)

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
            character = app_utils.get_character_by_id(character_id)
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
            app_utils.update_character_setting(character_id, 'wallet_balance_threshold', new_threshold)
            app_utils.load_characters_from_db()
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
    """Handles callbacks from the chart buttons by scheduling a Celery task."""
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

    character = app_utils.get_character_by_id(character_id)
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

    # Offload the chart generation to the Celery worker
    generate_chart_task.delay(
        chat_id=query.message.chat_id,
        character_id=character_id,
        chart_type=chart_type,
        generating_message_id=generating_message.message_id
    )

async def post_init(application: Application):
    """Starts background tasks after initialization."""
    asyncio.create_task(app_utils.master_wallet_journal_poll(application))
    asyncio.create_task(app_utils.master_wallet_transaction_poll(application))
    asyncio.create_task(app_utils.master_order_history_poll(application))
    asyncio.create_task(app_utils.master_orders_poll(application))
    asyncio.create_task(app_utils.master_contracts_poll(application))
    logging.info("Master polling loops have been started.")

def main() -> None:
    """Run the bot."""
    logging.info("Bot starting up...")
    app_utils.setup_database()
    app_utils.load_characters_from_db()

    application = Application.builder().token(os.getenv("TELEGRAM_BOT_TOKEN")).post_init(post_init).build()

    # Command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    application.run_polling()

if __name__ == "__main__":
    main()