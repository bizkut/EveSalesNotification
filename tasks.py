import logging
import os
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import asyncio
import io
import json
from datetime import datetime, timezone
from celery_app import celery

# These imports anticipate the refactoring of bot.py into app_utils.py in the next step.
# These functions will be made synchronous and moved to app_utils.
from app_utils import (
    load_characters_from_db,
    get_character_by_id,
    update_character_backfill_state,
    get_wallet_transactions,
    add_historical_transactions_to_db,
    add_purchase_lot,
    get_bot_state,
    set_bot_state,
    get_all_character_ids,
    process_character_wallet,
    process_character_orders,
    process_character_contracts,
    get_new_and_updated_character_info,
    seed_data_for_character,
    get_character_deletion_status,
    cancel_character_deletion,
    reset_update_notification_flag,
    get_characters_to_purge,
    delete_character,
    get_characters_with_daily_overview_enabled,
    send_daily_overview_for_character,
    send_main_menu_sync,
    send_main_menu_async,
    send_telegram_message_sync,
    get_cached_chart,
    save_chart_to_cache,
    generate_last_day_chart,
    generate_last_7_days_chart,
    generate_last_30_days_chart,
    generate_all_time_chart,
    prepare_historical_sales_data,
    _calculate_overview_data,
    _format_overview_message,
    get_characters_for_user,
    prepare_open_orders_data,
    prepare_historical_buys_data,
    prepare_character_info_data
)

# --- Telegram Bot Initialization & Helper ---

def get_bot():
    """Initializes and returns a telegram.Bot instance for use in tasks."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logging.error("TELEGRAM_BOT_TOKEN environment variable not set.")
        raise ValueError("Missing TELEGRAM_BOT_TOKEN")
    return telegram.Bot(token=token)

# --- New Tasks (Triggered by Webapp) ---

async def _send_welcome_sequence(bot: telegram.Bot, telegram_user_id: int, character_name: str):
    """A helper coroutine to run all async Telegram calls in one event loop."""
    # 1. Clean up the initial "Add Character" prompt
    prompt_key = f"add_character_prompt_{telegram_user_id}"
    prompt_message_info = get_bot_state(prompt_key)
    if prompt_message_info:
        try:
            chat_id_str, message_id_str = prompt_message_info.split(':')
            await bot.delete_message(chat_id=int(chat_id_str), message_id=int(message_id_str))
            logging.info(f"Deleted 'add character' prompt for user {telegram_user_id}")
            set_bot_state(prompt_key, "")  # Clear the state
        except Exception as e:
            # Log error but don't stop the sequence
            logging.error(f"Error deleting 'add character' prompt for user {telegram_user_id}: {e}")

    # 2. Construct welcome message and send it with the main menu
    welcome_msg = f"✅ Character **{character_name}** added! Starting initial data sync in the background. This might take a few minutes."
    await send_main_menu_async(bot, telegram_user_id, top_message=welcome_msg)


@celery.task(name='tasks.send_welcome_and_menu')
def send_welcome_and_menu(telegram_user_id: int, character_name: str):
    """
    Sends the initial welcome message and main menu to a new user by running
    an async sequence in a single event loop.
    """
    logging.info(f"Sending welcome message for new character {character_name} to user {telegram_user_id}.")
    bot = get_bot()
    try:
        # Run the entire async sequence in a single event loop.
        asyncio.run(_send_welcome_sequence(bot, telegram_user_id, character_name))
        logging.info(f"Successfully sent welcome sequence to user {telegram_user_id}.")
    except Exception as e:
        logging.error(f"Error running welcome sequence for user {telegram_user_id}: {e}", exc_info=True)


@celery.task(name='tasks.seed_character_data_task')
def seed_character_data_task(character_id: int):
    """
    Task to seed initial data for a new character in the background.
    This now runs silently without notifying the user on completion.
    """
    logging.info(f"Starting background data seed for character_id: {character_id}")
    character = get_character_by_id(character_id)
    if not character:
        logging.error(f"Cannot seed data: Character {character_id} not found.")
        return

    seed_successful = seed_data_for_character(character)

    if seed_successful:
        logging.info(f"Successfully completed silent data seed for character {character_id}.")
    else:
        logging.error(f"Failed to silently seed data for character {character_id}.")


# --- Dispatcher Tasks (Triggered by Celery Beat) ---

@celery.task(name='tasks.dispatch_character_polls')
def dispatch_character_polls():
    """Fetches all active character IDs and dispatches individual polling tasks for each."""
    logging.info("Dispatching character polls...")
    try:
        character_ids = get_all_character_ids()
        for char_id in character_ids:
            logging.debug(f"Queueing polling tasks for character_id: {char_id}")
            poll_wallet.delay(char_id)
            poll_orders.delay(char_id)
            poll_contracts.delay(char_id)
        logging.info(f"Dispatched polls for {len(character_ids)} characters.")
    except Exception as e:
        logging.error(f"Error in dispatch_character_polls: {e}", exc_info=True)


@celery.task(name='tasks.dispatch_daily_overviews')
def dispatch_daily_overviews():
    """Dispatches a daily overview task for each character that has it enabled."""
    logging.info("Dispatching daily overviews...")
    try:
        character_ids = get_characters_with_daily_overview_enabled()
        for char_id in character_ids:
            logging.debug(f"Queueing daily overview for character_id: {char_id}")
            send_daily_overview.delay(char_id)
        logging.info(f"Dispatched daily overviews for {len(character_ids)} characters.")
    except Exception as e:
        logging.error(f"Error in dispatch_daily_overviews: {e}", exc_info=True)

# --- Individual Character Tasks (Triggered by Dispatchers) ---

@celery.task(name='tasks.poll_wallet', autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def poll_wallet(character_id: int):
    """Polls wallet transactions and journal for a single character and sends notifications."""
    logging.info(f"Polling wallet for character_id: {character_id}")
    notifications = process_character_wallet(character_id)
    if notifications:
        bot = get_bot()
        for notification in notifications:
            send_telegram_message_sync(bot, **notification)

@celery.task(name='tasks.poll_orders', autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def poll_orders(character_id: int):
    """Polls market orders (open, undercut, history) for a single character."""
    logging.info(f"Polling orders for character_id: {character_id}")
    notifications = process_character_orders(character_id)
    if notifications:
        bot = get_bot()
        for notification in notifications:
            send_telegram_message_sync(bot, **notification)

@celery.task(name='tasks.poll_contracts', autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def poll_contracts(character_id: int):
    """Polls contracts for a single character and sends notifications."""
    logging.info(f"Polling contracts for character_id: {character_id}")
    notifications = process_character_contracts(character_id)
    if notifications:
        bot = get_bot()
        for notification in notifications:
            send_telegram_message_sync(bot, **notification)

@celery.task(name='tasks.send_daily_overview', autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def send_daily_overview(character_id: int):
    """Generates and sends the daily overview for a single character."""
    logging.info(f"Sending daily overview for character_id: {character_id}")
    bot = get_bot()
    send_daily_overview_for_character(character_id, bot)

# --- Maintenance Tasks (Triggered by Celery Beat) ---

@celery.task(name='tasks.check_new_characters')
def check_new_characters(character_id=None):
    """
    Checks for characters that have been re-authenticated.
    If character_id is provided, it processes that character directly.
    Otherwise, it scans the database for all characters needing an update.
    This handles both scheduled checks and immediate checks from the webapp.
    """
    # Force a reload of characters from the DB to ensure this worker has the latest data.
    load_characters_from_db()
    bot = get_bot()

    try:
        char_ids_to_process = []
        if character_id:
            # Direct invocation from webapp for a specific character
            logging.info(f"Running immediate check for updated character: {character_id}.")
            char_ids_to_process.append(character_id)
        else:
            # Scheduled job to find all characters needing an update
            logging.info("Running scheduled job to check for all updated characters.")
            db_chars_info = get_new_and_updated_character_info()
            if db_chars_info:
                char_ids_to_process = [
                    char_id for char_id, info in db_chars_info.items()
                    if info.get('needs_update')
                ]

        if not char_ids_to_process:
            logging.info("No updated characters to process.")
            return

        for char_id in char_ids_to_process:
            character = get_character_by_id(char_id)
            if not character:
                logging.error(f"Could not find details for character ID {char_id} in the database.")
                continue

            logging.info(f"Processing updated character: {character.name} ({char_id})")
            if get_character_deletion_status(char_id):
                cancel_character_deletion(char_id)
                msg = f"✅ Deletion cancelled for **{character.name}**. Welcome back!"
            else:
                msg = f"✅ Successfully updated permissions for character **{character.name}**."

            send_telegram_message_sync(bot, msg, character.telegram_user_id)
            reset_update_notification_flag(char_id)

            # After sending the status, show the main menu
            send_main_menu_sync(bot, character.telegram_user_id)

    except Exception as e:
        logging.error(f"Error in check_new_characters task: {e}", exc_info=True)


@celery.task(name='tasks.purge_deleted_characters')
def purge_deleted_characters():
    """Periodically purges characters whose deletion grace period has expired."""
    logging.info("Running job to purge deleted characters.")
    try:
        characters_to_purge = get_characters_to_purge()
        if not characters_to_purge:
            return

        bot = get_bot()
        for char_id, char_name, telegram_user_id in characters_to_purge:
            logging.warning(f"Purging character {char_name} ({char_id})...")
            delete_character(char_id)
            msg = f"🗑️ The one-hour grace period for **{char_name}** has expired, and all associated data has been permanently deleted."
            send_telegram_message_sync(bot, msg, chat_id=telegram_user_id)
            logging.warning(f"Purge complete for character {char_name} ({char_id}).")
    except Exception as e:
        logging.error(f"Error in purge_deleted_characters task: {e}", exc_info=True)


@celery.task(
    bind=True,
    name='tasks.continue_backfill_character_history',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 5}
)
def continue_backfill_character_history(self, character_id: int):
    """
    Celery task to gradually backfill transaction history for a character.
    This function now uses app_utils instead of bot.
    """
    logging.info(f"Starting background backfill task for character_id: {character_id}")
    character = get_character_by_id(character_id)
    if not character or not character.is_backfilling:
        logging.warning(f"Backfill task for character {character_id} stopping (is_backfilling is False or char not found).")
        return

    before_id = character.backfill_before_id
    logging.info(f"Fetching transaction history for {character.name} before transaction_id: {before_id or 'latest'}...")

    transactions = get_wallet_transactions(character, before_id=before_id)

    if transactions is None:
        raise Exception(f"ESI fetch failed for char {character_id} before_id {before_id}")

    if not transactions:
        logging.info(f"Backfill complete for character {character.name}.")
        update_character_backfill_state(character_id, is_backfilling=False, before_id=None)
        set_bot_state(f"history_backfilled_{character.id}", datetime.now(timezone.utc).isoformat())
        return

    add_historical_transactions_to_db(character_id, transactions)
    buy_transactions = [tx for tx in transactions if tx.get('is_buy')]
    for tx in buy_transactions:
        add_purchase_lot(character.id, tx['type_id'], tx['quantity'], tx['unit_price'], purchase_date=tx['date'])

    min_transaction_id = min(tx['transaction_id'] for tx in transactions)
    if before_id is not None and min_transaction_id >= before_id:
        logging.warning(f"Backfill for character {character_id} reached the end (min_transaction_id {min_transaction_id} >= before_id {before_id}). Finalizing.")
        update_character_backfill_state(character_id, is_backfilling=False, before_id=None)
        set_bot_state(f"history_backfilled_{character.id}", datetime.now(timezone.utc).isoformat()) # Explicitly mark as complete
        return

    update_character_backfill_state(character_id, is_backfilling=True, before_id=min_transaction_id)
    continue_backfill_character_history.apply_async(args=[character_id])


@celery.task(name='tasks.generate_chart_task')
def generate_chart_task(character_id: int, chart_type: str, chat_id: int, generating_message_id: int, character_index: int = None):
    """
    Celery task to generate and send a chart in the background, with caching.
    This replaces the bot's internal JobQueue for better scalability.
    """
    bot = get_bot()
    character = get_character_by_id(character_id)

    async def run_async_chart_logic():
        if not character:
            await bot.edit_message_text(text="Error: Could not find character for this chart.", chat_id=chat_id, message_id=generating_message_id)
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
        base_caption = f"{caption_map.get(chart_type, chart_type.capitalize())} chart for {character.name}"

        # Contextual "Back" button logic
        if character_index is not None:
            # Coming from a paginated view, so go back to that specific character's page
            back_callback_data = f"overview_char_all_{character_index}"
        else:
            # Coming from a single character view
            back_callback_data = f"overview_char_{character_id}"

        keyboard = [[InlineKeyboardButton("Back to Overview", callback_data=back_callback_data)]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Check for cached chart first
        if not (chart_type == 'alltime' and is_dirty):
            cached_item = get_cached_chart(chart_key)
            if cached_item:
                logging.info(f"Using cached chart for key: {chart_key}")
                cached_chart_data = cached_item.get('chart_data')
                cached_caption_suffix = cached_item.get('caption_suffix', "")
                full_caption = base_caption + (cached_caption_suffix or "")

                await bot.delete_message(chat_id=chat_id, message_id=generating_message_id)
                await bot.send_photo(chat_id=chat_id, photo=io.BytesIO(bytes(cached_chart_data)), caption=full_caption, parse_mode='Markdown', reply_markup=reply_markup)
                return

        logging.info(f"Generating new chart for key: {chart_key} (All-Time Dirty: {is_dirty if chart_type == 'alltime' else 'N/A'})")
        chart_buffer, caption_suffix = None, None
        try:
            # These chart generation functions are synchronous and CPU-bound,
            # so they are fine to call directly from a Celery task.
            if chart_type == 'lastday':
                chart_buffer, caption_suffix = generate_last_day_chart(character_id)
            elif chart_type == '7days':
                chart_buffer, caption_suffix = generate_last_7_days_chart(character_id)
            elif chart_type == '30days':
                chart_buffer, caption_suffix = generate_last_30_days_chart(character_id)
            elif chart_type == 'alltime':
                chart_buffer, caption_suffix = generate_all_time_chart(character_id)
        except Exception as e:
            logging.error(f"Error generating chart for char {character_id}: {e}", exc_info=True)
            await bot.edit_message_text(text=f"An error occurred while generating the chart for {character.name}.", chat_id=chat_id, message_id=generating_message_id, reply_markup=reply_markup)
            return

        # Delete the "Generating..." message
        await bot.delete_message(chat_id=chat_id, message_id=generating_message_id)

        if chart_buffer:
            # Save both the chart and the new caption suffix to the cache
            save_chart_to_cache(chart_key, character_id, chart_buffer.getvalue(), caption_suffix)
            if chart_type == 'alltime':
                set_bot_state(f"chart_cache_dirty_{character_id}", "false")

            full_caption = base_caption + (caption_suffix or "")
            chart_buffer.seek(0)
            await bot.send_photo(chat_id=chat_id, photo=chart_buffer, caption=full_caption, parse_mode='Markdown', reply_markup=reply_markup)
        else:
            await bot.send_message(chat_id=chat_id, text=f"Could not generate chart for {character.name}. No data available for the period.", reply_markup=reply_markup)

    try:
        asyncio.run(run_async_chart_logic())
    except Exception as e:
        logging.error(f"Error in generate_chart_task for character {character_id}: {e}", exc_info=True)


@celery.task(name='tasks.generate_historical_sales_task')
def generate_historical_sales_task(character_id: int, user_id: int, chat_id: int, page: int, message_id: int):
    """
    Celery task to generate and send the historical sales view.
    """
    bot = get_bot()

    message_text, reply_markup_json, status = prepare_historical_sales_data(character_id, user_id, page)

    reply_markup = None
    if reply_markup_json:
        reply_markup = InlineKeyboardMarkup.de_json(json.loads(reply_markup_json), bot)

    async def edit_message():
        try:
            await bot.edit_message_text(
                text=message_text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        except Exception as e:
            logging.error(f"Error editing message for historical sales task: {e}", exc_info=True)
            # As a fallback, send a new message
            await bot.send_message(
                chat_id=chat_id,
                text=message_text,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )

    try:
        asyncio.run(edit_message())
    except Exception as e:
        logging.error(f"Error running asyncio for generate_historical_sales_task: {e}", exc_info=True)


@celery.task(name='tasks.generate_overview_task')
def generate_overview_task(user_id: int, chat_id: int, message_id: int, character_id: int = None, character_index: int = None):
    """
    Celery task to generate and send a character overview.
    Handles both single character and paginated "All Characters" views.
    """
    bot = get_bot()
    user_characters = get_characters_for_user(user_id)
    character = None
    is_paginated = character_index is not None

    if is_paginated:
        if not user_characters or character_index >= len(user_characters):
            logging.error(f"Invalid character_index {character_index} for user {user_id}")
            asyncio.run(bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="Error: Invalid character index for paginated view."))
            return
        character = user_characters[character_index]
    else:
        character = get_character_by_id(character_id)

    if not character:
        logging.error(f"generate_overview_task: Could not find character.")
        asyncio.run(bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="Error: Could not find the requested character."))
        return

    async def send_overview():
        try:
            overview_data = _calculate_overview_data(character)

            pagination_chars = user_characters if is_paginated else None

            message, reply_markup = _format_overview_message(
                overview_data=overview_data,
                character=character,
                user_characters=pagination_chars,
                current_character_index=character_index if is_paginated else 0
            )

            # Add a "Back" button, directing to the character selection menu if it's not a paginated view.
            if not is_paginated:
                back_button_callback = "overview" if len(user_characters) > 1 else "start_command"
                new_keyboard = list(reply_markup.inline_keyboard)
                new_keyboard.append([InlineKeyboardButton("« Back", callback_data=back_button_callback)])
                reply_markup = InlineKeyboardMarkup(new_keyboard)

            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=message,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        except Exception as e:
            logging.error(f"Failed to generate and send overview for {character.name} in task: {e}", exc_info=True)
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=f"❌ An error occurred while generating the overview for {character.name}."
                )
            except Exception as final_e:
                logging.error(f"Failed to even send error message for overview task: {final_e}", exc_info=True)

    try:
        asyncio.run(send_overview())
    except Exception as e:
        logging.error(f"Error running asyncio for generate_overview_task: {e}", exc_info=True)


@celery.task(name='tasks.display_open_orders_task')
def display_open_orders_task(character_id: int, user_id: int, is_buy: bool, page: int, chat_id: int, message_id: int):
    """
    Celery task to generate and send the open orders view.
    """
    bot = get_bot()

    message_text, reply_markup_json, status = prepare_open_orders_data(character_id, user_id, is_buy, page)

    reply_markup = None
    if reply_markup_json:
        reply_markup = InlineKeyboardMarkup.de_json(json.loads(reply_markup_json), bot)

    async def edit_message():
        try:
            await bot.edit_message_text(
                text=message_text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        except Exception as e:
            logging.error(f"Error editing message for display_open_orders_task: {e}", exc_info=True)
            # As a fallback, send a new message
            await bot.send_message(
                chat_id=chat_id,
                text=message_text,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )

    try:
        asyncio.run(edit_message())
    except Exception as e:
        logging.error(f"Error running asyncio for display_open_orders_task: {e}", exc_info=True)


@celery.task(name='tasks.generate_historical_buys_task')
def generate_historical_buys_task(character_id: int, user_id: int, chat_id: int, page: int, message_id: int):
    """
    Celery task to generate and send the historical buys view.
    """
    bot = get_bot()

    message_text, reply_markup_json, status = prepare_historical_buys_data(character_id, user_id, page)

    reply_markup = None
    if reply_markup_json:
        reply_markup = InlineKeyboardMarkup.de_json(json.loads(reply_markup_json), bot)

    async def edit_message():
        try:
            await bot.edit_message_text(
                text=message_text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        except Exception as e:
            logging.error(f"Error editing message for historical buys task: {e}", exc_info=True)
            # As a fallback, send a new message
            await bot.send_message(
                chat_id=chat_id,
                text=message_text,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )

    try:
        asyncio.run(edit_message())
    except Exception as e:
        logging.error(f"Error running asyncio for generate_historical_buys_task: {e}", exc_info=True)


@celery.task(name='tasks.generate_character_info_task')
def generate_character_info_task(character_id: int, chat_id: int, message_id: int):
    """
    Celery task to generate and send the character info view (with image).
    """
    bot = get_bot()

    caption, image_bytes, reply_markup_json, status = prepare_character_info_data(character_id)

    reply_markup = None
    if reply_markup_json:
        reply_markup = InlineKeyboardMarkup.de_json(json.loads(reply_markup_json), bot)

    async def send_char_info():
        try:
            # First, delete the "Fetching..." message
            await bot.delete_message(chat_id=chat_id, message_id=message_id)

            if status == 'success' and image_bytes:
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=io.BytesIO(image_bytes),
                    caption=caption,
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )
            elif status == 'success': # Fallback to text if image creation failed
                 await bot.send_message(
                    chat_id=chat_id,
                    text=caption,
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )
            else: # Handle errors
                await bot.send_message(
                    chat_id=chat_id,
                    text=caption, # caption will contain the error message
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )

        except Exception as e:
            logging.error(f"Error sending character info for task: {e}", exc_info=True)
            # Try to send a text-only error message as a final fallback
            try:
                await bot.send_message(chat_id=chat_id, text="❌ An error occurred while generating the character info.")
            except Exception as final_e:
                logging.error(f"Failed to even send final error message for char info task: {final_e}")


    try:
        asyncio.run(send_char_info())
    except Exception as e:
        logging.error(f"Error running asyncio for generate_character_info_task: {e}", exc_info=True)