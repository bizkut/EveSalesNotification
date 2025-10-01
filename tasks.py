import logging
import os
import telegram
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
    send_telegram_message_sync
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

@celery.task(name='tasks.send_welcome_and_menu')
def send_welcome_and_menu(telegram_user_id: int, character_name: str):
    """
    Sends the initial welcome message and main menu to a new user.
    Also cleans up the original "add character" prompt.
    """
    logging.info(f"Sending welcome message for new character {character_name} to user {telegram_user_id}.")
    bot = get_bot()

    # Clean up the initial "Add Character" prompt
    prompt_key = f"add_character_prompt_{telegram_user_id}"
    prompt_message_info = get_bot_state(prompt_key)
    if prompt_message_info:
        try:
            chat_id_str, message_id_str = prompt_message_info.split(':')
            bot.delete_message(chat_id=int(chat_id_str), message_id=int(message_id_str))
            logging.info(f"Deleted 'add character' prompt for user {telegram_user_id}")
            set_bot_state(prompt_key, "") # Clear the state
        except Exception as e:
            logging.error(f"Error deleting 'add character' prompt for user {telegram_user_id}: {e}")

    # Send welcome message and main menu
    welcome_msg = f"✅ Character **{character_name}** added! Starting initial data sync in the background. This might take a few minutes."
    send_telegram_message_sync(bot, welcome_msg, telegram_user_id)
    send_main_menu_sync(bot, telegram_user_id)


@celery.task(name='tasks.seed_character_data_task')
def seed_character_data_task(character_id: int):
    """
    Task to seed initial data for a new character in the background.
    Notifies the user upon completion.
    """
    logging.info(f"Starting background data seed for character_id: {character_id}")
    character = get_character_by_id(character_id)
    if not character:
        logging.error(f"Cannot seed data: Character {character_id} not found.")
        return

    bot = get_bot()
    seed_successful = seed_data_for_character(character)

    if seed_successful:
        msg = f"✅ Sync complete for **{character.name}**! All historical data has been imported."
    else:
        msg = f"⚠️ Failed to import historical data for **{character.name}**. The process will be retried automatically."

    send_telegram_message_sync(bot, msg, character.telegram_user_id)


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
def check_new_characters():
    """
    Periodically checks for characters that have been re-authenticated
    (to update permissions) and notifies the user. The initial creation
    of new characters is now handled by tasks triggered from the webapp.
    """
    logging.info("Running job to check for updated characters.")
    # Force a reload of characters from the DB to ensure this worker has the latest data.
    load_characters_from_db()
    try:
        db_chars_info = get_new_and_updated_character_info()
        if not db_chars_info:
            return

        bot = get_bot()
        for char_id, info in db_chars_info.items():
            # This task now only cares about characters that need an update notification.
            if not info.get('needs_update'):
                continue

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
        set_bot_state(f"history_backfilled_{character.id}", "true")
        return

    add_historical_transactions_to_db(character_id, transactions)
    buy_transactions = [tx for tx in transactions if tx.get('is_buy')]
    for tx in buy_transactions:
        add_purchase_lot(character.id, tx['type_id'], tx['quantity'], tx['unit_price'], purchase_date=tx['date'])

    min_transaction_id = min(tx['transaction_id'] for tx in transactions)
    if before_id is not None and min_transaction_id >= before_id:
        logging.warning(f"Backfill for character {character_id} reached the end (min_transaction_id {min_transaction_id} >= before_id {before_id}). Finalizing.")
        update_character_backfill_state(character_id, is_backfilling=False, before_id=None)
        set_bot_state(f"history_backfilled_{character.id}", "true") # Explicitly mark as complete
        return

    update_character_backfill_state(character_id, is_backfilling=True, before_id=min_transaction_id)
    continue_backfill_character_history.apply_async(args=[character_id])