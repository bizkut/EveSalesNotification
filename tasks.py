import logging
from celery_app import celery

@celery.task(
    bind=True,
    name='tasks.continue_backfill_character_history',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 5}
)
def continue_backfill_character_history(self, character_id: int):
    """
    Celery task to gradually backfill transaction history for a character, using the last transaction ID as a cursor.
    """
    # Import bot functions locally to avoid circular dependencies
    import bot

    logging.info(f"Starting background backfill task for character_id: {character_id}")

    # It's crucial to get the most up-to-date character object from the DB
    character = bot.get_character_by_id(character_id)
    if not character:
        logging.error(f"Backfill task failed: Could not find character with ID {character_id}.")
        return

    # If the character is no longer marked for backfilling, stop the chain.
    if not character.is_backfilling:
        logging.warning(f"Backfill task for character {character_id} is stopping because is_backfilling is False.")
        return

    # The current 'before_id' is our cursor for the ESI call.
    before_id = character.backfill_before_id

    logging.info(f"Fetching transaction history for {character.name} before transaction_id: {before_id or 'latest'}...")

    # Fetch the next batch of transactions.
    transactions = bot.get_wallet_transactions(character, before_id=before_id)

    if transactions is None:
        # This indicates an ESI error. The task will retry automatically.
        logging.error(f"ESI request failed for character {character_id}, before_id {before_id}. Task will be retried.")
        raise Exception(f"ESI fetch failed for char {character_id} before_id {before_id}")

    if not transactions:
        # This is the end of the history. The backfill is complete.
        logging.info(f"Backfill complete for character {character.name}. No more transactions found.")
        bot.update_character_backfill_state(character_id, is_backfilling=False, before_id=None)
        # Also update the main history backfilled flag to remove the 1-hour grace period for notifications.
        bot.set_bot_state(f"history_backfilled_{character.id}", "true")
        logging.info(f"Finalized backfill status for {character.name}.")
        return

    # We got transactions, so save them to the historical table.
    logging.info(f"Found {len(transactions)} transactions for {character.name}. Saving to DB.")
    bot.add_historical_transactions_to_db(character_id, transactions)

    # Also add any buys to the purchase lots table for future FIFO tracking.
    buy_transactions = [tx for tx in transactions if tx.get('is_buy')]
    if buy_transactions:
        for tx in buy_transactions:
            bot.add_purchase_lot(character.id, tx['type_id'], tx['quantity'], tx['unit_price'], purchase_date=tx['date'])
        logging.info(f"Seeded {len(buy_transactions)} historical buy transactions from this batch for {character.name}.")

    # Find the oldest transaction ID from this batch to use as the next cursor.
    min_transaction_id = min(tx['transaction_id'] for tx in transactions)
    logging.info(f"Oldest transaction ID in this batch is {min_transaction_id}.")

    # Update the character's backfill cursor in the database.
    bot.update_character_backfill_state(character_id, is_backfilling=True, before_id=min_transaction_id)

    # Check for a stuck loop. This should ideally never happen.
    if before_id is not None and min_transaction_id >= before_id:
        logging.critical(
            f"Backfill for character {character_id} is stuck. "
            f"The next 'before_id' ({min_transaction_id}) is not less than the previous one ({before_id}). "
            f"Stopping backfill to prevent an infinite loop."
        )
        bot.update_character_backfill_state(character_id, is_backfilling=False, before_id=None)
        return

    # Re-queue the task for the next batch immediately.
    # Celery's built-in retry mechanism will handle rate-limiting or other ESI errors.
    logging.info(f"Queueing next backfill task for character {character_id}, before_id {min_transaction_id}.")
    continue_backfill_character_history.apply_async(args=[character_id])


@celery.task(
    bind=True,
    name='tasks.generate_chart_task',
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={'max_retries': 3}
)
def generate_chart_task(self, character_id: int, chart_type: str, chat_id: int, generating_message_id: int):
    """
    Celery task to generate and send a chart image to the user.
    """
    import os
    import io
    from datetime import datetime, timezone
    import telegram
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    import charts
    import bot

    # Initialize Telegram Bot
    telegram_bot = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))

    character = bot.get_character_by_id(character_id)
    if not character:
        telegram_bot.edit_message_text(text="Error: Could not find character for this chart.", chat_id=chat_id, message_id=generating_message_id)
        return

    # Caching logic
    now = datetime.now(timezone.utc)
    chart_key = f"chart:{character_id}:{chart_type}"
    if chart_type == 'lastday':
        chart_key += f":{now.strftime('%Y-%m-%d-%H')}"
    elif chart_type in ['7days', '30days']:
        chart_key += f":{now.strftime('%Y-%m-%d')}"

    is_dirty = bot.get_bot_state(f"chart_cache_dirty_{character_id}") == "true"

    caption_map = {
        'lastday': "Last Day", '7days': "Last 7 Days",
        '30days': "Last 30 Days", 'alltime': "All Time"
    }
    caption = f"{caption_map.get(chart_type, chart_type.capitalize())} chart for {character.name}"
    keyboard = [[InlineKeyboardButton("Back to Overview", callback_data=f"overview_back_{character_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if not (chart_type == 'alltime' and is_dirty):
        cached_chart_data = bot.get_cached_chart(chart_key)
        if cached_chart_data:
            logging.info(f"Using cached chart for key: {chart_key}")
            telegram_bot.delete_message(chat_id=chat_id, message_id=generating_message_id)
            telegram_bot.send_photo(chat_id=chat_id, photo=io.BytesIO(bytes(cached_chart_data)), caption=caption, reply_markup=reply_markup)
            return

    logging.info(f"Generating new chart for key: {chart_key} (All-Time Dirty: {is_dirty if chart_type == 'alltime' else 'N/A'})")
    chart_buffer = None
    try:
        if chart_type == 'lastday':
            chart_buffer = charts.generate_last_day_chart(character_id)
        elif chart_type == '7days':
            chart_buffer = charts.generate_last_7_days_chart(character_id)
        elif chart_type == '30days':
            chart_buffer = charts.generate_last_30_days_chart(character_id)
        elif chart_type == 'alltime':
            chart_buffer = charts.generate_all_time_chart(character_id)
    except Exception as e:
        logging.error(f"Error generating chart for char {character_id}: {e}", exc_info=True)
        telegram_bot.edit_message_text(text=f"An error occurred while generating the chart for {character.name}.", chat_id=chat_id, message_id=generating_message_id, reply_markup=reply_markup)
        return

    telegram_bot.delete_message(chat_id=chat_id, message_id=generating_message_id)

    if chart_buffer:
        bot.save_chart_to_cache(chart_key, character_id, chart_buffer.getvalue())
        if chart_type == 'alltime':
            bot.set_bot_state(f"chart_cache_dirty_{character_id}", "false")
        chart_buffer.seek(0)
        telegram_bot.send_photo(chat_id=chat_id, photo=chart_buffer, caption=caption, reply_markup=reply_markup)
    else:
        telegram_bot.send_message(chat_id=chat_id, text=f"Could not generate chart for {character.name}. No data available for the period.", reply_markup=reply_markup)