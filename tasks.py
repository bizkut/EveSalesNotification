import logging
from celery_app import celery
import telegram
import os
import io

# Import from the new decoupled modules
import app_utils
import charts


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
    logging.info(f"Starting background backfill task for character_id: {character_id}")

    # Use app_utils instead of bot
    character = app_utils.get_character_by_id(character_id)
    if not character:
        logging.error(f"Backfill task failed: Could not find character with ID {character_id}.")
        return

    if not character.is_backfilling:
        logging.warning(f"Backfill task for character {character_id} is stopping because is_backfilling is False.")
        return

    before_id = character.backfill_before_id
    logging.info(f"Fetching transaction history for {character.name} before transaction_id: {before_id or 'latest'}...")

    transactions = app_utils.get_wallet_transactions(character, before_id=before_id)

    if transactions is None:
        logging.error(f"ESI request failed for character {character_id}, before_id {before_id}. Task will be retried.")
        raise Exception(f"ESI fetch failed for char {character_id} before_id {before_id}")

    if not transactions:
        logging.info(f"Backfill complete for character {character.name}. No more transactions found.")
        app_utils.update_character_backfill_state(character_id, is_backfilling=False, before_id=None)
        app_utils.set_bot_state(f"history_backfilled_{character.id}", "true")
        logging.info(f"Finalized backfill status for {character.name}.")
        return

    logging.info(f"Found {len(transactions)} transactions for {character.name}. Saving to DB.")
    app_utils.add_historical_transactions_to_db(character_id, transactions)

    buy_transactions = [tx for tx in transactions if tx.get('is_buy')]
    if buy_transactions:
        for tx in buy_transactions:
            app_utils.add_purchase_lot(character.id, tx['type_id'], tx['quantity'], tx['unit_price'], purchase_date=tx['date'])
        logging.info(f"Seeded {len(buy_transactions)} historical buy transactions from this batch for {character.name}.")

    min_transaction_id = min(tx['transaction_id'] for tx in transactions)
    logging.info(f"Oldest transaction ID in this batch is {min_transaction_id}.")

    app_utils.update_character_backfill_state(character_id, is_backfilling=True, before_id=min_transaction_id)

    if before_id is not None and min_transaction_id >= before_id:
        logging.critical(
            f"Backfill for character {character_id} is stuck. "
            f"The next 'before_id' ({min_transaction_id}) is not less than the previous one ({before_id}). "
            f"Stopping backfill to prevent an infinite loop."
        )
        app_utils.update_character_backfill_state(character_id, is_backfilling=False, before_id=None)
        return

    logging.info(f"Queueing next backfill task for character {character_id}, before_id {min_transaction_id}.")
    continue_backfill_character_history.apply_async(args=[character_id])


@celery.task(name='tasks.generate_chart_task')
def generate_chart_task(chat_id: int, character_id: int, chart_type: str, generating_message_id: int):
    """
    Celery task to generate and send a chart image to the user.
    """
    bot = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
    character = app_utils.get_character_by_id(character_id)
    if not character:
        bot.edit_message_text(text="Error: Could not find character for this chart.", chat_id=chat_id, message_id=generating_message_id)
        return

    now = app_utils.datetime.now(app_utils.timezone.utc)
    chart_key = f"chart:{character_id}:{chart_type}"
    if chart_type == 'lastday':
        chart_key += f":{now.strftime('%Y-%m-%d-%H')}"
    elif chart_type in ['7days', '30days']:
        chart_key += f":{now.strftime('%Y-%m-%d')}"

    is_dirty = app_utils.get_bot_state(f"chart_cache_dirty_{character_id}") == "true"

    caption_map = {
        'lastday': "Last Day", '7days': "Last 7 Days",
        '30days': "Last 30 Days", 'alltime': "All Time"
    }
    caption = f"{caption_map.get(chart_type, chart_type.capitalize())} chart for {character.name}"

    if not (chart_type == 'alltime' and is_dirty):
        cached_chart_data = app_utils.get_cached_chart(chart_key)
        if cached_chart_data:
            logging.info(f"Using cached chart for key: {chart_key}")
            bot.delete_message(chat_id=chat_id, message_id=generating_message_id)
            bot.send_photo(chat_id=chat_id, photo=io.BytesIO(bytes(cached_chart_data)), caption=caption)
            return

    logging.info(f"Generating new chart for key: {chart_key}")
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
        bot.edit_message_text(text=f"An error occurred while generating the chart for {character.name}.", chat_id=chat_id, message_id=generating_message_id)
        return

    bot.delete_message(chat_id=chat_id, message_id=generating_message_id)

    if chart_buffer:
        app_utils.save_chart_to_cache(chart_key, character_id, chart_buffer.getvalue())
        if chart_type == 'alltime':
            app_utils.set_bot_state(f"chart_cache_dirty_{character_id}", "false")
        chart_buffer.seek(0)
        bot.send_photo(chat_id=chat_id, photo=chart_buffer, caption=caption)
    else:
        bot.send_message(chat_id=chat_id, text=f"Could not generate chart for {character.name}. No data available for the period.")