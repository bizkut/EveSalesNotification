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

    # Re-queue the task for the next batch with a small delay to be nice to the ESI API.
    logging.info(f"Queueing next backfill task for character {character_id}, before_id {min_transaction_id}.")
    continue_backfill_character_history.apply_async(args=[character_id], countdown=2) # 2-second delay.