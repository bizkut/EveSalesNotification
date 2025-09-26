import requests
import telegram
import time
import schedule
import logging
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from config import ESI_CLIENT_ID, ESI_SECRET_KEY, ESI_REFRESH_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions ---

DATA_DIR = "data"
DB_FILE = os.path.join(DATA_DIR, "bot_data.db")

def db_connection():
    """Creates a database connection."""
    os.makedirs(DATA_DIR, exist_ok=True)
    return sqlite3.connect(DB_FILE)

def setup_database():
    """Creates the necessary database tables if they don't exist."""
    conn = db_connection()
    cursor = conn.cursor()
    # Table for tracking individual transaction notifications
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_transactions (
            transaction_id INTEGER PRIMARY KEY
        )
    """)
    # Table for tracking journal entries for the daily summary
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_journal_entries (
            entry_id INTEGER PRIMARY KEY
        )
    """)
    conn.commit()
    conn.close()

def get_processed_transactions():
    """Retrieves all processed transaction IDs from the database."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT transaction_id FROM processed_transactions")
    processed_ids = {row[0] for row in cursor.fetchall()}
    conn.close()
    return processed_ids

def add_processed_transactions(transaction_ids):
    """Adds a list of transaction IDs to the database."""
    if not transaction_ids:
        return
    conn = db_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT OR IGNORE INTO processed_transactions (transaction_id) VALUES (?)",
        [(tx_id,) for tx_id in transaction_ids]
    )
    conn.commit()
    conn.close()

def get_processed_journal_entries():
    """Retrieves all processed journal entry IDs from the database."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT entry_id FROM processed_journal_entries")
    processed_ids = {row[0] for row in cursor.fetchall()}
    conn.close()
    return processed_ids

def add_processed_journal_entries(entry_ids):
    """Adds a list of journal entry IDs to the database."""
    if not entry_ids:
        return
    conn = db_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT OR IGNORE INTO processed_journal_entries (entry_id) VALUES (?)",
        [(entry_id,) for entry_id in entry_ids]
    )
    conn.commit()
    conn.close()

# --- ESI API Functions ---

def get_wallet_journal(access_token, character_id):
    """Fetches all wallet journal entries for a character, handling pagination."""
    if not character_id:
        return []

    journal_entries = []
    page = 1
    url = f"https://esi.evetech.net/v6/characters/{character_id}/wallet/journal/"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"datasource": "tranquility"}

    while True:
        try:
            params['page'] = page
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            if not data:
                break

            journal_entries.extend(data)

            pages_header = response.headers.get('x-pages')
            if pages_header and int(pages_header) <= page:
                break

            page += 1

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching wallet journal page {page}: {e}")
            return []

    return journal_entries

def get_access_token():
    """Refreshes the ESI access token."""
    url = "https://login.eveonline.com/v2/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Host": "login.eveonline.com"}
    data = {
        "grant_type": "refresh_token",
        "refresh_token": ESI_REFRESH_TOKEN,
        "client_id": ESI_CLIENT_ID,
        "client_secret": ESI_SECRET_KEY
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error refreshing access token: {e}")
        return None

def get_character_id(access_token):
    """Gets the character ID from the access token."""
    url = "https://login.eveonline.com/oauth/verify"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("CharacterID")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting character ID: {e}")
        return None

def get_wallet_transactions(access_token, character_id):
    """Fetches all wallet transactions for a character."""
    if not character_id:
        return []
    url = f"https://esi.evetech.net/v1/characters/{character_id}/wallet/transactions/"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"datasource": "tranquility"}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        # Returns all transactions, which can be filtered for buys or sells later
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching wallet transactions: {e}")
        return []

def get_names_from_ids(id_list):
    """Resolves a list of IDs to names."""
    if not id_list: return {}
    unique_ids = list(set(id_list))
    url = "https://esi.evetech.net/v3/universe/names/"
    try:
        id_to_name_map = {}
        for i in range(0, len(unique_ids), 1000):
            chunk = unique_ids[i:i+1000]
            response = requests.post(url, headers={"Content-Type": "application/json"}, json=chunk)
            response.raise_for_status()
            for item in response.json():
                id_to_name_map[item['id']] = item['name']
        return id_to_name_map
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching names from IDs: {e}")
        return {}

# --- Telegram Bot Functions ---

async def send_telegram_message(message):
    """Sends a message to the configured Telegram channel."""
    try:
        bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=message, parse_mode='Markdown')
        logging.info("Sent message to Telegram channel.")
    except Exception as e:
        logging.error(f"Error sending Telegram message: {e}")

# --- Main Application Logic ---

async def check_for_new_orders():
    """Main job function to check for and notify about new sales orders."""
    logging.info("Checking for new sales orders...")
    access_token = get_access_token()
    if not access_token:
        logging.error("Could not obtain access token. Skipping check."); return

    character_id = get_character_id(access_token)
    if not character_id:
        logging.error("Could not obtain character ID. Skipping check."); return

    all_transactions = get_wallet_transactions(access_token, character_id)
    filled_orders = [tx for tx in all_transactions if not tx.get('is_buy')]
    processed_transactions = get_processed_transactions()

    new_orders = [o for o in filled_orders if o['transaction_id'] not in processed_transactions]

    if not new_orders:
        logging.info("No new sales orders found."); return

    logging.info(f"Found {len(new_orders)} new sales orders. Grouping them by item and buyer before sending notifications...")

    grouped_sales = defaultdict(lambda: {'total_quantity': 0, 'total_value': 0, 'transaction_ids': []})
    for order in new_orders:
        key = (order['type_id'], order['client_id'])
        grouped_sales[key]['total_quantity'] += order['quantity']
        grouped_sales[key]['total_value'] += order['quantity'] * order['unit_price']
        grouped_sales[key]['transaction_ids'].append(order['transaction_id'])

    all_ids_to_fetch = {id for key in grouped_sales.keys() for id in key}
    id_to_name = get_names_from_ids(list(all_ids_to_fetch))

    for (type_id, client_id), data in grouped_sales.items():
        item_name = id_to_name.get(type_id, "Unknown Item")
        buyer_name = id_to_name.get(client_id, "Unknown Buyer")
        total_quantity = data['total_quantity']
        total_value = data['total_value']
        avg_price = total_value / total_quantity if total_quantity else 0

        message = (
            f"*Bulk Sale Summary* 📦\n\n"
            f"**Item:** `{item_name}` (`{type_id}`)\n"
            f"**Total Quantity:** `{total_quantity}`\n"
            f"**Average Unit Price:** `{avg_price:,.2f} ISK`\n"
            f"**Total Value:** `{total_value:,.2f} ISK`\n"
            f"**Buyer:** `{buyer_name}` (`{client_id}`)"
        )

        await send_telegram_message(message)
        time.sleep(1)

    all_new_transaction_ids = {tx_id for data in grouped_sales.values() for tx_id in data['transaction_ids']}
    add_processed_transactions(list(all_new_transaction_ids))

    logging.info(f"Check complete. Processed {len(all_new_transaction_ids)} new orders in {len(grouped_sales)} groups.")

def calculate_estimated_profit(sales_today, all_buy_transactions):
    """
    Calculates the estimated profit from a list of sales based on the
    average purchase price of those items over the last 30 days.
    """
    from datetime import datetime, timezone, timedelta

    if not sales_today:
        return 0

    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)

    # Create a lookup for average buy prices
    avg_buy_prices = {}
    item_types_sold = {sale['type_id'] for sale in sales_today}

    for type_id in item_types_sold:
        relevant_buys = [
            tx for tx in all_buy_transactions
            if tx['type_id'] == type_id and
            datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > thirty_days_ago
        ]

        if relevant_buys:
            total_isk_spent = sum(buy['quantity'] * buy['unit_price'] for buy in relevant_buys)
            total_quantity_bought = sum(buy['quantity'] for buy in relevant_buys)
            avg_buy_prices[type_id] = total_isk_spent / total_quantity_bought if total_quantity_bought else 0
        else:
            # If no recent buy history, we can't estimate cost, so assume it's 0.
            # This means profit will equal sales value for items not recently purchased.
            avg_buy_prices[type_id] = 0

    # Calculate total sales value and total estimated cost of goods sold
    total_sales_value = sum(sale['quantity'] * sale['unit_price'] for sale in sales_today)
    total_estimated_cogs = sum(
        sale['quantity'] * avg_buy_prices.get(sale['type_id'], 0) for sale in sales_today
    )

    return total_sales_value - total_estimated_cogs

async def run_daily_summary():
    """Calculates and sends a daily summary of market activities."""
    logging.info("Running daily summary...")
    access_token = get_access_token()
    if not access_token:
        logging.error("Could not obtain access token for daily summary."); return

    character_id = get_character_id(access_token)
    if not character_id:
        logging.error("Could not obtain character ID for daily summary."); return

    # --- Fetch all necessary data ---
    all_transactions = get_wallet_transactions(access_token, character_id)
    journal_entries = get_wallet_journal(access_token, character_id)
    processed_journal_entries = get_processed_journal_entries()

    new_journal_entries = [e for e in journal_entries if e['id'] not in processed_journal_entries]

    if not new_journal_entries and not any(datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > (datetime.now(timezone.utc) - timedelta(days=1)) for tx in all_transactions):
        logging.info("No new journal or transaction entries for daily summary."); return

    # --- Define time windows ---
    now = datetime.now(timezone.utc)
    one_day_ago = now - timedelta(days=1)
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # --- Process 24-Hour Stats ---
    total_brokers_fees_24h = sum(abs(e.get('amount', 0)) for e in new_journal_entries if e.get('ref_type') == 'brokers_fee' and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > one_day_ago)
    total_transaction_tax_24h = sum(abs(e.get('amount', 0)) for e in new_journal_entries if e.get('ref_type') == 'transaction_tax' and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > one_day_ago)

    sales_past_24_hours = [tx for tx in all_transactions if not tx.get('is_buy') and datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > one_day_ago]
    total_sales_24h = sum(s['quantity'] * s['unit_price'] for s in sales_past_24_hours)

    all_buy_transactions = [tx for tx in all_transactions if tx.get('is_buy')]
    estimated_profit_24h = calculate_estimated_profit(sales_past_24_hours, all_buy_transactions)
    estimated_profit_24h -= (total_brokers_fees_24h + total_transaction_tax_24h)

    # --- Process Monthly Stats ---
    total_sales_month = sum(e.get('amount', 0) for e in journal_entries if e.get('ref_type') == 'player_trading' and e.get('amount', 0) > 0 and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > start_of_month)
    total_brokers_fees_month = sum(abs(e.get('amount', 0)) for e in journal_entries if e.get('ref_type') == 'brokers_fee' and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > start_of_month)
    total_transaction_tax_month = sum(abs(e.get('amount', 0)) for e in journal_entries if e.get('ref_type') == 'transaction_tax' and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > start_of_month)

    total_fees_month = total_brokers_fees_month + total_transaction_tax_month
    gross_revenue_month = total_sales_month - total_fees_month

    # --- Format and Send Message ---
    message = (
        f"📊 *Daily Market Summary* ({now.strftime('%Y-%m-%d')})\n\n"
        f"**Past 24 Hours:**\n"
        f"  - Total Sales Value: `{total_sales_24h:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{(total_brokers_fees_24h + total_transaction_tax_24h):,.2f} ISK`\n"
        f"  - **Estimated Profit:** `{estimated_profit_24h:,.2f} ISK`*\n\n"
        f"---\n\n"
        f"🗓️ **Current Month Summary ({now.strftime('%B %Y')}):**\n"
        f"  - Total Sales Value: `{total_sales_month:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{total_fees_month:,.2f} ISK`\n"
        f"  - **Gross Revenue (Sales - Fees):** `{gross_revenue_month:,.2f} ISK`\n\n"
        f"_\*Profit is estimated based on the average purchase price of items over the last 30 days._"
    )

    await send_telegram_message(message)

    processed_ids_for_this_run = [entry['id'] for entry in new_journal_entries]
    add_processed_journal_entries(processed_ids_for_this_run)
    logging.info(f"Daily summary sent. Processed {len(processed_ids_for_this_run)} new journal entries.")

def initialize_journal_history():
    """On first run, seeds the journal history to avoid summarizing historical entries."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM processed_journal_entries")
    count = cursor.fetchone()[0]
    conn.close()

    if count > 0:
        logging.info(f"Existing journal history found ({count} records). Skipping seeding.")
        return

    logging.info("First run for daily summary detected. Seeding journal history...")
    access_token = get_access_token()
    if not access_token:
        logging.error("Could not obtain access token for journal seeding."); return

    character_id = get_character_id(access_token)
    if not character_id:
        logging.error("Could not obtain character ID for journal seeding."); return

    historical_journal = get_wallet_journal(access_token, character_id)
    if not historical_journal:
        logging.info("No historical journal entries found to seed."); return

    historical_ids = [entry['id'] for entry in historical_journal]
    add_processed_journal_entries(historical_ids)
    logging.info(f"Successfully seeded {len(historical_ids)} historical journal entries.")

def initialize_transaction_history():
    """On first run, seeds the transaction history to avoid notifying for historical orders."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM processed_transactions")
    count = cursor.fetchone()[0]
    conn.close()

    if count > 0:
        logging.info(f"Existing transaction history found ({count} records). Skipping seeding.")
        return

    logging.info("First run for sales notifications detected. Seeding transaction history...")
    access_token = get_access_token()
    if not access_token:
        logging.error("Could not obtain access token. Failed to seed history."); return

    character_id = get_character_id(access_token)
    if not character_id:
        logging.error("Could not obtain character ID. Failed to seed history."); return

    all_transactions = get_wallet_transactions(access_token, character_id)
    historical_orders = [tx for tx in all_transactions if not tx.get('is_buy')]
    if not historical_orders:
        logging.info("No historical orders found to seed."); return

    historical_ids = [order['transaction_id'] for order in historical_orders]
    add_processed_transactions(historical_ids)
    logging.info(f"Successfully seeded {len(historical_ids)} historical transactions. The bot will now only notify for new sales.")

if __name__ == "__main__":
    logging.info("Bot starting up...")

    # Create database tables if they don't exist
    setup_database()

    # Seed history for both sales and journal on the very first run
    initialize_transaction_history()
    initialize_journal_history()

    # Perform an initial check for sales notifications on startup
    logging.info("Performing initial check for any new sales orders...")
    import asyncio
    asyncio.run(check_for_new_orders())

    # --- Set up schedules ---
    schedule.every(5).minutes.do(lambda: asyncio.run(check_for_new_orders()))
    logging.info("Scheduled sales check: every 5 minutes.")

    # Schedule the daily summary for 22:00 UTC (6am GMT+8)
    schedule.every().day.at("22:00").do(lambda: asyncio.run(run_daily_summary()))
    logging.info("Scheduled daily summary: every day at 22:00 UTC (6am GMT+8).")

    logging.info("Entering main loop to run scheduler...")
    while True:
        schedule.run_pending()
        idle_seconds = schedule.idle_seconds()
        if idle_seconds is not None and idle_seconds > 1:
            logging.info(f"Next check in {int(idle_seconds)} seconds. Waiting...")
            time.sleep(idle_seconds)
        else:
            time.sleep(1)