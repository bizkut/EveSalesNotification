import requests
import telegram
import time
import schedule
import logging
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Character Dataclass and Global List ---

@dataclass
class Character:
    """Represents a single EVE Online character."""
    id: int
    name: str
    refresh_token: str

CHARACTERS: list[Character] = []


def load_characters():
    """Loads all character refresh tokens from the config file and populates the CHARACTERS list."""
    global CHARACTERS

    refresh_tokens = []
    for attr in dir(config):
        if attr.startswith("ESI_REFRESH_TOKEN_"):
            refresh_tokens.append(getattr(config, attr))

    if not refresh_tokens:
        logging.error("No ESI_REFRESH_TOKEN_X variables found in config.py. Bot cannot run.")
        return

    logging.info(f"Found {len(refresh_tokens)} refresh tokens. Verifying and loading character details...")

    for token in refresh_tokens:
        access_token = get_access_token(token)
        if not access_token:
            logging.error(f"Failed to get access token for a refresh token. Skipping.")
            continue

        char_id, char_name = get_character_details_from_token(access_token)
        if not char_id or not char_name:
            logging.error(f"Failed to get character details for a refresh token. Skipping.")
            continue

        # Check for duplicates
        if any(c.id == char_id for c in CHARACTERS):
            logging.warning(f"Character '{char_name}' ({char_id}) is already loaded. Skipping duplicate.")
            continue

        character = Character(id=char_id, name=char_name, refresh_token=token)
        CHARACTERS.append(character)
        logging.info(f"Successfully loaded character: {character.name} ({character.id})")

    if not CHARACTERS:
        logging.error("Could not load any characters successfully. Please check your refresh tokens in config.py.")


# --- Database Functions ---

DB_FILE = "bot_data.db"

def db_connection():
    """Creates a database connection."""
    return sqlite3.connect(DB_FILE)

def setup_database():
    """Creates the necessary database tables if they don't exist."""
    conn = db_connection()
    cursor = conn.cursor()
    # Table for tracking individual transaction notifications
    # A composite primary key is used because transaction_id is only unique per character.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_transactions (
            transaction_id INTEGER NOT NULL,
            character_id INTEGER NOT NULL,
            PRIMARY KEY (transaction_id, character_id)
        )
    """)
    # Table for tracking journal entries for the daily summary
    # A composite primary key is used because entry_id is only unique per character.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_journal_entries (
            entry_id INTEGER NOT NULL,
            character_id INTEGER NOT NULL,
            PRIMARY KEY (entry_id, character_id)
        )
    """)
    # Table for tracking the state of active market orders
    # A composite primary key is used because order_id is only unique per character.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_orders (
            order_id INTEGER NOT NULL,
            character_id INTEGER NOT NULL,
            volume_remain INTEGER NOT NULL,
            PRIMARY KEY (order_id, character_id)
        )
    """)
    # Key-value store for various bot states (e.g., last low-balance alert time per character)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def get_bot_state(key):
    """Retrieves a value from the bot_state key-value store."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def set_bot_state(key, value):
    """Sets a value in the bot_state key-value store."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO bot_state (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def get_tracked_market_orders(character_id):
    """Retrieves all tracked market orders for a specific character."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT order_id, volume_remain FROM market_orders WHERE character_id = ?", (character_id,))
    return {row[0]: row[1] for row in cursor.fetchall()}

def update_tracked_market_orders(character_id, orders):
    """Inserts or updates a list of market orders for a character in the database."""
    if not orders:
        return
    conn = db_connection()
    cursor = conn.cursor()
    orders_with_char_id = [(o[0], character_id, o[1]) for o in orders]
    cursor.executemany(
        "INSERT OR REPLACE INTO market_orders (order_id, character_id, volume_remain) VALUES (?, ?, ?)",
        orders_with_char_id
    )
    conn.commit()
    conn.close()

def remove_tracked_market_orders(character_id, order_ids):
    """Removes a list of market orders for a character from the database."""
    if not order_ids:
        return
    conn = db_connection()
    cursor = conn.cursor()
    orders_to_delete = [(order_id, character_id) for order_id in order_ids]
    cursor.executemany(
        "DELETE FROM market_orders WHERE order_id = ? AND character_id = ?",
        orders_to_delete
    )
    conn.commit()
    conn.close()

def get_processed_transactions(character_id):
    """Retrieves all processed transaction IDs for a character from the database."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT transaction_id FROM processed_transactions WHERE character_id = ?", (character_id,))
    processed_ids = {row[0] for row in cursor.fetchall()}
    conn.close()
    return processed_ids

def add_processed_transactions(character_id, transaction_ids):
    """Adds a list of transaction IDs for a character to the database."""
    if not transaction_ids:
        return
    conn = db_connection()
    cursor = conn.cursor()
    data_to_insert = [(tx_id, character_id) for tx_id in transaction_ids]
    cursor.executemany(
        "INSERT OR IGNORE INTO processed_transactions (transaction_id, character_id) VALUES (?, ?)",
        data_to_insert
    )
    conn.commit()
    conn.close()

def get_processed_journal_entries(character_id):
    """Retrieves all processed journal entry IDs for a character from the database."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT entry_id FROM processed_journal_entries WHERE character_id = ?", (character_id,))
    processed_ids = {row[0] for row in cursor.fetchall()}
    conn.close()
    return processed_ids

def add_processed_journal_entries(character_id, entry_ids):
    """Adds a list of journal entry IDs for a character to the database."""
    if not entry_ids:
        return
    conn = db_connection()
    cursor = conn.cursor()
    data_to_insert = [(entry_id, character_id) for entry_id in entry_ids]
    cursor.executemany(
        "INSERT OR IGNORE INTO processed_journal_entries (entry_id, character_id) VALUES (?, ?)",
        data_to_insert
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

def get_access_token(refresh_token):
    """Refreshes the ESI access token for a given refresh token."""
    url = "https://login.eveonline.com/v2/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Host": "login.eveonline.com"}
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": config.ESI_CLIENT_ID,
        "client_secret": config.ESI_SECRET_KEY
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error refreshing access token: {e}")
        return None

def get_character_details_from_token(access_token):
    """Gets the character ID and name from the access token."""
    url = "https://login.eveonline.com/oauth/verify"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("CharacterID"), data.get("CharacterName")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting character details: {e}")
        return None, None

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

def get_character_skills(access_token, character_id):
    """Fetches character skill levels from ESI."""
    if not character_id:
        return None
    url = f"https://esi.evetech.net/v4/characters/{character_id}/skills/"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"datasource": "tranquility"}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching character skills: {e}")
        return None

def get_market_orders(access_token, character_id):
    """Fetches a character's active market orders."""
    if not character_id:
        return []
    url = f"https://esi.evetech.net/v2/characters/{character_id}/orders/"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"datasource": "tranquility"}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching market orders: {e}")
        return []

def get_wallet_balance(access_token, character_id):
    """Fetches the character's wallet balance."""
    if not character_id:
        return None
    url = f"https://esi.evetech.net/v1/characters/{character_id}/wallet/"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"datasource": "tranquility"}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching wallet balance: {e}")
        return None

def get_market_history(type_id, region_id):
    """Fetches recent market history for a type in a region."""
    url = f"https://esi.evetech.net/v1/markets/{region_id}/history/"
    params = {"type_id": type_id, "datasource": "tranquility"}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        # Return the most recent day's history
        return response.json()[-1] if response.json() else None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching market history for type_id {type_id}: {e}")
        return None

def get_location_name(location_id, access_token):
    """Gets the name of a station or structure."""
    # Location IDs for player-owned structures are > 1,000,000,000
    # Stations and outposts are < 100,000,000
    if location_id > 1000000000:
        url = f"https://esi.evetech.net/v3/universe/structures/{location_id}/"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"datasource": "tranquility"}
    else:
        url = f"https://esi.evetech.net/v2/universe/stations/{location_id}/"
        headers = {}
        params = {"datasource": "tranquility"}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get('name', 'Unknown Location')
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching location name for ID {location_id}: {e}")
        return "Unknown Location"

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

async def check_market_activity_for_character(character: Character):
    """Checks for market activity for a single character."""
    logging.info(f"Checking market activity for {character.name}...")
    access_token = get_access_token(character.refresh_token)
    if not access_token:
        logging.error(f"Could not get access token for {character.name}. Skipping.")
        return

    # --- Wallet Balance Check ---
    wallet_balance = get_wallet_balance(access_token, character.id)
    if wallet_balance is not None and config.WALLET_BALANCE_THRESHOLD > 0:
        state_key = f"low_balance_alert_sent_at_{character.id}"
        last_alert_str = get_bot_state(state_key)
        alert_sent_recently = False
        if last_alert_str:
            last_alert_time = datetime.fromisoformat(last_alert_str)
            if (datetime.now(timezone.utc) - last_alert_time) < timedelta(days=1):
                alert_sent_recently = True

        if wallet_balance < config.WALLET_BALANCE_THRESHOLD and not alert_sent_recently:
            alert_message = (
                f"⚠️ *Low Wallet Balance Warning ({character.name})* ⚠️\n\n"
                f"Your wallet balance has dropped below `{config.WALLET_BALANCE_THRESHOLD:,.2f}` ISK.\n"
                f"**Current Balance:** `{wallet_balance:,.2f}` ISK"
            )
            await send_telegram_message(alert_message)
            set_bot_state(state_key, datetime.now(timezone.utc).isoformat())
        elif wallet_balance >= config.WALLET_BALANCE_THRESHOLD and last_alert_str:
            set_bot_state(state_key, '') # Reset the alert if balance is back up

    # --- Market Order Check ---
    live_orders = get_market_orders(access_token, character.id)
    tracked_orders = get_tracked_market_orders(character.id)

    sales_detected = defaultdict(lambda: {'quantity': 0, 'value': 0, 'location_id': 0, 'price': 0})
    buys_detected = defaultdict(lambda: {'quantity': 0, 'value': 0, 'location_id': 0})
    orders_to_update = []

    for order in live_orders:
        order_id, location_id = order['order_id'], order['location_id']
        orders_to_update.append((order_id, order['volume_remain']))

        if not (1 <= order.get('duration', 0) <= 90): continue

        if order_id in tracked_orders and order['volume_remain'] < tracked_orders[order_id]:
            quantity = tracked_orders[order_id] - order['volume_remain']
            price = order['price']

            if order.get('is_buy_order'):
                buys_detected[order['type_id']].update({
                    'quantity': buys_detected[order['type_id']]['quantity'] + quantity,
                    'value': buys_detected[order['type_id']]['value'] + (quantity * price),
                    'location_id': location_id
                })
            else:
                sales_detected[order['type_id']].update({
                    'quantity': sales_detected[order['type_id']]['quantity'] + quantity,
                    'value': sales_detected[order['type_id']]['value'] + (quantity * price),
                    'location_id': location_id,
                    'price': price # Store the price for market context
                })

    # --- Process Sales ---
    if config.ENABLE_SALES_NOTIFICATIONS.lower() == 'true' and sales_detected:
        logging.info(f"Detected {len(sales_detected)} groups of filled sell orders for {character.name}...")
        item_ids = list(sales_detected.keys())
        loc_ids = [data['location_id'] for data in sales_detected.values()]
        id_to_name = get_names_from_ids(item_ids + loc_ids + [config.REGION_ID])

        for type_id, data in sales_detected.items():
            history = get_market_history(type_id, config.REGION_ID)
            avg_price_str = f"{history['average']:,.2f} ISK" if history else "N/A"
            price_diff_str = f"({(data['price'] / history['average'] - 1):+.2%})" if history and history['average'] > 0 else ""

            message = (
                f"✅ *Market Sale ({character.name})* ✅\n\n"
                f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                f"**Quantity Sold:** `{data['quantity']}`\n"
                f"**Your Price:** `{data['price']:,.2f} ISK`\n"
                f"**{id_to_name.get(config.REGION_ID, 'Region')} Avg Price:** `{avg_price_str}` {price_diff_str}\n"
                f"**Location:** `{id_to_name.get(data['location_id'], 'Unknown')}`\n"
                f"**Wallet Balance:** `{wallet_balance:,.2f} ISK`"
            )
            await send_telegram_message(message)
            time.sleep(1) # Avoid rate-limiting

    # --- Process Buys ---
    if config.ENABLE_BUY_NOTIFICATIONS.lower() == 'true' and buys_detected:
        logging.info(f"Detected {len(buys_detected)} groups of filled buy orders for {character.name}...")
        item_ids = list(buys_detected.keys())
        loc_ids = [data['location_id'] for data in buys_detected.values()]
        id_to_name = get_names_from_ids(item_ids + loc_ids)

        for type_id, data in buys_detected.items():
            message = (
                f"🛒 *Market Buy ({character.name})* 🛒\n\n"
                f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                f"**Quantity Bought:** `{data['quantity']}`\n"
                f"**Total Cost:** `{data['value']:,.2f} ISK`\n"
                f"**Location:** `{id_to_name.get(data['location_id'], 'Unknown')}`\n"
                f"**Wallet Balance:** `{wallet_balance:,.2f} ISK`"
            )
            await send_telegram_message(message)
            time.sleep(1) # Avoid rate-limiting

    # --- Update Database State ---
    update_tracked_market_orders(character.id, orders_to_update)
    live_order_ids = {o['order_id'] for o in live_orders}
    stale_order_ids = set(tracked_orders.keys()) - live_order_ids
    if stale_order_ids:
        remove_tracked_market_orders(character.id, list(stale_order_ids))

    logging.info(f"Finished processing market activity for {character.name}.")


async def check_market_activity():
    """Wrapper function to check market activity for all configured characters."""
    logging.info("Starting market activity check for all characters...")
    for character in CHARACTERS:
        await check_market_activity_for_character(character)
    logging.info("Completed market activity check for all characters.")

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

async def run_daily_summary_for_character(character: Character, all_transactions, journal_entries):
    """Calculates and prepares the daily summary data for a single character."""
    logging.info(f"Calculating daily summary for {character.name}...")

    # --- Fetch wallet balance ---
    access_token = get_access_token(character.refresh_token)
    if not access_token:
        logging.error(f"Could not get access token for {character.name} during summary. Skipping.")
        return None
    wallet_balance = get_wallet_balance(access_token, character.id)

    processed_journal_entries = get_processed_journal_entries(character.id)
    new_journal_entries = [e for e in journal_entries if e['id'] not in processed_journal_entries]

    # Check if there's any new activity to report
    has_new_transactions_24h = any(datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > (datetime.now(timezone.utc) - timedelta(days=1)) for tx in all_transactions)
    if not new_journal_entries and not has_new_transactions_24h:
        logging.info(f"No new journal or transaction entries for {character.name}. Skipping summary message.")
        # Still need to mark journal entries as processed if any exist
        if new_journal_entries:
            add_processed_journal_entries(character.id, [e['id'] for e in new_journal_entries])
        return None

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
        f"📊 *Daily Market Summary ({character.name})*\n"
        f"_{now.strftime('%Y-%m-%d')}_\n\n"
        f"**Wallet Balance:** `{wallet_balance:,.2f} ISK`\n\n"
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

    # Mark journal entries as processed
    processed_ids_for_this_run = [entry['id'] for entry in new_journal_entries]
    add_processed_journal_entries(character.id, processed_ids_for_this_run)
    logging.info(f"Daily summary sent for {character.name}. Processed {len(processed_ids_for_this_run)} new journal entries.")

    # Return stats for combined summary
    return {
        "wallet_balance": wallet_balance,
        "total_sales_24h": total_sales_24h,
        "total_fees_24h": total_brokers_fees_24h + total_transaction_tax_24h,
        "estimated_profit_24h": estimated_profit_24h,
        "total_sales_month": total_sales_month,
        "total_fees_month": total_fees_month,
        "gross_revenue_month": gross_revenue_month
    }

async def run_daily_summary():
    """Wrapper to run the daily summary for all characters and send a combined report."""
    logging.info("Starting daily summary run for all characters...")

    all_character_stats = []

    for character in CHARACTERS:
        access_token = get_access_token(character.refresh_token)
        if not access_token:
            logging.error(f"Could not get access token for {character.name} in daily summary. Skipping.")
            continue

        all_transactions = get_wallet_transactions(access_token, character.id)
        journal_entries = get_wallet_journal(access_token, character.id)

        char_stats = await run_daily_summary_for_character(character, all_transactions, journal_entries)
        if char_stats:
            all_character_stats.append(char_stats)

        time.sleep(1) # Stagger requests slightly

    # If more than one character has stats, send a combined summary
    if len(all_character_stats) > 1:
        logging.info("Generating combined daily summary...")

        combined_wallet = sum(s['wallet_balance'] for s in all_character_stats)
        combined_sales_24h = sum(s['total_sales_24h'] for s in all_character_stats)
        combined_fees_24h = sum(s['total_fees_24h'] for s in all_character_stats)
        combined_profit_24h = sum(s['estimated_profit_24h'] for s in all_character_stats)
        combined_sales_month = sum(s['total_sales_month'] for s in all_character_stats)
        combined_fees_month = sum(s['total_fees_month'] for s in all_character_stats)
        combined_revenue_month = sum(s['gross_revenue_month'] for s in all_character_stats)

        now = datetime.now(timezone.utc)
        message = (
            f"📈 *Combined Daily Market Summary*\n"
            f"_{now.strftime('%Y-%m-%d')}_\n\n"
            f"**Combined Wallet Balance:** `{combined_wallet:,.2f} ISK`\n\n"
            f"**Past 24 Hours (All Characters):**\n"
            f"  - Total Sales Value: `{combined_sales_24h:,.2f} ISK`\n"
            f"  - Total Fees (Broker + Tax): `{combined_fees_24h:,.2f} ISK`\n"
            f"  - **Total Estimated Profit:** `{combined_profit_24h:,.2f} ISK`*\n\n"
            f"---\n\n"
            f"🗓️ **Current Month Summary (All Characters):**\n"
            f"  - Total Sales Value: `{combined_sales_month:,.2f} ISK`\n"
            f"  - Total Fees (Broker + Tax): `{combined_fees_month:,.2f} ISK`\n"
            f"  - **Total Gross Revenue:** `{combined_revenue_month:,.2f} ISK`"
        )
        await send_telegram_message(message)
        logging.info("Combined daily summary sent.")

    logging.info("Daily summary run completed for all characters.")

def initialize_journal_history():
    """On first run, seeds the journal history for each character to avoid summarizing historical entries."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM processed_journal_entries")
    count = cursor.fetchone()[0]
    conn.close()

    if count > 0:
        logging.info(f"Existing journal history found ({count} records). Skipping seeding.")
        return

    logging.info("First run for daily summary detected. Seeding journal history for all characters...")
    for character in CHARACTERS:
        logging.info(f"Seeding journal history for {character.name}...")
        access_token = get_access_token(character.refresh_token)
        if not access_token:
            logging.error(f"Could not obtain access token for {character.name} for journal seeding. Skipping."); continue

        historical_journal = get_wallet_journal(access_token, character.id)
        if not historical_journal:
            logging.info(f"No historical journal entries found to seed for {character.name}."); continue

        historical_ids = [entry['id'] for entry in historical_journal]
        add_processed_journal_entries(character.id, historical_ids)
        logging.info(f"Successfully seeded {len(historical_ids)} historical journal entries for {character.name}.")

def initialize_market_orders():
    """On first run, seeds the market order state for each character to avoid false positives."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM market_orders")
    count = cursor.fetchone()[0]
    conn.close()

    if count > 0:
        logging.info(f"Existing market order history found ({count} records). Skipping seeding.")
        return

    logging.info("First run for market notifications detected. Seeding initial order state for all characters...")
    for character in CHARACTERS:
        logging.info(f"Seeding market orders for {character.name}...")
        access_token = get_access_token(character.refresh_token)
        if not access_token:
            logging.error(f"Could not obtain access token for {character.name} for market order seeding. Skipping."); continue

        live_orders = get_market_orders(access_token, character.id)
        if not live_orders:
            logging.info(f"No active market orders found to seed for {character.name}."); continue

        orders_to_track = [(order['order_id'], order['volume_remain']) for order in live_orders]
        update_tracked_market_orders(character.id, orders_to_track)
        logging.info(f"Successfully seeded {len(orders_to_track)} active market orders for {character.name}.")

if __name__ == "__main__":
    import asyncio
    logging.info("Bot starting up...")

    # Create database tables if they don't exist
    setup_database()

    # Load characters from config
    load_characters()
    if not CHARACTERS:
        logging.error("No characters were loaded. Bot cannot continue. Please check your config.py and run get_refresh_token.py.")
        exit()

    # --- Set up schedules based on config ---
    if config.ENABLE_SALES_NOTIFICATIONS.lower() == 'true' or config.ENABLE_BUY_NOTIFICATIONS.lower() == 'true':
        initialize_market_orders()
        logging.info("Performing initial check for market activity...")
        asyncio.run(check_market_activity())
        schedule.every(60).seconds.do(lambda: asyncio.run(check_market_activity()))
        logging.info(f"Market activity notifications ENABLED (Sales: {config.ENABLE_SALES_NOTIFICATIONS}, Buys: {config.ENABLE_BUY_NOTIFICATIONS}). Checking every 60 seconds.")
    else:
        logging.info("Market activity notifications DISABLED by config.")

    if config.ENABLE_DAILY_SUMMARY.lower() == 'true':
        initialize_journal_history()
        schedule.every().day.at(config.DAILY_SUMMARY_TIME).do(lambda: asyncio.run(run_daily_summary()))
        logging.info(f"Daily summary ENABLED: scheduled for {config.DAILY_SUMMARY_TIME} UTC.")
    else:
        logging.info("Daily summary DISABLED by config.")

    if not schedule.jobs:
        logging.info("No features enabled. Bot will now exit.")
    else:
        logging.info("Entering main loop to run scheduler...")
        while True:
            schedule.run_pending()
            idle_seconds = schedule.idle_seconds()
            if idle_seconds is not None and idle_seconds > 1:
                logging.info(f"Next check in {int(idle_seconds)} seconds. Waiting...")
                time.sleep(idle_seconds)
            else:
                time.sleep(1)