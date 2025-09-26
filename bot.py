import requests
import time
import logging
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone, timedelta, time as dt_time
from dataclasses import dataclass
import asyncio
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler

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

DATA_DIR = "data"
DB_FILE = os.path.join(DATA_DIR, "bot_data.db")

def db_connection():
    """Creates a database connection."""
    os.makedirs(DATA_DIR, exist_ok=True)
    return sqlite3.connect(DB_FILE)

def setup_database():
    """Creates the necessary database tables if they don't exist and handles schema migration."""
    conn = db_connection()
    cursor = conn.cursor()

    # --- Schema Migration Check ---
    # Check if the 'market_orders' table exists and lacks the 'character_id' column.
    # This indicates an old schema that needs to be updated.
    try:
        cursor.execute("SELECT character_id FROM market_orders LIMIT 1")
    except sqlite3.OperationalError as e:
        if "no such column: character_id" in str(e):
            logging.info("Old database schema detected. Dropping and recreating tables.")
            # Drop old tables that are affected by the schema change
            cursor.execute("DROP TABLE IF EXISTS market_orders")
            cursor.execute("DROP TABLE IF EXISTS processed_transactions")
            cursor.execute("DROP TABLE IF EXISTS processed_journal_entries")
            conn.commit()
            logging.info("Old tables dropped successfully.")
        else:
            # Re-raise other operational errors
            raise

    # --- Table Creation ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_transactions (
            transaction_id INTEGER NOT NULL,
            character_id INTEGER NOT NULL,
            PRIMARY KEY (transaction_id, character_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_journal_entries (
            entry_id INTEGER NOT NULL,
            character_id INTEGER NOT NULL,
            PRIMARY KEY (entry_id, character_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_orders (
            order_id INTEGER NOT NULL,
            character_id INTEGER NOT NULL,
            volume_remain INTEGER NOT NULL,
            PRIMARY KEY (order_id, character_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monthly_summary (
            character_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            total_sales REAL NOT NULL,
            total_fees REAL NOT NULL,
            PRIMARY KEY (character_id, year, month)
        )
    """)
    conn.commit()
    conn.close()

def get_monthly_summary(character_id, year, month):
    """Retrieves the monthly summary for a character from the database."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT total_sales, total_fees FROM monthly_summary WHERE character_id = ? AND year = ? AND month = ?",
        (character_id, year, month)
    )
    result = cursor.fetchone()
    conn.close()
    if result:
        return {"total_sales": result[0], "total_fees": result[1]}
    return {"total_sales": 0, "total_fees": 0}

def update_monthly_summary(character_id, year, month, total_sales, total_fees):
    """Inserts or updates the monthly summary for a character."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO monthly_summary (character_id, year, month, total_sales, total_fees)
        VALUES (?, ?, ?, ?, ?)
        """,
        (character_id, year, month, total_sales, total_fees)
    )
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

def get_wallet_journal(access_token, character_id, processed_entry_ids=None):
    """
    Fetches wallet journal entries from ESI.
    If processed_entry_ids are provided, it will stop fetching when it encounters an already processed entry.
    """
    if not character_id: return []
    if processed_entry_ids is None:
        processed_entry_ids = set()

    new_journal_entries, page = [], 1
    url = f"https://esi.evetech.net/v6/characters/{character_id}/wallet/journal/"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"datasource": "tranquility"}

    stop_fetching = False
    while not stop_fetching:
        try:
            params['page'] = page
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            if not data:
                break # No more pages

            page_entries = []
            for entry in data:
                if entry['id'] in processed_entry_ids:
                    stop_fetching = True
                    break  # Stop processing entries in this page
                page_entries.append(entry)

            new_journal_entries.extend(page_entries)

            if stop_fetching:
                logging.info(f"Found previously processed journal entry. Stopping fetch for char {character_id}.")
                break # Stop fetching more pages

            # Check if there are more pages from header
            pages_header = response.headers.get('x-pages')
            if pages_header and int(pages_header) <= page:
                break # Last page reached

            page += 1
            # Add a small delay to be nice to ESI
            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching wallet journal page {page} for char {character_id}: {e}")
            return [] # Return empty list on error
    return new_journal_entries

def get_access_token(refresh_token):
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
    if not character_id: return []
    url = f"https://esi.evetech.net/v1/characters/{character_id}/wallet/transactions/"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"datasource": "tranquility"}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching wallet transactions: {e}")
        return []

def get_market_orders(access_token, character_id):
    if not character_id: return []
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
    if not character_id: return None
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
    url = f"https://esi.evetech.net/v1/markets/{region_id}/history/"
    params = {"type_id": type_id, "datasource": "tranquility"}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()[-1] if response.json() else None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching market history for type_id {type_id}: {e}")
        return None

def get_names_from_ids(id_list):
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

async def send_telegram_message(context: ContextTypes.DEFAULT_TYPE, message: str):
    """Sends a message to the configured Telegram channel."""
    try:
        await context.bot.send_message(chat_id=config.TELEGRAM_CHANNEL_ID, text=message, parse_mode='Markdown')
        logging.info("Sent message to Telegram channel.")
    except Exception as e:
        logging.error(f"Error sending Telegram message: {e}")

# --- Main Application Logic ---

async def check_market_activity_for_character(character: Character, context: ContextTypes.DEFAULT_TYPE):
    """Checks for market activity for a single character."""
    logging.info(f"Checking market activity for {character.name}...")
    access_token = get_access_token(character.refresh_token)
    if not access_token:
        logging.error(f"Could not get access token for {character.name}. Skipping.")
        return

    wallet_balance = get_wallet_balance(access_token, character.id)
    if wallet_balance is not None and getattr(config, 'WALLET_BALANCE_THRESHOLD', 0) > 0:
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
            await send_telegram_message(context, alert_message)
            set_bot_state(state_key, datetime.now(timezone.utc).isoformat())
        elif wallet_balance >= config.WALLET_BALANCE_THRESHOLD and last_alert_str:
            set_bot_state(state_key, '')

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
                    'price': price
                })

    if getattr(config, 'ENABLE_SALES_NOTIFICATIONS', 'false').lower() == 'true' and sales_detected:
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
            await send_telegram_message(context, message)
            await asyncio.sleep(1)

    if getattr(config, 'ENABLE_BUY_NOTIFICATIONS', 'false').lower() == 'true' and buys_detected:
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
            await send_telegram_message(context, message)
            await asyncio.sleep(1)

    update_tracked_market_orders(character.id, orders_to_update)
    live_order_ids = {o['order_id'] for o in live_orders}
    stale_order_ids = set(tracked_orders.keys()) - live_order_ids
    if stale_order_ids:
        remove_tracked_market_orders(character.id, list(stale_order_ids))
    logging.info(f"Finished processing market activity for {character.name}.")

async def check_market_activity(context: ContextTypes.DEFAULT_TYPE):
    """Wrapper function to check market activity for all configured characters."""
    logging.info("Starting market activity check for all characters...")
    await asyncio.gather(*(check_market_activity_for_character(c, context) for c in CHARACTERS))
    logging.info("Completed market activity check for all characters.")

def calculate_estimated_profit(sales_today, all_buy_transactions):
    if not sales_today: return 0
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
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
            avg_buy_prices[type_id] = 0
    total_sales_value = sum(sale['quantity'] * sale['unit_price'] for sale in sales_today)
    total_estimated_cogs = sum(
        sale['quantity'] * avg_buy_prices.get(sale['type_id'], 0) for sale in sales_today
    )
    return total_sales_value - total_estimated_cogs

async def run_daily_summary_for_character(character: Character, context: ContextTypes.DEFAULT_TYPE):
    """Calculates and prepares the daily summary data for a single character using optimized methods."""
    logging.info(f"Calculating daily summary for {character.name}...")
    access_token = get_access_token(character.refresh_token)
    if not access_token:
        logging.error(f"Could not get access token for {character.name} during summary. Skipping.")
        return None

    # --- Fetch new data only ---
    processed_journal_entries = get_processed_journal_entries(character.id)
    new_journal_entries = get_wallet_journal(access_token, character.id, processed_journal_entries)
    all_transactions = get_wallet_transactions(access_token, character.id) # Still fetches last 30 days, but is less of a bottleneck.

    if not new_journal_entries and not any(datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > (datetime.now(timezone.utc) - timedelta(days=1)) for tx in all_transactions):
        logging.info(f"No new journal or recent transaction entries for {character.name}. Skipping summary message.")
        return None

    now = datetime.now(timezone.utc)
    one_day_ago = now - timedelta(days=1)
    wallet_balance = get_wallet_balance(access_token, character.id)

    # --- 24-Hour Summary (from new entries) ---
    total_brokers_fees_24h = sum(abs(e.get('amount', 0)) for e in new_journal_entries if e.get('ref_type') == 'brokers_fee' and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > one_day_ago)
    total_transaction_tax_24h = sum(abs(e.get('amount', 0)) for e in new_journal_entries if e.get('ref_type') == 'transaction_tax' and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > one_day_ago)
    total_fees_24h = total_brokers_fees_24h + total_transaction_tax_24h

    sales_past_24_hours = [tx for tx in all_transactions if not tx.get('is_buy') and datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > one_day_ago]
    total_sales_24h = sum(s['quantity'] * s['unit_price'] for s in sales_past_24_hours)
    all_buy_transactions = [tx for tx in all_transactions if tx.get('is_buy')]
    estimated_profit_24h = calculate_estimated_profit(sales_past_24_hours, all_buy_transactions) - total_fees_24h

    # --- Monthly Summary (Optimized) ---
    last_summary_month_str = get_bot_state(f"last_summary_month_{character.id}")
    current_month_key = now.strftime('%Y-%m')

    # Check if the month has rolled over
    if last_summary_month_str != current_month_key:
        logging.info(f"Month has changed for {character.name}. Resetting monthly summary.")
        # If we want to be super accurate, we'd calculate the previous month's final summary here.
        # For simplicity, we just start the new month fresh.
        update_monthly_summary(character.id, now.year, now.month, 0, 0)
        set_bot_state(f"last_summary_month_{character.id}", current_month_key)

    # Get current stored totals
    monthly_summary = get_monthly_summary(character.id, now.year, now.month)
    total_sales_month = monthly_summary['total_sales']
    total_fees_month = monthly_summary['total_fees']

    # Add new amounts to totals
    new_sales_this_month = sum(e.get('amount', 0) for e in new_journal_entries if e.get('ref_type') == 'player_trading' and e.get('amount', 0) > 0)
    new_fees_this_month = sum(abs(e.get('amount', 0)) for e in new_journal_entries if e.get('ref_type') in ['brokers_fee', 'transaction_tax'])
    total_sales_month += new_sales_this_month
    total_fees_month += new_fees_this_month
    gross_revenue_month = total_sales_month - total_fees_month

    # --- Send Message & Update State ---
    message = (
        f"📊 *Daily Market Summary ({character.name})*\n"
        f"_{now.strftime('%Y-%m-%d')}_\n\n"
        f"*Wallet Balance:* `{wallet_balance:,.2f} ISK`\n\n"
        f"*Past 24 Hours:*\n"
        f"  - Total Sales Value: `{total_sales_24h:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{total_fees_24h:,.2f} ISK`\n"
        f"  - *Estimated Profit:* `{estimated_profit_24h:,.2f} ISK`\n\n"
        f"---\n\n"
        f"🗓️ *Current Month Summary ({now.strftime('%B %Y')}):*\n"
        f"  - Total Sales Value: `{total_sales_month:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{total_fees_month:,.2f} ISK`\n"
        f"  - *Gross Revenue (Sales - Fees):* `{gross_revenue_month:,.2f} ISK`\n\n"
        f"_Profit is estimated based on the average purchase price of items over the last 30 days._"
    )
    await send_telegram_message(context, message)

    # --- Persist new state ---
    update_monthly_summary(character.id, now.year, now.month, total_sales_month, total_fees_month)
    new_entry_ids = [entry['id'] for entry in new_journal_entries]
    if new_entry_ids:
        add_processed_journal_entries(character.id, new_entry_ids)
    logging.info(f"Daily summary sent for {character.name}. Processed {len(new_entry_ids)} new journal entries.")
    return {
        "wallet_balance": wallet_balance, "total_sales_24h": total_sales_24h,
        "total_fees_24h": total_fees_24h, "estimated_profit_24h": estimated_profit_24h,
        "total_sales_month": total_sales_month, "total_fees_month": total_fees_month,
        "gross_revenue_month": gross_revenue_month
    }

async def run_daily_summary(context: ContextTypes.DEFAULT_TYPE):
    """Wrapper to run the daily summary for all characters and send a combined report."""
    logging.info("Starting daily summary run for all characters...")
    all_character_stats = []
    for character in CHARACTERS:
        # The character-specific function now handles its own data fetching
        char_stats = await run_daily_summary_for_character(character, context)
        if char_stats:
            all_character_stats.append(char_stats)
        await asyncio.sleep(1) # Be nice to ESI, even though sub-functions have waits

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
        await send_telegram_message(context, message)
        logging.info("Combined daily summary sent.")
    logging.info("Daily summary run completed for all characters.")

def initialize_journal_history():
    """
    On first run, seeds the journal history and calculates the initial monthly summary state
    to ensure the first summary report is accurate.
    """
    conn = db_connection()
    cursor = conn.cursor()
    # Check if any character has been seeded before to prevent re-seeding for all
    cursor.execute("SELECT DISTINCT character_id FROM processed_journal_entries")
    seeded_char_ids = {row[0] for row in cursor.fetchall()}
    conn.close()

    logging.info("Checking for unseeded characters for summary history...")

    for character in CHARACTERS:
        if character.id in seeded_char_ids:
            logging.info(f"Character {character.name} already has seeded journal history. Skipping.")
            continue

        logging.info(f"First run for {character.name} detected. Seeding journal and summary history...")
        access_token = get_access_token(character.refresh_token)
        if not access_token:
            logging.error(f"Cannot get access token for {character.name} during seeding.")
            continue

        # Fetch all historical entries (don't pass processed_ids)
        historical_journal = get_wallet_journal(access_token, character.id)
        if not historical_journal:
            logging.warning(f"No historical journal found for {character.name}. Seeding complete with no data.")
            # Mark as processed to prevent trying again
            add_processed_journal_entries(character.id, [-1]) # Add a dummy entry
            continue

        now = datetime.now(timezone.utc)
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Calculate totals for the current month from all of history
        sales_this_month = sum(e.get('amount', 0) for e in historical_journal if e.get('ref_type') == 'player_trading' and e.get('amount', 0) > 0 and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > start_of_month)
        fees_this_month = sum(abs(e.get('amount', 0)) for e in historical_journal if e.get('ref_type') in ['brokers_fee', 'transaction_tax'] and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > start_of_month)

        # Store the calculated initial state
        update_monthly_summary(character.id, now.year, now.month, sales_this_month, fees_this_month)
        set_bot_state(f"last_summary_month_{character.id}", now.strftime('%Y-%m'))
        logging.info(f"Seeded monthly summary for {character.name}: Sales={sales_this_month:,.2f}, Fees={fees_this_month:,.2f}")

        # Mark all historical entries as processed
        historical_ids = [entry['id'] for entry in historical_journal]
        add_processed_journal_entries(character.id, historical_ids)
        logging.info(f"Seeded {len(historical_ids)} historical journal entries for {character.name}.")

def initialize_market_orders():
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM market_orders")
    count = cursor.fetchone()[0]
    conn.close()
    if count > 0:
        logging.info(f"Existing market order history found ({count} records). Skipping seeding.")
        return
    logging.info("First run for market notifications detected. Seeding initial order state...")
    for character in CHARACTERS:
        logging.info(f"Seeding market orders for {character.name}...")
        access_token = get_access_token(character.refresh_token)
        if not access_token: continue
        live_orders = get_market_orders(access_token, character.id)
        if not live_orders: continue
        orders_to_track = [(order['order_id'], order['volume_remain']) for order in live_orders]
        update_tracked_market_orders(character.id, orders_to_track)
        logging.info(f"Seeded {len(orders_to_track)} active market orders for {character.name}.")

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fetches and displays the wallet balance for all characters."""
    logging.info(f"Received /balance command from user {update.effective_user.name}")
    if not CHARACTERS:
        await update.message.reply_text("No characters are loaded. Cannot fetch balances.")
        return

    message_lines = ["💰 *Wallet Balances* 💰\n"]
    total_balance = 0

    for character in CHARACTERS:
        access_token = get_access_token(character.refresh_token)
        if not access_token:
            message_lines.append(f"• `{character.name}`: `Could not refresh token`")
            continue

        balance = get_wallet_balance(access_token, character.id)
        if balance is not None:
            message_lines.append(f"• `{character.name}`: `{balance:,.2f} ISK`")
            total_balance += balance
        else:
            message_lines.append(f"• `{character.name}`: `Error fetching balance`")

    if len(CHARACTERS) > 1:
        message_lines.append(f"\n**Combined Total:** `{total_balance:,.2f} ISK`")

    await update.message.reply_text("\n".join(message_lines), parse_mode='Markdown')

async def summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually triggers the daily summary report."""
    logging.info(f"Received /summary command from user {update.effective_user.name}")
    await update.message.reply_text("Manual summary requested. Generating report, please wait...")
    # This might take a few seconds, so it's good to give user feedback.
    await run_daily_summary(context)

async def _show_character_selection(update: Update, action: str) -> None:
    """Displays an inline keyboard for character selection for a given action (sales/buys)."""
    if not CHARACTERS:
        await update.message.reply_text("No characters are loaded. Cannot perform this action.")
        return

    keyboard = [
        [InlineKeyboardButton(character.name, callback_data=f"{action}:{character.id}")]
        for character in CHARACTERS
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(f"Please select a character to view recent {action}:", reply_markup=reply_markup)


async def sales_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays a character selection menu for viewing recent sales."""
    logging.info(f"Received /sales command from user {update.effective_user.name}")
    await _show_character_selection(update, "sales")


async def buys_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays a character selection menu for viewing recent buys."""
    logging.info(f"Received /buys command from user {update.effective_user.name}")
    await _show_character_selection(update, "buys")

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button clicks for character selection."""
    query = update.callback_query
    await query.answer()  # Acknowledge the button press

    try:
        action, character_id_str = query.data.split(':')
        character_id = int(character_id_str)
    except (ValueError, IndexError):
        await query.edit_message_text(text="Invalid callback data. Please try the command again.")
        return

    character = next((c for c in CHARACTERS if c.id == character_id), None)
    if not character:
        await query.edit_message_text(text="Could not find the selected character. Please try again.")
        return

    await query.edit_message_text(text=f"Fetching recent {action} for {character.name}, please wait...")

    access_token = get_access_token(character.refresh_token)
    if not access_token:
        await query.edit_message_text(text=f"Could not refresh token for {character.name}.")
        return

    all_transactions = get_wallet_transactions(access_token, character.id)
    if not all_transactions:
        await query.edit_message_text(text=f"No transaction history found for {character.name}.")
        return

    is_buy = True if action == 'buys' else False
    filtered_tx = sorted(
        [tx for tx in all_transactions if tx.get('is_buy') == is_buy],
        key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')),
        reverse=True
    )[:5]

    if not filtered_tx:
        await query.edit_message_text(text=f"No recent {action} found for {character.name}.")
        return

    item_ids = [tx['type_id'] for tx in filtered_tx]
    id_to_name = get_names_from_ids(item_ids)

    action_verb = "Bought" if is_buy else "Sold"
    message_lines = [f"✅ *Last 5 {action.capitalize()} for {character.name}* ✅\n"]
    for tx in filtered_tx:
        item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
        date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
        message_lines.append(
            f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each."
        )

    await query.edit_message_text(text="\n".join(message_lines), parse_mode='Markdown')

async def post_init(application: Application):
    """Sets the bot's commands in the Telegram menu after initialization."""
    commands = [
        BotCommand("balance", "Check wallet balances for all characters."),
        BotCommand("summary", "Manually trigger the daily summary report."),
        BotCommand("sales", "View recent sales for a character."),
        BotCommand("buys", "View recent buys for a character."),
    ]
    await application.bot.set_my_commands(commands)
    logging.info("Bot commands have been set in the Telegram menu.")

def main() -> None:
    """Run the bot."""
    logging.info("Bot starting up...")

    setup_database()
    load_characters()
    if not CHARACTERS:
        logging.error("No characters were loaded. Bot cannot continue.")
        return

    application = Application.builder().token(config.TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    # --- Add command handlers ---
    application.add_handler(CommandHandler("balance", balance_command))
    application.add_handler(CommandHandler("summary", summary_command))
    application.add_handler(CommandHandler("sales", sales_command))
    application.add_handler(CommandHandler("buys", buys_command))
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    # --- Schedule Jobs ---
    job_queue = application.job_queue
    if getattr(config, 'ENABLE_SALES_NOTIFICATIONS', 'false').lower() == 'true' or \
       getattr(config, 'ENABLE_BUY_NOTIFICATIONS', 'false').lower() == 'true':
        initialize_market_orders()
        job_queue.run_once(check_market_activity, 5)
        job_queue.run_repeating(check_market_activity, interval=60, first=60)
        logging.info("Market activity notifications ENABLED. Checking every 60 seconds.")
    else:
        logging.info("Market activity notifications DISABLED by config.")

    if getattr(config, 'ENABLE_DAILY_SUMMARY', 'false').lower() == 'true':
        initialize_journal_history()
        try:
            summary_time_str = getattr(config, 'DAILY_SUMMARY_TIME', '12:00')
            time_parts = summary_time_str.split(':')
            summary_time = dt_time(hour=int(time_parts[0]), minute=int(time_parts[1]), tzinfo=timezone.utc)
            job_queue.run_daily(run_daily_summary, time=summary_time)
            logging.info(f"Daily summary ENABLED: scheduled for {summary_time_str} UTC.")
        except (ValueError, TypeError) as e:
            logging.error(f"Could not schedule daily summary. Invalid DAILY_SUMMARY_TIME: {e}")
    else:
        logging.info("Daily summary DISABLED by config.")

    if not job_queue.jobs():
         logging.info("No features enabled. Bot will run without scheduled jobs.")

    logging.info("Bot is running. Polling for updates...")
    application.run_polling()

if __name__ == "__main__":
    main()