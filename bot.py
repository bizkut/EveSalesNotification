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
log_level_str = getattr(config, 'LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

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
        elif "no such table" in str(e):
            # This is expected on a fresh database, so we can ignore it and let the table creation proceed.
            logging.info("Fresh database detected. Proceeding with table creation.")
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
        CREATE TABLE IF NOT EXISTS purchase_lots (
            lot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            character_id INTEGER NOT NULL,
            type_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            purchase_date TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_orders (
            order_id INTEGER NOT NULL,
            character_id INTEGER NOT NULL,
            PRIMARY KEY (order_id, character_id)
        )
    """)
    conn.commit()
    conn.close()

def get_processed_orders(character_id):
    """Retrieves all processed order IDs for a character from the database."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT order_id FROM processed_orders WHERE character_id = ?", (character_id,))
    processed_ids = {row[0] for row in cursor.fetchall()}
    conn.close()
    return processed_ids

def add_processed_orders(character_id, order_ids):
    """Adds a list of order IDs for a character to the database."""
    if not order_ids:
        return
    conn = db_connection()
    cursor = conn.cursor()
    data_to_insert = [(o_id, character_id) for o_id in order_ids]
    cursor.executemany(
        "INSERT OR IGNORE INTO processed_orders (order_id, character_id) VALUES (?, ?)",
        data_to_insert
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


def add_purchase_lot(character_id, type_id, quantity, price, purchase_date=None):
    """Adds a new purchase lot to the database, with an optional historical date."""
    conn = db_connection()
    cursor = conn.cursor()
    if purchase_date is None:
        purchase_date = datetime.now(timezone.utc).isoformat()

    cursor.execute(
        """
        INSERT INTO purchase_lots (character_id, type_id, quantity, price, purchase_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        (character_id, type_id, quantity, price, purchase_date)
    )
    conn.commit()
    conn.close()
    logging.debug(f"Recorded purchase for char {character_id}: {quantity} of type {type_id} at {price:,.2f} ISK each on {purchase_date}.")


def get_purchase_lots(character_id, type_id):
    """Retrieves all purchase lots for a specific item, oldest first."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT lot_id, quantity, price FROM purchase_lots WHERE character_id = ? AND type_id = ? ORDER BY purchase_date ASC",
        (character_id, type_id)
    )
    lots = [{"lot_id": row[0], "quantity": row[1], "price": row[2]} for row in cursor.fetchall()]
    conn.close()
    return lots


def update_purchase_lot_quantity(lot_id, new_quantity):
    """Updates the remaining quantity of a purchase lot."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE purchase_lots SET quantity = ? WHERE lot_id = ?", (new_quantity, lot_id))
    conn.commit()
    conn.close()


def delete_purchase_lot(lot_id):
    """Deletes a purchase lot from the database, typically when it's fully consumed."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM purchase_lots WHERE lot_id = ?", (lot_id,))
    conn.commit()
    conn.close()


# --- ESI API Functions ---

ESI_CACHE = {}

def make_esi_request(url, character=None, params=None, data=None, return_headers=False):
    """
    Makes a request to the ESI API, handling caching via ETag and Expires headers.
    Returns the JSON response and optionally the response headers.
    """
    cache_key = f"{url}:{character.id if character else 'public'}:{str(params)}"
    cached_response = ESI_CACHE.get(cache_key)
    headers = {"Accept": "application/json"}
    if character:
        access_token = get_access_token(character.refresh_token)
        if not access_token:
            logging.error(f"Failed to get access token for {character.name}")
            return (None, None) if return_headers else None
        headers["Authorization"] = f"Bearer {access_token}"

    # Check if we have a cached response and if it's still valid
    if cached_response and cached_response.get('expires', datetime.min.replace(tzinfo=timezone.utc)) > datetime.now(timezone.utc):
        logging.debug(f"Returning cached data for {url}")
        return (cached_response['data'], cached_response['headers']) if return_headers else cached_response['data']

    # If we have an ETag, use it
    if cached_response and 'etag' in cached_response:
        headers['If-None-Match'] = cached_response['etag']

    try:
        if data: # POST request
            headers["Content-Type"] = "application/json"
            response = requests.post(url, headers=headers, json=data)
        else: # GET request
            response = requests.get(url, headers=headers, params=params)

        # Handle 'Not Modified' response
        if response.status_code == 304:
            logging.debug(f"304 Not Modified for {url}. Using cached data.")
            expires_dt = datetime.strptime(response.headers['Expires'], '%a, %d %b %Y %H:%M:%S GMT').replace(tzinfo=timezone.utc)
            cached_response['expires'] = expires_dt
            ESI_CACHE[cache_key] = cached_response
            return (cached_response['data'], cached_response['headers']) if return_headers else cached_response['data']

        response.raise_for_status()

        # Parse expires header
        expires_header = response.headers.get('Expires')
        if expires_header:
            expires_dt = datetime.strptime(expires_header, '%a, %d %b %Y %H:%M:%S GMT').replace(tzinfo=timezone.utc)
        else:
            # If no expires header, use a default short cache time
            expires_dt = datetime.now(timezone.utc) + timedelta(seconds=60)

        # Update cache
        new_data = response.json()
        new_cache_entry = {
            'etag': response.headers.get('ETag'),
            'expires': expires_dt,
            'data': new_data,
            'headers': dict(response.headers)
        }
        ESI_CACHE[cache_key] = new_cache_entry
        logging.debug(f"Cached new data for {url}. Expires at {expires_dt}")

        return (new_data, dict(response.headers)) if return_headers else new_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error making ESI request to {url}: {e}")
        # On failure, return the old cached data if it exists, to prevent temporary outages from breaking the bot
        if cached_response:
            logging.warning(f"Returning stale data for {url} due to request failure.")
            return (cached_response['data'], cached_response['headers']) if return_headers else cached_response['data']
        return (None, None) if return_headers else None


def get_wallet_journal(character, processed_entry_ids=None, fetch_all=False):
    """
    Fetches wallet journal entries from ESI.
    If fetch_all is True, retrieves all pages.
    Otherwise, stops fetching when it encounters an already processed entry.
    Returns the list of entries and the headers from the first page request.
    """
    if not character: return [], None
    if processed_entry_ids is None:
        processed_entry_ids = set()

    all_journal_entries, page = [], 1
    url = f"https://esi.evetech.net/v6/characters/{character.id}/wallet/journal/"

    stop_fetching = False
    first_page_headers = None

    while not stop_fetching:
        params = {"datasource": "tranquility", "page": page}
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True)

        if page == 1:
            first_page_headers = headers

        if not data:
            if page == 1:
                logging.error(f"Failed to fetch first page of wallet journal for {character.name}")
                return [], None
            break

        if fetch_all:
            all_journal_entries.extend(data)
        else:
            page_entries = []
            for entry in data:
                if entry['id'] in processed_entry_ids:
                    stop_fetching = True
                    break
                page_entries.append(entry)
            all_journal_entries.extend(page_entries)

            if stop_fetching:
                logging.info(f"Found previously processed journal entry. Stopping fetch for char {character.name}.")
                break

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1)

    return all_journal_entries, first_page_headers

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

def get_wallet_transactions(character, return_headers=False):
    if not character: return None
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/transactions/"
    return make_esi_request(url, character=character, return_headers=return_headers)

def get_market_orders(character, return_headers=False):
    if not character: return None
    url = f"https://esi.evetech.net/v2/characters/{character.id}/orders/"
    return make_esi_request(url, character=character, return_headers=return_headers)

def get_wallet_balance(character, return_headers=False):
    if not character: return None
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/"
    return make_esi_request(url, character=character, return_headers=return_headers)

def get_market_orders_history(character, return_headers=False):
    """
    Fetches all pages of historical market orders from ESI.
    """
    if not character:
        return (None, None) if return_headers else None

    all_orders = []
    page = 1
    url = f"https://esi.evetech.net/v1/characters/{character.id}/orders/history/"
    first_page_headers = None

    while True:
        params = {"datasource": "tranquility", "page": page}
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True)

        if page == 1:
            first_page_headers = headers

        if not data:
            break

        all_orders.extend(data)

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1)

    if return_headers:
        return all_orders, first_page_headers
    return all_orders

def get_market_history(type_id, region_id):
    url = f"https://esi.evetech.net/v1/markets/{region_id}/history/"
    params = {"type_id": type_id, "datasource": "tranquility"}
    history_data = make_esi_request(url, params=params)
    return history_data[-1] if history_data else None

def get_names_from_ids(id_list):
    if not id_list: return {}
    # Filter for valid, positive integer IDs and remove duplicates
    valid_ids = list(set(id for id in id_list if isinstance(id, int) and id > 0))
    if not valid_ids:
        return {}

    url = "https://esi.evetech.net/v3/universe/names/"
    id_to_name_map = {}
    # Break the list into chunks of 1000, the ESI limit
    for i in range(0, len(valid_ids), 1000):
        chunk = valid_ids[i:i+1000]
        # Note: POST requests to /names/ are publicly cached and don't need auth
        name_data = make_esi_request(url, data=chunk)
        if name_data:
            for item in name_data:
                id_to_name_map[item['id']] = item['name']
    return id_to_name_map

# --- Telegram Bot Functions ---

async def send_telegram_message(context: ContextTypes.DEFAULT_TYPE, message: str, chat_id: int = None):
    """Sends a message. If chat_id is provided, sends to that chat. Otherwise, defaults to the configured channel."""
    target_chat_id = chat_id if chat_id is not None else config.TELEGRAM_CHANNEL_ID
    if not target_chat_id:
        logging.error("No chat_id provided and no default TELEGRAM_CHANNEL_ID configured. Cannot send message.")
        return

    try:
        await context.bot.send_message(chat_id=target_chat_id, text=message, parse_mode='Markdown')
        logging.info(f"Sent message to chat_id: {target_chat_id}.")
    except Exception as e:
        logging.error(f"Error sending Telegram message to {target_chat_id}: {e}")

# --- Main Application Logic ---

def calculate_cogs_and_update_lots(character_id, type_id, quantity_sold):
    """
    Calculates the Cost of Goods Sold (COGS) for a sale using FIFO and updates the database.
    Returns the total COGS for the quantity sold.
    """
    lots = get_purchase_lots(character_id, type_id)
    if not lots:
        return None  # Indicates no purchase history

    cogs = 0
    remaining_to_sell = quantity_sold

    for lot in lots:
        if remaining_to_sell <= 0:
            break

        quantity_from_lot = min(remaining_to_sell, lot['quantity'])
        cogs += quantity_from_lot * lot['price']
        remaining_to_sell -= quantity_from_lot

        if quantity_from_lot < lot['quantity']:
            # Lot partially consumed
            new_quantity = lot['quantity'] - quantity_from_lot
            update_purchase_lot_quantity(lot['lot_id'], new_quantity)
            logging.debug(f"Partially consumed lot {lot['lot_id']}. New quantity: {new_quantity}")
        else:
            # Lot fully consumed
            delete_purchase_lot(lot['lot_id'])
            logging.debug(f"Fully consumed and deleted lot {lot['lot_id']}.")

    if remaining_to_sell > 0:
        # This can happen if the user sells items they acquired before the bot started tracking
        logging.warning(
            f"Could not find enough purchase history for char {character_id} to account for sale of {quantity_sold} of type {type_id}. "
            f"Profit calculation may be incomplete for this sale."
        )

    return cogs


def get_next_run_delay(headers):
    """Calculates the delay in seconds until the cache expires, with a small buffer."""
    if not headers or 'Expires' not in headers:
        return 60  # Default to 60s if no header
    try:
        expires_dt = datetime.strptime(headers['Expires'], '%a, %d %b %Y %H:%M:%S GMT').replace(tzinfo=timezone.utc)
        delay = (expires_dt - datetime.now(timezone.utc)).total_seconds()
        return max(delay, 0) + 5  # Add 5s buffer
    except (ValueError, TypeError):
        return 60

async def check_wallet_transactions_job(context: ContextTypes.DEFAULT_TYPE):
    """
    A self-scheduling job that checks for new wallet transactions and sends notifications.
    This is the primary, accurate source for sales and buy notifications.
    """
    character = context.job.data
    logging.debug(f"Running wallet transaction check for {character.name}")

    all_transactions, headers = get_wallet_transactions(character, return_headers=True)
    if all_transactions is None:
        logging.error(f"Failed to fetch wallet transactions for {character.name}. Retrying in 60s.")
        context.job_queue.run_once(check_wallet_transactions_job, 60, data=character)
        return

    processed_tx_ids = get_processed_transactions(character.id)
    new_transactions = [tx for tx in all_transactions if tx['transaction_id'] not in processed_tx_ids]

    if new_transactions:
        logging.info(f"Detected {len(new_transactions)} new transactions for {character.name}.")
        sales = defaultdict(list)
        buys = defaultdict(list)
        for tx in new_transactions:
            if tx['is_buy']:
                buys[tx['type_id']].append({'quantity': tx['quantity'], 'price': tx['unit_price'], 'location_id': tx['location_id']})
            else:
                sales[tx['type_id']].append({'quantity': tx['quantity'], 'price': tx['unit_price'], 'location_id': tx['location_id']})

        all_type_ids = list(sales.keys()) + list(buys.keys())
        all_loc_ids = [t['location_id'] for txs in sales.values() for t in txs] + [t['location_id'] for txs in buys.values() for t in txs]
        id_to_name = get_names_from_ids(list(set(all_type_ids + all_loc_ids + [config.REGION_ID])))

        wallet_balance = get_wallet_balance(character)

        # Check for low balance threshold
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

        batch_threshold = getattr(config, 'NOTIFICATION_BATCH_THRESHOLD', 3)
        enable_sales = getattr(config, 'ENABLE_SALES_NOTIFICATIONS', 'false').lower() == 'true'
        enable_buys = getattr(config, 'ENABLE_BUY_NOTIFICATIONS', 'false').lower() == 'true'

        if sales and enable_sales:
            if len(sales) > batch_threshold:
                message_lines = [f"✅ *Multiple Market Sales ({character.name})* ✅\n"]
                grand_total_value, grand_total_cogs = 0, 0
                for type_id, tx_group in sales.items():
                    total_quantity = sum(t['quantity'] for t in tx_group)
                    total_value = sum(t['quantity'] * t['price'] for t in tx_group)
                    grand_total_value += total_value
                    cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity)
                    if cogs is not None: grand_total_cogs += cogs
                    message_lines.append(f"  • Sold: `{total_quantity}` x `{id_to_name.get(type_id, 'Unknown')}`")
                profit_line = f"\n**Total Gross Profit:** `{grand_total_value - grand_total_cogs:,.2f} ISK`" if grand_total_cogs > 0 else ""
                message_lines.append(f"\n**Total Sale Value:** `{grand_total_value:,.2f} ISK`{profit_line}")
                if wallet_balance is not None: message_lines.append(f"**Wallet:** `{wallet_balance:,.2f} ISK`")
                await send_telegram_message(context, "\n".join(message_lines))
            else:
                for type_id, tx_group in sales.items():
                    total_quantity = sum(t['quantity'] for t in tx_group)
                    total_value = sum(t['quantity'] * t['price'] for t in tx_group)
                    avg_price = total_value / total_quantity
                    cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity)
                    profit_line = f"\n**Gross Profit:** `{total_value - cogs:,.2f} ISK`" if cogs is not None else "\n**Profit:** `N/A`"
                    history = get_market_history(type_id, config.REGION_ID)
                    price_diff_str = f"({(avg_price / history['average'] - 1):+.2%})" if history and history['average'] > 0 else ""
                    message = (f"✅ *Market Sale ({character.name})* ✅\n\n"
                               f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                               f"**Quantity:** `{total_quantity}` @ `{avg_price:,.2f} ISK`\n"
                               f"**{id_to_name.get(config.REGION_ID, 'Region')} Avg:** `{history['average'] if history else 'N/A':,.2f} ISK` {price_diff_str}\n"
                               f"{profit_line}\n"
                               f"**Location:** `{id_to_name.get(tx_group[0]['location_id'], 'Unknown')}`\n"
                               f"**Wallet:** `{wallet_balance:,.2f} ISK`")
                    await send_telegram_message(context, message)
                    await asyncio.sleep(1)

        if buys and enable_buys:
            for type_id, tx_group in buys.items():
                for tx in tx_group:
                    add_purchase_lot(character.id, type_id, tx['quantity'], tx['price'])
            if len(buys) > batch_threshold:
                message_lines = [f"🛒 *Multiple Market Buys ({character.name})* 🛒\n"]
                grand_total_cost = 0
                for type_id, tx_group in buys.items():
                    total_quantity = sum(t['quantity'] for t in tx_group)
                    grand_total_cost += sum(t['quantity'] * t['price'] for t in tx_group)
                    message_lines.append(f"  • Bought: `{total_quantity}` x `{id_to_name.get(type_id, 'Unknown')}`")
                message_lines.append(f"\n**Total Cost:** `{grand_total_cost:,.2f} ISK`")
                if wallet_balance is not None: message_lines.append(f"**Wallet:** `{wallet_balance:,.2f} ISK`")
                await send_telegram_message(context, "\n".join(message_lines))
            else:
                for type_id, tx_group in buys.items():
                    total_quantity = sum(t['quantity'] for t in tx_group)
                    total_cost = sum(t['quantity'] * t['price'] for t in tx_group)
                    message = (f"🛒 *Market Buy ({character.name})* 🛒\n\n"
                               f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                               f"**Quantity:** `{total_quantity}`\n"
                               f"**Total Cost:** `{total_cost:,.2f} ISK`\n"
                               f"**Location:** `{id_to_name.get(tx_group[0]['location_id'], 'Unknown')}`\n"
                               f"**Wallet:** `{wallet_balance:,.2f} ISK`")
                    await send_telegram_message(context, message)
                    await asyncio.sleep(1)

        add_processed_transactions(character.id, [tx['transaction_id'] for tx in new_transactions])

    delay = get_next_run_delay(headers)
    context.job_queue.run_once(check_wallet_transactions_job, delay, data=character)
    logging.info(f"Wallet transaction check for {character.name} complete. Next check in {delay:.2f} seconds.")


async def check_order_history_job(context: ContextTypes.DEFAULT_TYPE):
    """
    A self-scheduling job that checks for cancelled or expired orders.
    """
    character = context.job.data
    logging.debug(f"Running order history check for {character.name}")

    order_history, headers = get_market_orders_history(character, return_headers=True)
    if order_history is None:
        logging.error(f"Failed to fetch order history for {character.name}. Retrying in 3600s.")
        context.job_queue.run_once(check_order_history_job, 3600, data=character)
        return

    processed_order_ids = get_processed_orders(character.id)
    new_orders = [o for o in order_history if o['order_id'] not in processed_order_ids]

    if new_orders:
        logging.info(f"Detected {len(new_orders)} new historical orders for {character.name}.")
        # We only care about non-filled orders here. Fills are handled by the transaction job.
        cancelled_orders = [o for o in new_orders if o.get('state') == 'cancelled']
        expired_orders = [o for o in new_orders if o.get('state') == 'expired']

        if cancelled_orders:
            item_ids = [o['type_id'] for o in cancelled_orders]
            id_to_name = get_names_from_ids(item_ids)
            for order in cancelled_orders:
                message = (f"ℹ️ *Order Cancelled ({character.name})* ℹ️\n"
                           f"Your order for `{order['volume_total']}` x `{id_to_name.get(order['type_id'], 'Unknown')}` was cancelled.")
                await send_telegram_message(context, message)
                await asyncio.sleep(1)

        if expired_orders:
            item_ids = [o['type_id'] for o in expired_orders]
            id_to_name = get_names_from_ids(item_ids)
            for order in expired_orders:
                message = (f"ℹ️ *Order Expired ({character.name})* ℹ️\n"
                           f"Your order for `{order['volume_total']}` x `{id_to_name.get(order['type_id'], 'Unknown')}` has expired.")
                await send_telegram_message(context, message)
                await asyncio.sleep(1)

        add_processed_orders(character.id, [o['order_id'] for o in new_orders])

    delay = get_next_run_delay(headers)
    context.job_queue.run_once(check_order_history_job, delay, data=character)
    logging.info(f"Order history check for {character.name} complete. Next check in {delay:.2f} seconds.")


def calculate_fifo_profit_for_summary(sales_transactions, character_id):
    """
    Calculates the total profit for a given list of sales transactions using
    a read-only FIFO simulation. Does not modify the database.
    """
    if not sales_transactions:
        return 0

    total_sales_value = sum(s['quantity'] * s['unit_price'] for s in sales_transactions)
    total_cogs = 0

    # Group sales by type_id to process them efficiently
    sales_by_type = defaultdict(int)
    for sale in sales_transactions:
        sales_by_type[sale['type_id']] += sale['quantity']

    for type_id, quantity_sold in sales_by_type.items():
        lots = get_purchase_lots(character_id, type_id)
        if not lots:
            # No purchase history for this item, so we can't calculate its COGS
            continue

        remaining_to_account_for = quantity_sold
        for lot in lots:
            if remaining_to_account_for <= 0:
                break

            quantity_from_lot = min(remaining_to_account_for, lot['quantity'])
            total_cogs += quantity_from_lot * lot['price']
            remaining_to_account_for -= quantity_from_lot

    return total_sales_value - total_cogs


async def run_daily_summary_for_character(character: Character, context: ContextTypes.DEFAULT_TYPE, chat_id: int = None):
    """Calculates and prepares the daily summary data for a single character using a stateless approach."""
    logging.info(f"Calculating daily summary for {character.name}...")

    now = datetime.now(timezone.utc)
    one_day_ago = now - timedelta(days=1)

    # --- Fetch data ---
    all_transactions = get_wallet_transactions(character)
    processed_journal_entries = get_processed_journal_entries(character.id)
    new_journal_entries_24h, _ = get_wallet_journal(character, processed_journal_entries)
    all_journal_entries, _ = get_wallet_journal(character, fetch_all=True)

    # --- 24-Hour Summary ---
    total_sales_24h = 0
    profit_24h = 0
    if all_transactions:
        sales_past_24_hours = [tx for tx in all_transactions if not tx.get('is_buy') and datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > one_day_ago]
        total_sales_24h = sum(s['quantity'] * s['unit_price'] for s in sales_past_24_hours)
        total_brokers_fees_24h = sum(abs(e.get('amount', 0)) for e in new_journal_entries_24h if e.get('ref_type') == 'brokers_fee' and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > one_day_ago)
        total_transaction_tax_24h = sum(abs(e.get('amount', 0)) for e in new_journal_entries_24h if e.get('ref_type') == 'transaction_tax' and datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > one_day_ago)
        total_fees_24h = total_brokers_fees_24h + total_transaction_tax_24h
        profit_24h = calculate_fifo_profit_for_summary(sales_past_24_hours, character.id) - total_fees_24h
    else:
        total_fees_24h = 0

    # --- Monthly Summary (Stateless) ---
    total_sales_month = 0
    total_fees_month = 0
    if all_journal_entries:
        current_month_entries = [e for e in all_journal_entries if datetime.fromisoformat(e['date'].replace('Z', '+00:00')).month == now.month and datetime.fromisoformat(e['date'].replace('Z', '+00:00')).year == now.year]
        total_sales_month = sum(e.get('amount', 0) for e in current_month_entries if e.get('ref_type') == 'player_trading' and e.get('amount', 0) > 0)
        total_fees_month = sum(abs(e.get('amount', 0)) for e in current_month_entries if e.get('ref_type') in ['brokers_fee', 'transaction_tax'])

    gross_revenue_month = total_sales_month - total_fees_month
    wallet_balance = get_wallet_balance(character)

    # --- Send Message & Update State ---
    message = (
        f"📊 *Daily Market Summary ({character.name})*\n"
        f"_{now.strftime('%Y-%m-%d')}_\n\n"
        f"*Wallet Balance:* `{wallet_balance or 0:,.2f} ISK`\n\n"
        f"*Past 24 Hours:*\n"
        f"  - Total Sales Value: `{total_sales_24h:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{total_fees_24h:,.2f} ISK`\n"
        f"  - **Profit (FIFO):** `{profit_24h:,.2f} ISK`\n\n"
        f"---\n\n"
        f"🗓️ *Current Month Summary ({now.strftime('%B %Y')}):*\n"
        f"  - Total Sales Value: `{total_sales_month:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{total_fees_month:,.2f} ISK`\n"
        f"  - *Gross Revenue (Sales - Fees):* `{gross_revenue_month:,.2f} ISK`"
    )
    await send_telegram_message(context, message, chat_id=chat_id)

    # Persist the newly processed journal entries for the next 24h summary
    new_entry_ids = [entry['id'] for entry in new_journal_entries_24h]
    if new_entry_ids:
        add_processed_journal_entries(character.id, new_entry_ids)
    logging.info(f"Daily summary sent for {character.name}. Processed {len(new_entry_ids)} new journal entries for 24h summary.")

    return {
        "wallet_balance": wallet_balance, "total_sales_24h": total_sales_24h,
        "total_fees_24h": total_fees_24h, "profit_24h": profit_24h,
        "total_sales_month": total_sales_month, "total_fees_month": total_fees_month,
        "gross_revenue_month": gross_revenue_month
    }

async def run_daily_summary(context: ContextTypes.DEFAULT_TYPE, chat_id: int = None):
    """Wrapper to run the daily summary for all characters and send a combined report."""
    logging.info("Starting daily summary run for all characters...")
    all_character_stats = []
    for character in CHARACTERS:
        # The character-specific function now handles its own data fetching
        char_stats = await run_daily_summary_for_character(character, context, chat_id=chat_id)
        if char_stats:
            all_character_stats.append(char_stats)
        await asyncio.sleep(1) # Be nice to ESI, even though sub-functions have waits

    if len(all_character_stats) > 1:
        logging.info("Generating combined daily summary...")
        combined_wallet = sum(s['wallet_balance'] for s in all_character_stats)
        combined_sales_24h = sum(s['total_sales_24h'] for s in all_character_stats)
        combined_fees_24h = sum(s['total_fees_24h'] for s in all_character_stats)
        combined_profit_24h = sum(s['profit_24h'] for s in all_character_stats)
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
            f"  - **Total Profit (FIFO):** `{combined_profit_24h:,.2f} ISK`\n\n"
            f"---\n\n"
            f"🗓️ **Current Month Summary (All Characters):**\n"
            f"  - Total Sales Value: `{combined_sales_month:,.2f} ISK`\n"
            f"  - Total Fees (Broker + Tax): `{combined_fees_month:,.2f} ISK`\n"
            f"  - **Total Gross Revenue:** `{combined_revenue_month:,.2f} ISK`"
        )
        await send_telegram_message(context, message, chat_id=chat_id)
        logging.info("Combined daily summary sent.")
    logging.info("Daily summary run completed for all characters.")

def initialize_journal_history():
    """
    On first run, seeds the journal history to prevent old entries from appearing in the 24h summary.
    """
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT character_id FROM processed_journal_entries")
    seeded_char_ids = {row[0] for row in cursor.fetchall()}
    conn.close()

    logging.info("Checking for unseeded characters for journal history...")
    for character in CHARACTERS:
        if character.id in seeded_char_ids:
            logging.info(f"Character {character.name} already has seeded journal history. Skipping.")
            continue

        logging.info(f"First run for {character.name} detected. Seeding journal history...")
        # We only care about marking existing entries as processed, so we don't need the headers.
        # We also don't need to fetch all pages, just enough to find an overlap or hit the end.
        historical_journal, _ = get_wallet_journal(character, fetch_all=True)
        if not historical_journal:
            logging.warning(f"No historical journal found for {character.name}. Seeding complete with no data.")
            add_processed_journal_entries(character.id, [-1]) # Dummy entry to mark as processed
            continue

        historical_ids = [entry['id'] for entry in historical_journal]
        add_processed_journal_entries(character.id, historical_ids)
        logging.info(f"Seeded {len(historical_ids)} historical journal entries for {character.name}.")


def initialize_purchase_history():
    """On first run, seeds the database with historical buy transactions to enable profit tracking."""
    logging.info("Checking for unseeded characters for purchase history...")
    for character in CHARACTERS:
        state_key = f"purchase_history_seeded_{character.id}"
        if get_bot_state(state_key) == 'true':
            logging.info(f"Purchase history already seeded for {character.name}. Skipping.")
            continue

        logging.info(f"Seeding purchase history for {character.name}...")
        all_transactions = get_wallet_transactions(character)
        if not all_transactions:
            logging.info(f"No historical transactions found for {character.name}.")
            set_bot_state(state_key, 'true')
            continue

        buy_transactions = [tx for tx in all_transactions if tx.get('is_buy')]
        if not buy_transactions:
            logging.info(f"No historical buy transactions found for {character.name}.")
            set_bot_state(state_key, 'true')
            continue

        for tx in buy_transactions:
            add_purchase_lot(character.id, tx['type_id'], tx['quantity'], tx['unit_price'], purchase_date=tx['date'])

        set_bot_state(state_key, 'true')
        logging.info(f"Successfully seeded {len(buy_transactions)} historical buy transactions for {character.name}.")


def initialize_order_history():
    """On first run, seeds the database with all historical orders to prevent old notifications."""
    logging.info("Checking for unseeded characters for order history...")
    for character in CHARACTERS:
        state_key = f"order_history_seeded_{character.id}"
        if get_bot_state(state_key) == 'true':
            logging.info(f"Order history already seeded for {character.name}. Skipping.")
            continue

        logging.info(f"Seeding order history for {character.name}...")
        all_historical_orders = get_market_orders_history(character)
        if all_historical_orders:
            order_ids = [o['order_id'] for o in all_historical_orders]
            add_processed_orders(character.id, order_ids)
            logging.info(f"Successfully seeded {len(order_ids)} historical orders for {character.name}.")
        else:
            logging.info(f"No historical orders found to seed for {character.name}.")

        set_bot_state(state_key, 'true')


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fetches and displays the wallet balance for the configured character(s)."""
    logging.info(f"Received /balance command from user {update.effective_user.name}")
    if not CHARACTERS:
        await update.message.reply_text("No characters are loaded. Cannot fetch balances.")
        return

    if len(CHARACTERS) == 1:
        character = CHARACTERS[0]
        await update.message.reply_text(f"Fetching balance for {character.name}...")
        balance = get_wallet_balance(character)
        if balance is not None:
            message = f"💰 *Wallet Balance for {character.name}*\n\n`{balance:,.2f} ISK`"
            await update.message.reply_text(text=message, parse_mode='Markdown')
        else:
            await update.message.reply_text(f"Error fetching balance for {character.name}.")
    else:
        await _show_character_selection(update, "balance", include_all=True)


async def summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually triggers the daily summary report for the configured character(s)."""
    logging.info(f"Received /summary command from user {update.effective_user.name}")
    if not CHARACTERS:
        await update.message.reply_text("No characters are loaded. Cannot run summary.")
        return

    chat_id = update.effective_chat.id
    if len(CHARACTERS) == 1:
        character = CHARACTERS[0]
        await update.message.reply_text(f"Generating summary for {character.name}...")
        await run_daily_summary_for_character(character, context, chat_id=chat_id)
    else:
        await _show_character_selection(update, "summary", include_all=True)

async def _show_character_selection(update: Update, action: str, include_all: bool = False) -> None:
    """Displays an inline keyboard for character selection for a given action."""
    keyboard = [
        [InlineKeyboardButton(character.name, callback_data=f"{action}:{character.id}")]
        for character in CHARACTERS
    ]
    if include_all and len(CHARACTERS) > 1:
        keyboard.append([InlineKeyboardButton("All Characters", callback_data=f"{action}:all")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(f"Please select a character for the /{action} command:", reply_markup=reply_markup)


async def sales_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the 5 most recent sales for the configured character(s)."""
    logging.info(f"Received /sales command from user {update.effective_user.name}")
    if not CHARACTERS:
        await update.message.reply_text("No characters are loaded. Cannot fetch sales.")
        return

    if len(CHARACTERS) == 1:
        character = CHARACTERS[0]
        await update.message.reply_text(f"Fetching recent sales for {character.name}...")
        all_transactions = get_wallet_transactions(character)
        if not all_transactions:
            await update.message.reply_text(f"No transaction history found for {character.name}.")
            return

        filtered_tx = sorted([tx for tx in all_transactions if not tx.get('is_buy')], key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)[:5]
        if not filtered_tx:
            await update.message.reply_text(f"No recent sales found for {character.name}.")
            return

        item_ids = [tx['type_id'] for tx in filtered_tx]
        id_to_name = get_names_from_ids(item_ids)
        message_lines = [f"✅ *Last 5 Sales for {character.name}* ✅\n"]
        for tx in filtered_tx:
            item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
            date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each.")
        await update.message.reply_text("\n".join(message_lines), parse_mode='Markdown')
    else:
        await _show_character_selection(update, "sales")


async def buys_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the 5 most recent buys for the configured character(s)."""
    logging.info(f"Received /buys command from user {update.effective_user.name}")
    if not CHARACTERS:
        await update.message.reply_text("No characters are loaded. Cannot fetch buys.")
        return

    if len(CHARACTERS) == 1:
        character = CHARACTERS[0]
        await update.message.reply_text(f"Fetching recent buys for {character.name}...")
        all_transactions = get_wallet_transactions(character)
        if not all_transactions:
            await update.message.reply_text(f"No transaction history found for {character.name}.")
            return

        filtered_tx = sorted([tx for tx in all_transactions if tx.get('is_buy')], key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)[:5]
        if not filtered_tx:
            await update.message.reply_text(f"No recent buys found for {character.name}.")
            return

        item_ids = [tx['type_id'] for tx in filtered_tx]
        id_to_name = get_names_from_ids(item_ids)
        message_lines = [f"🛒 *Last 5 Buys for {character.name}* 🛒\n"]
        for tx in filtered_tx:
            item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
            date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each.")
        await update.message.reply_text("\n".join(message_lines), parse_mode='Markdown')
    else:
        await _show_character_selection(update, "buys")

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button clicks for character selection."""
    query = update.callback_query
    await query.answer()

    try:
        action, character_id_str = query.data.split(':')
    except (ValueError, IndexError):
        await query.edit_message_text(text="Invalid callback data.")
        return

    chat_id = update.effective_chat.id

    if character_id_str == "all":
        if action == "balance":
            await query.edit_message_text(text="Fetching balances for all characters...")
            message_lines = ["💰 *Wallet Balances* 💰\n"]
            total_balance = 0
            for char in CHARACTERS:
                balance = get_wallet_balance(char)
                if balance is not None:
                    message_lines.append(f"• `{char.name}`: `{balance:,.2f} ISK`")
                    total_balance += balance
                else:
                    message_lines.append(f"• `{char.name}`: `Error fetching balance`")
            message_lines.append(f"\n**Combined Total:** `{total_balance:,.2f} ISK`")
            await query.edit_message_text(text="\n".join(message_lines), parse_mode='Markdown')
        elif action == "summary":
            await query.edit_message_text(text="Generating summary for all characters...")
            await run_daily_summary(context, chat_id=chat_id)
        return

    try:
        character_id = int(character_id_str)
    except ValueError:
        await query.edit_message_text(text="Invalid character ID.")
        return

    character = next((c for c in CHARACTERS if c.id == character_id), None)
    if not character:
        await query.edit_message_text(text="Could not find the selected character.")
        return

    await query.edit_message_text(text=f"Processing /{action} for {character.name}, please wait...")

    if action == "balance":
        balance = get_wallet_balance(character)
        if balance is not None:
            message = f"💰 *Wallet Balance for {character.name}*\n\n`{balance:,.2f} ISK`"
            await query.edit_message_text(text=message, parse_mode='Markdown')
        else:
            await query.edit_message_text(text=f"Error fetching balance for {character.name}.")
    elif action == "summary":
        await run_daily_summary_for_character(character, context, chat_id=chat_id)
    elif action in ["sales", "buys"]:
        all_transactions = get_wallet_transactions(character)
        if not all_transactions:
            await query.edit_message_text(text=f"No transaction history found for {character.name}.")
            return

        is_buy = True if action == 'buys' else False
        filtered_tx = sorted([tx for tx in all_transactions if tx.get('is_buy') == is_buy], key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)[:5]
        if not filtered_tx:
            await query.edit_message_text(text=f"No recent {action} found for {character.name}.")
            return

        item_ids = [tx['type_id'] for tx in filtered_tx]
        id_to_name = get_names_from_ids(item_ids)
        message_lines = [f"✅ *Last 5 {action.capitalize()} for {character.name}* ✅\n"]
        for tx in filtered_tx:
            item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
            date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each.")
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
        initialize_purchase_history()
        initialize_order_history()
        for character in CHARACTERS:
            # Start the self-scheduling jobs for each character
            job_queue.run_once(check_wallet_transactions_job, 5, data=character, name=f"wallet_transactions_{character.id}")
            job_queue.run_once(check_order_history_job, 15, data=character, name=f"order_history_{character.id}")
        logging.info("Market activity notifications ENABLED. Jobs are now self-scheduling based on cache timers.")
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