import requests
import time
import logging
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone, timedelta, time as dt_time
from dataclasses import dataclass
import asyncio
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters

import config

# Configure logging
log_level_str = getattr(config, 'LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Character Dataclass and Global List ---

@dataclass
class Character:
    """Represents a single EVE Online character and their settings."""
    id: int
    name: str
    refresh_token: str
    telegram_user_id: int
    notifications_enabled: bool
    region_id: int
    daily_summary_time: str
    wallet_balance_threshold: int
    enable_sales_notifications: bool
    enable_buy_notifications: bool
    enable_daily_summary: bool
    notification_batch_threshold: int

CHARACTERS: list[Character] = []

# --- Constants for the Reply Keyboard ---
ADD_CHARACTER_TEXT = "➕ Add Character"
NOTIFICATIONS_TEXT = "🔔 Manage Notifications"
SETTINGS_TEXT = "⚙️ Settings"
BALANCE_TEXT = "💰 View Balances"
SUMMARY_TEXT = "📊 Request Summary"
SALES_TEXT = "📈 View Sales"
BUYS_TEXT = "🛒 View Buys"

MAIN_MENU_KEYBOARD = [
    [ADD_CHARACTER_TEXT, NOTIFICATIONS_TEXT, SETTINGS_TEXT],
    [BALANCE_TEXT, SUMMARY_TEXT],
    [SALES_TEXT, BUYS_TEXT]
]

def load_characters_from_db():
    """Loads all characters and their settings from the database."""
    global CHARACTERS
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            character_id, character_name, refresh_token, telegram_user_id,
            notifications_enabled, region_id, daily_summary_time, wallet_balance_threshold,
            enable_sales_notifications, enable_buy_notifications, enable_daily_summary,
            notification_batch_threshold
        FROM characters
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logging.warning("No characters found in the database.")
        return

    logging.info(f"Loading {len(rows)} characters from the database...")
    CHARACTERS = []  # Clear the list to allow for reloading
    for row in rows:
        (
            char_id, name, refresh_token, telegram_user_id, notifications_enabled,
            region_id, daily_summary_time, wallet_balance_threshold,
            enable_sales, enable_buys, enable_summary, batch_threshold
        ) = row

        if any(c.id == char_id for c in CHARACTERS):
            logging.warning(f"Character '{name}' ({char_id}) is already loaded. Skipping duplicate.")
            continue

        character = Character(
            id=char_id, name=name, refresh_token=refresh_token,
            telegram_user_id=telegram_user_id,
            notifications_enabled=bool(notifications_enabled),
            region_id=region_id, daily_summary_time=daily_summary_time,
            wallet_balance_threshold=wallet_balance_threshold,
            enable_sales_notifications=bool(enable_sales),
            enable_buy_notifications=bool(enable_buys),
            enable_daily_summary=bool(enable_summary),
            notification_batch_threshold=batch_threshold
        )
        CHARACTERS.append(character)
        logging.info(f"Loaded character: {character.name} ({character.id})")

    if not CHARACTERS:
        logging.error("Could not load any characters successfully from the database.")


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

    # --- New Tables for Multi-User Support ---
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telegram_users (
            telegram_id INTEGER PRIMARY KEY
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS characters (
            character_id INTEGER PRIMARY KEY,
            character_name TEXT NOT NULL,
            refresh_token TEXT NOT NULL,
            telegram_user_id INTEGER NOT NULL,
            -- Per-character settings with sane defaults
            notifications_enabled BOOLEAN DEFAULT 1,
            region_id INTEGER DEFAULT 10000002,
            daily_summary_time TEXT DEFAULT '22:00',
            wallet_balance_threshold INTEGER DEFAULT 0,
            enable_sales_notifications BOOLEAN DEFAULT 1,
            enable_buy_notifications BOOLEAN DEFAULT 1,
            enable_daily_summary BOOLEAN DEFAULT 1,
            notification_batch_threshold INTEGER DEFAULT 3,
            FOREIGN KEY (telegram_user_id) REFERENCES telegram_users (telegram_id)
        )
    """)

    # --- Existing Tables ---
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS esi_names (
            item_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    logging.info("Database setup/verification complete. All tables are present.")

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


def get_names_from_db(id_list):
    """Retrieves a mapping of id -> name from the local database for the given IDs."""
    if not id_list:
        return {}
    conn = db_connection()
    cursor = conn.cursor()
    # Create a string of placeholders for the query
    placeholders = ','.join('?' for _ in id_list)
    cursor.execute(f"SELECT item_id, name FROM esi_names WHERE item_id IN ({placeholders})", id_list)
    id_to_name_map = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    logging.debug(f"Resolved {len(id_to_name_map)} names from local DB cache.")
    return id_to_name_map


def save_names_to_db(id_to_name_map):
    """Saves a mapping of id -> name to the local database."""
    if not id_to_name_map:
        return
    conn = db_connection()
    cursor = conn.cursor()
    data_to_insert = list(id_to_name_map.items())
    cursor.executemany(
        "INSERT OR IGNORE INTO esi_names (item_id, name) VALUES (?, ?)",
        data_to_insert
    )
    conn.commit()
    conn.close()
    logging.debug(f"Saved {len(data_to_insert)} new names to local DB cache.")


def get_characters_for_user(telegram_user_id):
    """Retrieves all characters and their settings for a given Telegram user ID."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            character_id, character_name, refresh_token, telegram_user_id,
            notifications_enabled, region_id, daily_summary_time, wallet_balance_threshold,
            enable_sales_notifications, enable_buy_notifications, enable_daily_summary,
            notification_batch_threshold
        FROM characters WHERE telegram_user_id = ?
    """, (telegram_user_id,))
    rows = cursor.fetchall()
    conn.close()

    user_characters = []
    for row in rows:
        (
            char_id, name, refresh_token, telegram_user_id, notifications_enabled,
            region_id, daily_summary_time, wallet_balance_threshold,
            enable_sales, enable_buys, enable_summary, batch_threshold
        ) = row

        user_characters.append(Character(
            id=char_id, name=name, refresh_token=refresh_token,
            telegram_user_id=telegram_user_id,
            notifications_enabled=bool(notifications_enabled),
            region_id=region_id, daily_summary_time=daily_summary_time,
            wallet_balance_threshold=wallet_balance_threshold,
            enable_sales_notifications=bool(enable_sales),
            enable_buy_notifications=bool(enable_buys),
            enable_daily_summary=bool(enable_summary),
            notification_batch_threshold=batch_threshold
        ))
    return user_characters


def set_character_notification_status(character_id, new_status: bool):
    """Updates the notification status for a specific character."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE characters SET notifications_enabled = ? WHERE character_id = ?", (int(new_status), character_id))
    conn.commit()
    conn.close()
    logging.info(f"Set notification status for character {character_id} to {new_status}.")


def get_character_by_id(character_id: int) -> Character | None:
    """Retrieves a single character and their settings by character ID."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            character_id, character_name, refresh_token, telegram_user_id,
            notifications_enabled, region_id, daily_summary_time, wallet_balance_threshold,
            enable_sales_notifications, enable_buy_notifications, enable_daily_summary,
            notification_batch_threshold
        FROM characters WHERE character_id = ?
    """, (character_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    (
        char_id, name, refresh_token, telegram_user_id, notifications_enabled,
        region_id, daily_summary_time, wallet_balance_threshold,
        enable_sales, enable_buys, enable_summary, batch_threshold
    ) = row

    return Character(
        id=char_id, name=name, refresh_token=refresh_token,
        telegram_user_id=telegram_user_id,
        notifications_enabled=bool(notifications_enabled),
        region_id=region_id, daily_summary_time=daily_summary_time,
        wallet_balance_threshold=wallet_balance_threshold,
        enable_sales_notifications=bool(enable_sales),
        enable_buy_notifications=bool(enable_buys),
        enable_daily_summary=bool(enable_summary),
        notification_batch_threshold=batch_threshold
    )


def update_character_setting(character_id: int, setting: str, value: any):
    """Updates a specific setting for a character in the database."""
    # Whitelist settings to prevent SQL injection
    allowed_settings = ["region_id", "daily_summary_time", "wallet_balance_threshold"]
    if setting not in allowed_settings:
        logging.error(f"Attempted to update an invalid setting: {setting}")
        return

    conn = db_connection()
    cursor = conn.cursor()
    query = f"UPDATE characters SET {setting} = ? WHERE character_id = ?"
    cursor.execute(query, (value, character_id))
    conn.commit()
    conn.close()
    logging.info(f"Updated {setting} for character {character_id} to {value}.")


# --- ESI API Functions ---

ESI_CACHE = {}

def make_esi_request(url, character=None, params=None, data=None, return_headers=False, force_revalidate=False):
    """
    Makes a request to the ESI API, handling caching via ETag and Expires headers.
    If force_revalidate is True, it will ignore the time-based cache and use an ETag.
    Returns the JSON response and optionally the response headers.
    """
    # Create a cache key that includes request parameters and the POST body (if any)
    # to ensure uniqueness for different API calls.
    data_key_part = ""
    if data:
        # For POST requests with a list of IDs, sort them to ensure cache key consistency.
        if isinstance(data, list):
            data_key_part = str(sorted(data))
        else:
            data_key_part = str(data)
    cache_key = f"{url}:{character.id if character else 'public'}:{str(params)}:{data_key_part}"
    cached_response = ESI_CACHE.get(cache_key)
    headers = {"Accept": "application/json"}
    if character:
        access_token = get_access_token(character.refresh_token)
        if not access_token:
            logging.error(f"Failed to get access token for {character.name}")
            return (None, None) if return_headers else None
        headers["Authorization"] = f"Bearer {access_token}"

    # Check if we have a cached response and if it's still valid
    if not force_revalidate and cached_response and cached_response.get('expires', datetime.min.replace(tzinfo=timezone.utc)) > datetime.now(timezone.utc):
        logging.debug(f"Returning cached data for {url}")
        return (cached_response['data'], cached_response['headers']) if return_headers else cached_response['data']

    # If we have an ETag, use it. This is the core of revalidation.
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

def get_wallet_transactions(character, return_headers=False, force_revalidate=False):
    if not character: return None
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/transactions/"
    return make_esi_request(url, character=character, return_headers=return_headers, force_revalidate=force_revalidate)

def get_market_orders(character, return_headers=False, force_revalidate=False):
    if not character: return None
    url = f"https://esi.evetech.net/v2/characters/{character.id}/orders/"
    return make_esi_request(url, character=character, return_headers=return_headers, force_revalidate=force_revalidate)

def get_wallet_balance(character, return_headers=False):
    if not character: return None
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/"
    return make_esi_request(url, character=character, return_headers=return_headers)

def get_market_orders_history(character, return_headers=False, force_revalidate=False):
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
        # Only force revalidation on the first page, as subsequent pages are unlikely to change
        # if the first one hasn't. This is a small optimization.
        revalidate_this_page = force_revalidate and page == 1
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=revalidate_this_page)

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


def get_region_market_orders(region_id, type_id, force_revalidate=False):
    """
    Fetches all pages of market orders for a specific type in a region from ESI.
    """
    all_orders = []
    page = 1
    url = f"https://esi.evetech.net/v1/markets/{region_id}/orders/"

    while True:
        params = {"datasource": "tranquility", "page": page, "type_id": type_id}
        # Only force revalidation on the first page.
        revalidate_this_page = force_revalidate and page == 1
        data, headers = make_esi_request(url, params=params, return_headers=True, force_revalidate=revalidate_this_page)

        if not data:
            break

        all_orders.extend(data)

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1) # Be nice to ESI

    return all_orders


def get_market_history(type_id, region_id, force_revalidate=False):
    url = f"https://esi.evetech.net/v1/markets/{region_id}/history/"
    params = {"type_id": type_id, "datasource": "tranquility"}
    history_data = make_esi_request(url, params=params, force_revalidate=force_revalidate)
    return history_data[-1] if history_data else None

def get_names_from_ids(id_list):
    """
    Resolves a list of IDs to names, using a local database cache
    to avoid unnecessary ESI calls.
    """
    if not id_list:
        return {}

    # Filter for unique, valid, positive integer IDs.
    unique_ids = list(set(id for id in id_list if isinstance(id, int) and id > 0))
    if not unique_ids:
        logging.warning("get_names_from_ids: No valid IDs provided.")
        return {}

    logging.debug(f"get_names_from_ids called for {len(unique_ids)} unique IDs.")

    # 1. Check local database cache first
    all_resolved_names = get_names_from_db(unique_ids)

    # 2. Identify which names are missing from the cache
    missing_ids = [id for id in unique_ids if id not in all_resolved_names]

    # 3. If there are missing IDs, fetch them from ESI
    if missing_ids:
        # Filter out IDs > 10^10, which are typically player-owned structures
        # and not resolvable by the /universe/names/ endpoint.
        valid_esi_ids = [id for id in missing_ids if id < 10000000000]

        if valid_esi_ids:
            logging.info(f"Resolving {len(valid_esi_ids)} new names from ESI.")
            url = "https://esi.evetech.net/v3/universe/names/"
            newly_resolved_names = {}

            # Break the list into chunks of 1000 (ESI limit)
            for i in range(0, len(valid_esi_ids), 1000):
                chunk = valid_esi_ids[i:i+1000]
                logging.debug(f"Requesting ESI names for chunk of {len(chunk)} IDs.")
                name_data = make_esi_request(url, data=chunk)
                if name_data:
                    for item in name_data:
                        newly_resolved_names[item['id']] = item['name']
                else:
                    logging.error(f"Failed to resolve ESI names for chunk starting with ID {chunk[0]}.")

            # 4. Save the newly resolved names to the database cache
            if newly_resolved_names:
                save_names_to_db(newly_resolved_names)
                all_resolved_names.update(newly_resolved_names)

        else:
            logging.debug("All missing IDs were filtered out before ESI request (e.g., structure IDs).")

    logging.info(f"get_names_from_ids resolved a total of {len(all_resolved_names)}/{len(unique_ids)} names.")
    return all_resolved_names

# --- Telegram Bot Functions ---

async def send_telegram_message(context: ContextTypes.DEFAULT_TYPE, message: str, chat_id: int):
    """Sends a message to a specific chat_id."""
    if not chat_id:
        logging.error("No chat_id provided. Cannot send message.")
        return

    try:
        await context.bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown')
        logging.info(f"Sent message to chat_id: {chat_id}.")
    except Exception as e:
        if "bot was blocked by the user" in str(e).lower():
            logging.warning(f"Could not send message to {chat_id}: Bot was blocked by the user.")
            # Future improvement: Disable notifications for this user in the database.
        else:
            logging.error(f"Error sending Telegram message to {chat_id}: {e}")


# --- Main Application Logic ---

async def send_paginated_message(context: ContextTypes.DEFAULT_TYPE, header: str, item_lines: list, footer: str, chat_id: int):
    """
    Sends a potentially long message by splitting the item_lines into chunks,
    ensuring each message respects Telegram's character limit.
    """
    # A safe number of lines per message to avoid hitting the character limit.
    CHUNK_SIZE = 30

    if not item_lines:
        message = header + "\n" + footer
        await send_telegram_message(context, message, chat_id)
        return

    # Send the first chunk with the header
    first_chunk = item_lines[:CHUNK_SIZE]
    message = header + "\n" + "\n".join(first_chunk)

    # If this is the only message, add the footer and send
    if len(item_lines) <= CHUNK_SIZE:
        message += "\n" + footer
        await send_telegram_message(context, message, chat_id)
        return
    else:
        # Send the first part (header + first chunk)
        await send_telegram_message(context, message, chat_id)
        # Give telegram a moment
        await asyncio.sleep(0.5)

    # Send the intermediate chunks
    remaining_lines = item_lines[CHUNK_SIZE:]
    for i in range(0, len(remaining_lines), CHUNK_SIZE):
        chunk = remaining_lines[i:i + CHUNK_SIZE]

        # If this is the last chunk, append the footer
        if (i + CHUNK_SIZE) >= len(remaining_lines):
            message = "\n".join(chunk) + "\n" + footer
        else:
            message = "\n".join(chunk)

        await send_telegram_message(context, message, chat_id)
        await asyncio.sleep(0.5)

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
        logging.debug(
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
    character: Character = context.job.data

    if not character.notifications_enabled:
        logging.debug(f"Skipping wallet transaction check for {character.name} as notifications are disabled.")
        context.job_queue.run_once(check_wallet_transactions_job, 60, data=character, name=f"wallet_transactions_{character.id}")
        return

    logging.debug(f"Running wallet transaction check for {character.name}")

    all_transactions, headers = get_wallet_transactions(character, return_headers=True, force_revalidate=True)
    if all_transactions is None:
        logging.error(f"Failed to fetch wallet transactions for {character.name}. Retrying in 60s.")
        context.job_queue.run_once(check_wallet_transactions_job, 60, data=character, name=f"wallet_transactions_{character.id}")
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
        id_to_name = get_names_from_ids(list(set(all_type_ids + all_loc_ids + [character.region_id])))

        wallet_balance = get_wallet_balance(character)

        # Check for low balance threshold
        if wallet_balance is not None and character.wallet_balance_threshold > 0:
            state_key = f"low_balance_alert_sent_at_{character.id}"
            last_alert_str = get_bot_state(state_key)
            alert_sent_recently = False
            if last_alert_str:
                last_alert_time = datetime.fromisoformat(last_alert_str)
                if (datetime.now(timezone.utc) - last_alert_time) < timedelta(days=1):
                    alert_sent_recently = True

            if wallet_balance < character.wallet_balance_threshold and not alert_sent_recently:
                alert_message = (
                    f"⚠️ *Low Wallet Balance Warning ({character.name})* ⚠️\n\n"
                    f"Your wallet balance has dropped below `{character.wallet_balance_threshold:,.2f}` ISK.\n"
                    f"**Current Balance:** `{wallet_balance:,.2f}` ISK"
                )
                await send_telegram_message(context, alert_message, chat_id=character.telegram_user_id)
                set_bot_state(state_key, datetime.now(timezone.utc).isoformat())
            elif wallet_balance >= character.wallet_balance_threshold and last_alert_str:
                set_bot_state(state_key, '')

        if sales and character.enable_sales_notifications:
            if len(sales) > character.notification_batch_threshold:
                header = f"✅ *Multiple Market Sales ({character.name})* ✅"
                item_lines = []
                grand_total_value, grand_total_cogs = 0, 0
                for type_id, tx_group in sales.items():
                    total_quantity = sum(t['quantity'] for t in tx_group)
                    total_value = sum(t['quantity'] * t['price'] for t in tx_group)
                    grand_total_value += total_value
                    cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity)
                    if cogs is not None: grand_total_cogs += cogs
                    item_lines.append(f"  • Sold: `{total_quantity}` x `{id_to_name.get(type_id, 'Unknown')}`")

                profit_line = f"\n**Total Gross Profit:** `{grand_total_value - grand_total_cogs:,.2f} ISK`" if grand_total_cogs > 0 else ""
                footer = f"\n**Total Sale Value:** `{grand_total_value:,.2f} ISK`{profit_line}"
                if wallet_balance is not None: footer += f"\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                await send_paginated_message(context, header, item_lines, footer, chat_id=character.telegram_user_id)
            else:
                for type_id, tx_group in sales.items():
                    total_quantity = sum(t['quantity'] for t in tx_group)
                    total_value = sum(t['quantity'] * t['price'] for t in tx_group)
                    avg_price = total_value / total_quantity
                    cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity)
                    profit_line = f"\n**Gross Profit:** `{total_value - cogs:,.2f} ISK`" if cogs is not None else "\n**Profit:** `N/A`"

                    # Fetch live market data for better price comparison
                    region_orders = get_region_market_orders(character.region_id, type_id, force_revalidate=True)
                    best_buy_order_price = 0
                    if region_orders:
                        buy_orders = [o['price'] for o in region_orders if o.get('is_buy_order')]
                        if buy_orders:
                            best_buy_order_price = max(buy_orders)

                    region_name = id_to_name.get(character.region_id, 'Region')
                    price_comparison_line = ""
                    if best_buy_order_price > 0:
                        price_diff_str = f"({(avg_price / best_buy_order_price - 1):+.2%})"
                        price_comparison_line = f"**{region_name} Best Buy:** `{best_buy_order_price:,.2f} ISK` {price_diff_str}"
                    else:
                        # Fallback to historical average if no live buy orders are found
                        history = get_market_history(type_id, character.region_id, force_revalidate=True)
                        if history and history['average'] > 0:
                            price_diff_str = f"({(avg_price / history['average'] - 1):+.2%})"
                            price_comparison_line = f"**{region_name} Avg:** `{history['average']:,.2f} ISK` {price_diff_str}"
                        else:
                            price_comparison_line = f"**{region_name} Avg:** `N/A`"

                    message = (f"✅ *Market Sale ({character.name})* ✅\n\n"
                               f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                               f"**Quantity:** `{total_quantity}` @ `{avg_price:,.2f} ISK`\n"
                               f"{price_comparison_line}\n"
                               f"{profit_line}\n"
                               f"**Location:** `{id_to_name.get(tx_group[0]['location_id'], 'Unknown')}`\n"
                               f"**Wallet:** `{wallet_balance:,.2f} ISK`")
                    await send_telegram_message(context, message, chat_id=character.telegram_user_id)
                    await asyncio.sleep(1)

        if buys and character.enable_buy_notifications:
            for type_id, tx_group in buys.items():
                for tx in tx_group:
                    add_purchase_lot(character.id, type_id, tx['quantity'], tx['price'])
            if len(buys) > character.notification_batch_threshold:
                header = f"🛒 *Multiple Market Buys ({character.name})* 🛒"
                item_lines = []
                grand_total_cost = 0
                for type_id, tx_group in buys.items():
                    total_quantity = sum(t['quantity'] for t in tx_group)
                    grand_total_cost += sum(t['quantity'] * t['price'] for t in tx_group)
                    item_lines.append(f"  • Bought: `{total_quantity}` x `{id_to_name.get(type_id, 'Unknown')}`")

                footer = f"\n**Total Cost:** `{grand_total_cost:,.2f} ISK`"
                if wallet_balance is not None: footer += f"\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                await send_paginated_message(context, header, item_lines, footer, chat_id=character.telegram_user_id)
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
                    await send_telegram_message(context, message, chat_id=character.telegram_user_id)
                    await asyncio.sleep(1)

        add_processed_transactions(character.id, [tx['transaction_id'] for tx in new_transactions])

    # A fixed 60-second delay is used for polling. ESI's ETag mechanism is leveraged
    # via `force_revalidate=True` in the API calls. This ensures that if data hasn't
    # changed, the bot receives a lightweight '304 Not Modified' response, respecting
    # the API server while still allowing for near real-time notifications.
    delay = 60
    context.job_queue.run_once(check_wallet_transactions_job, delay, data=character, name=f"wallet_transactions_{character.id}")
    logging.info(f"Wallet transaction check for {character.name} complete. Next check in {delay} seconds.")


async def check_order_history_job(context: ContextTypes.DEFAULT_TYPE):
    """
    A self-scheduling job that checks for cancelled or expired orders.
    """
    character: Character = context.job.data

    if not character.notifications_enabled:
        logging.debug(f"Skipping order history check for {character.name} as notifications are disabled.")
        context.job_queue.run_once(check_order_history_job, 60, data=character, name=f"order_history_{character.id}")
        return

    logging.debug(f"Running order history check for {character.name}")

    order_history, headers = get_market_orders_history(character, return_headers=True, force_revalidate=True)
    if order_history is None:
        logging.error(f"Failed to fetch order history for {character.name}. Retrying in 3600s.")
        context.job_queue.run_once(check_order_history_job, 3600, data=character, name=f"order_history_{character.id}")
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
                await send_telegram_message(context, message, chat_id=character.telegram_user_id)
                await asyncio.sleep(1)

        if expired_orders:
            item_ids = [o['type_id'] for o in expired_orders]
            id_to_name = get_names_from_ids(item_ids)
            for order in expired_orders:
                message = (f"ℹ️ *Order Expired ({character.name})* ℹ️\n"
                           f"Your order for `{order['volume_total']}` x `{id_to_name.get(order['type_id'], 'Unknown')}` has expired.")
                await send_telegram_message(context, message, chat_id=character.telegram_user_id)
                await asyncio.sleep(1)

        add_processed_orders(character.id, [o['order_id'] for o in new_orders])

    # A fixed 60-second delay is used for polling. ESI's ETag mechanism is leveraged
    # via `force_revalidate=True` in the API calls. This ensures that if data hasn't
    # changed, the bot receives a lightweight '304 Not Modified' response, respecting
    # the API server while still allowing for near real-time notifications.
    delay = 60
    context.job_queue.run_once(check_order_history_job, delay, data=character, name=f"order_history_{character.id}")
    logging.info(f"Order history check for {character.name} complete. Next check in {delay} seconds.")


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


async def run_daily_summary_for_character(character: Character, context: ContextTypes.DEFAULT_TYPE):
    """Calculates and prepares the daily summary data for a single character."""
    logging.info(f"Calculating daily summary for {character.name}...")

    now = datetime.now(timezone.utc)
    one_day_ago = now - timedelta(days=1)

    # --- Fetch data ---
    all_transactions = get_wallet_transactions(character)
    all_journal_entries, _ = get_wallet_journal(character, fetch_all=True)

    # --- 24-Hour Summary (Stateless) ---
    total_sales_24h, total_fees_24h, profit_24h = 0, 0, 0
    if all_transactions:
        sales_past_24_hours = [tx for tx in all_transactions if not tx.get('is_buy') and datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > one_day_ago]
        total_sales_24h = sum(s['quantity'] * s['unit_price'] for s in sales_past_24_hours)

    if all_journal_entries:
        journal_past_24_hours = [e for e in all_journal_entries if datetime.fromisoformat(e['date'].replace('Z', '+00:00')) > one_day_ago]
        total_brokers_fees_24h = sum(abs(e.get('amount', 0)) for e in journal_past_24_hours if e.get('ref_type') == 'brokers_fee')
        total_transaction_tax_24h = sum(abs(e.get('amount', 0)) for e in journal_past_24_hours if e.get('ref_type') == 'transaction_tax')
        total_fees_24h = total_brokers_fees_24h + total_transaction_tax_24h

    if all_transactions:
        profit_24h = calculate_fifo_profit_for_summary(sales_past_24_hours, character.id) - total_fees_24h

    # --- Monthly Summary (Stateless) ---
    total_sales_month, total_fees_month = 0, 0
    if all_transactions:
        sales_this_month = [tx for tx in all_transactions if not tx.get('is_buy') and datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).month == now.month and datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).year == now.year]
        total_sales_month = sum(s['quantity'] * s['unit_price'] for s in sales_this_month)

    if all_journal_entries:
        journal_this_month = [e for e in all_journal_entries if datetime.fromisoformat(e['date'].replace('Z', '+00:00')).month == now.month and datetime.fromisoformat(e['date'].replace('Z', '+00:00')).year == now.year]
        total_fees_month = sum(abs(e.get('amount', 0)) for e in journal_this_month if e.get('ref_type') in ['brokers_fee', 'transaction_tax'])

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
    await send_telegram_message(context, message, chat_id=character.telegram_user_id)
    logging.info(f"Daily summary sent for {character.name}.")


async def daily_summary_schedule_checker_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Runs every minute to check if any character's daily summary is due.
    """
    now = datetime.now(timezone.utc)
    current_time_str = now.strftime("%H:%M")
    current_date_str = now.strftime("%Y-%m-%d")

    logging.debug(f"Running daily summary check at {current_time_str} UTC.")

    # Create a copy of the list to avoid issues if it's modified during iteration
    characters_to_check = list(CHARACTERS)

    for character in characters_to_check:
        if not character.enable_daily_summary:
            continue

        if character.daily_summary_time == current_time_str:
            state_key = f"summary_sent_{character.id}"
            last_sent_date = get_bot_state(state_key)
            if last_sent_date == current_date_str:
                logging.debug(f"Summary for {character.name} already sent today. Skipping.")
                continue

            logging.info(f"Summary time matched for {character.name}. Triggering summary.")
            try:
                await run_daily_summary_for_character(character, context)
                # On success, mark it as sent for today
                set_bot_state(state_key, current_date_str)
            except Exception as e:
                logging.error(f"Error running daily summary for {character.name}: {e}")

def initialize_journal_history_for_character(character: Character):
    """On first add, seeds the journal history to prevent old entries from appearing in the 24h summary."""
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_journal_entries WHERE character_id = ? LIMIT 1", (character.id,))
    is_seeded = cursor.fetchone()
    conn.close()

    if is_seeded:
        logging.info(f"Character {character.name} already has seeded journal history. Skipping.")
        return

    logging.info(f"First run for {character.name} detected. Seeding journal history...")
    historical_journal, _ = get_wallet_journal(character, fetch_all=True)
    if not historical_journal:
        logging.warning(f"No historical journal found for {character.name}. Seeding complete with no data.")
        add_processed_journal_entries(character.id, [-1])  # Dummy entry to mark as processed
        return

    historical_ids = [entry['id'] for entry in historical_journal]
    add_processed_journal_entries(character.id, historical_ids)
    logging.info(f"Seeded {len(historical_ids)} historical journal entries for {character.name}.")


def initialize_all_transactions_for_character(character: Character):
    """On first add, seeds the database with all historical wallet transactions."""
    state_key = f"transactions_seeded_{character.id}"
    if get_bot_state(state_key) == 'true':
        logging.info(f"Transaction history already seeded for {character.name}. Skipping.")
        return

    logging.info(f"Seeding transaction history for {character.name}...")
    all_transactions = get_wallet_transactions(character)
    if not all_transactions:
        logging.info(f"No historical transactions found to seed for {character.name}.")
        set_bot_state(state_key, 'true')
        return

    buy_transactions = [tx for tx in all_transactions if tx.get('is_buy')]
    if buy_transactions:
        for tx in buy_transactions:
            add_purchase_lot(character.id, tx['type_id'], tx['quantity'], tx['unit_price'], purchase_date=tx['date'])
        logging.info(f"Seeded {len(buy_transactions)} historical buy transactions for FIFO tracking for {character.name}.")

    transaction_ids = [tx['transaction_id'] for tx in all_transactions]
    add_processed_transactions(character.id, transaction_ids)
    logging.info(f"Marked {len(transaction_ids)} historical transactions as processed for {character.name}.")
    set_bot_state(state_key, 'true')


def initialize_order_history_for_character(character: Character):
    """On first add, seeds the database with all historical orders."""
    state_key = f"order_history_seeded_{character.id}"
    if get_bot_state(state_key) == 'true':
        logging.info(f"Order history already seeded for {character.name}. Skipping.")
        return

    logging.info(f"Seeding order history for {character.name}...")
    all_historical_orders = get_market_orders_history(character)
    if all_historical_orders:
        order_ids = [o['order_id'] for o in all_historical_orders]
        add_processed_orders(character.id, order_ids)
        logging.info(f"Seeded {len(order_ids)} historical orders for {character.name}.")
    else:
        logging.info(f"No historical orders found to seed for {character.name}.")
    set_bot_state(state_key, 'true')


def start_monitoring_for_character(character: Character, application: Application):
    """Initializes and starts all monitoring jobs for a single character."""
    logging.info(f"Starting monitoring for character: {character.name} ({character.id})")

    # Initialize data for the new character
    initialize_journal_history_for_character(character)
    initialize_all_transactions_for_character(character)
    initialize_order_history_for_character(character)

    # Start the self-scheduling jobs
    job_queue = application.job_queue
    job_queue.run_once(check_wallet_transactions_job, 5, data=character, name=f"wallet_transactions_{character.id}")
    job_queue.run_once(check_order_history_job, 15, data=character, name=f"order_history_{character.id}")
    logging.info(f"Scheduled initial notification jobs for {character.name}.")


async def check_for_new_characters_job(context: ContextTypes.DEFAULT_TYPE):
    """Periodically checks the database for new characters and starts monitoring them."""
    logging.debug("Running job to check for new characters.")
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT character_id FROM characters")
    db_char_ids = {row[0] for row in cursor.fetchall()}
    conn.close()

    monitored_char_ids = {c.id for c in CHARACTERS}
    new_char_ids = db_char_ids - monitored_char_ids

    if new_char_ids:
        logging.info(f"Detected {len(new_char_ids)} new characters in the database.")
        for char_id in new_char_ids:
            character = get_character_by_id(char_id)
            if character:
                CHARACTERS.append(character)
                start_monitoring_for_character(character, context.application)
                await send_telegram_message(
                    context,
                    f"✅ Successfully added character **{character.name}**! I will now start monitoring their market activity.",
                    chat_id=character.telegram_user_id
                )
            else:
                logging.error(f"Could not find details for newly detected character ID {char_id} in the database.")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays the main menu keyboard.
    """
    user_name = update.effective_user.first_name
    reply_markup = ReplyKeyboardMarkup(MAIN_MENU_KEYBOARD, resize_keyboard=True)
    await update.message.reply_text(
        f"Welcome, {user_name}! Please choose an option from the menu below.",
        reply_markup=reply_markup
    )


async def notifications_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to view and toggle notification settings for their characters.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /notifications command from user {user_id}")

    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text("You have no characters added. Use /addcharacter to add one.")
        return

    keyboard = []
    for char in user_characters:
        status_text = "✅ On" if char.notifications_enabled else "❌ Off"
        # The callback data will be 'notify:character_id:new_status_as_int'
        # So if it's currently on (True), the button will offer to turn it off (0)
        new_status_int = 0 if char.notifications_enabled else 1
        button = InlineKeyboardButton(
            f"{char.name}: {status_text}",
            callback_data=f"notify:{char.id}:{new_status_int}"
        )
        keyboard.append([button])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Manage notifications for your characters:", reply_markup=reply_markup)


async def add_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Provides a link for the user to add a new EVE Online character.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /addcharacter command from user {user_id}")

    # The base URL of the webapp, where the /login route is.
    # This should match the address exposed in docker-compose.yml.
    # In a real deployment, this would be the public-facing URL.
    webapp_base_url = getattr(config, 'WEBAPP_URL', 'http://localhost:5000')
    login_url = f"{webapp_base_url}/login?user={user_id}"

    keyboard = [[InlineKeyboardButton("Authorize with EVE Online", url=login_url)]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = (
        "To add a new character, please click the button below and authorize with EVE Online.\n\n"
        "You will be redirected to the official EVE Online login page. After logging in and "
        "authorizing, you can close the browser window."
    )
    await update.message.reply_text(message, reply_markup=reply_markup)


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fetches and displays the wallet balance for the user's character(s)."""
    user_id = update.effective_user.id
    logging.info(f"Received balance command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text("You have no characters added. Use /addcharacter to add one.")
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        await update.message.reply_text(f"Fetching balance for {character.name}...")
        balance = get_wallet_balance(character)
        if balance is not None:
            message = f"💰 *Wallet Balance for {character.name}*\n\n`{balance:,.2f} ISK`"
            await update.message.reply_text(text=message, parse_mode='Markdown')
        else:
            await update.message.reply_text(f"Error fetching balance for {character.name}.")
    else:
        await _show_character_selection(update, "balance", user_characters, include_all=True)


async def summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually triggers the daily summary report for the user's character(s)."""
    user_id = update.effective_user.id
    logging.info(f"Received summary command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text("You have no characters added. Use /addcharacter to add one.")
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        await update.message.reply_text(f"Generating summary for {character.name}...")
        await run_daily_summary_for_character(character, context)
    else:
        await _show_character_selection(update, "summary", user_characters, include_all=True)


async def _show_character_selection(update: Update, action: str, characters: list, include_all: bool = False) -> None:
    """Displays an inline keyboard for character selection for a given action."""
    keyboard = [
        [InlineKeyboardButton(character.name, callback_data=f"{action}:{character.id}")]
        for character in characters
    ]
    if include_all and len(characters) > 1:
        keyboard.append([InlineKeyboardButton("All Characters", callback_data=f"{action}:all")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(f"Please select a character:", reply_markup=reply_markup)


async def sales_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the 5 most recent sales for the user's character(s)."""
    user_id = update.effective_user.id
    logging.info(f"Received sales command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text("You have no characters added. Use /addcharacter to add one.")
        return

    if len(user_characters) == 1:
        character = user_characters[0]
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
        loc_ids = [tx['location_id'] for tx in filtered_tx]
        id_to_name = get_names_from_ids(list(set(item_ids + loc_ids)))
        message_lines = [f"✅ *Last 5 Sales for {character.name}* ✅\n"]
        for tx in filtered_tx:
            item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
            loc_name = id_to_name.get(tx['location_id'], 'Unknown Location')
            date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each at `{loc_name}`.")
        await update.message.reply_text("\n".join(message_lines), parse_mode='Markdown')
    else:
        await _show_character_selection(update, "sales", user_characters)


async def buys_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the 5 most recent buys for the user's character(s)."""
    user_id = update.effective_user.id
    logging.info(f"Received buys command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text("You have no characters added. Use /addcharacter to add one.")
        return

    if len(user_characters) == 1:
        character = user_characters[0]
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
        loc_ids = [tx['location_id'] for tx in filtered_tx]
        id_to_name = get_names_from_ids(list(set(item_ids + loc_ids)))
        message_lines = [f"🛒 *Last 5 Buys for {character.name}* 🛒\n"]
        for tx in filtered_tx:
            item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
            loc_name = id_to_name.get(tx['location_id'], 'Unknown Location')
            date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each at `{loc_name}`.")
        await update.message.reply_text("\n".join(message_lines), parse_mode='Markdown')
    else:
        await _show_character_selection(update, "buys", user_characters)

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Allows a user to select a character to manage their settings."""
    user_id = update.effective_user.id
    logging.info(f"Received settings command from user {user_id}")

    user_characters = get_characters_for_user(user_id)
    if not user_characters:
        await update.message.reply_text("You have no characters added. Use /addcharacter to add one.")
        return

    keyboard = [[InlineKeyboardButton(char.name, callback_data=f"settings:{char.id}")] for char in user_characters]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Please select a character to manage their settings:", reply_markup=reply_markup)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles regular text messages, either as replies to settings prompts or as
    main menu button presses.
    """
    user_id = update.effective_user.id
    text = update.message.text

    # Check if we're waiting for a settings update
    next_action = context.user_data.get('next_action')
    if next_action:
        action, character_id = next_action

        # --- Handle Region ID Update ---
        if action == 'set_region':
            try:
                new_region_id = int(text)
                update_character_setting(character_id, 'region_id', new_region_id)
                await update.message.reply_text(f"✅ Region ID updated to {new_region_id}.")
            except ValueError:
                await update.message.reply_text("❌ Invalid input. Please enter a numeric Region ID.")

        # --- Handle Summary Time Update ---
        elif action == 'set_time':
            try:
                datetime.strptime(text, '%H:%M')
                update_character_setting(character_id, 'daily_summary_time', text)
                await update.message.reply_text(f"✅ Daily summary time updated to {text} UTC.")
            except ValueError:
                await update.message.reply_text("❌ Invalid format. Please use HH:MM format (e.g., 22:00).")

        # --- Handle Wallet Threshold Update ---
        elif action == 'set_wallet':
            try:
                new_threshold = int(text.replace(',', '').replace('.', ''))
                update_character_setting(character_id, 'wallet_balance_threshold', new_threshold)
                await update.message.reply_text(f"✅ Wallet balance alert threshold updated to {new_threshold:,.0f} ISK.")
            except ValueError:
                await update.message.reply_text("❌ Invalid input. Please enter a valid number.")

        # Clear the next_action and reload characters to get fresh data
        del context.user_data['next_action']
        load_characters_from_db()
        return

    # If not a settings reply, handle as a main menu button press
    if text == ADD_CHARACTER_TEXT:
        await add_character_command(update, context)
    elif text == NOTIFICATIONS_TEXT:
        await notifications_command(update, context)
    elif text == SETTINGS_TEXT:
        await settings_command(update, context)
    elif text == BALANCE_TEXT:
        await balance_command(update, context)
    elif text == SUMMARY_TEXT:
        await summary_command(update, context)
    elif text == SALES_TEXT:
        await sales_command(update, context)
    elif text == BUYS_TEXT:
        await buys_command(update, context)
    else:
        await update.message.reply_text("Please use one of the menu buttons.")


async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all inline button clicks."""
    query = update.callback_query
    await query.answer()

    try:
        parts = query.data.split(':')
        action = parts[0]
    except (ValueError, IndexError):
        await query.edit_message_text(text="Invalid callback data.")
        return

    user_id = update.effective_user.id

    # --- Notification Toggle Logic ---
    if action == "notify":
        try:
            character_id = int(parts[1])
            new_status = bool(int(parts[2]))
        except (ValueError, IndexError):
            await query.edit_message_text(text="Invalid notification callback.")
            return

        # Security check: ensure the user owns this character
        user_characters = get_characters_for_user(user_id)
        if not any(c.id == character_id for c in user_characters):
            await query.edit_message_text(text="Error: You do not own this character.")
            return

        set_character_notification_status(character_id, new_status)

        # Update the character in the global list to reflect the change immediately
        for char in CHARACTERS:
            if char.id == character_id:
                char.notifications_enabled = new_status
                break

        # Re-generate the keyboard with the updated status
        updated_user_characters = get_characters_for_user(user_id)
        keyboard = []
        for char in updated_user_characters:
            status_text = "✅ On" if char.notifications_enabled else "❌ Off"
            new_status_int = 0 if char.notifications_enabled else 1
            button = InlineKeyboardButton(
                f"{char.name}: {status_text}",
                callback_data=f"notify:{char.id}:{new_status_int}"
            )
            keyboard.append([button])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Manage notifications for your characters:", reply_markup=reply_markup)
        return

    # --- Back Button to Settings Character List ---
    if action == "settings_back":
        # This is a simplified version of the settings_command logic
        user_characters = get_characters_for_user(user_id)
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"settings:{char.id}")] for char in user_characters]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Please select a character to manage their settings:", reply_markup=reply_markup)
        return

    # --- Main Settings Menu ---
    if action == "settings":
        character_id = int(parts[1])
        character = next((c for c in CHARACTERS if c.id == character_id and c.telegram_user_id == user_id), None)
        if not character:
            await query.edit_message_text(text="Error: Could not find this character.")
            return

        keyboard = [
            [InlineKeyboardButton(f"Region: {character.region_id}", callback_data=f"set_region:{character.id}")],
            [InlineKeyboardButton(f"Summary Time: {character.daily_summary_time} UTC", callback_data=f"set_time:{character.id}")],
            [InlineKeyboardButton(f"Wallet Alert: {character.wallet_balance_threshold:,.0f} ISK", callback_data=f"set_wallet:{character.id}")],
            [InlineKeyboardButton("⬅️ Back to Character List", callback_data="settings_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"Settings for {character.name}:", reply_markup=reply_markup)
        return

    # --- Prompts for Changing a Setting ---
    if action in ["set_region", "set_time", "set_wallet"]:
        character_id = int(parts[1])
        context.user_data['next_action'] = (action, character_id)

        prompt_text = {
            "set_region": "Please enter the new Region ID for market price comparisons (e.g., 10000002 for Jita).",
            "set_time": "Please enter the new time for your daily summary in HH:MM format (24-hour UTC).",
            "set_wallet": "Please enter the new wallet balance threshold for low-balance warnings (e.g., 100000000 for 100m ISK)."
        }
        await query.edit_message_text(prompt_text[action])
        return

    character_id_str = parts[1]
    if character_id_str == "all":
        if action == "balance":
            await query.edit_message_text(text="Fetching balances for all your characters...")
            user_characters = get_characters_for_user(user_id)
            message_lines = ["💰 *Wallet Balances* 💰\n"]
            total_balance = 0
            for char in user_characters:
                balance = get_wallet_balance(char)
                if balance is not None:
                    message_lines.append(f"• `{char.name}`: `{balance:,.2f} ISK`")
                    total_balance += balance
                else:
                    message_lines.append(f"• `{char.name}`: `Error fetching balance`")
            message_lines.append(f"\n**Combined Total:** `{total_balance:,.2f} ISK`")
            await query.edit_message_text(text="\n".join(message_lines), parse_mode='Markdown')
        elif action == "summary":
            await query.edit_message_text(text="Generating summary for all your characters...")
            user_characters = get_characters_for_user(user_id)
            for char in user_characters:
                await run_daily_summary_for_character(char, context)
                await asyncio.sleep(1)
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
        loc_ids = [tx['location_id'] for tx in filtered_tx]
        id_to_name = get_names_from_ids(list(set(item_ids + loc_ids)))
        icon = "🛒" if is_buy else "✅"
        message_lines = [f"{icon} *Last 5 {action.capitalize()} for {character.name}* {icon}\n"]
        for tx in filtered_tx:
            item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
            loc_name = id_to_name.get(tx['location_id'], 'Unknown Location')
            date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each at `{loc_name}`.")
        await query.edit_message_text(text="\n".join(message_lines), parse_mode='Markdown')

async def post_init(application: Application):
    """Sets the bot's commands in the Telegram menu after initialization."""
    commands = [
        BotCommand("start", "Show the main menu"),
    ]
    await application.bot.set_my_commands(commands)
    logging.info("Bot commands have been set in the Telegram menu.")

def main() -> None:
    """Run the bot."""
    logging.info("Bot starting up...")

    setup_database()
    load_characters_from_db()
    # The bot can now start without characters and wait for them to be added.

    application = Application.builder().token(config.TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    # --- Add command handlers ---
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    # --- Schedule Jobs ---
    job_queue = application.job_queue
    # Schedule the job to check for new characters
    job_queue.run_repeating(check_for_new_characters_job, interval=60, first=10)

    for character in CHARACTERS:
        start_monitoring_for_character(character, application)

    # Schedule the new checker job to run every minute
    job_queue.run_repeating(daily_summary_schedule_checker_job, interval=60, first=5)
    logging.info("Daily summary checker job scheduled. Summaries will be sent based on per-character times.")


    if not job_queue.jobs():
         logging.info("No features enabled, but core jobs are running. Bot will monitor for new characters and summaries.")

    logging.info("Bot is running. Polling for updates...")
    application.run_polling()

if __name__ == "__main__":
    main()