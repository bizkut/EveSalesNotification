import requests
import time
import logging
import os
import database
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta, time as dt_time
from dataclasses import dataclass
import asyncio
import telegram
from telegram.error import BadRequest
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters

# Configure logging
log_level_str = os.getenv('LOG_LEVEL', 'WARNING').upper()
log_level = getattr(logging, log_level_str, logging.WARNING)
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

async def _edit_message(query: "CallbackQuery", *args, **kwargs):
    """A wrapper for query.edit_message_text that handles the 'message is not modified' error."""
    try:
        await query.edit_message_text(*args, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            # This error is expected when the user clicks a button that doesn't change the message
            # (e.g., hitting a "Back" button that was already on the screen). We can safely ignore it.
            pass
        else:
            # Log other BadRequest errors for debugging, but don't crash the bot.
            logging.error(f"An unexpected BadRequest error occurred: {e}")

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
    wallet_balance_threshold: int
    enable_sales_notifications: bool
    enable_buy_notifications: bool
    enable_daily_summary: bool
    notification_batch_threshold: int

CHARACTERS: list[Character] = []


def load_characters_from_db():
    """Loads all characters and their settings from the database."""
    global CHARACTERS
    conn = database.get_db_connection()
    rows = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    character_id, character_name, refresh_token, telegram_user_id,
                    notifications_enabled, region_id, wallet_balance_threshold,
                    enable_sales_notifications, enable_buy_notifications, enable_daily_summary,
                    notification_batch_threshold
                FROM characters
            """)
            rows = cursor.fetchall()
    finally:
        database.release_db_connection(conn)

    if not rows:
        logging.warning("No characters found in the database.")
        return

    logging.info(f"Loading {len(rows)} characters from the database...")
    CHARACTERS = []  # Clear the list to allow for reloading
    for row in rows:
        (
            char_id, name, refresh_token, telegram_user_id, notifications_enabled,
            region_id, wallet_balance_threshold,
            enable_sales, enable_buys, enable_summary, batch_threshold
        ) = row

        if any(c.id == char_id for c in CHARACTERS):
            logging.warning(f"Character '{name}' ({char_id}) is already loaded. Skipping duplicate.")
            continue

        character = Character(
            id=char_id, name=name, refresh_token=refresh_token,
            telegram_user_id=telegram_user_id,
            notifications_enabled=bool(notifications_enabled),
            region_id=region_id,
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

def setup_database():
    """Creates the necessary PostgreSQL database tables if they don't exist."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Foreign key reference needs the telegram_users table to exist first.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS telegram_users (
                    telegram_id BIGINT PRIMARY KEY
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS characters (
                    character_id INTEGER PRIMARY KEY,
                    character_name TEXT NOT NULL,
                    refresh_token TEXT NOT NULL,
                    telegram_user_id BIGINT NOT NULL REFERENCES telegram_users(telegram_id),
                    notifications_enabled BOOLEAN DEFAULT TRUE,
                    region_id INTEGER DEFAULT 10000002,
                    wallet_balance_threshold BIGINT DEFAULT 0,
                    enable_sales_notifications BOOLEAN DEFAULT TRUE,
                    enable_buy_notifications BOOLEAN DEFAULT TRUE,
                    enable_daily_summary BOOLEAN DEFAULT TRUE,
                    notification_batch_threshold INTEGER DEFAULT 3
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_transactions (
                    transaction_id BIGINT NOT NULL,
                    character_id INTEGER NOT NULL,
                    PRIMARY KEY (transaction_id, character_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_journal_entries (
                    entry_id BIGINT NOT NULL,
                    character_id INTEGER NOT NULL,
                    PRIMARY KEY (entry_id, character_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_orders (
                    order_id BIGINT NOT NULL,
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
                    lot_id SERIAL PRIMARY KEY,
                    character_id INTEGER NOT NULL,
                    type_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    price NUMERIC(17, 2) NOT NULL,
                    purchase_date TIMESTAMP WITH TIME ZONE NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_orders (
                    order_id BIGINT NOT NULL,
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
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS esi_cache (
                    cache_key TEXT PRIMARY KEY,
                    response JSONB NOT NULL,
                    etag TEXT,
                    expires TIMESTAMP WITH TIME ZONE NOT NULL,
                    headers JSONB
                )
            """)
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.info("Database setup/verification complete. All tables are present.")

def get_processed_orders(character_id):
    """Retrieves all processed order IDs for a character from the database."""
    conn = database.get_db_connection()
    processed_ids = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT order_id FROM processed_orders WHERE character_id = %s", (character_id,))
            processed_ids = {row[0] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)
    return processed_ids

def add_processed_orders(character_id, order_ids):
    """Adds a list of order IDs for a character to the database."""
    if not order_ids:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = [(o_id, character_id) for o_id in order_ids]
            cursor.executemany(
                "INSERT INTO processed_orders (order_id, character_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                data_to_insert
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)

def get_bot_state(key):
    """Retrieves a value from the bot_state key-value store."""
    conn = database.get_db_connection()
    result = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT value FROM bot_state WHERE key = %s", (key,))
            row = cursor.fetchone()
            if row:
                result = row[0]
    finally:
        database.release_db_connection(conn)
    return result

def set_bot_state(key, value):
    """Sets a value in the bot_state key-value store."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bot_state (key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, str(value))
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)

def get_tracked_market_orders(character_id):
    """Retrieves all tracked market orders for a specific character."""
    conn = database.get_db_connection()
    orders = {}
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT order_id, volume_remain FROM market_orders WHERE character_id = %s", (character_id,))
            orders = {row[0]: row[1] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)
    return orders

def update_tracked_market_orders(character_id, orders):
    """Inserts or updates a list of market orders for a character in the database."""
    if not orders:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            orders_with_char_id = [(o[0], character_id, o[1]) for o in orders]
            upsert_query = """
                INSERT INTO market_orders (order_id, character_id, volume_remain)
                VALUES (%s, %s, %s)
                ON CONFLICT (order_id, character_id) DO UPDATE
                SET volume_remain = EXCLUDED.volume_remain;
            """
            cursor.executemany(upsert_query, orders_with_char_id)
            conn.commit()
    finally:
        database.release_db_connection(conn)

def remove_tracked_market_orders(character_id, order_ids):
    """Removes a list of market orders for a character from the database."""
    if not order_ids:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            orders_to_delete = [(order_id, character_id) for order_id in order_ids]
            cursor.executemany(
                "DELETE FROM market_orders WHERE order_id = %s AND character_id = %s",
                orders_to_delete
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)

def get_processed_transactions(character_id):
    """Retrieves all processed transaction IDs for a character from the database."""
    conn = database.get_db_connection()
    processed_ids = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT transaction_id FROM processed_transactions WHERE character_id = %s", (character_id,))
            processed_ids = {row[0] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)
    return processed_ids

def add_processed_transactions(character_id, transaction_ids):
    """Adds a list of transaction IDs for a character to the database."""
    if not transaction_ids:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = [(tx_id, character_id) for tx_id in transaction_ids]
            cursor.executemany(
                "INSERT INTO processed_transactions (transaction_id, character_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                data_to_insert
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)

def get_processed_journal_entries(character_id):
    """Retrieves all processed journal entry IDs for a character from the database."""
    conn = database.get_db_connection()
    processed_ids = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT entry_id FROM processed_journal_entries WHERE character_id = %s", (character_id,))
            processed_ids = {row[0] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)
    return processed_ids

def add_processed_journal_entries(character_id, entry_ids):
    """Adds a list of journal entry IDs for a character to the database."""
    if not entry_ids:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = [(entry_id, character_id) for entry_id in entry_ids]
            cursor.executemany(
                "INSERT INTO processed_journal_entries (entry_id, character_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                data_to_insert
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


def add_purchase_lot(character_id, type_id, quantity, price, purchase_date=None):
    """Adds a new purchase lot to the database, with an optional historical date."""
    conn = database.get_db_connection()
    if purchase_date is None:
        purchase_date = datetime.now(timezone.utc).isoformat()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO purchase_lots (character_id, type_id, quantity, price, purchase_date)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (character_id, type_id, quantity, price, purchase_date)
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.debug(f"Recorded purchase for char {character_id}: {quantity} of type {type_id} at {price:,.2f} ISK each on {purchase_date}.")


def get_purchase_lots(character_id, type_id):
    """
    Retrieves all purchase lots for a specific item, oldest first.
    Converts price from Decimal to float.
    """
    conn = database.get_db_connection()
    lots = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT lot_id, quantity, price FROM purchase_lots WHERE character_id = %s AND type_id = %s ORDER BY purchase_date ASC",
                (character_id, type_id)
            )
            # Convert Decimal price from DB to float to prevent type errors during calculation
            lots = [{"lot_id": row[0], "quantity": row[1], "price": float(row[2])} for row in cursor.fetchall()]
    finally:
        database.release_db_connection(conn)
    return lots


def update_purchase_lot_quantity(lot_id, new_quantity):
    """Updates the remaining quantity of a purchase lot."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE purchase_lots SET quantity = %s WHERE lot_id = %s", (new_quantity, lot_id))
            conn.commit()
    finally:
        database.release_db_connection(conn)


def delete_purchase_lot(lot_id):
    """Deletes a purchase lot from the database, typically when it's fully consumed."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM purchase_lots WHERE lot_id = %s", (lot_id,))
            conn.commit()
    finally:
        database.release_db_connection(conn)


def get_names_from_db(id_list):
    """Retrieves a mapping of id -> name from the local database for the given IDs."""
    if not id_list:
        return {}
    conn = database.get_db_connection()
    id_to_name_map = {}
    try:
        with conn.cursor() as cursor:
            # Create a string of placeholders for the query
            placeholders = ','.join(['%s'] * len(id_list))
            cursor.execute(f"SELECT item_id, name FROM esi_names WHERE item_id IN ({placeholders})", id_list)
            id_to_name_map = {row[0]: row[1] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)
    logging.debug(f"Resolved {len(id_to_name_map)} names from local DB cache.")
    return id_to_name_map


def save_names_to_db(id_to_name_map):
    """Saves a mapping of id -> name to the local database."""
    if not id_to_name_map:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = list(id_to_name_map.items())
            cursor.executemany(
                "INSERT INTO esi_names (item_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                data_to_insert
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.debug(f"Saved {len(id_to_name_map)} new names to local DB cache.")


def get_characters_for_user(telegram_user_id):
    """Retrieves all characters and their settings for a given Telegram user ID."""
    conn = database.get_db_connection()
    user_characters = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    character_id, character_name, refresh_token, telegram_user_id,
                    notifications_enabled, region_id, wallet_balance_threshold,
                    enable_sales_notifications, enable_buy_notifications, enable_daily_summary,
                    notification_batch_threshold
                FROM characters WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            rows = cursor.fetchall()
            for row in rows:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    region_id, wallet_balance_threshold,
                    enable_sales, enable_buys, enable_summary, batch_threshold
                ) = row

                user_characters.append(Character(
                    id=char_id, name=name, refresh_token=refresh_token,
                    telegram_user_id=telegram_user_id,
                    notifications_enabled=bool(notifications_enabled),
                    region_id=region_id,
                    wallet_balance_threshold=wallet_balance_threshold,
                    enable_sales_notifications=bool(enable_sales),
                    enable_buy_notifications=bool(enable_buys),
                    enable_daily_summary=bool(enable_summary),
                    notification_batch_threshold=batch_threshold
                ))
    finally:
        database.release_db_connection(conn)
    return user_characters


def get_character_by_id(character_id: int) -> Character | None:
    """Retrieves a single character and their settings by character ID."""
    conn = database.get_db_connection()
    character = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    character_id, character_name, refresh_token, telegram_user_id,
                    notifications_enabled, region_id, wallet_balance_threshold,
                    enable_sales_notifications, enable_buy_notifications, enable_daily_summary,
                    notification_batch_threshold
                FROM characters WHERE character_id = %s
            """, (character_id,))
            row = cursor.fetchone()

            if row:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    region_id, wallet_balance_threshold,
                    enable_sales, enable_buys, enable_summary, batch_threshold
                ) = row

                character = Character(
                    id=char_id, name=name, refresh_token=refresh_token,
                    telegram_user_id=telegram_user_id,
                    notifications_enabled=bool(notifications_enabled),
                    region_id=region_id,
                    wallet_balance_threshold=wallet_balance_threshold,
                    enable_sales_notifications=bool(enable_sales),
                    enable_buy_notifications=bool(enable_buys),
                    enable_daily_summary=bool(enable_summary),
                    notification_batch_threshold=batch_threshold
                )
    finally:
        database.release_db_connection(conn)
    return character


def update_character_setting(character_id: int, setting: str, value: any):
    """Updates a specific setting for a character in the database."""
    allowed_settings = ["region_id", "wallet_balance_threshold"]
    if setting not in allowed_settings:
        logging.error(f"Attempted to update an invalid setting: {setting}")
        return

    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = f"UPDATE characters SET {setting} = %s WHERE character_id = %s"
            cursor.execute(query, (value, character_id))
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.info(f"Updated {setting} for character {character_id} to {value}.")


def update_character_notification_setting(character_id: int, setting: str, value: bool):
    """Updates a specific notification setting for a character in the database."""
    allowed_settings = {
        "sales": "enable_sales_notifications",
        "buys": "enable_buy_notifications",
        "summary": "enable_daily_summary"
    }
    if setting not in allowed_settings:
        logging.error(f"Attempted to update an invalid notification setting: {setting}")
        return

    column_name = allowed_settings[setting]
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = f"UPDATE characters SET {column_name} = %s WHERE character_id = %s"
            cursor.execute(query, (value, character_id))
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.info(f"Updated {column_name} for character {character_id} to {value}.")


# --- ESI API Functions ---

def get_esi_cache_from_db(cache_key):
    """Retrieves a cached ESI response from the database."""
    conn = database.get_db_connection()
    cached_item = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT response, etag, expires, headers FROM esi_cache WHERE cache_key = %s", (cache_key,))
            row = cursor.fetchone()
            if row:
                response_json, etag, expires_dt, headers_json = row
                cached_item = {
                    'data': response_json,  # Already parsed as dict by psycopg2
                    'etag': etag,
                    'expires': expires_dt, # Already a datetime object
                    'headers': headers_json # Already parsed as dict by psycopg2
                }
    finally:
        database.release_db_connection(conn)
    return cached_item


def save_esi_cache_to_db(cache_key, data, etag, expires_dt, headers):
    """Saves an ESI response to the database cache."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            upsert_query = """
                INSERT INTO esi_cache (cache_key, response, etag, expires, headers)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cache_key) DO UPDATE SET
                    response = EXCLUDED.response,
                    etag = EXCLUDED.etag,
                    expires = EXCLUDED.expires,
                    headers = EXCLUDED.headers;
            """
            cursor.execute(
                upsert_query,
                (
                    cache_key,
                    json.dumps(data),
                    etag,
                    expires_dt, # No need for isoformat, psycopg2 handles datetime
                    json.dumps(headers) if headers else None
                )
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


def make_esi_request(url, character=None, params=None, data=None, return_headers=False, force_revalidate=False):
    """
    Makes a request to the ESI API, handling caching via ETag and Expires headers.
    If force_revalidate is True, it will ignore the time-based cache and use an ETag.
    Returns the JSON response and optionally the response headers.
    """
    data_key_part = ""
    if data:
        if isinstance(data, list):
            data_key_part = str(sorted(data))
        else:
            data_key_part = str(data)
    cache_key = f"{url}:{character.id if character else 'public'}:{str(params)}:{data_key_part}"

    cached_response = get_esi_cache_from_db(cache_key)
    headers = {"Accept": "application/json"}

    if character:
        access_token = get_access_token(character.refresh_token)
        if not access_token:
            logging.error(f"Failed to get access token for {character.name}")
            return (None, None) if return_headers else None
        headers["Authorization"] = f"Bearer {access_token}"

    if not force_revalidate and cached_response and cached_response.get('expires', datetime.min.replace(tzinfo=timezone.utc)) > datetime.now(timezone.utc):
        logging.debug(f"Returning DB-cached data for {url}")
        return (cached_response['data'], cached_response['headers']) if return_headers else cached_response['data']

    if cached_response and 'etag' in cached_response:
        headers['If-None-Match'] = cached_response['etag']

    try:
        if data:
            headers["Content-Type"] = "application/json"
            response = requests.post(url, headers=headers, json=data)
        else:
            response = requests.get(url, headers=headers, params=params)

        if response.status_code == 304:
            logging.debug(f"304 Not Modified for {url}. Using DB-cached data.")
            new_expires_dt = datetime.strptime(response.headers['Expires'], '%a, %d %b %Y %H:%M:%S GMT').replace(tzinfo=timezone.utc)
            # Update the expiry time in the cache for the existing data
            save_esi_cache_to_db(cache_key, cached_response['data'], cached_response['etag'], new_expires_dt, dict(response.headers))
            return (cached_response['data'], cached_response['headers']) if return_headers else cached_response['data']

        response.raise_for_status()

        expires_header = response.headers.get('Expires')
        if expires_header:
            expires_dt = datetime.strptime(expires_header, '%a, %d %b %Y %H:%M:%S GMT').replace(tzinfo=timezone.utc)
        else:
            expires_dt = datetime.now(timezone.utc) + timedelta(seconds=60)

        new_data = response.json()
        response_headers = dict(response.headers)
        new_etag = response_headers.get('ETag')

        save_esi_cache_to_db(cache_key, new_data, new_etag, expires_dt, response_headers)
        logging.debug(f"Cached new data for {url} to DB. Expires at {expires_dt}")

        return (new_data, response_headers) if return_headers else new_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error making ESI request to {url}: {e}")
        if cached_response:
            logging.warning(f"Returning stale DB-cached data for {url} due to request failure.")
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
        "client_id": os.getenv("ESI_CLIENT_ID"),
        "client_secret": os.getenv("ESI_SECRET_KEY")
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

async def master_wallet_transaction_poll(application: Application):
    """
    A single, continuous polling loop that checks for new wallet transactions
    for all monitored characters. It ensures data processing is separate from
    notification sending.
    """
    context = ContextTypes.DEFAULT_TYPE(application=application)
    while True:
        logging.info("Starting master wallet transaction polling cycle.")
        characters_to_poll = list(CHARACTERS)  # Poll all characters for data integrity

        if not characters_to_poll:
            logging.debug("No characters to poll for wallet transactions. Sleeping.")
        else:
            for character in characters_to_poll:
                logging.debug(f"Polling wallet for {character.name}")
                try:
                    all_transactions, _ = get_wallet_transactions(character, return_headers=True, force_revalidate=True)
                    if all_transactions is None:
                        logging.error(f"Failed to fetch wallet transactions for {character.name}. Skipping.")
                        continue

                    processed_tx_ids = get_processed_transactions(character.id)
                    new_transactions = [tx for tx in all_transactions if tx['transaction_id'] not in processed_tx_ids]

                    if not new_transactions:
                        continue

                    logging.info(f"Detected {len(new_transactions)} new transactions for {character.name}.")
                    sales = defaultdict(list)
                    buys = defaultdict(list)
                    for tx in new_transactions:
                        if tx['is_buy']:
                            buys[tx['type_id']].append(tx)
                        else:
                            sales[tx['type_id']].append(tx)

                    all_type_ids = list(sales.keys()) + list(buys.keys())
                    all_loc_ids = [t['location_id'] for txs in sales.values() for t in txs] + [t['location_id'] for txs in buys.values() for t in txs]
                    id_to_name = get_names_from_ids(list(set(all_type_ids + all_loc_ids + [character.region_id])))
                    wallet_balance = get_wallet_balance(character)

                    # --- Unconditional Data Processing ---

                    # Process Buys for FIFO accounting
                    if buys:
                        for type_id, tx_group in buys.items():
                            for tx in tx_group:
                                add_purchase_lot(character.id, type_id, tx['quantity'], tx['unit_price'])

                    # Process Sales for FIFO accounting and prepare details for potential notification
                    sales_details_for_notification = []
                    if sales:
                        for type_id, tx_group in sales.items():
                            total_quantity = sum(t['quantity'] for t in tx_group)
                            cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity)
                            sales_details_for_notification.append({
                                'type_id': type_id,
                                'tx_group': tx_group,
                                'cogs': cogs
                            })

                    # --- Conditional Notifications ---
                    if character.notifications_enabled:
                        # Low Balance Alert
                        if wallet_balance is not None and character.wallet_balance_threshold > 0:
                            state_key = f"low_balance_alert_sent_at_{character.id}"
                            last_alert_str = get_bot_state(state_key)
                            alert_sent_recently = False
                            if last_alert_str:
                                last_alert_time = datetime.fromisoformat(last_alert_str)
                                if (datetime.now(timezone.utc) - last_alert_time) < timedelta(days=1):
                                    alert_sent_recently = True
                            if wallet_balance < character.wallet_balance_threshold and not alert_sent_recently:
                                alert_message = (f"⚠️ *Low Wallet Balance Warning ({character.name})* ⚠️\n\n"
                                                 f"Your wallet balance has dropped below `{character.wallet_balance_threshold:,.2f}` ISK.\n"
                                                 f"**Current Balance:** `{wallet_balance:,.2f}` ISK")
                                await send_telegram_message(context, alert_message, chat_id=character.telegram_user_id)
                                set_bot_state(state_key, datetime.now(timezone.utc).isoformat())
                            elif wallet_balance >= character.wallet_balance_threshold and last_alert_str:
                                set_bot_state(state_key, '')

                        # Buy Notifications
                        if buys and character.enable_buy_notifications:
                            if len(buys) > character.notification_batch_threshold:
                                header = f"🛒 *Multiple Market Buys ({character.name})* 🛒"
                                item_lines = []
                                grand_total_cost = sum(tx['quantity'] * tx['unit_price'] for tx_group in buys.values() for tx in tx_group)
                                for type_id, tx_group in buys.items():
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    item_lines.append(f"  • Bought: `{total_quantity}` x `{id_to_name.get(type_id, 'Unknown')}`")
                                footer = f"\n**Total Cost:** `{grand_total_cost:,.2f} ISK`"
                                if wallet_balance is not None: footer += f"\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                                await send_paginated_message(context, header, item_lines, footer, chat_id=character.telegram_user_id)
                            else:
                                for type_id, tx_group in buys.items():
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    total_cost = sum(t['quantity'] * t['unit_price'] for t in tx_group)
                                    message = (f"🛒 *Market Buy ({character.name})* 🛒\n\n"
                                               f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                                               f"**Quantity:** `{total_quantity}`\n"
                                               f"**Total Cost:** `{total_cost:,.2f} ISK`\n"
                                               f"**Location:** `{id_to_name.get(tx_group[0]['location_id'], 'Unknown')}`\n"
                                               f"**Wallet:** `{wallet_balance:,.2f} ISK`")
                                    await send_telegram_message(context, message, chat_id=character.telegram_user_id)
                                    await asyncio.sleep(1)

                        # Sale Notifications
                        if sales and character.enable_sales_notifications:
                            if len(sales_details_for_notification) > character.notification_batch_threshold:
                                header = f"✅ *Multiple Market Sales ({character.name})* ✅"
                                item_lines = []
                                grand_total_value, grand_total_cogs = 0, 0
                                for sale_info in sales_details_for_notification:
                                    total_quantity = sum(t['quantity'] for t in sale_info['tx_group'])
                                    total_value = sum(t['quantity'] * t['unit_price'] for t in sale_info['tx_group'])
                                    grand_total_value += total_value
                                    if sale_info['cogs'] is not None: grand_total_cogs += sale_info['cogs']
                                    item_lines.append(f"  • Sold: `{total_quantity}` x `{id_to_name.get(sale_info['type_id'], 'Unknown')}`")
                                profit_line = f"\n**Total Gross Profit:** `{grand_total_value - grand_total_cogs:,.2f} ISK`" if grand_total_cogs > 0 else ""
                                footer = f"\n**Total Sale Value:** `{grand_total_value:,.2f} ISK`{profit_line}"
                                if wallet_balance is not None: footer += f"\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                                await send_paginated_message(context, header, item_lines, footer, chat_id=character.telegram_user_id)
                            else:
                                for sale_info in sales_details_for_notification:
                                    type_id = sale_info['type_id']
                                    tx_group = sale_info['tx_group']
                                    cogs = sale_info['cogs']
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    total_value = sum(t['quantity'] * t['unit_price'] for t in tx_group)
                                    avg_price = total_value / total_quantity
                                    profit_line = f"\n**Gross Profit:** `{total_value - cogs:,.2f} ISK`" if cogs is not None else "\n**Profit:** `N/A`"
                                    region_orders = get_region_market_orders(character.region_id, type_id, force_revalidate=True)
                                    best_buy_order_price = 0
                                    if region_orders:
                                        buy_orders = [o['price'] for o in region_orders if o.get('is_buy_order')]
                                        if buy_orders: best_buy_order_price = max(buy_orders)
                                    region_name = id_to_name.get(character.region_id, 'Region')
                                    price_comparison_line = ""
                                    if best_buy_order_price > 0:
                                        price_diff_str = f"({(avg_price / best_buy_order_price - 1):+.2%})"
                                        price_comparison_line = f"**{region_name} Best Buy:** `{best_buy_order_price:,.2f} ISK` {price_diff_str}"
                                    else:
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

                    add_processed_transactions(character.id, [tx['transaction_id'] for tx in new_transactions])

                except Exception as e:
                    logging.error(f"Error processing wallet for {character.name}: {e}", exc_info=True)
                finally:
                    await asyncio.sleep(1)

        logging.info("Master wallet transaction polling cycle complete. Sleeping for 60 seconds.")
        await asyncio.sleep(60)


async def master_order_history_poll(application: Application):
    """
    A single, continuous polling loop that checks for cancelled or expired orders
    for all monitored characters.
    """
    context = ContextTypes.DEFAULT_TYPE(application=application)
    while True:
        logging.info("Starting master order history polling cycle.")
        characters_to_poll = [c for c in list(CHARACTERS) if c.notifications_enabled]

        if not characters_to_poll:
            logging.debug("No characters to poll for order history. Sleeping.")
        else:
            for character in characters_to_poll:
                logging.debug(f"Polling order history for {character.name}")
                try:
                    order_history, _ = get_market_orders_history(character, return_headers=True, force_revalidate=True)
                    if order_history is None:
                        logging.error(f"Failed to fetch order history for {character.name}. Skipping.")
                        continue

                    processed_order_ids = get_processed_orders(character.id)
                    new_orders = [o for o in order_history if o['order_id'] not in processed_order_ids]

                    if not new_orders:
                        continue

                    logging.info(f"Detected {len(new_orders)} new historical orders for {character.name}.")
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

                except Exception as e:
                    logging.error(f"Error processing order history for {character.name}: {e}", exc_info=True)
                finally:
                    await asyncio.sleep(1)

        logging.info("Master order history polling cycle complete. Sleeping for 60 seconds.")
        await asyncio.sleep(60)


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


async def master_daily_summary_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Runs once per day and sends a summary to all characters who have it enabled.
    """
    logging.info("Starting daily summary job for all characters.")
    characters_to_summarize = [c for c in CHARACTERS if c.enable_daily_summary]

    if not characters_to_summarize:
        logging.info("No characters have daily summary enabled. Skipping.")
        return

    logging.info(f"Found {len(characters_to_summarize)} characters to summarize.")
    for character in characters_to_summarize:
        try:
            await run_daily_summary_for_character(character, context)
            # Add a small delay to avoid rate-limiting issues
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error running daily summary for {character.name}: {e}")
    logging.info("Finished daily summary job.")

def initialize_journal_history_for_character(character: Character):
    """On first add, seeds the journal history to prevent old entries from appearing in the 24h summary."""
    conn = database.get_db_connection()
    is_seeded = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM processed_journal_entries WHERE character_id = %s LIMIT 1", (character.id,))
            is_seeded = cursor.fetchone()
    finally:
        database.release_db_connection(conn)

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


def seed_data_for_character(character: Character):
    """Initializes all historical data for a character if it hasn't been done before."""
    logging.info(f"Checking/seeding historical data for character: {character.name} ({character.id})")
    initialize_journal_history_for_character(character)
    initialize_all_transactions_for_character(character)
    initialize_order_history_for_character(character)
    logging.info(f"Finished checking/seeding data for {character.name}.")


async def check_for_new_characters_job(context: ContextTypes.DEFAULT_TYPE):
    """Periodically checks the database for new characters and starts monitoring them."""
    logging.debug("Running job to check for new characters.")
    conn = database.get_db_connection()
    db_char_ids = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT character_id FROM characters")
            db_char_ids = {row[0] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)

    monitored_char_ids = {c.id for c in CHARACTERS}
    new_char_ids = db_char_ids - monitored_char_ids

    if new_char_ids:
        logging.info(f"Detected {len(new_char_ids)} new characters in the database.")
        for char_id in new_char_ids:
            character = get_character_by_id(char_id)
            if character:
                # Seed the historical data for the new character
                seed_data_for_character(character)
                # Add the character to the global list to be picked up by the master polls
                CHARACTERS.append(character)
                logging.info(f"Added new character {character.name} to the polling list.")
                await send_telegram_message(
                    context,
                    f"✅ Successfully added character **{character.name}**! I will now start monitoring their market activity.",
                    chat_id=character.telegram_user_id
                )
            else:
                logging.error(f"Could not find details for newly detected character ID {char_id} in the database.")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays the main menu as an inline keyboard. Can be called by a command
    or a callback query.
    """
    query = update.callback_query
    if query:
        await query.answer()

    user = update.effective_user
    user_characters = get_characters_for_user(user.id)

    # Welcome message changes based on context
    if query:
        welcome_message = "Main Menu"
    else:
        welcome_message = f"Welcome, {user.first_name}!"

    # Build the message and keyboard based on whether the user has characters
    if not user_characters:
        keyboard = [[InlineKeyboardButton("➕ Add Character", callback_data="add_character")]]
        message = (
            f"{welcome_message}\n\nIt looks like you don't have any EVE Online characters "
            "added yet. To get started, please click the button below to add your first one."
        )
    else:
        keyboard = [
            [
                InlineKeyboardButton("➕ Add Character", callback_data="add_character"),
                InlineKeyboardButton("🔔 Notifications", callback_data="notifications")
            ],
            [
                InlineKeyboardButton("💰 View Balances", callback_data="balance"),
                InlineKeyboardButton("📊 Request Summary", callback_data="summary")
            ],
            [
                InlineKeyboardButton("📈 View Sales", callback_data="sales"),
                InlineKeyboardButton("🛒 View Buys", callback_data="buys")
            ],
            [
                InlineKeyboardButton("⚙️ Settings", callback_data="settings")
            ]
        ]
        message = f"{welcome_message}\n\nYou have {len(user_characters)} character(s) registered. Please choose an option from the menu."

    reply_markup = InlineKeyboardMarkup(keyboard)

    # If it's a callback, edit the message. Otherwise, send a new one.
    if query:
        await _edit_message(query, text=message, reply_markup=reply_markup)
    else:
        # Also remove the reply keyboard if a user is migrating from the old version
        await update.message.reply_text(text=message, reply_markup=reply_markup)


async def notifications_command(query: "CallbackQuery", context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to manage their notification settings.
    Edits the message with an inline keyboard.
    """
    user_id = query.from_user.id
    logging.info(f"Received notifications command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        keyboard = [
            [InlineKeyboardButton("➕ Add Character", callback_data="add_character")],
            [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(
            query,
            "You have no characters added. Please add one first.",
            reply_markup=reply_markup
        )
        return

    keyboard = [[InlineKeyboardButton(char.name, callback_data=f"notify_menu:{char.id}")] for char in user_characters]
    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await _edit_message(query, "Please select a character to manage their notification settings:", reply_markup=reply_markup)


async def add_character_command(query: "CallbackQuery", context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Provides a link for the user to add a new EVE Online character.
    Edits the message with an inline keyboard.
    """
    user_id = query.from_user.id
    logging.info(f"Received add_character command from user {user_id}")
    webapp_base_url = os.getenv('WEBAPP_URL', 'http://localhost:5000')
    login_url = f"{webapp_base_url}/login?user={user_id}"

    keyboard = [
        [InlineKeyboardButton("Authorize with EVE Online", url=login_url)],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = (
        "To add a new character, please click the button below and authorize with EVE Online.\n\n"
        "You will be redirected to the official EVE Online login page. After logging in and "
        "authorizing, you can close the browser window.\n\n"
        "*Please note:* It may take a minute or two for the character to be fully registered "
        "with the bot after authorization."
    )
    await _edit_message(query, message, reply_markup=reply_markup)


async def balance_command(query: "CallbackQuery", context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fetches and displays the wallet balance for the user's character(s)."""
    user_id = query.from_user.id
    logging.info(f"Received balance command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        keyboard = [
            [InlineKeyboardButton("➕ Add Character", callback_data="add_character")],
            [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(
            query,
            "You have no characters added. Please add one first.",
            reply_markup=reply_markup
        )
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        await _edit_message(query, f"Fetching balance for {character.name}...")
        balance = get_wallet_balance(character)
        message = f"💰 *Wallet Balance for {character.name}*\n\n`{balance:,.2f} ISK`" if balance is not None else f"Error fetching balance for {character.name}."
        keyboard = [[InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(query, text=message, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await _show_character_selection(query, "balance", user_characters, include_all=True)


async def summary_command(query: "CallbackQuery", context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manually triggers the daily summary report for the user's character(s)."""
    user_id = query.from_user.id
    logging.info(f"Received summary command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        keyboard = [
            [InlineKeyboardButton("➕ Add Character", callback_data="add_character")],
            [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(
            query,
            "You have no characters added. Please add one first.",
            reply_markup=reply_markup
        )
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        await _edit_message(query, f"Generating summary for {character.name}...")
        # The summary function sends its own message. We'll just confirm it was sent.
        await run_daily_summary_for_character(character, context)
        await _edit_message(
            query,
            f"✅ Summary sent for {character.name}!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]])
        )
    else:
        await _show_character_selection(query, "summary", user_characters, include_all=True)


async def _show_character_selection(query: "CallbackQuery", action: str, characters: list, include_all: bool = False) -> None:
    """
    Displays an inline keyboard for character selection for a given action.
    Edits the message.
    """
    keyboard = [
        [InlineKeyboardButton(character.name, callback_data=f"{action}:{character.id}")]
        for character in characters
    ]
    if include_all and len(characters) > 1:
        keyboard.append([InlineKeyboardButton("All Characters", callback_data=f"{action}:all")])

    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await _edit_message(query, f"Please select a character:", reply_markup=reply_markup)


async def sales_command(query: "CallbackQuery", context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the 5 most recent sales for the user's character(s)."""
    user_id = query.from_user.id
    logging.info(f"Received sales command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        keyboard = [
            [InlineKeyboardButton("➕ Add Character", callback_data="add_character")],
            [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(
            query,
            "You have no characters added. Please add one first.",
            reply_markup=reply_markup
        )
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        await _edit_message(query, f"Fetching recent sales for {character.name}...")
        all_transactions = get_wallet_transactions(character)
        message_lines = [f"✅ *Last 5 Sales for {character.name}* ✅\n"]
        if not all_transactions:
            message_lines.append(f"No transaction history found for {character.name}.")
        else:
            filtered_tx = sorted([tx for tx in all_transactions if not tx.get('is_buy')], key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)[:5]
            if not filtered_tx:
                message_lines.append(f"No recent sales found for {character.name}.")
            else:
                item_ids = [tx['type_id'] for tx in filtered_tx]
                loc_ids = [tx['location_id'] for tx in filtered_tx]
                id_to_name = get_names_from_ids(list(set(item_ids + loc_ids)))
                for tx in filtered_tx:
                    item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
                    loc_name = id_to_name.get(tx['location_id'], 'Unknown Location')
                    date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                    message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each at `{loc_name}`.")

        keyboard = [[InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(query, "\n".join(message_lines), parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await _show_character_selection(query, "sales", user_characters)


async def buys_command(query: "CallbackQuery", context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the 5 most recent buys for the user's character(s)."""
    user_id = query.from_user.id
    logging.info(f"Received buys command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        keyboard = [
            [InlineKeyboardButton("➕ Add Character", callback_data="add_character")],
            [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(
            query,
            "You have no characters added. Please add one first.",
            reply_markup=reply_markup
        )
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        await _edit_message(query, f"Fetching recent buys for {character.name}...")
        all_transactions = get_wallet_transactions(character)
        message_lines = [f"🛒 *Last 5 Buys for {character.name}* 🛒\n"]
        if not all_transactions:
            message_lines.append(f"No transaction history found for {character.name}.")
        else:
            filtered_tx = sorted([tx for tx in all_transactions if tx.get('is_buy')], key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)[:5]
            if not filtered_tx:
                message_lines.append(f"No recent buys found for {character.name}.")
            else:
                item_ids = [tx['type_id'] for tx in filtered_tx]
                loc_ids = [tx['location_id'] for tx in filtered_tx]
                id_to_name = get_names_from_ids(list(set(item_ids + loc_ids)))
                for tx in filtered_tx:
                    item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
                    loc_name = id_to_name.get(tx['location_id'], 'Unknown Location')
                    date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                    message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each at `{loc_name}`.")

        keyboard = [[InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(query, "\n".join(message_lines), parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await _show_character_selection(query, "buys", user_characters)

async def settings_command(query: "CallbackQuery", context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to manage their settings.
    Edits the message with an inline keyboard.
    """
    user_id = query.from_user.id
    logging.info(f"Received settings command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        keyboard = [
            [InlineKeyboardButton("➕ Add Character", callback_data="add_character")],
            [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(
            query,
            "You have no characters added. Please add one first.",
            reply_markup=reply_markup
        )
        return

    keyboard = [[InlineKeyboardButton(char.name, callback_data=f"settings:{char.id}")] for char in user_characters]
    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await _edit_message(query, "Please select a character to manage their settings:", reply_markup=reply_markup)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles regular text messages, which are now only expected as replies
    to settings prompts.
    """
    user_id = update.effective_user.id
    text = update.message.text

    # Check if we're waiting for a settings update
    next_action = context.user_data.get('next_action')
    if not next_action:
        # If we receive a text message but aren't expecting one, just show the menu.
        await start_command(update, context)
        return

    action, character_id = next_action

    # --- Handle Region ID Update ---
    if action == 'set_region':
        try:
            new_region_id = int(text)
            update_character_setting(character_id, 'region_id', new_region_id)
            await update.message.reply_text(f"✅ Region ID updated to {new_region_id}.")
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a numeric Region ID.")

    # --- Handle Wallet Threshold Update ---
    elif action == 'set_wallet':
        try:
            new_threshold = int(text.replace(',', '').replace('.', ''))
            update_character_setting(character_id, 'wallet_balance_threshold', new_threshold)
            await update.message.reply_text(f"✅ Wallet balance alert threshold updated to {new_threshold:,.0f} ISK.")
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a valid number.")

    # Clear the next_action, reload characters, and show the settings menu again
    del context.user_data['next_action']
    load_characters_from_db()

    # Create a fake query object to pass to the settings menu
    query = type('FakeQuery', (object,), {'data': f'settings:{character_id}', 'message': update.message, 'answer': lambda: None})()
    await button_callback_handler(type('FakeUpdate', (object,), {'callback_query': query, 'effective_user': update.effective_user})(), context)


async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all inline button clicks."""
    query = update.callback_query
    await query.answer()

    try:
        parts = query.data.split(':')
        action = parts[0]
    except (ValueError, IndexError):
        await _edit_message(query, text="Invalid callback data.")
        return

    user_id = update.effective_user.id

    # --- Granular Notification Toggle Logic ---
    if action == "toggle_notify":
        try:
            notify_type = parts[1]  # 'sales', 'buys', or 'summary'
            character_id = int(parts[2])
        except (ValueError, IndexError):
            await _edit_message(query, text="Invalid notification toggle callback.")
            return

        # Security check: ensure the user owns this character
        character = get_character_by_id(character_id)
        if not character or character.telegram_user_id != user_id:
            await _edit_message(query, text="Error: You do not own this character.")
            return

        # Determine which setting to toggle and its new value
        current_value = False
        if notify_type == "sales":
            current_value = character.enable_sales_notifications
        elif notify_type == "buys":
            current_value = character.enable_buy_notifications
        elif notify_type == "summary":
            current_value = character.enable_daily_summary

        new_value = not current_value
        update_character_notification_setting(character_id, notify_type, new_value)

        # Update the character in the global list to reflect the change immediately
        for char in CHARACTERS:
            if char.id == character_id:
                if notify_type == "sales": char.enable_sales_notifications = new_value
                elif notify_type == "buys": char.enable_buy_notifications = new_value
                elif notify_type == "summary": char.enable_daily_summary = new_value
                break

        # Re-trigger the menu to show the updated state
        query.data = f"notify_menu:{character_id}"
        await button_callback_handler(update, context)
        return

    # --- Main Menu Navigation ---
    if action == "main_menu":
        await start_command(update, context)
        return
    elif action == "add_character":
        await add_character_command(query, context)
        return
    elif action == "notifications":
        await notifications_command(query, context)
        return
    elif action == "settings":
        await settings_command(query, context)
        return
    elif action == "balance":
        await balance_command(query, context)
        return
    elif action == "summary":
        await summary_command(query, context)
        return
    elif action == "sales":
        await sales_command(query, context)
        return
    elif action == "buys":
        await buys_command(query, context)
        return

    # --- Back Button to Character Lists ---
    if action == "notify_back":
        await notifications_command(query, context)
        return
    if action == "settings_back":
        await settings_command(query, context)
        return

    # --- Sub-Menu Logic (e.g., selecting a character) ---

    # --- Notification Settings Menu ---
    if action == "notify_menu":
        character_id = int(parts[1])
        # Re-fetch character to ensure we have the latest settings
        character = get_character_by_id(character_id)
        if not character or character.telegram_user_id != user_id:
            await _edit_message(query, text="Error: Could not find this character.")
            return

        keyboard = [
            [InlineKeyboardButton(
                f"Sales: {'✅ On' if character.enable_sales_notifications else '❌ Off'}",
                callback_data=f"toggle_notify:sales:{character.id}"
            )],
            [InlineKeyboardButton(
                f"Buys: {'✅ On' if character.enable_buy_notifications else '❌ Off'}",
                callback_data=f"toggle_notify:buys:{character.id}"
            )],
            [InlineKeyboardButton(
                f"Daily Summary: {'✅ On' if character.enable_daily_summary else '❌ Off'}",
                callback_data=f"toggle_notify:summary:{character.id}"
            )],
            [InlineKeyboardButton("⬅️ Back to Character List", callback_data="notify_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(query, f"Notification settings for {character.name}:", reply_markup=reply_markup)
        return

    # --- Back Button to Settings Character List ---
    if action == "settings_back":
        # This is a simplified version of the settings_command logic
        user_characters = get_characters_for_user(user_id)
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"settings:{char.id}")] for char in user_characters]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(query, "Please select a character to manage their settings:", reply_markup=reply_markup)
        return

    # --- Main Settings Menu ---
    if action == "settings":
        character_id = int(parts[1])
        character = next((c for c in CHARACTERS if c.id == character_id and c.telegram_user_id == user_id), None)
        if not character:
            await _edit_message(query, text="Error: Could not find this character.")
            return

        keyboard = [
            [InlineKeyboardButton(f"Region: {character.region_id}", callback_data=f"set_region:{character.id}")],
            [InlineKeyboardButton(f"Wallet Alert: {character.wallet_balance_threshold:,.0f} ISK", callback_data=f"set_wallet:{character.id}")],
            [InlineKeyboardButton("⬅️ Back to Character List", callback_data="settings_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _edit_message(query, f"Settings for {character.name}:", reply_markup=reply_markup)
        return

    # --- Prompts for Changing a Setting ---
    if action in ["set_region", "set_wallet"]:
        character_id = int(parts[1])
        context.user_data['next_action'] = (action, character_id)

        prompt_text = {
            "set_region": "Please enter the new Region ID for market price comparisons (e.g., 10000002 for Jita).",
            "set_wallet": "Please enter the new wallet balance threshold for low-balance warnings (e.g., 100000000 for 100m ISK)."
        }
        await _edit_message(query, prompt_text[action])
        return

    character_id_str = parts[1]
    if character_id_str == "all":
        if action == "balance":
            await _edit_message(query, text="Fetching balances for all your characters...")
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
            reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]])
            await _edit_message(query, text="\n".join(message_lines), parse_mode='Markdown', reply_markup=reply_markup)
        elif action == "summary":
            await _edit_message(query, text="Generating summary for all your characters...")
            user_characters = get_characters_for_user(user_id)
            for char in user_characters:
                # This function sends its own message
                await run_daily_summary_for_character(char, context)
                await asyncio.sleep(1) # Be nice to Telegram
            await _edit_message(
                query,
                "✅ Summaries sent for all characters!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]])
            )
        return

    try:
        character_id = int(character_id_str)
    except ValueError:
        await _edit_message(query, text="Invalid character ID.")
        return

    character = next((c for c in CHARACTERS if c.id == character_id), None)
    if not character:
        await _edit_message(query, text="Could not find the selected character.")
        return

    await _edit_message(query, text=f"Processing /{action} for {character.name}, please wait...")

    if action == "balance":
        balance = get_wallet_balance(character)
        if balance is not None:
            message = f"💰 *Wallet Balance for {character.name}*\n\n`{balance:,.2f} ISK`"
            await _edit_message(query, text=message, parse_mode='Markdown')
        else:
            await _edit_message(query, text=f"Error fetching balance for {character.name}.")
    elif action == "summary":
        await _edit_message(query, f"Generating summary for {character.name}...")
        # This function sends its own message
        await run_daily_summary_for_character(character, context)
        await _edit_message(
            query,
            f"✅ Summary sent for {character.name}!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]])
        )
    elif action in ["sales", "buys"]:
        all_transactions = get_wallet_transactions(character)
        if not all_transactions:
            await _edit_message(query, text=f"No transaction history found for {character.name}.")
            return

        is_buy = True if action == 'buys' else False
        filtered_tx = sorted([tx for tx in all_transactions if tx.get('is_buy') == is_buy], key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)[:5]
        if not filtered_tx:
            await _edit_message(query, text=f"No recent {action} found for {character.name}.")
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
        await _edit_message(query, text="\n".join(message_lines), parse_mode='Markdown')

async def post_init(application: Application):
    """
    Sets bot commands and starts background tasks after initialization.
    """
    commands = [
        BotCommand("start", "Show the main menu"),
    ]
    await application.bot.set_my_commands(commands)
    logging.info("Bot commands have been set in the Telegram menu.")

    # Start the master polling loops as background tasks
    asyncio.create_task(master_wallet_transaction_poll(application))
    asyncio.create_task(master_order_history_poll(application))
    logging.info("Master polling loops for transactions and orders have been started.")


def main() -> None:
    """Run the bot."""
    logging.info("Bot starting up...")

    setup_database()
    load_characters_from_db()

    application = Application.builder().token(os.getenv("TELEGRAM_BOT_TOKEN")).post_init(post_init).build()

    # --- Add command handlers ---
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    # --- Schedule Jobs ---
    job_queue = application.job_queue
    # Schedule the job to check for new characters to add to the polling loops
    job_queue.run_repeating(check_for_new_characters_job, interval=60, first=10)

    # Seed initial data for any characters already in the database on startup
    for character in CHARACTERS:
        seed_data_for_character(character)

    # Schedule the master daily summary job to run at 11:00 UTC
    job_queue.run_daily(master_daily_summary_job, time=dt_time(11, 0, tzinfo=timezone.utc))
    logging.info("Master daily summary job scheduled to run at 11:00 UTC.")

    logging.info("Bot is running. Polling for updates...")
    application.run_polling()

if __name__ == "__main__":
    main()