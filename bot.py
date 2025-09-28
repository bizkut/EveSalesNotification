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
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
import io
import matplotlib
matplotlib.use('Agg')  # Use a non-interactive backend
import matplotlib.pyplot as plt
import calendar

# Configure logging
log_level_str = os.getenv('LOG_LEVEL', 'WARNING').upper()
log_level = getattr(logging, log_level_str, logging.WARNING)
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
    wallet_balance_threshold: int
    enable_sales_notifications: bool
    enable_buy_notifications: bool
    enable_immediate_sales_notifications: bool
    enable_immediate_buy_notifications: bool
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
                    enable_sales_notifications, enable_buy_notifications,
                    enable_immediate_sales_notifications, enable_immediate_buy_notifications,
                    enable_daily_summary, notification_batch_threshold
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
            enable_sales, enable_buys, enable_immediate_sales, enable_immediate_buys,
            enable_summary, batch_threshold
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
            enable_immediate_sales_notifications=bool(enable_immediate_sales),
            enable_immediate_buy_notifications=bool(enable_immediate_buys),
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
                    enable_immediate_sales_notifications BOOLEAN DEFAULT FALSE,
                    enable_immediate_buy_notifications BOOLEAN DEFAULT FALSE,
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
                    item_id BIGINT PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)

            # Migration: Alter esi_names.item_id from INTEGER to BIGINT for structure IDs
            cursor.execute("""
                SELECT data_type FROM information_schema.columns
                WHERE table_name = 'esi_names' AND column_name = 'item_id'
            """)
            result = cursor.fetchone()
            if result and result[0] == 'integer':
                logging.info("Applying migration: Altering esi_names.item_id to BIGINT...")
                cursor.execute("ALTER TABLE esi_names ALTER COLUMN item_id TYPE BIGINT;")
                logging.info("Migration complete.")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS esi_cache (
                    cache_key TEXT PRIMARY KEY,
                    response JSONB NOT NULL,
                    etag TEXT,
                    expires TIMESTAMP WITH TIME ZONE NOT NULL,
                    headers JSONB
                )
            """)

            # Migration: Add immediate notification columns if they don't exist
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='characters' AND column_name='enable_immediate_sales_notifications'
            """)
            if cursor.fetchone() is None:
                logging.info("Applying migration: Adding 'enable_immediate_sales_notifications' to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN enable_immediate_sales_notifications BOOLEAN DEFAULT FALSE")
                logging.info("Migration complete.")

            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='characters' AND column_name='enable_immediate_buy_notifications'
            """)
            if cursor.fetchone() is None:
                logging.info("Applying migration: Adding 'enable_immediate_buy_notifications' to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN enable_immediate_buy_notifications BOOLEAN DEFAULT FALSE")
                logging.info("Migration complete.")

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
                    enable_sales_notifications, enable_buy_notifications,
                    enable_immediate_sales_notifications, enable_immediate_buy_notifications,
                    enable_daily_summary, notification_batch_threshold
                FROM characters WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            rows = cursor.fetchall()
            for row in rows:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    region_id, wallet_balance_threshold,
                    enable_sales, enable_buys, enable_immediate_sales, enable_immediate_buys,
                    enable_summary, batch_threshold
                ) = row

                user_characters.append(Character(
                    id=char_id, name=name, refresh_token=refresh_token,
                    telegram_user_id=telegram_user_id,
                    notifications_enabled=bool(notifications_enabled),
                    region_id=region_id,
                    wallet_balance_threshold=wallet_balance_threshold,
                    enable_sales_notifications=bool(enable_sales),
                    enable_buy_notifications=bool(enable_buys),
                    enable_immediate_sales_notifications=bool(enable_immediate_sales),
                    enable_immediate_buy_notifications=bool(enable_immediate_buys),
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
                    enable_sales_notifications, enable_buy_notifications,
                    enable_immediate_sales_notifications, enable_immediate_buy_notifications,
                    enable_daily_summary, notification_batch_threshold
                FROM characters WHERE character_id = %s
            """, (character_id,))
            row = cursor.fetchone()

            if row:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    region_id, wallet_balance_threshold,
                    enable_sales, enable_buys, enable_immediate_sales, enable_immediate_buys,
                    enable_summary, batch_threshold
                ) = row

                character = Character(
                    id=char_id, name=name, refresh_token=refresh_token,
                    telegram_user_id=telegram_user_id,
                    notifications_enabled=bool(notifications_enabled),
                    region_id=region_id,
                    wallet_balance_threshold=wallet_balance_threshold,
                    enable_sales_notifications=bool(enable_sales),
                    enable_buy_notifications=bool(enable_buys),
                    enable_immediate_sales_notifications=bool(enable_immediate_sales),
                    enable_immediate_buy_notifications=bool(enable_immediate_buys),
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
        "immediate_sales": "enable_immediate_sales_notifications",
        "immediate_buys": "enable_immediate_buy_notifications",
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


def delete_character(character_id: int):
    """Deletes a character and all of their associated data from the database."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            logging.warning(f"Starting deletion process for character_id: {character_id}")

            # List of tables with a direct character_id foreign key
            tables_to_delete_from = [
                "processed_transactions",
                "processed_journal_entries",
                "market_orders",
                "purchase_lots",
                "processed_orders",
            ]
            for table in tables_to_delete_from:
                cursor.execute(f"DELETE FROM {table} WHERE character_id = %s", (character_id,))
                logging.info(f"Deleted records from {table} for character {character_id}.")

            # Clean up bot_state entries
            cursor.execute("DELETE FROM bot_state WHERE key LIKE %s", (f"%_{character_id}",))
            logging.info(f"Deleted bot_state entries for character {character_id}.")

            # Finally, delete the character from the main table
            cursor.execute("DELETE FROM characters WHERE character_id = %s", (character_id,))
            logging.info(f"Deleted character {character_id} from characters table.")

            conn.commit()
            logging.warning(f"Successfully deleted all data for character_id: {character_id}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error deleting character {character_id}: {e}", exc_info=True)
    finally:
        database.release_db_connection(conn)


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

def get_wallet_transactions(character, processed_tx_ids=None, fetch_all=False, return_headers=False):
    """
    Fetches wallet transactions from ESI.
    If fetch_all is True, retrieves all pages.
    Otherwise, stops fetching when it encounters an already processed transaction.
    Returns the list of transactions and optionally the headers from the first page request.
    """
    if not character:
        return ([], None) if return_headers else []
    if processed_tx_ids is None:
        processed_tx_ids = set()

    all_transactions, page = [], 1
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/transactions/"

    stop_fetching = False
    first_page_headers = None

    while not stop_fetching:
        params = {"datasource": "tranquility", "page": page}
        # When fetching for new data, we should always revalidate.
        # When fetching all, we can allow the cache to work for subsequent pages.
        revalidate_this_page = (not fetch_all) or (page == 1)
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=revalidate_this_page)

        if page == 1:
            first_page_headers = headers

        if data is None:
            if page == 1 and not fetch_all: # Only error if we expected data
                logging.error(f"Failed to fetch wallet transactions for {character.name}, ESI request failed.")
            return ([], None) if return_headers else []

        if not data: # An empty list is a valid response, it just means no more pages.
            break

        if fetch_all:
            all_transactions.extend(data)
        else:
            page_transactions = []
            for tx in data:
                if tx['transaction_id'] in processed_tx_ids:
                    stop_fetching = True
                    break
                page_transactions.append(tx)
            all_transactions.extend(page_transactions)

            if stop_fetching:
                logging.info(f"Found previously processed transaction. Stopping fetch for char {character.name}.")
                break

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1)

    if return_headers:
        return all_transactions, first_page_headers
    return all_transactions

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

def get_names_from_ids(id_list, character: Character = None):
    """
    Resolves a list of IDs to names, using a local database cache and authenticated
    calls for private structures if a character is provided.
    """
    if not id_list:
        return {}

    unique_ids = list(set(int(i) for i in id_list if isinstance(i, int) and i > 0))
    if not unique_ids:
        return {}

    logging.debug(f"get_names_from_ids called for {len(unique_ids)} unique IDs.")
    all_resolved_names = get_names_from_db(unique_ids)
    missing_ids = [id for id in unique_ids if id not in all_resolved_names]

    if not missing_ids:
        return all_resolved_names

    # ESI's /universe/names/ endpoint can resolve most public IDs.
    # It has a limit of 1000 IDs per request.
    public_ids_to_fetch = [id for id in missing_ids if id < 10000000000]
    if public_ids_to_fetch:
        logging.info(f"Resolving {len(public_ids_to_fetch)} public names from ESI.")
        url = "https://esi.evetech.net/v3/universe/names/"
        for i in range(0, len(public_ids_to_fetch), 1000):
            chunk = public_ids_to_fetch[i:i+1000]
            name_data = make_esi_request(url, data=chunk)
            if name_data:
                for item in name_data:
                    all_resolved_names[item['id']] = item['name']

    # Structure IDs are private and require an authenticated endpoint.
    # These must be fetched one by one.
    structure_ids_to_fetch = [id for id in missing_ids if id >= 10000000000]
    if structure_ids_to_fetch and character:
        logging.info(f"Resolving {len(structure_ids_to_fetch)} structure names for {character.name}.")
        for struct_id in structure_ids_to_fetch:
            # Check if we already resolved it via the public endpoint by chance
            if struct_id in all_resolved_names:
                continue
            url = f"https://esi.evetech.net/v2/universe/structures/{struct_id}/"
            # Allow structure names to be cached, as they rarely change.
            struct_data = make_esi_request(url, character=character)
            if struct_data and 'name' in struct_data:
                logging.debug(f"Resolved structure {struct_id} to name '{struct_data['name']}'.")
                all_resolved_names[struct_id] = struct_data['name']
            else:
                logging.warning(f"Could not resolve structure name for ID {struct_id} with character {character.name}.")
                all_resolved_names[struct_id] = f"Structure ID {struct_id}" # Fallback name

    # Save all newly found names (both public and structure) to the DB for future use.
    newly_resolved_names = {id: name for id, name in all_resolved_names.items() if id in missing_ids}
    if newly_resolved_names:
        save_names_to_db(newly_resolved_names)

    logging.info(f"get_names_from_ids resolved a total of {len(all_resolved_names)}/{len(unique_ids)} names.")
    return all_resolved_names

# --- Telegram Bot Functions ---

async def send_telegram_message(context: ContextTypes.DEFAULT_TYPE, message: str, chat_id: int, reply_markup=None):
    """Sends a message to a specific chat_id."""
    if not chat_id:
        logging.error("No chat_id provided. Cannot send message.")
        return

    try:
        await context.bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown', reply_markup=reply_markup)
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
                    processed_tx_ids = get_processed_transactions(character.id)
                    new_transactions, _ = get_wallet_transactions(character, processed_tx_ids=processed_tx_ids, return_headers=True)

                    if not new_transactions:
                        continue

                    logging.info(f"Detected {len(new_transactions)} new transactions for {character.name}.")
                    # --- Transaction Classification ---
                    order_history = get_market_orders_history(character, force_revalidate=True)
                    immediate_sales, immediate_buys = defaultdict(list), defaultdict(list)
                    non_immediate_sales, non_immediate_buys = defaultdict(list), defaultdict(list)

                    for tx in new_transactions:
                        is_immediate = False
                        if order_history:
                            # Find candidate orders that match the transaction's item and type (buy/sell)
                            # and were issued before the transaction occurred.
                            candidate_orders = [
                                o for o in order_history
                                if o.get('type_id') == tx.get('type_id') and
                                   o.get('is_buy_order') == tx.get('is_buy') and
                                   o.get('issued') and
                                   datetime.fromisoformat(o['issued'].replace('Z', '+00:00')) <= datetime.fromisoformat(tx['date'].replace('Z', '+00:00'))
                            ]

                            if candidate_orders:
                                # Find the most recent order placed before the transaction.
                                best_match_order = max(candidate_orders, key=lambda o: datetime.fromisoformat(o['issued'].replace('Z', '+00:00')))

                                # If the transaction happened within 2 minutes of the order being placed, it's immediate.
                                time_diff = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) - datetime.fromisoformat(best_match_order['issued'].replace('Z', '+00:00'))
                                if time_diff.total_seconds() < 120:
                                    is_immediate = True

                        if tx['is_buy']:
                            (immediate_buys if is_immediate else non_immediate_buys)[tx['type_id']].append(tx)
                        else:
                            (immediate_sales if is_immediate else non_immediate_sales)[tx['type_id']].append(tx)

                    all_type_ids = list(immediate_sales.keys()) + list(immediate_buys.keys()) + list(non_immediate_sales.keys()) + list(non_immediate_buys.keys())
                    all_loc_ids = [t['location_id'] for txs in list(immediate_sales.values()) + list(immediate_buys.values()) + list(non_immediate_sales.values()) + list(non_immediate_buys.values()) for t in txs]
                    id_to_name = get_names_from_ids(list(set(all_type_ids + all_loc_ids + [character.region_id])), character=character)
                    wallet_balance = get_wallet_balance(character)

                    # --- Unconditional Data Processing ---

                    # Process ALL Buys for FIFO accounting
                    if immediate_buys:
                        for type_id, tx_group in immediate_buys.items():
                            for tx in tx_group:
                                add_purchase_lot(character.id, type_id, tx['quantity'], tx['unit_price'])
                    if non_immediate_buys:
                        for type_id, tx_group in non_immediate_buys.items():
                            for tx in tx_group:
                                add_purchase_lot(character.id, type_id, tx['quantity'], tx['unit_price'])

                    # Process Sales for FIFO accounting and prepare details for potential notification
                    non_immediate_sales_details = []
                    if non_immediate_sales:
                        for type_id, tx_group in non_immediate_sales.items():
                            total_quantity = sum(t['quantity'] for t in tx_group)
                            cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity)
                            non_immediate_sales_details.append({'type_id': type_id, 'tx_group': tx_group, 'cogs': cogs})

                    immediate_sales_details = []
                    if immediate_sales:
                        for type_id, tx_group in immediate_sales.items():
                            total_quantity = sum(t['quantity'] for t in tx_group)
                            cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity)
                            immediate_sales_details.append({'type_id': type_id, 'tx_group': tx_group, 'cogs': cogs})

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

                        # Non-Immediate Buy Notifications
                        if non_immediate_buys and character.enable_buy_notifications:
                            if len(non_immediate_buys) > character.notification_batch_threshold:
                                header = f"🛒 *Multiple Market Buys ({character.name})* 🛒"
                                item_lines = []
                                grand_total_cost = sum(tx['quantity'] * tx['unit_price'] for tx_group in non_immediate_buys.values() for tx in tx_group)
                                for type_id, tx_group in non_immediate_buys.items():
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    item_lines.append(f"  • Bought: `{total_quantity}` x `{id_to_name.get(type_id, 'Unknown')}`")
                                footer = f"\n**Total Cost:** `{grand_total_cost:,.2f} ISK`"
                                if wallet_balance is not None: footer += f"\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                                await send_paginated_message(context, header, item_lines, footer, chat_id=character.telegram_user_id)
                            else:
                                for type_id, tx_group in non_immediate_buys.items():
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

                        # Immediate Buy Notifications
                        if immediate_buys and character.enable_immediate_buy_notifications:
                            if len(immediate_buys) > character.notification_batch_threshold:
                                header = f"⚡ *Immediate Market Buys ({character.name})* ⚡"
                                item_lines = []
                                grand_total_cost = sum(tx['quantity'] * tx['unit_price'] for tx_group in immediate_buys.values() for tx in tx_group)
                                for type_id, tx_group in immediate_buys.items():
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    item_lines.append(f"  • Bought: `{total_quantity}` x `{id_to_name.get(type_id, 'Unknown')}`")
                                footer = f"\n**Total Cost:** `{grand_total_cost:,.2f} ISK`"
                                if wallet_balance is not None: footer += f"\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                                await send_paginated_message(context, header, item_lines, footer, chat_id=character.telegram_user_id)
                            else:
                                for type_id, tx_group in immediate_buys.items():
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    total_cost = sum(t['quantity'] * t['unit_price'] for t in tx_group)
                                    message = (f"⚡ *Immediate Market Buy ({character.name})* ⚡\n\n"
                                               f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                                               f"**Quantity:** `{total_quantity}`\n"
                                               f"**Total Cost:** `{total_cost:,.2f} ISK`\n"
                                               f"**Location:** `{id_to_name.get(tx_group[0]['location_id'], 'Unknown')}`\n"
                                               f"**Wallet:** `{wallet_balance:,.2f} ISK`")
                                    await send_telegram_message(context, message, chat_id=character.telegram_user_id)
                                    await asyncio.sleep(1)

                        # Non-Immediate Sale Notifications
                        if non_immediate_sales_details and character.enable_sales_notifications:
                            if len(non_immediate_sales_details) > character.notification_batch_threshold:
                                header = f"✅ *Multiple Market Sales ({character.name})* ✅"
                                item_lines = []
                                grand_total_value, grand_total_cogs = 0, 0
                                for sale_info in non_immediate_sales_details:
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
                                for sale_info in non_immediate_sales_details:
                                    type_id, tx_group, cogs = sale_info['type_id'], sale_info['tx_group'], sale_info['cogs']
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    total_value = sum(t['quantity'] * t['unit_price'] for t in tx_group)
                                    avg_price = total_value / total_quantity
                                    profit_line = f"\n**Gross Profit:** `{total_value - cogs:,.2f} ISK`" if cogs is not None else "\n**Profit:** `N/A`"
                                    region_orders = get_region_market_orders(character.region_id, type_id, force_revalidate=True)
                                    best_buy_order_price = max([o['price'] for o in region_orders if o.get('is_buy_order')], default=0)
                                    price_comparison_line = f"**{id_to_name.get(character.region_id, 'Region')} Best Buy:** `N/A`"
                                    if best_buy_order_price > 0:
                                        price_diff_str = f"({(avg_price / best_buy_order_price - 1):+.2%})"
                                        price_comparison_line = f"**{id_to_name.get(character.region_id, 'Region')} Best Buy:** `{best_buy_order_price:,.2f} ISK` {price_diff_str}"
                                    message = (f"✅ *Market Sale ({character.name})* ✅\n\n"
                                               f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                                               f"**Quantity:** `{total_quantity}` @ `{avg_price:,.2f} ISK`\n"
                                               f"{price_comparison_line}\n{profit_line}\n"
                                               f"**Location:** `{id_to_name.get(tx_group[0]['location_id'], 'Unknown')}`\n"
                                               f"**Wallet:** `{wallet_balance:,.2f} ISK`")
                                    await send_telegram_message(context, message, chat_id=character.telegram_user_id)
                                    await asyncio.sleep(1)

                        # Immediate Sale Notifications
                        if immediate_sales_details and character.enable_immediate_sales_notifications:
                            if len(immediate_sales_details) > character.notification_batch_threshold:
                                header = f"⚡ *Immediate Market Sales ({character.name})* ⚡"
                                item_lines = []
                                grand_total_value, grand_total_cogs = 0, 0
                                for sale_info in immediate_sales_details:
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
                                for sale_info in immediate_sales_details:
                                    type_id, tx_group, cogs = sale_info['type_id'], sale_info['tx_group'], sale_info['cogs']
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    total_value = sum(t['quantity'] * t['unit_price'] for t in tx_group)
                                    avg_price = total_value / total_quantity
                                    profit_line = f"\n**Gross Profit:** `{total_value - cogs:,.2f} ISK`" if cogs is not None else "\n**Profit:** `N/A`"
                                    region_orders = get_region_market_orders(character.region_id, type_id, force_revalidate=True)
                                    best_buy_order_price = max([o['price'] for o in region_orders if o.get('is_buy_order')], default=0)
                                    price_comparison_line = f"**{id_to_name.get(character.region_id, 'Region')} Best Buy:** `N/A`"
                                    if best_buy_order_price > 0:
                                        price_diff_str = f"({(avg_price / best_buy_order_price - 1):+.2%})"
                                        price_comparison_line = f"**{id_to_name.get(character.region_id, 'Region')} Best Buy:** `{best_buy_order_price:,.2f} ISK` {price_diff_str}"
                                    message = (f"⚡ *Immediate Market Sale ({character.name})* ⚡\n\n"
                                               f"**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n"
                                               f"**Quantity:** `{total_quantity}` @ `{avg_price:,.2f} ISK`\n"
                                               f"{price_comparison_line}\n{profit_line}\n"
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
                    order_history, headers = get_market_orders_history(character, return_headers=True, force_revalidate=True)
                    if order_history is None:
                        logging.error(f"Failed to fetch order history for {character.name}. Skipping.")
                        continue

                    processed_order_ids = get_processed_orders(character.id)
                    new_orders = [o for o in order_history if o['order_id'] not in processed_order_ids]

                    if not new_orders:
                        continue

                    logging.info(f"Detected {len(new_orders)} new historical orders for {character.name}.")

                    # Get the current time from the ESI response header for accuracy
                    try:
                        poll_time = datetime.strptime(headers['Date'], '%a, %d %b %Y %H:%M:%S GMT').replace(tzinfo=timezone.utc)
                    except (KeyError, ValueError) as e:
                        logging.warning(f"Could not parse 'Date' header: {e}. Falling back to local time.")
                        poll_time = datetime.now(timezone.utc)

                    cancelled_orders, expired_orders = [], []
                    for order in new_orders:
                        if order.get('state') == 'cancelled':
                            cancelled_orders.append(order)
                        elif order.get('state') == 'expired':
                            # Verify if the order is truly expired or just cancelled near expiry
                            try:
                                issued_dt = datetime.fromisoformat(order['issued'].replace('Z', '+00:00'))
                                duration = timedelta(days=order.get('duration', 90)) # Default to 90 if not present
                                expiration_dt = issued_dt + duration

                                # If the poll happened before the calculated expiration, it was cancelled.
                                if poll_time < expiration_dt:
                                    logging.info(f"Reclassifying order {order['order_id']} from 'expired' to 'cancelled' based on timestamps.")
                                    cancelled_orders.append(order)
                                else:
                                    expired_orders.append(order)
                            except (ValueError, KeyError) as e:
                                logging.error(f"Could not parse timestamp for order {order['order_id']}: {e}. Treating as expired.")
                                expired_orders.append(order) # Fallback

                    if cancelled_orders:
                        item_ids = [o['type_id'] for o in cancelled_orders]
                        id_to_name = get_names_from_ids(item_ids)
                        for order in cancelled_orders:
                            order_type = "Buy" if order.get('is_buy_order') else "Sell"
                            if (order_type == "Buy" and not character.enable_buy_notifications) or \
                               (order_type == "Sell" and not character.enable_sales_notifications):
                                continue

                            message = (f"ℹ️ *{order_type} Order Cancelled ({character.name})* ℹ️\n"
                                       f"Your order for `{order['volume_total']}` x `{id_to_name.get(order['type_id'], 'Unknown')}` was cancelled.")
                            await send_telegram_message(context, message, chat_id=character.telegram_user_id)
                            await asyncio.sleep(1)

                    if expired_orders:
                        item_ids = [o['type_id'] for o in expired_orders]
                        id_to_name = get_names_from_ids(item_ids)
                        for order in expired_orders:
                            order_type = "Buy" if order.get('is_buy_order') else "Sell"
                            if (order_type == "Buy" and not character.enable_buy_notifications) or \
                               (order_type == "Sell" and not character.enable_sales_notifications):
                                continue

                            message = (f"ℹ️ *{order_type} Order Expired ({character.name})* ℹ️\n"
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
    all_transactions = get_wallet_transactions(character, fetch_all=True)
    all_journal_entries, _ = get_wallet_journal(character, fetch_all=True)

    # --- 24-Hour Summary (Stateless) ---
    total_sales_24h, total_fees_24h, profit_24h = 0, 0, 0
    sales_past_24_hours = []
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

    # Dynamically generate year buttons
    available_years = sorted(list(set(datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).year for tx in all_transactions)))
    year_buttons = [InlineKeyboardButton(f"Chart {year}", callback_data=f"chart_yearly_{character.id}_{year}") for year in available_years]

    # Always include the current month chart button
    keyboard = [[InlineKeyboardButton("Show Monthly Chart", callback_data=f"chart_monthly_{character.id}_{now.year}")]]
    # Add year buttons, chunked into rows of 4 for readability
    for i in range(0, len(year_buttons), 4):
        keyboard.append(year_buttons[i:i+4])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await send_telegram_message(context, message, chat_id=character.telegram_user_id, reply_markup=reply_markup)
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
    all_transactions = get_wallet_transactions(character, fetch_all=True)
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
    Displays the main menu using a ReplyKeyboardMarkup and welcomes the user.
    """
    user = update.effective_user
    user_characters = get_characters_for_user(user.id)
    welcome_message = f"Welcome, {user.first_name}!"

    if not user_characters:
        keyboard = [["/add_character"]]
        message = (
            f"{welcome_message}\n\nIt looks like you don't have any EVE Online characters "
            "added yet. To get started, please use the `/add_character` command."
        )
    else:
        keyboard = [
            ["/balance", "/summary"],
            ["/sales", "/buys"],
            ["/notifications", "/settings"],
            ["/add_character", "/remove"]
        ]
        message = f"{welcome_message}\n\nYou have {len(user_characters)} character(s) registered. Please choose an option from the menu."

    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(text=message, reply_markup=reply_markup)


async def notifications_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to manage their notification settings
    using a ReplyKeyboardMarkup.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /notifications command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text(
            "You have no characters added. Please use `/add_character` first."
        )
        return

    if len(user_characters) == 1:
        # If only one character, go straight to the settings for them
        await _show_notification_settings(update, context, user_characters[0])
    else:
        # If multiple, ask which one to manage
        keyboard = [[char.name] for char in user_characters]
        keyboard.append(["Main Menu"]) # Option to go back
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        context.user_data['next_action'] = ('select_char_for_notifications', {char.name: char.id for char in user_characters})
        await update.message.reply_text(
            "Please select a character to manage their notification settings:",
            reply_markup=reply_markup
        )


async def add_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Provides a link for the user to add a new EVE Online character via a slash command.
    Sends a new message with an inline keyboard button.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /add_character command from user {user_id}")
    webapp_base_url = os.getenv('WEBAPP_URL', 'http://localhost:5000')
    login_url = f"{webapp_base_url}/login?user={user_id}"

    keyboard = [[InlineKeyboardButton("Authorize with EVE Online", url=login_url)]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message = (
        "To add a new character, please click the button below and authorize with EVE Online.\n\n"
        "You will be redirected to the official EVE Online login page. After logging in and "
        "authorizing, you can close the browser window.\n\n"
        "It may take a minute or two for the character to be fully registered with the bot."
    )
    await update.message.reply_text(message, reply_markup=reply_markup)


async def _show_balance_for_characters(update: Update, context: ContextTypes.DEFAULT_TYPE, characters: list[Character]):
    """Helper function to fetch and display balances for a list of characters."""
    char_names = ", ".join([c.name for c in characters])
    await update.message.reply_text(f"Fetching balance(s) for {char_names}...")

    message_lines = ["💰 *Wallet Balances* 💰\n"]
    total_balance = 0
    errors = False
    for char in characters:
        balance = get_wallet_balance(char)
        if balance is not None:
            message_lines.append(f"• `{char.name}`: `{balance:,.2f} ISK`")
            total_balance += balance
        else:
            message_lines.append(f"• `{char.name}`: `Error fetching balance`")
            errors = True

    if len(characters) > 1 and not errors:
        message_lines.append(f"\n**Combined Total:** `{total_balance:,.2f} ISK`")

    # Use the main menu keyboard for the reply
    keyboard = [
        ["/balance", "/summary"],
        ["/sales", "/buys"],
        ["/notifications", "/settings"],
        ["/add_character"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    await update.message.reply_text(
        text="\n".join(message_lines),
        parse_mode='Markdown',
        reply_markup=reply_markup
    )


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Fetches and displays wallet balance. Prompts for character selection
    if the user has multiple characters.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /balance command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text(
            "You have no characters added. Please use `/add_character` first."
        )
        return

    if len(user_characters) == 1:
        await _show_balance_for_characters(update, context, user_characters)
    else:
        keyboard = [[char.name] for char in user_characters]
        keyboard.append(["All Characters"])
        keyboard.append(["Main Menu"])  # Option to go back
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        context.user_data['next_action'] = ('select_char_for_balance', {char.name: char.id for char in user_characters})
        await update.message.reply_text(
            "Please select a character (or all) to view the balance:",
            reply_markup=reply_markup
        )


async def summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Manually triggers the daily summary report. Prompts for character
    selection if the user has multiple characters.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /summary command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text(
            "You have no characters added. Please use `/add_character` first."
        )
        return

    if len(user_characters) == 1:
        char_names = ", ".join([c.name for c in user_characters])
        await update.message.reply_text(f"Generating summary for {char_names}...")
        for char in user_characters:
            await run_daily_summary_for_character(char, context)
            await asyncio.sleep(1)
        await update.message.reply_text(f"✅ Summaries sent for {char_names}!")
    else:
        keyboard = [[char.name] for char in user_characters]
        keyboard.append(["All Characters"])
        keyboard.append(["Main Menu"])
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        context.user_data['next_action'] = ('select_char_for_summary', {char.name: char.id for char in user_characters})
        await update.message.reply_text(
            "Please select a character (or all) to generate a summary for:",
            reply_markup=reply_markup
        )


async def _get_last_5_transactions(update: Update, context: ContextTypes.DEFAULT_TYPE, is_buy: bool, characters: list[Character]) -> None:
    """Helper function to get the last 5 transactions (sales or buys) for a specific list of characters."""
    user_id = update.effective_user.id
    action = "buys" if is_buy else "sales"
    icon = "🛒" if is_buy else "✅"

    char_names = ", ".join([c.name for c in characters])
    await update.message.reply_text(f"Fetching recent {action} for {char_names}...")

    all_transactions = []
    all_order_history = []
    for char in characters:
        transactions = get_wallet_transactions(char, fetch_all=True)
        if transactions:
            for tx in transactions:
                tx['character_name'] = char.name # Add character name for context
            all_transactions.extend(transactions)
        order_history = get_market_orders_history(char)
        if order_history:
            all_order_history.extend(order_history)

    if not all_transactions:
        await update.message.reply_text(f"No transaction history found for {char_names}.")
        return

    filtered_tx = sorted(
        [tx for tx in all_transactions if tx.get('is_buy') == is_buy],
        key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')),
        reverse=True
    )[:5]

    if not filtered_tx:
        await update.message.reply_text(f"No recent {action} found for {char_names}.")
        return

    # Correct the location ID for each transaction by matching it to a historical order
    for tx in filtered_tx:
        if all_order_history:
            candidate_orders = [
                o for o in all_order_history
                if o.get('type_id') == tx.get('type_id') and
                   o.get('is_buy_order') == tx.get('is_buy') and
                   o.get('issued') and
                   datetime.fromisoformat(o['issued'].replace('Z', '+00:00')) <= datetime.fromisoformat(tx['date'].replace('Z', '+00:00'))
            ]
            if candidate_orders:
                best_match_order = max(candidate_orders, key=lambda o: datetime.fromisoformat(o['issued'].replace('Z', '+00:00')))
                tx['location_id'] = best_match_order.get('location_id', tx['location_id'])

    item_ids = [tx['type_id'] for tx in filtered_tx]
    loc_ids = [tx['location_id'] for tx in filtered_tx]
    id_to_name = get_names_from_ids(list(set(item_ids + loc_ids)), character=characters[0])

    title_char_name = char_names if len(characters) == 1 else "All Characters"
    message_lines = [f"{icon} *Last 5 {action.capitalize()} ({title_char_name})* {icon}\n"]
    for tx in filtered_tx:
        item_name = id_to_name.get(tx['type_id'], 'Unknown Item')
        loc_name = id_to_name.get(tx['location_id'], 'Unknown Location')
        date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
        char_name_str = f" ({tx['character_name']})" if len(characters) > 1 else ""
        message_lines.append(f"• `{date_str}`: `{tx['quantity']}` x `{item_name}` for `{tx['unit_price']:,.2f} ISK` each at `{loc_name}`.{char_name_str}")

    # Use the main menu keyboard for the reply
    keyboard = [
        ["/balance", "/summary"],
        ["/sales", "/buys"],
        ["/notifications", "/settings"],
        ["/add_character"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("\n".join(message_lines), parse_mode='Markdown', reply_markup=reply_markup)


def format_isk(value):
    """Formats a number into a human-readable ISK string."""
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}b"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.2f}k"
    return f"{value:.2f}"


def generate_monthly_chart(character_id: int):
    """Generates a line chart of sales, fees, and profit for the current month."""
    character = get_character_by_id(character_id)
    if not character:
        return None

    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    _, num_days = calendar.monthrange(year, month)
    days = list(range(1, num_days + 1))

    all_transactions = get_wallet_transactions(character, fetch_all=True)
    all_journal_entries, _ = get_wallet_journal(character, fetch_all=True)

    daily_sales = {day: 0 for day in days}
    daily_fees = {day: 0 for day in days}
    daily_profit = {day: 0 for day in days}

    # Filter for current month
    monthly_sales_tx = [
        tx for tx in all_transactions if not tx.get('is_buy') and
        datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).year == year and
        datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).month == month
    ]
    monthly_journal = [
        e for e in all_journal_entries if
        datetime.fromisoformat(e['date'].replace('Z', '+00:00')).year == year and
        datetime.fromisoformat(e['date'].replace('Z', '+00:00')).month == month
    ]

    if not monthly_sales_tx and not monthly_journal:
        return None # No data for this month

    for day in days:
        day_sales_tx = [tx for tx in monthly_sales_tx if datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).day == day]
        day_journal = [e for e in monthly_journal if datetime.fromisoformat(e['date'].replace('Z', '+00:00')).day == day]

        sales = sum(s['quantity'] * s['unit_price'] for s in day_sales_tx)
        fees = sum(abs(e.get('amount', 0)) for e in day_journal if e.get('ref_type') in ['brokers_fee', 'transaction_tax'])
        profit = calculate_fifo_profit_for_summary(day_sales_tx, character_id) - fees

        daily_sales[day] = sales
        daily_fees[day] = fees
        daily_profit[day] = profit

    # --- Chart Generation ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(days, list(daily_sales.values()), label='Sales', color='cyan', marker='o', linestyle='-')
    ax.plot(days, list(daily_profit.values()), label='Profit', color='lime', marker='o', linestyle='-')
    ax.plot(days, list(daily_fees.values()), label='Fees', color='red', marker='o', linestyle='-')

    ax.set_title(f'Monthly Performance for {character.name} - {now.strftime("%B %Y")}', color='white', fontsize=16)
    ax.set_xlabel('Day of Month', color='white', fontsize=12)
    ax.set_ylabel('ISK', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
    ax.legend()
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))

    buf = io.BytesIO()
    plt.savefig(buf, format='png', transparent=True, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf

def generate_yearly_chart(character_id: int, year: int):
    """Generates a line chart of sales, fees, and profit for a specific year."""
    character = get_character_by_id(character_id)
    if not character:
        return None

    months = list(range(1, 13))
    month_names = [calendar.month_abbr[m] for m in months]

    all_transactions = get_wallet_transactions(character, fetch_all=True)
    all_journal_entries, _ = get_wallet_journal(character, fetch_all=True)

    monthly_sales = {m: 0 for m in months}
    monthly_fees = {m: 0 for m in months}
    monthly_profit = {m: 0 for m in months}

    # Filter for current year
    yearly_sales_tx = [
        tx for tx in all_transactions if not tx.get('is_buy') and
        datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).year == year
    ]
    yearly_journal = [
        e for e in all_journal_entries if
        datetime.fromisoformat(e['date'].replace('Z', '+00:00')).year == year
    ]

    if not yearly_sales_tx and not yearly_journal:
        return None # No data for this year

    for month in months:
        month_sales_tx = [tx for tx in yearly_sales_tx if datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).month == month]
        month_journal = [e for e in yearly_journal if datetime.fromisoformat(e['date'].replace('Z', '+00:00')).month == month]

        sales = sum(s['quantity'] * s['unit_price'] for s in month_sales_tx)
        fees = sum(abs(e.get('amount', 0)) for e in month_journal if e.get('ref_type') in ['brokers_fee', 'transaction_tax'])
        profit = calculate_fifo_profit_for_summary(month_sales_tx, character_id) - fees

        monthly_sales[month] = sales
        monthly_fees[month] = fees
        monthly_profit[month] = profit

    # --- Chart Generation ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(month_names, list(monthly_sales.values()), label='Sales', color='cyan', marker='o', linestyle='-')
    ax.plot(month_names, list(monthly_profit.values()), label='Profit', color='lime', marker='o', linestyle='-')
    ax.plot(month_names, list(monthly_fees.values()), label='Fees', color='red', marker='o', linestyle='-')

    ax.set_title(f'Yearly Performance for {character.name} - {year}', color='white', fontsize=16)
    ax.set_xlabel('Month', color='white', fontsize=12)
    ax.set_ylabel('ISK', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
    ax.legend()
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))

    buf = io.BytesIO()
    plt.savefig(buf, format='png', transparent=True, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return buf


async def generate_chart_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job to generate and send a chart in the background."""
    job = context.job
    chat_id = job.chat_id
    character_id = job.data['character_id']
    chart_type = job.data['chart_type']
    year = job.data['year']
    generating_message_id = job.data['generating_message_id']

    character = get_character_by_id(character_id)
    if not character:
        await context.bot.edit_message_text(
            text="Error: Could not find character for this chart.",
            chat_id=chat_id,
            message_id=generating_message_id
        )
        return

    chart_buffer = None
    try:
        if chart_type == 'monthly':
            chart_buffer = generate_monthly_chart(character_id)
        elif chart_type == 'yearly':
            chart_buffer = generate_yearly_chart(character_id, year)
    except Exception as e:
        logging.error(f"Error generating chart for char {character_id}: {e}", exc_info=True)
        await context.bot.edit_message_text(
            text=f"An error occurred while generating the chart for {character.name}.",
            chat_id=chat_id,
            message_id=generating_message_id
        )
        return

    # Delete the "Generating..." message first
    await context.bot.delete_message(chat_id=chat_id, message_id=generating_message_id)

    keyboard = [[InlineKeyboardButton("Back to Summary", callback_data=f"summary_back_{character_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if chart_buffer:
        # Send the photo as a new message
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=chart_buffer,
            caption=f"{chart_type.capitalize()} chart for {character.name}",
            reply_markup=reply_markup
        )
    else:
        # Send a new message indicating no data was found
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Could not generate {chart_type} chart for {character.name}. No data available for the period.",
            reply_markup=reply_markup
        )


async def chart_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles callbacks from the chart buttons by scheduling a background job."""
    query = update.callback_query
    await query.answer()

    try:
        parts = query.data.split('_')
        action = parts[0]
        chart_type = parts[1]
        character_id = int(parts[2])
        year = int(parts[3]) if len(parts) > 3 else datetime.now(timezone.utc).year

        if action != 'chart':
            return

    except (IndexError, ValueError):
        await query.edit_message_text(text="Invalid chart request.")
        return

    character = get_character_by_id(character_id)
    if not character:
        await query.edit_message_text(text="Error: Could not find character for this chart.")
        return

    # Edit the original message to show that we're working on it.
    await query.edit_message_text(text=f"⏳ Generating {chart_type} chart for {character.name}. This may take a moment...")

    job_data = {
        'character_id': character_id,
        'chart_type': chart_type,
        'year': year,
        'generating_message_id': query.message.message_id
    }
    context.job_queue.run_once(generate_chart_job, when=1, data=job_data, chat_id=query.message.chat_id)


async def back_to_summary_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the 'Back to Summary' button press."""
    query = update.callback_query
    await query.answer()

    try:
        character_id = int(query.data.split('_')[2])
    except (IndexError, ValueError):
        # This message will be ephemeral as the original message is deleted.
        await context.bot.send_message(chat_id=query.message.chat_id, text="Invalid request.")
        return

    character = get_character_by_id(character_id)
    if not character:
        await context.bot.send_message(chat_id=query.message.chat_id, text="Error: Could not find character.")
        return

    # Delete the chart message
    await query.message.delete()

    # Let the user know we're working on it by sending a new message
    await context.bot.send_message(chat_id=query.message.chat_id, text=f"Generating summary for {character.name}...")

    # Regenerate the summary for just this one character
    await run_daily_summary_for_character(character, context)
    await context.bot.send_message(chat_id=query.message.chat_id, text=f"✅ Summary sent for {character.name}!")


async def sales_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays the 5 most recent sales. Prompts for character selection
    if the user has multiple characters.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /sales command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text(
            "You have no characters added. Please use `/add_character` first."
        )
        return

    if len(user_characters) == 1:
        await _get_last_5_transactions(update, context, is_buy=False, characters=user_characters)
    else:
        keyboard = [[char.name] for char in user_characters]
        keyboard.append(["All Characters"])
        keyboard.append(["Main Menu"])
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        context.user_data['next_action'] = ('select_char_for_sales', {char.name: char.id for char in user_characters})
        await update.message.reply_text(
            "Please select a character (or all) to view recent sales:",
            reply_markup=reply_markup
        )


async def buys_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays the 5 most recent buys. Prompts for character selection
    if the user has multiple characters.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /buys command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text(
            "You have no characters added. Please use `/add_character` first."
        )
        return

    if len(user_characters) == 1:
        await _get_last_5_transactions(update, context, is_buy=True, characters=user_characters)
    else:
        keyboard = [[char.name] for char in user_characters]
        keyboard.append(["All Characters"])
        keyboard.append(["Main Menu"])
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        context.user_data['next_action'] = ('select_char_for_buys', {char.name: char.id for char in user_characters})
        await update.message.reply_text(
            "Please select a character (or all) to view recent buys:",
            reply_markup=reply_markup
        )

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to manage their general settings
    using a ReplyKeyboardMarkup.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /settings command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text(
            "You have no characters added. Please use `/add_character` first."
        )
        return

    if len(user_characters) == 1:
        # If only one character, go straight to the settings for them
        await _show_character_settings(update, context, user_characters[0])
    else:
        # If multiple, ask which one to manage
        keyboard = [[char.name] for char in user_characters]
        keyboard.append(["Main Menu"]) # Option to go back
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        context.user_data['next_action'] = ('select_char_for_settings', {char.name: char.id for char in user_characters})
        await update.message.reply_text(
            "Please select a character to manage their settings:",
            reply_markup=reply_markup
        )


async def _show_notification_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Displays the notification settings menu for a specific character."""
    # Re-fetch character to ensure we have the latest settings
    character = get_character_by_id(character.id)
    if not character:
        await update.message.reply_text("Error: Could not find this character.")
        return

    user_characters = get_characters_for_user(character.telegram_user_id)
    back_button_text = "Back to Main Menu" if len(user_characters) <= 1 else "Back to Notifications Menu"

    keyboard = [
        [f"Toggle Sales: {'On' if character.enable_sales_notifications else 'Off'}"],
        [f"Toggle Buys: {'On' if character.enable_buy_notifications else 'Off'}"],
        [f"Toggle Immediate Sales: {'On' if character.enable_immediate_sales_notifications else 'Off'}"],
        [f"Toggle Immediate Buys: {'On' if character.enable_immediate_buy_notifications else 'Off'}"],
        [f"Toggle Daily Summary: {'On' if character.enable_daily_summary else 'Off'}"],
        [back_button_text]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
    context.user_data['next_action'] = ('manage_notifications', character.id)
    await update.message.reply_text(
        f"Notification settings for {character.name}:",
        reply_markup=reply_markup
    )


async def _show_character_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Displays the general settings menu for a specific character."""
    # Re-fetch character to ensure we have the latest settings
    character = get_character_by_id(character.id)
    if not character:
        await update.message.reply_text("Error: Could not find this character.")
        return

    user_characters = get_characters_for_user(character.telegram_user_id)
    back_button_text = "Back to Main Menu" if len(user_characters) <= 1 else "Back to Settings Menu"

    keyboard = [
        [f"Set Region ID ({character.region_id})"],
        [f"Set Wallet Alert ({character.wallet_balance_threshold:,.0f} ISK)"],
        [back_button_text]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
    context.user_data['next_action'] = ('manage_settings', character.id)
    await update.message.reply_text(
        f"Settings for {character.name}:",
        reply_markup=reply_markup
    )


async def remove_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to remove. If only one character exists,
    it proceeds directly to confirmation.
    """
    user_id = update.effective_user.id
    logging.info(f"Received /remove command from user {user_id}")
    user_characters = get_characters_for_user(user_id)

    if not user_characters:
        await update.message.reply_text("You have no characters to remove.")
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        context.user_data['next_action'] = ('confirm_removal', character.id)
        await update.message.reply_text(
            f"⚠️ *This is permanent and cannot be undone.* ⚠️\n\n"
            f"Are you sure you want to remove your character **{character.name}** and all their associated data?\n\n"
            f"Type `YES` to confirm.",
            parse_mode='Markdown'
        )
    else:
        keyboard = [[char.name] for char in user_characters]
        keyboard.append(["Main Menu"])  # Option to go back
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        context.user_data['next_action'] = ('select_char_for_removal', {char.name: char.id for char in user_characters})
        await update.message.reply_text(
            "Please select a character to remove. This action is permanent and will delete all of their data.",
            reply_markup=reply_markup
        )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all non-command text messages, routing them based on conversation state."""
    user_id = update.effective_user.id
    text = update.message.text

    # Allow returning to main menu at any time
    if text == "/start" or text == "Main Menu":
        context.user_data.clear()
        await start_command(update, context)
        return

    next_action_tuple = context.user_data.get('next_action')
    if not next_action_tuple:
        await start_command(update, context)
        return

    action_type, data = next_action_tuple

    # --- Character Selection for Data Views (Balance, Summary, etc.) ---
    if action_type in ['select_char_for_balance', 'select_char_for_summary', 'select_char_for_sales', 'select_char_for_buys']:
        char_map = data
        characters_to_query = []
        if text == "All Characters":
            characters_to_query = get_characters_for_user(user_id)
        else:
            char_id = char_map.get(text)
            if char_id:
                characters_to_query.append(get_character_by_id(char_id))

        if not characters_to_query:
            await update.message.reply_text("Invalid selection. Please use the keyboard or type `/start`.")
            return

        if action_type == 'select_char_for_balance':
            await _show_balance_for_characters(update, context, characters_to_query)
        elif action_type == 'select_char_for_summary':
            char_names = ", ".join([c.name for c in characters_to_query])
            await update.message.reply_text(f"Generating summary for {char_names}...")
            for char in characters_to_query:
                await run_daily_summary_for_character(char, context)
                await asyncio.sleep(1)
            await update.message.reply_text(f"✅ Summaries sent for {char_names}!")
        elif action_type == 'select_char_for_sales':
            await _get_last_5_transactions(update, context, is_buy=False, characters=characters_to_query)
        elif action_type == 'select_char_for_buys':
            await _get_last_5_transactions(update, context, is_buy=True, characters=characters_to_query)

        context.user_data.clear()
        return

    # --- Character Selection for Settings ---
    if action_type == 'select_char_for_notifications':
        char_id = data.get(text)
        if char_id:
            await _show_notification_settings(update, context, get_character_by_id(char_id))
        else:
            await update.message.reply_text("Invalid selection. Please use the keyboard or type `/start`.")
        return

    if action_type == 'select_char_for_settings':
        char_id = data.get(text)
        if char_id:
            await _show_character_settings(update, context, get_character_by_id(char_id))
        else:
            await update.message.reply_text("Invalid selection. Please use the keyboard or type `/start`.")
        return

    # --- Notification Management ---
    if action_type == 'manage_notifications':
        character_id = data
        character = get_character_by_id(character_id)

        if text in ["Back to Notifications Menu", "Back to Main Menu"]:
            context.user_data.clear()
            user_characters = get_characters_for_user(user_id)
            if len(user_characters) > 1:
                await notifications_command(update, context)
            else:
                await start_command(update, context)
            return

        setting_to_toggle, current_value = None, None
        if text.startswith("Toggle Sales"):
            setting_to_toggle, current_value = "sales", character.enable_sales_notifications
        elif text.startswith("Toggle Buys"):
            setting_to_toggle, current_value = "buys", character.enable_buy_notifications
        elif text.startswith("Toggle Immediate Sales"):
            setting_to_toggle, current_value = "immediate_sales", character.enable_immediate_sales_notifications
        elif text.startswith("Toggle Immediate Buys"):
            setting_to_toggle, current_value = "immediate_buys", character.enable_immediate_buy_notifications
        elif text.startswith("Toggle Daily Summary"):
            setting_to_toggle, current_value = "summary", character.enable_daily_summary

        if setting_to_toggle:
            update_character_notification_setting(character_id, setting_to_toggle, not current_value)
            load_characters_from_db()  # Reload global list
            await _show_notification_settings(update, context, get_character_by_id(character_id))
        else:
            await update.message.reply_text("Invalid selection. Please use the keyboard or type `/start`.")
        return

    # --- Character Removal Flow ---
    if action_type == 'select_char_for_removal':
        char_map = data
        char_id_to_remove = char_map.get(text)
        if char_id_to_remove:
            context.user_data['next_action'] = ('confirm_removal', char_id_to_remove)
            await update.message.reply_text(
                f"⚠️ *This is permanent and cannot be undone.* ⚠️\n\n"
                f"Are you sure you want to remove the character **{text}** and all their associated data?\n\n"
                f"Type `YES` to confirm.",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("Invalid selection. Please use the keyboard or type `/start`.")
        return

    if action_type == 'confirm_removal':
        character_id = data
        if text == "YES":
            character = get_character_by_id(character_id)
            char_name = character.name if character else f"ID {character_id}"
            await update.message.reply_text(f"Removing character {char_name} and deleting all associated data...")

            delete_character(character_id)
            load_characters_from_db()  # Refresh the global list

            await update.message.reply_text(f"✅ Character {char_name} has been successfully removed.")
            context.user_data.clear()
            await start_command(update, context)
        else:
            await update.message.reply_text("Removal cancelled. Returning to the main menu.")
            context.user_data.clear()
            await start_command(update, context)
        return

    # --- General Settings Management ---
    if action_type == 'manage_settings':
        character_id = data
        if text in ["Back to Settings Menu", "Back to Main Menu"]:
            context.user_data.clear()
            user_characters = get_characters_for_user(user_id)
            if len(user_characters) > 1:
                await settings_command(update, context)
            else:
                await start_command(update, context)
            return

        if text.startswith("Set Region ID"):
            context.user_data['next_action'] = ('set_region_value', character_id)
            await update.message.reply_text("Please enter the new Region ID (e.g., 10000002 for Jita).\n\nType `cancel` to go back.")
        elif text.startswith("Set Wallet Alert"):
            context.user_data['next_action'] = ('set_wallet_value', character_id)
            await update.message.reply_text("Please enter the new wallet balance threshold (e.g., 100000000 for 100m ISK).\n\nType `cancel` to go back.")
        else:
            await update.message.reply_text("Invalid selection. Please use the keyboard or type `/start`.")
        return

    # --- Value Input for Settings ---
    if action_type == 'set_region_value':
        character_id = data
        if text.lower() == 'cancel':
            await update.message.reply_text("Action cancelled.")
            await _show_character_settings(update, context, get_character_by_id(character_id))
            return
        try:
            new_region_id = int(text)
            update_character_setting(character_id, 'region_id', new_region_id)
            await update.message.reply_text(f"✅ Region ID updated to {new_region_id}.")
            load_characters_from_db()
            await _show_character_settings(update, context, get_character_by_id(character_id))
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a numeric Region ID. Try again or type `cancel`.")
        return

    if action_type == 'set_wallet_value':
        character_id = data
        if text.lower() == 'cancel':
            await update.message.reply_text("Action cancelled.")
            await _show_character_settings(update, context, get_character_by_id(character_id))
            return
        try:
            new_threshold = int(text.replace(',', '').replace('.', ''))
            update_character_setting(character_id, 'wallet_balance_threshold', new_threshold)
            await update.message.reply_text(f"✅ Wallet balance alert threshold updated to {new_threshold:,.0f} ISK.")
            load_characters_from_db()
            await _show_character_settings(update, context, get_character_by_id(character_id))
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a valid number. Try again or type `cancel`.")
        return

    # Fallback for any unhandled state
    context.user_data.clear()
    await start_command(update, context)

async def post_init(application: Application):
    """
    Sets bot commands and starts background tasks after initialization.
    """
    commands = [
        BotCommand("start", "Show the main menu & keyboard"),
        BotCommand("add_character", "Authorize a new character"),
        BotCommand("balance", "View wallet balance(s)"),
        BotCommand("summary", "Request a market summary"),
        BotCommand("sales", "View recent sales"),
        BotCommand("buys", "View recent buys"),
        BotCommand("notifications", "Manage notification settings"),
        BotCommand("settings", "Manage character settings"),
        BotCommand("remove", "Remove a character"),
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
    application.add_handler(CommandHandler("add_character", add_character_command))
    application.add_handler(CommandHandler("notifications", notifications_command))
    application.add_handler(CommandHandler("balance", balance_command))
    application.add_handler(CommandHandler("summary", summary_command))
    application.add_handler(CommandHandler("sales", sales_command))
    application.add_handler(CommandHandler("buys", buys_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("remove", remove_character_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(chart_callback_handler, pattern="^chart_"))
    application.add_handler(CallbackQueryHandler(back_to_summary_handler, pattern="^summary_back_"))

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