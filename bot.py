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
# Get the root logger
logger = logging.getLogger()
logger.setLevel(log_level)
# Remove any existing handlers to prevent conflicts
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
# Create a new stream handler
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# Add the new handler to the root logger
logger.addHandler(handler)

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
    created_at: datetime

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
                    enable_daily_summary, notification_batch_threshold, created_at
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
            enable_sales, enable_buys,
            enable_summary, batch_threshold, created_at
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
            notification_batch_threshold=batch_threshold,
            created_at=created_at
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
                    notification_batch_threshold INTEGER DEFAULT 3,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('UTC', now())
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

            # Migration: Add created_at to characters table for notification grace period
            cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'characters' AND column_name = 'created_at'
            """)
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'created_at' column to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('UTC', now());")
                cursor.execute("UPDATE characters SET created_at = timezone('UTC', now()) WHERE created_at IS NULL;")
                logging.info("Migration for 'created_at' complete.")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS esi_cache (
                    cache_key TEXT PRIMARY KEY,
                    response JSONB NOT NULL,
                    etag TEXT,
                    expires TIMESTAMP WITH TIME ZONE NOT NULL,
                    headers JSONB
                )
            """)

            # New tables for persistent historical data
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_transactions (
                transaction_id BIGINT NOT NULL,
                character_id INTEGER NOT NULL,
                client_id INTEGER NOT NULL,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                is_buy BOOLEAN NOT NULL,
                is_personal BOOLEAN NOT NULL,
                journal_ref_id BIGINT NOT NULL,
                location_id BIGINT NOT NULL,
                quantity INTEGER NOT NULL,
                type_id INTEGER NOT NULL,
                unit_price DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (transaction_id, character_id)
            )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_hist_trans_char_date ON historical_transactions (character_id, date DESC);")

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_journal_entries (
                entry_id BIGINT NOT NULL,
                character_id INTEGER NOT NULL,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                ref_type TEXT NOT NULL,
                amount DOUBLE PRECISION,
                balance DOUBLE PRECISION,
                context_id BIGINT,
                context_id_type TEXT,
                description TEXT,
                first_party_id INTEGER,
                reason TEXT,
                second_party_id INTEGER,
                tax DOUBLE PRECISION,
                tax_receiver_id INTEGER,
                PRIMARY KEY (entry_id, character_id)
            )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_hist_journal_char_date ON historical_journal_entries (character_id, date DESC);")


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
                    enable_daily_summary, notification_batch_threshold, created_at
                FROM characters WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            rows = cursor.fetchall()
            for row in rows:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    region_id, wallet_balance_threshold,
                    enable_sales, enable_buys,
                    enable_summary, batch_threshold, created_at
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
                    notification_batch_threshold=batch_threshold,
                    created_at=created_at
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
                    enable_daily_summary, notification_batch_threshold, created_at
                FROM characters WHERE character_id = %s
            """, (character_id,))
            row = cursor.fetchone()

            if row:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    region_id, wallet_balance_threshold,
                    enable_sales, enable_buys,
                    enable_summary, batch_threshold, created_at
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
                    notification_batch_threshold=batch_threshold,
                    created_at=created_at
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


def get_historical_transactions_from_db(character_id: int) -> list:
    """Retrieves all historical transactions for a character from the local database."""
    conn = database.get_db_connection()
    transactions = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT transaction_id, client_id, date, is_buy, is_personal, journal_ref_id, location_id, quantity, type_id, unit_price FROM historical_transactions WHERE character_id = %s",
                (character_id,)
            )
            rows = cursor.fetchall()
            # Reconstruct the dictionary to match the ESI response format
            for row in rows:
                transactions.append({
                    "transaction_id": row[0],
                    "client_id": row[1],
                    "date": row[2].isoformat(),
                    "is_buy": row[3],
                    "is_personal": row[4],
                    "journal_ref_id": row[5],
                    "location_id": row[6],
                    "quantity": row[7],
                    "type_id": row[8],
                    "unit_price": row[9]
                })
    finally:
        database.release_db_connection(conn)
    return transactions

def get_historical_journal_entries_from_db(character_id: int) -> list:
    """Retrieves all historical journal entries for a character from the local database."""
    conn = database.get_db_connection()
    entries = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT entry_id, date, ref_type, amount, balance, context_id, context_id_type, description, first_party_id, reason, second_party_id, tax, tax_receiver_id FROM historical_journal_entries WHERE character_id = %s",
                (character_id,)
            )
            rows = cursor.fetchall()
            # Reconstruct the dictionary to match the ESI response format
            for row in rows:
                entries.append({
                    "id": row[0],
                    "date": row[1].isoformat(),
                    "ref_type": row[2],
                    "amount": row[3],
                    "balance": row[4],
                    "context_id": row[5],
                    "context_id_type": row[6],
                    "description": row[7],
                    "first_party_id": row[8],
                    "reason": row[9],
                    "second_party_id": row[10],
                    "tax": row[11],
                    "tax_receiver_id": row[12]
                })
    finally:
        database.release_db_connection(conn)
    return entries


def delete_character(character_id: int):
    """Deletes a character and all of their associated data from the database."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            logging.warning(f"Starting deletion process for character_id: {character_id}")

            # List of tables with a direct character_id foreign key
            tables_to_delete_from = [
                "market_orders",
                "purchase_lots",
                "processed_orders",
                "historical_transactions",
                "historical_journal_entries"
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


def get_wallet_journal(character, fetch_all=False, return_headers=False):
    """
    Fetches wallet journal entries from ESI.
    If fetch_all is True, retrieves all pages. Otherwise, fetches only the first page.
    Returns the list of entries, or None on failure. Optionally returns headers.
    """
    if not character:
        return (None, None) if return_headers else None

    all_journal_entries, page = [], 1
    url = f"https://esi.evetech.net/v6/characters/{character.id}/wallet/journal/"
    first_page_headers = None

    while True:
        params = {"datasource": "tranquility", "page": page}
        revalidate_this_page = (not fetch_all) or (page == 1)
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=revalidate_this_page)

        if data is None: # Explicitly check for API failure
            logging.error(f"Failed to fetch page {page} of wallet journal for {character.name}.")
            return (None, None) if return_headers else None

        if page == 1:
            first_page_headers = headers

        if not data: # Empty list means no more pages
            break

        all_journal_entries.extend(data)

        if not fetch_all:
            break

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1)

    if return_headers:
        return all_journal_entries, first_page_headers
    return all_journal_entries

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

def get_wallet_transactions(character, fetch_all=False, return_headers=False):
    """
    Fetches wallet transactions from ESI.
    If fetch_all is True, retrieves all pages. Otherwise, fetches only the first page.
    Returns the list of transactions, or None on failure. Optionally returns headers.
    """
    if not character:
        return (None, None) if return_headers else None

    all_transactions, page = [], 1
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/transactions/"
    first_page_headers = None

    while True:
        params = {"datasource": "tranquility", "page": page}
        revalidate_this_page = (not fetch_all) or (page == 1)
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=revalidate_this_page)

        if data is None: # Explicitly check for API failure
            logging.error(f"Failed to fetch page {page} of wallet transactions for {character.name}.")
            return (None, None) if return_headers else None

        if page == 1:
            first_page_headers = headers

        if not data: # Empty list means no more pages
            break

        all_transactions.extend(data)

        if not fetch_all:
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

def get_character_skills(character, force_revalidate=False):
    """Fetches a character's skills from ESI."""
    if not character: return None
    url = f"https://esi.evetech.net/v4/characters/{character.id}/skills/"
    return make_esi_request(url, character=character, force_revalidate=force_revalidate)

def get_wallet_balance(character, return_headers=False):
    if not character: return None
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/"
    return make_esi_request(url, character=character, return_headers=return_headers)

def get_market_orders_history(character, return_headers=False, force_revalidate=False):
    """
    Fetches all pages of historical market orders from ESI.
    Returns the list of orders, or None on failure. Optionally returns headers.
    """
    if not character:
        return (None, None) if return_headers else None

    all_orders = []
    page = 1
    url = f"https://esi.evetech.net/v1/characters/{character.id}/orders/history/"
    first_page_headers = None

    while True:
        params = {"datasource": "tranquility", "page": page}
        revalidate_this_page = force_revalidate and page == 1
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=revalidate_this_page)

        if data is None: # Explicitly check for API failure
            logging.error(f"Failed to fetch page {page} of order history for {character.name}.")
            return (None, None) if return_headers else None

        if page == 1:
            first_page_headers = headers

        if not data: # Empty list means no more pages
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

def get_ids_from_db(table_name: str, id_column: str, character_id: int, ids_to_check: list) -> set:
    """Checks a table for which of the given IDs already exist for a character."""
    if not ids_to_check:
        return set()
    conn = database.get_db_connection()
    existing_ids = set()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(ids_to_check))
            query = f"SELECT {id_column} FROM {table_name} WHERE character_id = %s AND {id_column} IN ({placeholders})"
            # Note: The character_id must be passed as a tuple/list element
            cursor.execute(query, [character_id] + ids_to_check)
            existing_ids = {row[0] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)
    return existing_ids


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
                # Security Check: Ensure character hasn't been deleted since the job started.
                if character.id not in [c.id for c in CHARACTERS]:
                    logging.warning(f"Skipping wallet poll for character {character.name} ({character.id}) because they have been removed.")
                    continue

                # Grace period for new characters to allow initial sync to complete
                if (datetime.now(timezone.utc) - character.created_at) < timedelta(minutes=5):
                    logging.info(f"Skipping wallet poll for new character {character.name} (within 5-minute grace period).")
                    continue

                logging.debug(f"Polling wallet for {character.name}")
                try:
                    # Fetch only the most recent page of transactions from ESI
                    recent_transactions = get_wallet_transactions(character)
                    if not recent_transactions:
                        continue

                    # Find which of these are genuinely new by checking against our historical DB
                    tx_ids_from_esi = [tx['transaction_id'] for tx in recent_transactions]
                    existing_tx_ids = get_ids_from_db('historical_transactions', 'transaction_id', character.id, tx_ids_from_esi)
                    new_transaction_ids = set(tx_ids_from_esi) - existing_tx_ids

                    if not new_transaction_ids:
                        continue

                    new_transactions = [tx for tx in recent_transactions if tx['transaction_id'] in new_transaction_ids]
                    logging.info(f"Detected {len(new_transactions)} new transactions for {character.name}.")

                    # Add new transactions to our historical database
                    add_historical_transactions_to_db(character.id, new_transactions)

                    # --- Now handle journal entries ---
                    recent_journal_entries = get_wallet_journal(character)
                    if recent_journal_entries:
                        journal_ids_from_esi = [e['id'] for e in recent_journal_entries]
                        existing_journal_ids = get_ids_from_db('historical_journal_entries', 'entry_id', character.id, journal_ids_from_esi)
                        new_journal_entry_ids = set(journal_ids_from_esi) - existing_journal_ids

                        if new_journal_entry_ids:
                            new_journal_entries = [e for e in recent_journal_entries if e['id'] in new_journal_entry_ids]
                            logging.info(f"Detected {len(new_journal_entries)} new journal entries for {character.name}.")
                            add_historical_journal_entries_to_db(character.id, new_journal_entries)


                    # --- Transaction Classification ---
                    sales, buys = defaultdict(list), defaultdict(list)
                    for tx in new_transactions:
                        if tx['is_buy']:
                            buys[tx['type_id']].append(tx)
                        else:
                            sales[tx['type_id']].append(tx)

                    all_type_ids = list(sales.keys()) + list(buys.keys())
                    all_loc_ids = [t['location_id'] for txs in list(sales.values()) + list(buys.values()) for t in txs]
                    id_to_name = get_names_from_ids(list(set(all_type_ids + all_loc_ids + [character.region_id])), character=character)
                    wallet_balance = get_wallet_balance(character)

                    # --- Unconditional Data Processing ---

                    # Process ALL Buys for FIFO accounting
                    if buys:
                        for type_id, tx_group in buys.items():
                            for tx in tx_group:
                                add_purchase_lot(character.id, type_id, tx['quantity'], tx['unit_price'], purchase_date=tx['date'])

                    # Process Sales for FIFO accounting and prepare details for potential notification
                    sales_details = []
                    if sales:
                        for type_id, tx_group in sales.items():
                            total_quantity = sum(t['quantity'] for t in tx_group)
                            cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity)
                            sales_details.append({'type_id': type_id, 'tx_group': tx_group, 'cogs': cogs})

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
                            # Filter out transactions that occurred before the character was added
                            recent_buys = {}
                            for type_id, tx_group in buys.items():
                                recent_tx_group = [tx for tx in tx_group if datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > character.created_at]
                                if recent_tx_group:
                                    recent_buys[type_id] = recent_tx_group

                            if len(recent_buys) > character.notification_batch_threshold:
                                header = f"🛒 *Multiple Market Buys ({character.name})* 🛒"
                                item_lines = []
                                grand_total_cost = sum(tx['quantity'] * tx['unit_price'] for tx_group in recent_buys.values() for tx in tx_group)
                                for type_id, tx_group in recent_buys.items():
                                    total_quantity = sum(t['quantity'] for t in tx_group)
                                    item_lines.append(f"  • Bought: `{total_quantity}` x `{id_to_name.get(type_id, 'Unknown')}`")
                                footer = f"\n**Total Cost:** `{grand_total_cost:,.2f} ISK`"
                                if wallet_balance is not None: footer += f"\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                                await send_paginated_message(context, header, item_lines, footer, chat_id=character.telegram_user_id)
                            elif recent_buys:
                                for type_id, tx_group in recent_buys.items():
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
                        if sales_details and character.enable_sales_notifications:
                            # Filter out sales that occurred before the character was added
                            recent_sales_details = []
                            for sale in sales_details:
                                recent_tx_group = [tx for tx in sale['tx_group'] if datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > character.created_at]
                                if recent_tx_group:
                                    # We need to create a new sale_info dict because the cogs was for the whole group
                                    # This is a simplification; for perfect accuracy, COGS would need recalculation.
                                    # However, for notification purposes, this is acceptable as it prevents spam.
                                    new_sale_info = sale.copy()
                                    new_sale_info['tx_group'] = recent_tx_group
                                    recent_sales_details.append(new_sale_info)

                            if len(recent_sales_details) > character.notification_batch_threshold:
                                header = f"✅ *Multiple Market Sales ({character.name})* ✅"
                                item_lines = []
                                grand_total_value, grand_total_cogs = 0, 0
                                for sale_info in recent_sales_details:
                                    total_quantity = sum(t['quantity'] for t in sale_info['tx_group'])
                                    total_value = sum(t['quantity'] * t['unit_price'] for t in sale_info['tx_group'])
                                    grand_total_value += total_value
                                    if sale_info['cogs'] is not None: grand_total_cogs += sale_info['cogs']
                                    item_lines.append(f"  • Sold: `{total_quantity}` x `{id_to_name.get(sale_info['type_id'], 'Unknown')}`")
                                profit_line = f"\n**Total Gross Profit:** `{grand_total_value - grand_total_cogs:,.2f} ISK`" if grand_total_cogs > 0 else ""
                                footer = f"\n**Total Sale Value:** `{grand_total_value:,.2f} ISK`{profit_line}"
                                if wallet_balance is not None: footer += f"\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                                await send_paginated_message(context, header, item_lines, footer, chat_id=character.telegram_user_id)
                            elif recent_sales_details:
                                for sale_info in recent_sales_details:
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
                # Security Check: Ensure character hasn't been deleted since the job started.
                if character.id not in [c.id for c in CHARACTERS]:
                    logging.warning(f"Skipping order history poll for character {character.name} ({character.id}) because they have been removed.")
                    continue

                # Grace period for new characters to allow initial sync to complete
                if (datetime.now(timezone.utc) - character.created_at) < timedelta(minutes=5):
                    logging.info(f"Skipping order history poll for new character {character.name} (within 5-minute grace period).")
                    continue

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
                            # Skip notifications for orders issued before the character was added
                            try:
                                issued_dt = datetime.fromisoformat(order['issued'].replace('Z', '+00:00'))
                                if issued_dt < character.created_at:
                                    logging.info(f"Skipping notification for historical cancelled order {order['order_id']}.")
                                    continue
                            except (ValueError, KeyError):
                                pass  # If date is missing, process it to be safe

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
                            # Skip notifications for orders issued before the character was added
                            try:
                                issued_dt = datetime.fromisoformat(order['issued'].replace('Z', '+00:00'))
                                if issued_dt < character.created_at:
                                    logging.info(f"Skipping notification for historical expired order {order['order_id']}.")
                                    continue
                            except (ValueError, KeyError):
                                pass  # If date is missing, process it to be safe

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


def _calculate_summary_data(character: Character) -> dict:
    """Fetches all necessary data from the local DB and calculates summary statistics."""
    logging.info(f"Calculating summary data for {character.name} from local database...")

    now = datetime.now(timezone.utc)
    one_day_ago = now - timedelta(days=1)

    # --- Fetch data from local database ---
    all_transactions = get_historical_transactions_from_db(character.id)
    all_journal_entries = get_historical_journal_entries_from_db(character.id)

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

    # --- Available Years for Charts ---
    available_years = []
    if all_transactions:
        available_years = sorted(list(set(datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).year for tx in all_transactions)))

    return {
        "now": now,
        "wallet_balance": wallet_balance,
        "total_sales_24h": total_sales_24h,
        "total_fees_24h": total_fees_24h,
        "profit_24h": profit_24h,
        "total_sales_month": total_sales_month,
        "total_fees_month": total_fees_month,
        "gross_revenue_month": gross_revenue_month,
        "available_years": available_years
    }


def _format_summary_message(summary_data: dict, character: Character) -> tuple[str, InlineKeyboardMarkup]:
    """Formats the summary data into a message string and keyboard."""
    now = summary_data['now']
    message = (
        f"📊 *Daily Market Summary ({character.name})*\n"
        f"_{now.strftime('%Y-%m-%d %H:%M UTC')}_\n\n"
        f"*Wallet Balance:* `{summary_data['wallet_balance'] or 0:,.2f} ISK`\n\n"
        f"*Past 24 Hours:*\n"
        f"  - Total Sales Value: `{summary_data['total_sales_24h']:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{summary_data['total_fees_24h']:,.2f} ISK`\n"
        f"  - **Profit (FIFO):** `{summary_data['profit_24h']:,.2f} ISK`\n\n"
        f"---\n\n"
        f"🗓️ *Current Month Summary ({now.strftime('%B %Y')}):*\n"
        f"  - Total Sales Value: `{summary_data['total_sales_month']:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{summary_data['total_fees_month']:,.2f} ISK`\n"
        f"  - *Gross Revenue (Sales - Fees):* `{summary_data['gross_revenue_month']:,.2f} ISK`"
    )

    # Dynamically generate year buttons, sorted from newest to oldest
    available_years = sorted(summary_data['available_years'], reverse=True)
    year_buttons = [InlineKeyboardButton(f"Chart {year}", callback_data=f"chart_yearly_{character.id}_{year}") for year in available_years]

    # Always include the current month chart button
    keyboard = [[InlineKeyboardButton("Daily Chart (This Month)", callback_data=f"chart_monthly_{character.id}_{now.year}")]]
    # Add year buttons, chunked into rows of 4 for readability
    for i in range(0, len(year_buttons), 4):
        keyboard.append(year_buttons[i:i+4])

    keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    return message, reply_markup


async def _generate_and_send_summary(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Handles the interactive flow of generating and sending a summary message."""
    target_chat_id = update.effective_chat.id
    sent_message = await context.bot.send_message(chat_id=target_chat_id, text=f"⏳ Generating summary for {character.name}...")

    try:
        # Run the synchronous data calculation in a thread to avoid blocking
        summary_data = await asyncio.to_thread(_calculate_summary_data, character)
        message, reply_markup = _format_summary_message(summary_data, character)

        # Edit the placeholder message with the final content
        await context.bot.edit_message_text(
            chat_id=target_chat_id,
            message_id=sent_message.message_id,
            text=message,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except Exception as e:
        logging.error(f"Failed to generate and send summary for {character.name}: {e}", exc_info=True)
        await context.bot.edit_message_text(
            chat_id=target_chat_id,
            message_id=sent_message.message_id,
            text=f"❌ An error occurred while generating the summary for {character.name}."
        )


async def run_daily_summary_for_character(character: Character, context: ContextTypes.DEFAULT_TYPE):
    """Calculates and sends the daily summary for a single character (for scheduled jobs)."""
    logging.info(f"Running scheduled daily summary for {character.name}...")
    try:
        summary_data = _calculate_summary_data(character)
        message, reply_markup = _format_summary_message(summary_data, character)
        await send_telegram_message(context, message, chat_id=character.telegram_user_id, reply_markup=reply_markup)
        logging.info(f"Daily summary sent for {character.name}.")
    except Exception as e:
        logging.error(f"Failed to send daily summary for {character.name}: {e}", exc_info=True)


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

def add_historical_transactions_to_db(character_id: int, transactions: list):
    """Adds a list of transaction records to the historical_transactions table."""
    if not transactions:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = [
                (
                    tx['transaction_id'], character_id, tx['client_id'], tx['date'],
                    tx['is_buy'], tx['is_personal'], tx['journal_ref_id'],
                    tx['location_id'], tx['quantity'], tx['type_id'], tx['unit_price']
                ) for tx in transactions
            ]
            cursor.executemany(
                """
                INSERT INTO historical_transactions (
                    transaction_id, character_id, client_id, date, is_buy, is_personal,
                    journal_ref_id, location_id, quantity, type_id, unit_price
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id, character_id) DO NOTHING
                """,
                data_to_insert
            )
            conn.commit()
            logging.info(f"Inserted/updated {len(data_to_insert)} records into historical_transactions for char {character_id}.")
    finally:
        database.release_db_connection(conn)


def add_historical_journal_entries_to_db(character_id: int, entries: list):
    """Adds a list of journal entries to the historical_journal_entries table."""
    if not entries:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = [
                (
                    e['id'], character_id, e['date'], e['ref_type'],
                    e.get('amount'), e.get('balance'), e.get('context_id'),
                    e.get('context_id_type'), e.get('description'), e.get('first_party_id'),
                    e.get('reason'), e.get('second_party_id'), e.get('tax'), e.get('tax_receiver_id')
                ) for e in entries
            ]
            cursor.executemany(
                """
                INSERT INTO historical_journal_entries (
                    entry_id, character_id, date, ref_type, amount, balance, context_id,
                    context_id_type, description, first_party_id, reason, second_party_id,
                    tax, tax_receiver_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (entry_id, character_id) DO NOTHING
                """,
                data_to_insert
            )
            conn.commit()
            logging.info(f"Inserted/updated {len(data_to_insert)} records into historical_journal_entries for char {character_id}.")
    finally:
        database.release_db_connection(conn)


def backfill_all_character_history(character: Character) -> bool:
    """
    Performs a one-time backfill of all transaction and journal history from
    ESI into the local database for a given character.
    Returns True on success, False on any ESI failure.
    """
    state_key = f"history_backfilled_{character.id}"
    if get_bot_state(state_key) == 'true':
        logging.info(f"Full history already backfilled for {character.name}. Skipping.")
        return True # Already done, so it's a success in this context

    logging.warning(f"Starting full history backfill for {character.name}. This may take some time...")

    # --- Backfill Transactions ---
    logging.info(f"Fetching all wallet transactions for {character.name}...")
    all_transactions = get_wallet_transactions(character, fetch_all=True)
    if all_transactions is None: # Explicitly check for None, as [] is a valid success
        logging.error(f"Failed to fetch wallet transactions during history backfill for {character.name}.")
        return False

    add_historical_transactions_to_db(character.id, all_transactions)
    # Also populate purchase lots for FIFO, same as the old seeding logic
    buy_transactions = [tx for tx in all_transactions if tx.get('is_buy')]
    if buy_transactions:
        for tx in buy_transactions:
            add_purchase_lot(character.id, tx['type_id'], tx['quantity'], tx['unit_price'], purchase_date=tx['date'])
        logging.info(f"Seeded {len(buy_transactions)} historical buy transactions for FIFO tracking for {character.name}.")


    # --- Backfill Journal Entries ---
    logging.info(f"Fetching all wallet journal entries for {character.name}...")
    all_journal_entries = get_wallet_journal(character, fetch_all=True)
    if all_journal_entries is None:
        logging.error(f"Failed to fetch wallet journal during history backfill for {character.name}.")
        return False
    add_historical_journal_entries_to_db(character.id, all_journal_entries)

    # --- Backfill Order History (for cancelled/expired notifications) ---
    logging.info(f"Seeding order history for {character.name}...")
    all_historical_orders = get_market_orders_history(character, force_revalidate=True)
    if all_historical_orders is None:
        logging.error(f"Failed to fetch order history during backfill for {character.name}.")
        return False

    order_ids = [o['order_id'] for o in all_historical_orders]
    # We still need to mark these as "processed" for the notification system
    add_processed_orders(character.id, order_ids)
    logging.info(f"Seeded {len(order_ids)} historical orders for {character.name}.")


    # --- Mark as complete ---
    set_bot_state(state_key, 'true')
    logging.warning(f"Full history backfill for {character.name} is complete.")
    return True


def seed_data_for_character(character: Character) -> bool:
    """
    Initializes all historical data for a character if it hasn't been done before.
    Returns True on success, False on failure.
    """
    logging.info(f"Checking/seeding historical data for character: {character.name} ({character.id})")
    success = backfill_all_character_history(character)
    logging.info(f"Finished checking/seeding data for {character.name}. Success: {success}")
    return success


async def check_for_new_characters_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Periodically checks the database for new characters, seeds their historical
    data, and adds them to the live monitoring list upon success.
    """
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
            if not character:
                logging.error(f"Could not find details for newly detected character ID {char_id} in the database.")
                continue

            # Attempt to seed the historical data for the new character.
            # This is a blocking, synchronous function.
            seed_successful = seed_data_for_character(character)

            if seed_successful:
                # Add the character to the global list to be picked up by the master polls
                CHARACTERS.append(character)
                logging.info(f"Added new character {character.name} to the polling list after successful data seed.")

                # Create the main menu keyboard to send with the success message
                keyboard = [
                    [
                        InlineKeyboardButton("💰 View Balances", callback_data="balance"),
                        InlineKeyboardButton("📊 Open Orders", callback_data="open_orders")
                    ],
                    [
                        InlineKeyboardButton("📈 View Sales", callback_data="sales"),
                        InlineKeyboardButton("🛒 View Buys", callback_data="buys")
                    ],
                    [
                        InlineKeyboardButton("📊 Request Summary", callback_data="summary"),
                        InlineKeyboardButton("⚙️ Settings", callback_data="settings")
                    ],
                    [
                        InlineKeyboardButton("➕ Add Character", callback_data="add_character"),
                        InlineKeyboardButton("🗑️ Remove", callback_data="remove")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                await send_telegram_message(
                    context,
                    f"✅ Successfully added and synced **{character.name}**! I will now start monitoring their market activity.",
                    chat_id=character.telegram_user_id,
                    reply_markup=reply_markup
                )
            else:
                # If seeding fails, do NOT add them to the monitoring list.
                # Inform the user and the bot will automatically try again on the next cycle.
                logging.error(f"Failed to seed historical data for {character.name}. They will not be monitored yet.")
                await send_telegram_message(
                    context,
                    f"⚠️ Failed to import historical data for **{character.name}** due to a temporary ESI API issue. "
                    f"I will automatically retry in a few minutes. Monitoring will begin once the import succeeds.",
                    chat_id=character.telegram_user_id
                )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the main menu using an InlineKeyboardMarkup."""
    user = update.effective_user
    user_characters = get_characters_for_user(user.id)
    welcome_message = f"Welcome, {user.first_name}!"

    # Determine the message and keyboard based on whether the user has characters
    if not user_characters:
        message = (
            f"{welcome_message}\n\nIt looks like you don't have any EVE Online characters "
            "added yet. To get started, please add one."
        )
        keyboard = [[InlineKeyboardButton("➕ Add Character", callback_data="add_character")]]
    else:
        message = (
            f"{welcome_message}\n\nYou have {len(user_characters)} character(s) registered. "
            "Please choose an option:"
        )
        keyboard = [
            [
                InlineKeyboardButton("💰 View Balances", callback_data="balance"),
                InlineKeyboardButton("📊 Open Orders", callback_data="open_orders")
            ],
            [
                InlineKeyboardButton("📈 View Sales", callback_data="sales"),
                InlineKeyboardButton("🛒 View Buys", callback_data="buys")
            ],
            [
                InlineKeyboardButton("📊 Request Summary", callback_data="summary"),
                InlineKeyboardButton("⚙️ Settings", callback_data="settings")
            ],
            [
                InlineKeyboardButton("➕ Add Character", callback_data="add_character"),
                InlineKeyboardButton("🗑️ Remove", callback_data="remove")
            ]
        ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    # If the command was triggered by a button press (callback query), edit the message.
    # Otherwise, if it was triggered by /start (message), send a new message.
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(text=message, reply_markup=reply_markup)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise e # Re-raise if it's not the expected error
            logging.info("Message not modified, skipping edit.")
    else:
        await update.message.reply_text(text=message, reply_markup=reply_markup)


async def notifications_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to manage their notification settings
    using an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _show_notification_settings(update, context, user_characters[0])
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"notifications_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to manage their notification settings:"

        # Edit the existing message if from a callback, otherwise send a new one
        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def add_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Provides a link for the user to add a new EVE Online character.
    Can be triggered by a command or a callback query.
    """
    user_id = update.effective_user.id
    logging.info(f"Received add_character request from user {user_id}")
    webapp_base_url = os.getenv('WEBAPP_URL', 'http://localhost:5000')
    login_url = f"{webapp_base_url}/login?user={user_id}"

    keyboard = [[InlineKeyboardButton("Authorize with EVE Online", url=login_url)]]
    # Add a back button for a better UX when coming from a callback
    if update.callback_query:
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
    reply_markup = InlineKeyboardMarkup(keyboard)


    message = (
        "To add a new character, please click the button below and authorize with EVE Online.\n\n"
        "You will be redirected to the official EVE Online login page. After logging in and "
        "authorizing, you can close the browser window.\n\n"
        "It may take a minute or two for the character to be fully registered with the bot."
    )

    if update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    else:
        await update.message.reply_text(message, reply_markup=reply_markup)


async def _show_balance_for_characters(update: Update, context: ContextTypes.DEFAULT_TYPE, characters: list[Character]):
    """Helper function to fetch and display balances for a list of characters."""
    query = update.callback_query
    char_names = ", ".join([c.name for c in characters])

    # For callbacks, let the user know we're working on it
    if query:
        await query.edit_message_text(f"⏳ Fetching balance(s) for {char_names}...")

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

    final_text = "\n".join(message_lines)

    if query:
        # For callbacks, edit the message and add a "Back" button
        keyboard = [[InlineKeyboardButton("« Back", callback_data="start_command")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text=final_text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        # For commands, send a new message with a ReplyKeyboardMarkup
        keyboard = [
            ["/balance", "/summary"],
            ["/sales", "/buys"],
            ["/notifications", "/settings"],
            ["/add_character"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            text=final_text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Fetches and displays wallet balance. Prompts for character selection
    if the user has multiple characters via an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _show_balance_for_characters(update, context, user_characters)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"balance_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("All Characters", callback_data="balance_char_all")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character (or all) to view the balance:"

        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def summary_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Manually triggers the daily summary report. Prompts for character
    selection if the user has multiple characters via an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _generate_and_send_summary(update, context, user_characters[0])
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"summary_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("All Characters", callback_data="summary_char_all")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character (or all) to generate a summary for:"

        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def _get_last_5_transactions(update: Update, context: ContextTypes.DEFAULT_TYPE, is_buy: bool, characters: list[Character]) -> None:
    """Helper function to get the last 5 transactions (sales or buys) for a specific list of characters."""
    query = update.callback_query
    action = "buys" if is_buy else "sales"
    icon = "🛒" if is_buy else "✅"
    char_names = ", ".join([c.name for c in characters])

    # For callbacks, let the user know we're working on it
    if query:
        await query.edit_message_text(f"⏳ Fetching recent {action} for {char_names}...")

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

    message_text = f"No transaction history found for {char_names}."
    if all_transactions:
        filtered_tx = sorted(
            [tx for tx in all_transactions if tx.get('is_buy') == is_buy],
            key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')),
            reverse=True
        )[:5]

        if not filtered_tx:
            message_text = f"No recent {action} found for {char_names}."
        else:
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
            message_text = "\n".join(message_lines)

    if query:
        keyboard = [[InlineKeyboardButton("« Back", callback_data="start_command")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text=message_text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        keyboard = [
            ["/balance", "/summary"],
            ["/sales", "/buys"],
            ["/notifications", "/settings"],
            ["/add_character"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(message_text, parse_mode='Markdown', reply_markup=reply_markup)


def format_isk(value):
    """Formats a number into a human-readable ISK string."""
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}b"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.2f}k"
    return f"{value:.2f}"


def generate_daily_chart_for_month(character_id: int):
    """Generates a line chart of sales, fees, and profit for the current month."""
    character = get_character_by_id(character_id)
    if not character:
        return None

    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    _, num_days = calendar.monthrange(year, month)
    days = list(range(1, num_days + 1))

    # Fetch data directly from the historical tables in the database
    all_transactions = get_historical_transactions_from_db(character_id)
    all_journal_entries = get_historical_journal_entries_from_db(character_id)


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
    fig.patch.set_facecolor('#1c1c1c')  # Set outer background color
    ax.set_facecolor('#282828')  # Set inner plot background color

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
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
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

    # Fetch data directly from the historical tables in the database
    all_transactions = get_historical_transactions_from_db(character_id)
    all_journal_entries = get_historical_journal_entries_from_db(character_id)


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
    fig.patch.set_facecolor('#1c1c1c')  # Set outer background color
    ax.set_facecolor('#282828')  # Set inner plot background color

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
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
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
            chart_buffer = generate_daily_chart_for_month(character_id)
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
    """Handles the 'Back to Summary' button press by deleting the chart and regenerating the summary."""
    query = update.callback_query
    await query.answer()

    try:
        character_id = int(query.data.split('_')[2])
    except (IndexError, ValueError):
        await query.edit_message_text(text="Invalid request.")
        return

    character = get_character_by_id(character_id)
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    # Delete the chart message
    await query.message.delete()

    # Regenerate the summary for just this one character using the new interactive handler
    await _generate_and_send_summary(update, context, character)


async def sales_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays the 5 most recent sales. Prompts for character selection
    if the user has multiple characters via an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _get_last_5_transactions(update, context, is_buy=False, characters=user_characters)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"sales_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("All Characters", callback_data="sales_char_all")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character (or all) to view recent sales:"

        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def buys_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Displays the 5 most recent buys. Prompts for character selection
    if the user has multiple characters via an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _get_last_5_transactions(update, context, is_buy=True, characters=user_characters)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"buys_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("All Characters", callback_data="buys_char_all")])
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character (or all) to view recent buys:"

        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to manage their general settings
    using an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters added. Please use `/add_character` first.")
        return

    if len(user_characters) == 1:
        await _show_character_settings(update, context, user_characters[0])
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"settings_char_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to manage their settings:"

        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def _show_notification_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Displays the notification settings menu for a specific character using an InlineKeyboardMarkup."""
    character = get_character_by_id(character.id) # Re-fetch to ensure latest settings
    if not character:
        await context.bot.send_message(update.effective_chat.id, "Error: Could not find this character.")
        return

    user_characters = get_characters_for_user(character.telegram_user_id)
    back_callback = "start_command" if len(user_characters) <= 1 else "notifications"

    keyboard = [
        [InlineKeyboardButton(f"Sales Notifications: {'✅ On' if character.enable_sales_notifications else '❌ Off'}", callback_data=f"toggle_sales_{character.id}")],
        [InlineKeyboardButton(f"Buy Notifications: {'✅ On' if character.enable_buy_notifications else '❌ Off'}", callback_data=f"toggle_buys_{character.id}")],
        [InlineKeyboardButton(f"Daily Summary: {'✅ On' if character.enable_daily_summary else '❌ Off'}", callback_data=f"toggle_summary_{character.id}")],
        [InlineKeyboardButton("« Back", callback_data=back_callback)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = f"🔔 Notification settings for *{character.name}*:"

    if update.callback_query:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=update.effective_message.message_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


async def _show_character_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, character: Character):
    """Displays the general settings menu for a specific character using an InlineKeyboardMarkup."""
    character = get_character_by_id(character.id) # Re-fetch for latest data
    if not character:
        await context.bot.send_message(update.effective_chat.id, "Error: Could not find this character.")
        return

    user_characters = get_characters_for_user(character.telegram_user_id)
    back_callback = "start_command" if len(user_characters) <= 1 else "settings"

    keyboard = [
        [InlineKeyboardButton(f"Trade Region: {character.region_id}", callback_data=f"set_region_{character.id}")],
        [InlineKeyboardButton(f"Low Wallet Alert: {character.wallet_balance_threshold:,.0f} ISK", callback_data=f"set_wallet_{character.id}")],
        [InlineKeyboardButton("« Back", callback_data=back_callback)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = f"⚙️ General settings for *{character.name}*:"

    if update.callback_query:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=update.effective_message.message_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )


async def remove_character_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Allows a user to select a character to remove using an InlineKeyboardMarkup.
    """
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id

    if not user_characters:
        await context.bot.send_message(chat_id, "You have no characters to remove.")
        return

    if len(user_characters) == 1:
        character = user_characters[0]
        keyboard = [
            [InlineKeyboardButton("YES, REMOVE a character", callback_data=f"remove_confirm_{character.id}")],
            [InlineKeyboardButton("NO, CANCEL", callback_data="start_command")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = (
            f"⚠️ *This is permanent and cannot be undone.* ⚠️\n\n"
            f"Are you sure you want to remove your character **{character.name}** and all their associated data?"
        )
        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        keyboard = [[InlineKeyboardButton(f"Remove {char.name}", callback_data=f"remove_select_{char.id}")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = "Please select a character to remove. This action is permanent and will delete all of their data."
        if update.callback_query:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=reply_markup)


async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles text input when the bot is expecting a specific value from the user."""
    user_id = update.effective_user.id
    text = update.message.text
    next_action_tuple = context.user_data.get('next_action')

    if not next_action_tuple:
        # If we are not expecting any input, just show the main menu
        await start_command(update, context)
        return

    action_type, data = next_action_tuple
    character_id = data

    if text.lower() == 'cancel':
        await update.message.reply_text("Action cancelled.")
        context.user_data.clear()
        # Create a fake update object to show the settings again
        fake_query = type('obj', (object,), {'data': f'settings_char_{character_id}'})
        fake_update = type('obj', (object,), {'callback_query': fake_query, 'effective_chat': update.effective_chat, 'effective_message': update.message, 'effective_user': update.effective_user})
        await _show_character_settings(fake_update, context, get_character_by_id(character_id))
        return

    if action_type == 'set_region_value':
        try:
            new_region_id = int(text)
            update_character_setting(character_id, 'region_id', new_region_id)
            await update.message.reply_text(f"✅ Region ID updated to {new_region_id}.")
            load_characters_from_db() # Reload characters to reflect change
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a numeric Region ID. Try again or type `cancel`.")
            return # Keep waiting for valid input

    elif action_type == 'set_wallet_value':
        try:
            new_threshold = int(text.replace(',', '').replace('.', ''))
            update_character_setting(character_id, 'wallet_balance_threshold', new_threshold)
            await update.message.reply_text(f"✅ Wallet balance alert threshold updated to {new_threshold:,.0f} ISK.")
            load_characters_from_db()
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a valid number. Try again or type `cancel`.")
            return # Keep waiting

    # Clear the state and show the settings menu again
    context.user_data.clear()
    await _show_character_settings(update, context, get_character_by_id(character_id))

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

    # Schedule the job to check for new characters
    application.job_queue.run_repeating(check_for_new_characters_job, interval=10, first=10)
    logging.info("Scheduled job to check for new characters every 10 seconds.")


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
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    # Start the bot
    application.run_polling()


async def open_orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays a menu to choose between open buy or sell orders."""
    keyboard = [
        [
            InlineKeyboardButton("📈 Open Sale Orders", callback_data="open_orders_sales"),
            InlineKeyboardButton("🛒 Open Buy Orders", callback_data="open_orders_buys")
        ],
        [InlineKeyboardButton("« Back", callback_data="start_command")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(
        text="Please select which open orders you would like to view:",
        reply_markup=reply_markup
    )


async def _display_open_orders(update: Update, context: ContextTypes.DEFAULT_TYPE, character_id: int, is_buy: bool, page: int = 0):
    """Fetches and displays a paginated list of open orders."""
    query = update.callback_query
    character = get_character_by_id(character_id)
    if not character:
        await query.edit_message_text(text="Error: Could not find character.")
        return

    # Let the user know we're working on it
    await query.edit_message_text(text=f"⏳ Fetching open orders for {character.name}...")

    # --- ESI Calls ---
    # Use asyncio.gather to run skill and order fetches concurrently
    results = await asyncio.gather(
        asyncio.to_thread(get_market_orders, character, force_revalidate=True),
        asyncio.to_thread(get_character_skills, character)
    )
    all_orders, skills_data = results
    if all_orders is None:
        await query.edit_message_text(text=f"❌ Could not fetch market orders for {character.name}. The ESI API might be unavailable.")
        return

    # --- Order Capacity Calculation (must happen before filtering) ---
    order_capacity_str = ""
    if skills_data and 'skills' in skills_data:
        skill_map = {s['skill_id']: s['active_skill_level'] for s in skills_data['skills']}
        # Skill IDs: Trade (3443), Broker Relations (3446)
        trade_level = skill_map.get(3443, 0)
        broker_relations_level = skill_map.get(3446, 0)

        # Formula for max orders based on EVE University Wiki:
        # 5 (base) + (10 * Trade level) + (4 * Broker Relations level)
        max_orders = 5 + (trade_level * 10) + (broker_relations_level * 4)
        # Use len(all_orders) for the current count, not the filtered count
        order_capacity_str = f"({len(all_orders)} / {max_orders} orders)"

    # Filter for buy or sell orders
    filtered_orders = [order for order in all_orders if bool(order.get('is_buy_order')) == is_buy]
    order_type_str = "Buy" if is_buy else "Sale"

    # --- Message Formatting ---
    header = f"📄 *Open {order_type_str} Orders for {character.name}* {order_capacity_str}\n\n"

    if not filtered_orders:
        # Provide a back button
        keyboard = [[InlineKeyboardButton("« Back", callback_data="open_orders")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        # Combine header and the "no orders" message
        message = header + f"✅ No open {order_type_str.lower()} orders found."
        await query.edit_message_text(
            text=message,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return

    # Sort orders by issued date, newest first
    filtered_orders.sort(key=lambda x: datetime.fromisoformat(x['issued'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 10
    total_items = len(filtered_orders)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1)) # clamp page number
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_orders = filtered_orders[start_index:end_index]

    # --- Name Resolution ---
    type_ids = [order['type_id'] for order in paginated_orders]
    location_ids = [order['location_id'] for order in paginated_orders]
    id_to_name = get_names_from_ids(list(set(type_ids + location_ids)), character)
    message_lines = []
    for order in paginated_orders:
        item_name = id_to_name.get(order['type_id'], f"Type ID {order['type_id']}")
        location_name = id_to_name.get(order['location_id'], f"Location ID {order['location_id']}")
        remaining_vol = order['volume_remain']
        total_vol = order['volume_total']
        price = order['price']

        line = (
            f"*{item_name}*\n"
            f"  `{remaining_vol:,}` of `{total_vol:,}` @ `{price:,.2f}` ISK\n"
            f"  *Location:* `{location_name}`"
        )
        message_lines.append(line)

    # --- Keyboard ---
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"openorders_list_{character_id}_{str(is_buy).lower()}_{page - 1}"))

    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))

    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"openorders_list_{character_id}_{str(is_buy).lower()}_{page + 1}"))

    if nav_row:
        keyboard.append(nav_row)

    # Add a back button to the character selection or the open orders menu
    user_characters = get_characters_for_user(query.from_user.id)
    back_callback = "open_orders" if len(user_characters) > 1 else "start_command"
    keyboard.append([InlineKeyboardButton("« Back", callback_data=back_callback)])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # --- Send Message ---
    full_message = header + "\n\n".join(message_lines)
    await query.edit_message_text(text=full_message, parse_mode='Markdown', reply_markup=reply_markup)


async def _select_character_for_open_orders(update: Update, context: ContextTypes.DEFAULT_TYPE, is_buy: bool):
    """Asks the user to select a character to view their open orders."""
    user_id = update.effective_user.id
    user_characters = get_characters_for_user(user_id)
    chat_id = update.effective_chat.id
    message_id = update.effective_message.message_id
    order_type_str = "buy" if is_buy else "sales"

    if not user_characters:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="You have no characters added.")
        await start_command(update, context)
        return

    if len(user_characters) == 1:
        await _display_open_orders(update, context, character_id=user_characters[0].id, is_buy=is_buy, page=0)
    else:
        keyboard = [[InlineKeyboardButton(char.name, callback_data=f"openorders_list_{char.id}_{str(is_buy).lower()}_0")] for char in user_characters]
        keyboard.append([InlineKeyboardButton("« Back", callback_data="open_orders")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = f"Please select a character to view their open {order_type_str} orders:"
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=reply_markup)


async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """The single, main handler for all callback queries from inline keyboards."""
    query = update.callback_query
    await query.answer()

    # Simple router based on the callback data prefix
    data = query.data
    logging.info(f"Received callback query with data: {data}")

    # --- Main Menu Navigation ---
    if data == "start_command": await start_command(update, context)
    elif data == "balance": await balance_command(update, context)
    elif data == "open_orders": await open_orders_command(update, context)
    elif data == "summary": await summary_command(update, context)
    elif data == "sales": await sales_command(update, context)
    elif data == "buys": await buys_command(update, context)
    elif data == "notifications": await notifications_command(update, context)
    elif data == "settings": await settings_command(update, context)
    elif data == "add_character": await add_character_command(update, context)
    elif data == "remove": await remove_character_command(update, context)

    # --- Open Orders Flow ---
    elif data == "open_orders_sales":
        await _select_character_for_open_orders(update, context, is_buy=False)
    elif data == "open_orders_buys":
        await _select_character_for_open_orders(update, context, is_buy=True)
    elif data.startswith("openorders_list_"):
        _, _, char_id_str, is_buy_str, page_str = data.split('_')
        character_id = int(char_id_str)
        is_buy = is_buy_str == 'true'
        page = int(page_str)
        await _display_open_orders(update, context, character_id, is_buy, page)

    # --- Character Selection for Data Views ---
    elif data.startswith("balance_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        chars_to_query = get_characters_for_user(user_id) if char_id_str == "all" else [get_character_by_id(int(char_id_str))]
        await _show_balance_for_characters(update, context, chars_to_query)

    elif data.startswith("summary_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        chars_to_query = get_characters_for_user(user_id) if char_id_str == "all" else [get_character_by_id(int(char_id_str))]
        for char in chars_to_query:
            await _generate_and_send_summary(update, context, char)
            await asyncio.sleep(1)

    elif data.startswith("sales_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        chars_to_query = get_characters_for_user(user_id) if char_id_str == "all" else [get_character_by_id(int(char_id_str))]
        await _get_last_5_transactions(update, context, is_buy=False, characters=chars_to_query)

    elif data.startswith("buys_char_"):
        user_id = update.effective_user.id
        char_id_str = data.split('_')[-1]
        chars_to_query = get_characters_for_user(user_id) if char_id_str == "all" else [get_character_by_id(int(char_id_str))]
        await _get_last_5_transactions(update, context, is_buy=True, characters=chars_to_query)

    # --- Character Selection for Settings Menus ---
    elif data.startswith("notifications_char_"):
        char_id = int(data.split('_')[-1])
        await _show_notification_settings(update, context, get_character_by_id(char_id))

    elif data.startswith("settings_char_"):
        char_id = int(data.split('_')[-1])
        await _show_character_settings(update, context, get_character_by_id(char_id))

    # --- Toggling Notification Settings ---
    elif data.startswith("toggle_"):
        _, setting, char_id_str = data.split('_')
        char_id = int(char_id_str)
        character = get_character_by_id(char_id)
        current_value = getattr(character, f"enable_{setting}_notifications" if setting != 'summary' else "enable_daily_summary")
        update_character_notification_setting(char_id, setting, not current_value)
        load_characters_from_db() # Reload to get fresh data
        await _show_notification_settings(update, context, get_character_by_id(char_id)) # Refresh the menu

    # --- General Settings Value Input ---
    elif data.startswith("set_region_"):
        char_id = int(data.split('_')[-1])
        context.user_data['next_action'] = ('set_region_value', char_id)
        await query.message.reply_text("Please enter the new Region ID (e.g., 10000002 for The Forge).\n\nType `cancel` to go back.")

    elif data.startswith("set_wallet_"):
        char_id = int(data.split('_')[-1])
        context.user_data['next_action'] = ('set_wallet_value', char_id)
        await query.message.reply_text("Please enter the new wallet balance threshold (e.g., 100000000 for 100m ISK).\n\nType `cancel` to go back.")

    # --- Character Removal Flow ---
    elif data.startswith("remove_select_"):
        char_id = int(data.split('_')[-1])
        character = get_character_by_id(char_id)
        if not character:
            await query.edit_message_text(text="Error: Character not found.")
            return

        keyboard = [
            [InlineKeyboardButton("YES, REMOVE THIS CHARACTER", callback_data=f"remove_confirm_{character.id}")],
            [InlineKeyboardButton("NO, CANCEL", callback_data="start_command")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = (
            f"⚠️ *This is permanent and cannot be undone.* ⚠️\n\n"
            f"Are you sure you want to remove your character **{character.name}** and all their associated data?"
        )
        await query.edit_message_text(text=message_text, reply_markup=reply_markup, parse_mode='Markdown')

    elif data.startswith("remove_confirm_"):
        char_id = int(data.split('_')[-1])
        character = get_character_by_id(char_id)
        char_name = character.name if character else f"ID {char_id}"
        await query.edit_message_text(f"Removing character {char_name} and deleting all associated data...")
        delete_character(char_id)
        load_characters_from_db() # Refresh global list
        await query.edit_message_text(f"✅ Character {char_name} has been successfully removed.")
        await asyncio.sleep(2)
        await start_command(update, context) # Show main menu again

    # --- Charting Callbacks ---
    elif data.startswith("chart_"):
        await chart_callback_handler(update, context)
    elif data.startswith("summary_back_"):
        await back_to_summary_handler(update, context)

    elif data == "noop":
        return # Do nothing, it's just a label


if __name__ == "__main__":
    main()