import requests
import time
import logging
import os
import database
import json
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import psycopg2
from collections import defaultdict
import asyncio
import io
from PIL import Image
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import calendar

grace_period_hours = 1

# --- Character Dataclass and Global List ---

@dataclass
class Character:
    """Represents a single EVE Online character and their settings."""
    id: int
    name: str
    refresh_token: str
    telegram_user_id: int
    notifications_enabled: bool
    wallet_balance_threshold: int
    enable_sales_notifications: bool
    enable_buys_notifications: bool
    enable_daily_overview: bool
    enable_undercut_notifications: bool
    enable_contracts_notifications: bool
    notification_batch_threshold: int
    created_at: datetime
    is_backfilling: bool
    backfill_before_id: int | None
    buy_broker_fee: float
    sell_broker_fee: float
    wallet_balance: float | None
    wallet_balance_last_updated: datetime | None
    net_worth: float | None
    net_worth_last_updated: datetime | None

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
                    notifications_enabled, wallet_balance_threshold,
                    enable_sales_notifications, enable_buys_notifications,
                    enable_daily_overview, enable_undercut_notifications,
                    enable_contracts_notifications, notification_batch_threshold, created_at,
                    is_backfilling, backfill_before_id, buy_broker_fee, sell_broker_fee,
                    wallet_balance, wallet_balance_last_updated, net_worth, net_worth_last_updated
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
            wallet_balance_threshold,
            enable_sales, enable_buys,
            enable_overview, enable_undercut, enable_contracts, batch_threshold, created_at,
            is_backfilling, backfill_before_id, buy_broker_fee, sell_broker_fee,
            wallet_balance, wallet_balance_last_updated, net_worth, net_worth_last_updated
        ) = row

        if any(c.id == char_id for c in CHARACTERS):
            logging.warning(f"Character '{name}' ({char_id}) is already loaded. Skipping duplicate.")
            continue

        character = Character(
            id=char_id, name=name, refresh_token=refresh_token,
            telegram_user_id=telegram_user_id,
            notifications_enabled=bool(notifications_enabled),
            wallet_balance_threshold=wallet_balance_threshold,
            enable_sales_notifications=bool(enable_sales),
            enable_buys_notifications=bool(enable_buys),
            enable_daily_overview=bool(enable_overview),
            enable_undercut_notifications=bool(enable_undercut),
            enable_contracts_notifications=bool(enable_contracts),
            notification_batch_threshold=batch_threshold,
            created_at=created_at,
            is_backfilling=bool(is_backfilling),
            backfill_before_id=backfill_before_id,
            buy_broker_fee=float(buy_broker_fee),
            sell_broker_fee=float(sell_broker_fee),
            wallet_balance=float(wallet_balance) if wallet_balance is not None else None,
            wallet_balance_last_updated=wallet_balance_last_updated,
            net_worth=float(net_worth) if net_worth is not None else None,
            net_worth_last_updated=net_worth_last_updated
        )
        CHARACTERS.append(character)
        logging.debug(f"Loaded character: {character.name} ({character.id})")

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
                    wallet_balance_threshold BIGINT DEFAULT 0,
                    enable_sales_notifications BOOLEAN DEFAULT TRUE,
                    enable_buys_notifications BOOLEAN DEFAULT TRUE,
                    enable_daily_overview BOOLEAN DEFAULT TRUE,
                    enable_undercut_notifications BOOLEAN DEFAULT TRUE,
                    enable_contracts_notifications BOOLEAN DEFAULT TRUE,
                    notification_batch_threshold INTEGER DEFAULT 2,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('UTC', now()),
                    needs_update_notification BOOLEAN DEFAULT FALSE,
                    deletion_scheduled_at TIMESTAMP WITH TIME ZONE,
                    buy_broker_fee NUMERIC(5, 2) DEFAULT 3.0,
                    sell_broker_fee NUMERIC(5, 2) DEFAULT 3.0,
                    is_backfilling BOOLEAN DEFAULT FALSE,
                    backfill_before_id BIGINT,
                    wallet_balance NUMERIC(17, 2),
                    wallet_balance_last_updated TIMESTAMP WITH TIME ZONE,
                    net_worth NUMERIC(17, 2),
                    net_worth_last_updated TIMESTAMP WITH TIME ZONE
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_orders (
                    order_id BIGINT NOT NULL,
                    character_id INTEGER NOT NULL,
                    volume_remain INTEGER NOT NULL,
                    price NUMERIC(17, 2) NOT NULL,
                    order_data JSONB,
                    PRIMARY KEY (order_id, character_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trading_fees (
                    fee_id SERIAL PRIMARY KEY,
                    character_id INTEGER NOT NULL,
                    fee_amount NUMERIC(17, 2) NOT NULL,
                    reason TEXT NOT NULL,
                    date TIMESTAMP WITH TIME ZONE NOT NULL
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trading_fees_char_date ON trading_fees (character_id, date DESC);")
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
            CREATE TABLE IF NOT EXISTS historical_journal (
                ref_id BIGINT NOT NULL,
                character_id INTEGER NOT NULL,
                PRIMARY KEY (ref_id, character_id)
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS wallet_journal (
                id BIGINT NOT NULL,
                character_id INTEGER NOT NULL,
                amount DOUBLE PRECISION,
                balance DOUBLE PRECISION,
                context_id BIGINT,
                context_id_type TEXT,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                description TEXT NOT NULL,
                first_party_id INTEGER,
                reason TEXT,
                ref_type TEXT NOT NULL,
                second_party_id INTEGER,
                tax DOUBLE PRECISION,
                tax_receiver_id INTEGER,
                PRIMARY KEY (id, character_id)
            )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_journal_char_date ON wallet_journal (character_id, date DESC);")
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
                CREATE TABLE IF NOT EXISTS image_cache (
                    url TEXT PRIMARY KEY,
                    etag TEXT,
                    data BYTEA NOT NULL,
                    last_fetched TIMESTAMP WITH TIME ZONE NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chart_cache (
                    chart_key TEXT PRIMARY KEY,
                    character_id INTEGER NOT NULL,
                    chart_data BYTEA NOT NULL,
                    generated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    caption_suffix TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS undercut_statuses (
                    order_id BIGINT NOT NULL,
                    character_id INTEGER NOT NULL,
                    is_undercut BOOLEAN NOT NULL,
                    competitor_price NUMERIC(17, 2),
                    competitor_location_id BIGINT,
                    PRIMARY KEY (order_id, character_id)
                )
            """)
            # Add competitor_volume column if it doesn't exist for backward compatibility
            cursor.execute("""
                ALTER TABLE undercut_statuses
                ADD COLUMN IF NOT EXISTS competitor_volume INTEGER;
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS contracts (
                    contract_id INTEGER NOT NULL,
                    character_id INTEGER NOT NULL,
                    issuer_id INTEGER NOT NULL,
                    assignee_id INTEGER,
                    start_location_id BIGINT,
                    end_location_id BIGINT,
                    type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    date_issued TIMESTAMP WITH TIME ZONE NOT NULL,
                    date_expired TIMESTAMP WITH TIME ZONE NOT NULL,
                    for_corporation BOOLEAN NOT NULL,
                    contract_data JSONB,
                    PRIMARY KEY (contract_id, character_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_contracts (
                    contract_id INTEGER NOT NULL,
                    character_id INTEGER NOT NULL,
                    PRIMARY KEY (contract_id, character_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS location_cache (
                    location_id BIGINT PRIMARY KEY,
                    system_id INTEGER,
                    region_id INTEGER
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jump_distances (
                    origin_system_id INTEGER NOT NULL,
                    destination_system_id INTEGER NOT NULL,
                    jumps INTEGER NOT NULL,
                    PRIMARY KEY (origin_system_id, destination_system_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS access_tokens (
                    character_id INTEGER PRIMARY KEY,
                    access_token TEXT NOT NULL,
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bot_stats (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    status_code INTEGER NOT NULL
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


def add_processed_journal_refs(character_id, ref_ids):
    """Adds a list of journal ref IDs for a character to the database."""
    if not ref_ids:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = [(ref_id, character_id) for ref_id in ref_ids]
            cursor.executemany(
                "INSERT INTO historical_journal (ref_id, character_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                data_to_insert
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


def get_processed_contracts(character_id):
    """Retrieves all processed contract IDs for a character from the database."""
    conn = database.get_db_connection()
    processed_ids = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT contract_id FROM processed_contracts WHERE character_id = %s", (character_id,))
            processed_ids = {row[0] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)
    return processed_ids

def add_processed_contracts(character_id, contract_ids):
    """Adds a list of contract IDs for a character to the database."""
    if not contract_ids:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = [(c_id, character_id) for c_id in contract_ids]
            cursor.executemany(
                "INSERT INTO processed_contracts (contract_id, character_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                data_to_insert
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)

def update_contracts_cache(character_id, contracts):
    """Inserts or updates a list of contracts for a character in the database."""
    if not contracts:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            contracts_with_char_id = [
                (
                    c['contract_id'],
                    character_id,
                    c['issuer_id'],
                    c.get('assignee_id'),
                    c.get('start_location_id'),
                    c.get('end_location_id'),
                    c['type'],
                    c['status'],
                    c['date_issued'],
                    c['date_expired'],
                    c['for_corporation'],
                    json.dumps(c)
                ) for c in contracts
            ]
            upsert_query = """
                INSERT INTO contracts (
                    contract_id, character_id, issuer_id, assignee_id, start_location_id,
                    end_location_id, type, status, date_issued, date_expired,
                    for_corporation, contract_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (contract_id, character_id) DO UPDATE
                SET issuer_id = EXCLUDED.issuer_id,
                    assignee_id = EXCLUDED.assignee_id,
                    start_location_id = EXCLUDED.start_location_id,
                    end_location_id = EXCLUDED.end_location_id,
                    type = EXCLUDED.type,
                    status = EXCLUDED.status,
                    date_issued = EXCLUDED.date_issued,
                    date_expired = EXCLUDED.date_expired,
                    for_corporation = EXCLUDED.for_corporation,
                    contract_data = EXCLUDED.contract_data;
            """
            cursor.executemany(upsert_query, contracts_with_char_id)
            conn.commit()
    finally:
        database.release_db_connection(conn)

def remove_stale_contracts(character_id, current_contract_ids):
    """Removes contracts that are no longer active from the cache."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            if not current_contract_ids:
                cursor.execute("DELETE FROM contracts WHERE character_id = %s", (character_id,))
            else:
                placeholders = ','.join(['%s'] * len(current_contract_ids))
                query = f"DELETE FROM contracts WHERE character_id = %s AND contract_id NOT IN ({placeholders})"
                cursor.execute(query, [character_id] + current_contract_ids)
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
    """Retrieves all tracked market orders for a specific character from the cache."""
    conn = database.get_db_connection()
    orders = []
    try:
        with conn.cursor() as cursor:
            # The order_data column is of type JSONB, so psycopg2 will automatically parse it into a dict.
            cursor.execute("SELECT order_data FROM market_orders WHERE character_id = %s", (character_id,))
            orders = [row[0] for row in cursor.fetchall()]
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
            # The `orders` variable is a list of full order dictionaries from the ESI.
            # We also need to convert the full order object to a JSON string for storing in the JSONB column.
            orders_with_char_id = [
                (
                    o['order_id'],
                    character_id,
                    o['volume_remain'],
                    o['price'],
                    json.dumps(o)  # Serialize the whole order dict to a JSON string
                ) for o in orders
            ]
            upsert_query = """
                INSERT INTO market_orders (order_id, character_id, volume_remain, price, order_data)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id, character_id) DO UPDATE
                SET volume_remain = EXCLUDED.volume_remain,
                    price = EXCLUDED.price,
                    order_data = EXCLUDED.order_data;
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
                    notifications_enabled, wallet_balance_threshold,
                    enable_sales_notifications, enable_buys_notifications,
                    enable_daily_overview, enable_undercut_notifications,
                    enable_contracts_notifications, notification_batch_threshold, created_at,
                    is_backfilling, backfill_before_id, buy_broker_fee, sell_broker_fee,
                    wallet_balance, wallet_balance_last_updated, net_worth, net_worth_last_updated
                FROM characters WHERE telegram_user_id = %s AND deletion_scheduled_at IS NULL
            """, (telegram_user_id,))
            rows = cursor.fetchall()
            for row in rows:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    wallet_balance_threshold,
                    enable_sales, enable_buys,
                    enable_overview, enable_undercut, enable_contracts, batch_threshold, created_at,
                    is_backfilling, backfill_before_id, buy_broker_fee, sell_broker_fee,
                    wallet_balance, wallet_balance_last_updated, net_worth, net_worth_last_updated
                ) = row

                user_characters.append(Character(
                    id=char_id, name=name, refresh_token=refresh_token,
                    telegram_user_id=telegram_user_id,
                    notifications_enabled=bool(notifications_enabled),
                    wallet_balance_threshold=wallet_balance_threshold,
                    enable_sales_notifications=bool(enable_sales),
                    enable_buys_notifications=bool(enable_buys),
                    enable_daily_overview=bool(enable_overview),
                    enable_undercut_notifications=bool(enable_undercut),
                    enable_contracts_notifications=bool(enable_contracts),
                    notification_batch_threshold=batch_threshold,
                    created_at=created_at,
                    is_backfilling=bool(is_backfilling),
                    backfill_before_id=backfill_before_id,
                    buy_broker_fee=float(buy_broker_fee),
                    sell_broker_fee=float(sell_broker_fee),
                    wallet_balance=float(wallet_balance) if wallet_balance is not None else None,
                    wallet_balance_last_updated=wallet_balance_last_updated,
                    net_worth=float(net_worth) if net_worth is not None else None,
                    net_worth_last_updated=net_worth_last_updated
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
                    notifications_enabled, wallet_balance_threshold,
                    enable_sales_notifications, enable_buys_notifications,
                    enable_daily_overview, enable_undercut_notifications,
                    enable_contracts_notifications, notification_batch_threshold, created_at,
                    is_backfilling, backfill_before_id, buy_broker_fee, sell_broker_fee,
                    wallet_balance, wallet_balance_last_updated, net_worth, net_worth_last_updated
                FROM characters WHERE character_id = %s
            """, (character_id,))
            row = cursor.fetchone()

            if row:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    wallet_balance_threshold,
                    enable_sales, enable_buys,
                    enable_overview, enable_undercut, enable_contracts, batch_threshold, created_at,
                    is_backfilling, backfill_before_id, buy_broker_fee, sell_broker_fee,
                    wallet_balance, wallet_balance_last_updated, net_worth, net_worth_last_updated
                ) = row

                character = Character(
                    id=char_id, name=name, refresh_token=refresh_token,
                    telegram_user_id=telegram_user_id,
                    notifications_enabled=bool(notifications_enabled),
                    wallet_balance_threshold=wallet_balance_threshold,
                    enable_sales_notifications=bool(enable_sales),
                    enable_buys_notifications=bool(enable_buys),
                    enable_daily_overview=bool(enable_overview),
                    enable_undercut_notifications=bool(enable_undercut),
                    enable_contracts_notifications=bool(enable_contracts),
                    notification_batch_threshold=batch_threshold,
                    created_at=created_at,
                    is_backfilling=bool(is_backfilling),
                    backfill_before_id=backfill_before_id,
                    buy_broker_fee=float(buy_broker_fee),
                    sell_broker_fee=float(sell_broker_fee),
                    wallet_balance=float(wallet_balance) if wallet_balance is not None else None,
                    wallet_balance_last_updated=wallet_balance_last_updated,
                    net_worth=float(net_worth) if net_worth is not None else None,
                    net_worth_last_updated=net_worth_last_updated
                )
    finally:
        database.release_db_connection(conn)
    return character


def update_character_setting(character_id: int, setting: str, value: any):
    """Updates a specific setting for a character in the database."""
    allowed_settings = [
        "wallet_balance_threshold"
    ]
    if setting not in allowed_settings:
        logging.error(f"Attempted to update an invalid setting: {setting}")
        return

    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Use a parameterized query for the column name to prevent SQL injection,
            # even though we've already validated the setting against a safelist.
            # psycopg2 doesn't support parameterizing column names directly, so we format it in.
            # This is safe due to the allowlist check above.
            query = f"UPDATE characters SET {setting} = %s WHERE character_id = %s"
            cursor.execute(query, (value, character_id))
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.info(f"Updated {setting} for character {character_id} to {value}.")


def update_character_wallet_balance(character_id: int, balance: float, last_updated: datetime):
    """Updates the wallet balance and timestamp for a character in the database."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE characters SET wallet_balance = %s, wallet_balance_last_updated = %s WHERE character_id = %s",
                (balance, last_updated, character_id)
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.info(f"Updated wallet balance for character {character_id} to {balance:,.2f} ISK.")


def update_character_net_worth(character_id: int, net_worth: float, last_updated: datetime):
    """Updates the net worth and timestamp for a character in the database."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE characters SET net_worth = %s, net_worth_last_updated = %s WHERE character_id = %s",
                (net_worth, last_updated, character_id)
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.info(f"Updated net worth for character {character_id} to {net_worth:,.2f} ISK.")


def update_character_fee_setting(character_id: int, fee_type: str, value: float):
    """Updates a broker fee setting for a character in the database."""
    allowed_fee_types = {
        "buy": "buy_broker_fee",
        "sell": "sell_broker_fee"
    }
    if fee_type not in allowed_fee_types:
        logging.error(f"Attempted to update an invalid fee type: {fee_type}")
        return

    column_name = allowed_fee_types[fee_type]
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            query = f"UPDATE characters SET {column_name} = %s WHERE character_id = %s"
            cursor.execute(query, (value, character_id))
            conn.commit()
    finally:
        database.release_db_connection(conn)
    logging.info(f"Updated {column_name} for character {character_id} to {value}%.")


def update_character_backfill_state(character_id: int, is_backfilling: bool, before_id: int | None):
    """Updates the backfill state for a character in the database."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE characters SET is_backfilling = %s, backfill_before_id = %s WHERE character_id = %s",
                (is_backfilling, before_id, character_id)
            )
            conn.commit()
        logging.info(f"Updated backfill state for character {character_id}: is_backfilling={is_backfilling}, before_id={before_id}")
    except Exception as e:
        logging.error(f"Error updating backfill state for character {character_id}: {e}", exc_info=True)
        conn.rollback()
    finally:
        database.release_db_connection(conn)


def update_character_notification_setting(character_id: int, setting: str, value: bool):
    """Updates a specific notification setting for a character in the database."""
    allowed_settings = {
        "sales": "enable_sales_notifications",
        "buys": "enable_buys_notifications",
        "overview": "enable_daily_overview",
        "undercut": "enable_undercut_notifications",
        "contracts": "enable_contracts_notifications"
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


def reset_update_notification_flag(character_id: int):
    """Resets the needs_update_notification flag for a character to FALSE."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE characters SET needs_update_notification = FALSE WHERE character_id = %s",
                (character_id,)
            )
            conn.commit()
        logging.info(f"Reset update notification flag for character {character_id}.")
    finally:
        database.release_db_connection(conn)


def schedule_character_deletion(character_id: int):
    """Schedules a character for deletion by setting the deletion_scheduled_at timestamp."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            deletion_time = datetime.now(timezone.utc) + timedelta(hours=1)
            cursor.execute(
                "UPDATE characters SET deletion_scheduled_at = %s WHERE character_id = %s",
                (deletion_time, character_id)
            )
            conn.commit()
        logging.info(f"Scheduled character {character_id} for deletion at {deletion_time}.")
    except Exception as e:
        logging.error(f"Error scheduling character deletion for {character_id}: {e}", exc_info=True)
        conn.rollback()
    finally:
        database.release_db_connection(conn)


def cancel_character_deletion(character_id: int):
    """Cancels a scheduled character deletion by setting deletion_scheduled_at to NULL."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE characters SET deletion_scheduled_at = NULL WHERE character_id = %s",
                (character_id,)
            )
            conn.commit()
        logging.info(f"Cancelled scheduled deletion for character {character_id}.")
    except Exception as e:
        logging.error(f"Error cancelling character deletion for {character_id}: {e}", exc_info=True)
        conn.rollback()
    finally:
        database.release_db_connection(conn)


def get_character_deletion_status(character_id: int):
    """Checks if a character is scheduled for deletion and returns the timestamp if so."""
    conn = database.get_db_connection()
    deletion_time = None
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT deletion_scheduled_at FROM characters WHERE character_id = %s",
                (character_id,)
            )
            result = cursor.fetchone()
            if result:
                deletion_time = result[0]
    finally:
        database.release_db_connection(conn)
    return deletion_time


def get_contract_profits_from_db(character_id: int) -> list:
    """Retrieves all contract profits for a character."""
    conn = database.get_db_connection()
    profits = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT contract_id, profit, date FROM contract_profits WHERE character_id = %s", (character_id,))
            rows = cursor.fetchall()
            for row in rows:
                profits.append({
                    "contract_id": row[0],
                    "profit": float(row[1]), # Convert Decimal to float
                    "date": row[2]
                })
    finally:
        database.release_db_connection(conn)
    return profits

def add_contract_profit(character_id, contract_id, profit, date):
    """Adds a calculated contract profit to the database."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO contract_profits (character_id, contract_id, profit, date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (contract_id, character_id) DO NOTHING
                """,
                (character_id, contract_id, profit, date)
            )
            conn.commit()
            logging.info(f"Stored profit of {profit:,.2f} for contract {contract_id} for character {character_id}.")
    finally:
        database.release_db_connection(conn)

def get_journal_entry_by_context_id(character_id: int, context_id: int, ref_type: str):
    """Retrieves a specific journal entry by context ID and ref_type from the database."""
    conn = database.get_db_connection()
    entry = None
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, amount, balance, context_id, context_id_type, date, description, first_party_id, reason, ref_type, second_party_id, tax, tax_receiver_id FROM wallet_journal WHERE character_id = %s AND context_id = %s AND ref_type = %s",
                (character_id, context_id, ref_type)
            )
            row = cursor.fetchone()
            if row:
                colnames = [desc[0] for desc in cursor.description]
                entry = dict(zip(colnames, row))
                if isinstance(entry['date'], str):
                    entry['date'] = datetime.fromisoformat(entry['date'].replace('Z', '+00:00'))
    finally:
        database.release_db_connection(conn)
    return entry


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


def get_full_wallet_journal_from_db(character_id: int):
    """
    Retrieves all wallet journal entries for a given character from the local database,
    parsing the date into a datetime object for easier processing.
    Note: This table is not currently populated by any polling mechanism.
    It would need to be filled by a backfill and continuous polling of the
    /characters/{character_id}/wallet/journal ESI endpoint.
    """
    conn = database.get_db_connection()
    processed_entries = []
    try:
        with conn.cursor() as cursor:
            # This assumes a 'wallet_journal' table exists with a similar structure to the ESI response.
            # The current schema in setup_database() does not create this table.
            # This function is added based on the assumption it will be used in a future implementation.
            # If the table doesn't exist, this will raise a psycopg2.errors.UndefinedTable.
            cursor.execute(
                'SELECT * FROM wallet_journal WHERE character_id = %s ORDER BY date ASC',
                (character_id,)
            )
            journal_entries = cursor.fetchall()
            # Fetch column names from cursor.description
            colnames = [desc[0] for desc in cursor.description]
            for row in journal_entries:
                entry = dict(zip(colnames, row))
                # The 'date' column is already a datetime object from the DB.
                # If for some reason it's a string, we parse it.
                if isinstance(entry['date'], str):
                    entry['date'] = datetime.fromisoformat(entry['date'].replace('Z', '+00:00'))
                processed_entries.append(entry)
    except psycopg2.errors.UndefinedTable:
        logging.warning("Attempted to query 'wallet_journal' table, but it does not exist. Returning empty list.")
        # In a real scenario, you might want to handle this more gracefully.
        # For now, we return an empty list as if there were no entries.
        return []
    finally:
        database.release_db_connection(conn)
    return processed_entries

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
                "trading_fees",
                "historical_journal",
                "wallet_journal",
                "chart_cache"
            ]
            for table in tables_to_delete_from:
                cursor.execute(f"DELETE FROM {table} WHERE character_id = %s", (character_id,))
                logging.info(f"Deleted records from {table} for character {character_id}.")

            # Clean up ESI cache entries
            esi_cache_pattern = f"%:{character_id}:%"
            cursor.execute("DELETE FROM esi_cache WHERE cache_key LIKE %s", (esi_cache_pattern,))
            logging.info(f"Deleted esi_cache entries for character {character_id}.")

            # Clean up bot_state entries for this character
            keys_to_delete = [
                f"history_backfilled_{character_id}",
                f"low_balance_alert_sent_at_{character_id}",
                f"chart_cache_dirty_{character_id}"
            ]
            cursor.execute("DELETE FROM bot_state WHERE key = ANY(%s)", (keys_to_delete,))
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


def get_cached_chart(chart_key: str):
    """Retrieves a cached chart and its caption suffix from the database."""
    conn = database.get_db_connection()
    cached_data = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT chart_data, caption_suffix FROM chart_cache WHERE chart_key = %s", (chart_key,))
            row = cursor.fetchone()
            if row:
                cached_data = {'chart_data': row[0], 'caption_suffix': row[1]}
    finally:
        database.release_db_connection(conn)
    return cached_data


def save_chart_to_cache(chart_key: str, character_id: int, chart_data: bytes, caption_suffix: str = None):
    """Saves or updates a chart and its caption suffix in the database cache."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            binary_data = psycopg2.Binary(chart_data)
            cursor.execute(
                """
                INSERT INTO chart_cache (chart_key, character_id, chart_data, generated_at, caption_suffix)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (chart_key) DO UPDATE SET
                    chart_data = EXCLUDED.chart_data,
                    generated_at = EXCLUDED.generated_at,
                    caption_suffix = EXCLUDED.caption_suffix;
                """,
                (chart_key, character_id, binary_data, datetime.now(timezone.utc), caption_suffix)
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


def get_undercut_statuses(character_id: int) -> dict[int, dict]:
    """
    Retrieves the last known undercut status and competitor info for all of a character's orders.
    Returns a dict mapping order_id to {'is_undercut': bool, 'competitor_price': float|None, 'competitor_location_id': int|None, 'competitor_volume': int|None}.
    """
    conn = database.get_db_connection()
    statuses = {}
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT order_id, is_undercut, competitor_price, competitor_location_id, competitor_volume FROM undercut_statuses WHERE character_id = %s",
                (character_id,)
            )
            for row in cursor.fetchall():
                order_id, is_undercut, competitor_price, competitor_location_id, competitor_volume = row
                statuses[order_id] = {
                    'is_undercut': is_undercut,
                    'competitor_price': float(competitor_price) if competitor_price is not None else None,
                    'competitor_location_id': competitor_location_id,
                    'competitor_volume': competitor_volume
                }
    finally:
        database.release_db_connection(conn)
    return statuses

def update_undercut_statuses(character_id: int, statuses: list[dict]):
    """Inserts or updates the undercut status and competitor info for a list of orders."""
    if not statuses:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            # `statuses` is a list of dicts: [{'order_id': X, 'is_undercut': Y, 'competitor_price': Z, 'competitor_location_id': A, 'competitor_volume': B}, ...]
            data_to_insert = [
                (s['order_id'], character_id, s['is_undercut'], s.get('competitor_price'), s.get('competitor_location_id'), s.get('competitor_volume')) for s in statuses
            ]
            upsert_query = """
                INSERT INTO undercut_statuses (order_id, character_id, is_undercut, competitor_price, competitor_location_id, competitor_volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id, character_id) DO UPDATE
                SET is_undercut = EXCLUDED.is_undercut,
                    competitor_price = EXCLUDED.competitor_price,
                    competitor_location_id = EXCLUDED.competitor_location_id,
                    competitor_volume = EXCLUDED.competitor_volume;
            """
            cursor.executemany(upsert_query, data_to_insert)
            conn.commit()
    finally:
        database.release_db_connection(conn)

def remove_stale_undercut_statuses(character_id: int, current_order_ids: list[int]):
    """Removes statuses for orders that are no longer open."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            if not current_order_ids:
                # If there are no current orders, remove all statuses for that character
                cursor.execute("DELETE FROM undercut_statuses WHERE character_id = %s", (character_id,))
            else:
                # Create a string of placeholders for the query
                placeholders = ','.join(['%s'] * len(current_order_ids))
                query = f"DELETE FROM undercut_statuses WHERE character_id = %s AND order_id NOT IN ({placeholders})"
                # Note: The character_id must be passed as a tuple/list element
                cursor.execute(query, [character_id] + current_order_ids)
            conn.commit()
    finally:
        database.release_db_connection(conn)


def get_location_from_cache(location_id: int) -> dict | None:
    """Retrieves a location's region and system from the local cache."""
    conn = database.get_db_connection()
    location_info = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT system_id, region_id FROM location_cache WHERE location_id = %s", (location_id,))
            row = cursor.fetchone()
            if row:
                location_info = {'system_id': row[0], 'region_id': row[1]}
    finally:
        database.release_db_connection(conn)
    return location_info

def save_location_to_cache(location_id: int, system_id: int, region_id: int):
    """Saves a location's resolved system and region to the cache."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO location_cache (location_id, system_id, region_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (location_id, system_id, region_id)
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


def get_jump_distance_from_db(origin_system_id: int, destination_system_id: int) -> int | None:
    """Retrieves a cached jump distance from the database."""
    conn = database.get_db_connection()
    jumps = None
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT jumps FROM jump_distances WHERE origin_system_id = %s AND destination_system_id = %s",
                (origin_system_id, destination_system_id)
            )
            row = cursor.fetchone()
            if row:
                jumps = row[0]
    finally:
        database.release_db_connection(conn)
    return jumps


def save_jump_distance_to_db(origin_system_id: int, destination_system_id: int, jumps: int):
    """Saves a jump distance to the database cache."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Insert for both directions to optimize lookups
            cursor.execute(
                """
                INSERT INTO jump_distances (origin_system_id, destination_system_id, jumps)
                VALUES (%s, %s, %s), (%s, %s, %s)
                ON CONFLICT (origin_system_id, destination_system_id) DO NOTHING
                """,
                (origin_system_id, destination_system_id, jumps, destination_system_id, origin_system_id, jumps)
            )
            conn.commit()
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


def get_last_known_wallet_balance(character: Character) -> float | None:
    """
    Retrieves the most recent wallet balance for a character from the local DB cache.
    This function does NOT make an ESI call.
    """
    if not character:
        return None
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/"
    # Construct the cache key exactly as make_esi_request would for this endpoint
    cache_key = f"{url}:{character.id}:None:"
    cached_response = get_esi_cache_from_db(cache_key)
    if cached_response and 'data' in cached_response:
        # The balance is stored directly as the JSON response
        return float(cached_response['data'])
    logging.warning(f"No cached wallet balance found for character {character.name} ({character.id}).")
    return None


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
        access_token = get_access_token(character.id, character.refresh_token)
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

        log_esi_request(response.status_code)

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


# --- ESI Access Token Caching ---
def get_token_from_db(character_id: int):
    """Retrieves a token from the database for a given character ID."""
    conn = database.get_db_connection()
    token_info = None
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT access_token, expires_at FROM access_tokens WHERE character_id = %s",
                (character_id,)
            )
            row = cursor.fetchone()
            if row:
                token_info = {'access_token': row[0], 'expires_at': row[1]}
    finally:
        database.release_db_connection(conn)
    return token_info

def save_token_to_db(character_id: int, access_token: str, expires_at: datetime):
    """Saves or updates a token in the database for a given character ID."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO access_tokens (character_id, access_token, expires_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (character_id) DO UPDATE SET
                    access_token = EXCLUDED.access_token,
                    expires_at = EXCLUDED.expires_at;
                """,
                (character_id, access_token, expires_at)
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


def get_access_token(character_id, refresh_token):
    """
    Retrieves a valid access token for a character, using a database cache
    to avoid redundant requests and support multiple workers.
    """
    # 1. Check DB for a valid token
    token_info = get_token_from_db(character_id)
    if token_info and token_info['expires_at'] > datetime.now(timezone.utc) + timedelta(seconds=60):
        logging.debug(f"Returning DB-cached access token for character {character_id}")
        return token_info['access_token']

    # 2. If no valid token, request a new one
    logging.info(f"No valid cached token in DB for character {character_id}. Requesting a new one.")
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
        token_data = response.json()
        access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 1200)  # Default to 20 minutes

        # 3. Save the new token to DB
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        save_token_to_db(character_id, access_token, expires_at)

        logging.info(f"Successfully obtained and cached new access token in DB for character {character_id}")
        return access_token
    except requests.exceptions.RequestException as e:
        logging.error(f"Error refreshing access token for character {character_id}: {e}")
        # As a fallback, try to use the (likely expired) token from the DB if one exists.
        # This might allow some requests to succeed if the token is only just expired.
        if token_info:
            logging.warning(f"Returning stale access token for character {character_id} due to refresh failure.")
            return token_info['access_token']
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

def get_wallet_journal(character, fetch_all=False, return_headers=False):
    """
    Fetches wallet journal from ESI.
    If fetch_all is True, retrieves all pages. Otherwise, fetches only the first page.
    Returns the list of journal entries, or None on failure. Optionally returns headers.
    """
    if not character:
        return (None, None) if return_headers else None

    all_entries, page = [], 1
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

        all_entries.extend(data)

        if not fetch_all:
            break

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1)

    if return_headers:
        return all_entries, first_page_headers
    return all_entries


def get_wallet_transactions(character, before_id=None, return_headers=False):
    """
    Fetches one batch of wallet transactions from ESI.
    If before_id is provided, it acts as a cursor to fetch transactions older than that ID.
    Returns the list of transactions, or None on failure. Optionally returns headers.
    """
    if not character:
        return (None, None) if return_headers else None

    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/transactions/"
    params = {"datasource": "tranquility"}
    if before_id:
        # The ESI parameter is `from_id`, which fetches transactions *before* the given ID.
        params['from_id'] = before_id

    # We always force revalidation for this endpoint to ensure we get the latest data
    # for live notifications and for the backfill process to work correctly.
    data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=True)

    if data is None: # Explicitly check for API failure
        logging.error(f"Failed to fetch wallet transactions for {character.name} with from_id={before_id}.")
        return (None, None) if return_headers else None

    if return_headers:
        return data, headers
    return data

def get_market_orders(character, return_headers=False, force_revalidate=False):
    if not character: return None
    url = f"https://esi.evetech.net/v2/characters/{character.id}/orders/"
    return make_esi_request(url, character=character, return_headers=return_headers, force_revalidate=force_revalidate)

def get_character_assets(character, return_headers=False, force_revalidate=False):
    """Fetches all pages of assets from ESI."""
    if not character:
        return (None, None) if return_headers else None

    all_assets = []
    page = 1
    url = f"https://esi.evetech.net/v5/characters/{character.id}/assets/"
    first_page_headers = None

    while True:
        params = {"datasource": "tranquility", "page": page}
        revalidate_this_page = force_revalidate and page == 1
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=revalidate_this_page)

        if data is None:
            logging.error(f"Failed to fetch page {page} of assets for {character.name}.")
            return (None, None) if return_headers else None

        if page == 1:
            first_page_headers = headers

        if not data:
            break

        all_assets.extend(data)

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1)

    if return_headers:
        return all_assets, first_page_headers
    return all_assets


def get_character_blueprints(character, return_headers=False, force_revalidate=False):
    """Fetches all pages of blueprints from ESI."""
    if not character:
        return (None, None) if return_headers else None

    all_blueprints = []
    page = 1
    url = f"https://esi.evetech.net/v3/characters/{character.id}/blueprints/"
    first_page_headers = None

    while True:
        params = {"datasource": "tranquility", "page": page}
        revalidate_this_page = force_revalidate and page == 1
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=revalidate_this_page)

        if data is None:
            logging.error(f"Failed to fetch page {page} of blueprints for {character.name}.")
            return (None, None) if return_headers else None

        if page == 1:
            first_page_headers = headers

        if not data:
            break

        all_blueprints.extend(data)

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1)

    if return_headers:
        return all_blueprints, first_page_headers
    return all_blueprints


def get_character_ship(character, return_headers=False, force_revalidate=False):
    """Fetches the character's current ship from ESI."""
    if not character: return None
    url = f"https://esi.evetech.net/v2/characters/{character.id}/ship/"
    return make_esi_request(url, character=character, return_headers=return_headers, force_revalidate=force_revalidate)


def get_market_prices(return_headers=False, force_revalidate=False):
    """Fetches market prices from ESI."""
    url = "https://esi.evetech.net/v1/markets/prices/"
    return make_esi_request(url, return_headers=return_headers, force_revalidate=force_revalidate)

def get_character_skills(character, force_revalidate=False):
    """Fetches a character's skills from ESI."""
    if not character: return None
    url = f"https://esi.evetech.net/v4/characters/{character.id}/skills/"
    return make_esi_request(url, character=character, force_revalidate=force_revalidate)

def get_wallet_balance(character: Character, return_headers=False, force_revalidate=False):
    """
    Fetches a character's wallet balance, using a 2-minute cache stored in the database.
    If force_revalidate is True, it bypasses the cache check but still updates the cache.
    """
    if not character:
        return None

    # 1. Check for a fresh, cached balance if not forcing a revalidation
    if not force_revalidate and character.wallet_balance is not None and character.wallet_balance_last_updated is not None:
        if (datetime.now(timezone.utc) - character.wallet_balance_last_updated) < timedelta(minutes=2):
            logging.debug(f"Returning cached wallet balance for {character.name}.")
            return character.wallet_balance

    # 2. If cache is stale, non-existent, or revalidation is forced, fetch from ESI
    logging.info(f"Fetching wallet balance from ESI for {character.name}...")
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/"
    # We use force_revalidate=True in the ESI call itself to ensure we get the absolute latest data from ESI's side,
    # as their own cache can be up to 120 seconds old. Our own logic determines if we should even make the call.
    response_data, headers = make_esi_request(url, character=character, return_headers=True, force_revalidate=True)

    # 3. If the fetch is successful, update our cache and the in-memory object
    if response_data is not None:
        new_balance = float(response_data)
        now = datetime.now(timezone.utc)

        # Update the database
        update_character_wallet_balance(character.id, new_balance, now)

        # Update the in-memory Character object to prevent stale reads until next reload
        character.wallet_balance = new_balance
        character.wallet_balance_last_updated = now

        return (new_balance, headers) if return_headers else new_balance

    # 4. If ESI fetch fails, return the stale balance if we have one, otherwise None
    logging.error(f"Failed to fetch new wallet balance for {character.name} from ESI.")
    if character.wallet_balance is not None:
        logging.warning(f"Returning stale cached balance of {character.wallet_balance} for {character.name}.")
        return character.wallet_balance

    return None

def get_contracts(character, return_headers=False, force_revalidate=False):
    """
    Fetches all pages of contracts from ESI.
    Returns the list of contracts, or None on failure. Optionally returns headers.
    """
    if not character:
        return (None, None) if return_headers else None

    all_contracts = []
    page = 1
    url = f"https://esi.evetech.net/v1/characters/{character.id}/contracts/"
    first_page_headers = None

    while True:
        params = {"datasource": "tranquility", "page": page}
        revalidate_this_page = force_revalidate and page == 1
        data, headers = make_esi_request(url, character=character, params=params, return_headers=True, force_revalidate=revalidate_this_page)

        if data is None: # Explicitly check for API failure
            logging.error(f"Failed to fetch page {page} of contracts for {character.name}.")
            return (None, None) if return_headers else None

        if page == 1:
            first_page_headers = headers

        if not data: # Empty list means no more pages
            break

        all_contracts.extend(data)

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1)

    if return_headers:
        return all_contracts, first_page_headers
    return all_contracts


def get_market_orders_history(character, return_headers=False, force_revalidate=False):
    """
    Fetches all pages of historical market orders from ESI.
    Returns the list of orders, or None on failure. Optionally returns headers.
    """
    if not character:
        return (None, None) if return_headers else None

    all_orders = []
    page = 1
    url = f"https://esi.evetech.net/v2/characters/{character.id}/orders/history/"
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


def get_structure_market_orders(character: Character, structure_id: int, force_revalidate=False):
    """
    Fetches all pages of market orders for a specific structure from ESI.
    Requires an authenticated character.
    """
    if not character:
        return None

    all_orders = []
    page = 1
    url = f"https://esi.evetech.net/v1/markets/structures/{structure_id}/"

    while True:
        params = {"datasource": "tranquility", "page": page}
        # Only force revalidation on the first page.
        revalidate_this_page = force_revalidate and page == 1
        data, headers = make_esi_request(
            url,
            character=character,
            params=params,
            return_headers=True,
            force_revalidate=revalidate_this_page
        )

        if data is None: # A None response indicates an error, not just an empty page
            logging.error(f"Failed to fetch market orders for structure {structure_id} on page {page}.")
            # Return None to indicate failure, as partial data could be misleading.
            return None

        if not data: # An empty list means we've reached the last page
            break

        all_orders.extend(data)

        pages_header = headers.get('x-pages') if headers else None
        if not pages_header or int(pages_header) <= page:
            break

        page += 1
        time.sleep(0.1) # Be nice to ESI

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

def get_character_online_status(character: Character):
    """Fetches a character's online status from ESI."""
    if not character: return None
    url = f"https://esi.evetech.net/v3/characters/{character.id}/online/"
    return make_esi_request(url, character=character, force_revalidate=True)


def get_character_location(character: Character):
    """Fetches a character's current location from ESI."""
    if not character: return None
    url = f"https://esi.evetech.net/v2/characters/{character.id}/location/"
    return make_esi_request(url, character=character, force_revalidate=True)

def get_character_public_info(character_id: int):
    """Fetches public character information from ESI."""
    url = f"https://esi.evetech.net/v5/characters/{character_id}/"
    return make_esi_request(url)

def get_corporation_info(corporation_id: int):
    """Fetches public corporation information from ESI."""
    url = f"https://esi.evetech.net/v5/corporations/{corporation_id}/"
    return make_esi_request(url)

def get_alliance_info(alliance_id: int):
    """Fetches public alliance information from ESI."""
    url = f"https://esi.evetech.net/v4/alliances/{alliance_id}/"
    return make_esi_request(url)


def get_station_info(station_id: int):
    """Fetches public station information from ESI."""
    url = f"https://esi.evetech.net/v2/universe/stations/{station_id}/"
    return make_esi_request(url)


def get_system_info(system_id: int):
    """Fetches public solar system information from ESI."""
    url = f"https://esi.evetech.net/v4/universe/systems/{system_id}/"
    return make_esi_request(url)


def get_constellation_info(constellation_id: int):
    """Fetches public constellation information from ESI."""
    url = f"https://esi.evetech.net/latest/universe/constellations/{constellation_id}/"
    return make_esi_request(url)


def get_structure_info(character: Character, structure_id: int):
    """Fetches structure information from ESI, requires authentication."""
    if not character:
        return None
    url = f"https://esi.evetech.net/v2/universe/structures/{structure_id}/"
    return make_esi_request(url, character=character)


def get_route(origin_system_id: int, destination_system_id: int):
    """Fetches the route between two solar systems from ESI."""
    url = f"https://esi.evetech.net/v1/route/{origin_system_id}/{destination_system_id}/"
    return make_esi_request(url)


def get_image_from_cache(url: str):
    """Retrieves a cached image (data and etag) from the database."""
    conn = database.get_db_connection()
    cached_image = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT etag, data FROM image_cache WHERE url = %s", (url,))
            row = cursor.fetchone()
            if row:
                cached_image = {'etag': row[0], 'data': row[1]}
    finally:
        database.release_db_connection(conn)
    return cached_image


def save_image_to_cache(url: str, etag: str, data: bytes):
    """Saves or updates an image in the database cache."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Use psycopg2's binary adapter for the bytea data
            binary_data = psycopg2.Binary(data)
            cursor.execute(
                """
                INSERT INTO image_cache (url, etag, data, last_fetched)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                    etag = EXCLUDED.etag,
                    data = EXCLUDED.data,
                    last_fetched = EXCLUDED.last_fetched;
                """,
                (url, etag, binary_data, datetime.now(timezone.utc))
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


def get_cached_image(url: str) -> bytes | None:
    """
    Fetches an image, using a database cache to handle ETags.
    Returns the image data as bytes, or None on failure.
    """
    cached = get_image_from_cache(url)
    headers = {}
    if cached and cached.get('etag'):
        headers['If-None-Match'] = cached['etag']

    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 304:  # Not Modified
            logging.debug(f"Returning cached image for {url} (304 Not Modified).")
            return bytes(cached['data']) # Return the stored binary data

        res.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        new_etag = res.headers.get('ETag')
        image_data = res.content
        if new_etag:
            save_image_to_cache(url, new_etag, image_data)

        return image_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download image from {url}: {e}")
        # If the request fails, still try to return the cached version if it exists
        if cached:
            logging.warning(f"Returning stale cached image for {url} due to request failure.")
            return bytes(cached['data'])
        return None


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


def add_wallet_journal_entries_to_db(character_id: int, journal_entries: list):
    """Adds a list of wallet journal entries to the wallet_journal table."""
    if not journal_entries:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            data_to_insert = []
            for entry in journal_entries:
                # ESI response can have missing optional keys, so we use .get()
                data_tuple = (
                    entry['id'],
                    character_id,
                    entry.get('amount'),
                    entry.get('balance'),
                    entry.get('context_id'),
                    entry.get('context_id_type'),
                    entry['date'],
                    entry['description'],
                    entry.get('first_party_id'),
                    entry.get('reason'),
                    entry['ref_type'],
                    entry.get('second_party_id'),
                    entry.get('tax'),
                    entry.get('tax_receiver_id')
                )
                data_to_insert.append(data_tuple)

            cursor.executemany(
                """
                INSERT INTO wallet_journal (
                    id, character_id, amount, balance, context_id, context_id_type,
                    date, description, first_party_id, reason, ref_type,
                    second_party_id, tax, tax_receiver_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id, character_id) DO NOTHING
                """,
                data_to_insert
            )
            conn.commit()
            logging.info(f"Inserted {len(data_to_insert)} records into wallet_journal for char {character_id}.")
    finally:
        database.release_db_connection(conn)


def backfill_character_journal_history(character: Character) -> bool:
    """
    Performs a one-time backfill of a character's wallet journal history.
    This is separate to handle cases where characters were added before this
    feature was implemented. Returns True on success, False on failure.
    """
    state_key = f"journal_history_backfilled_{character.id}"
    if get_bot_state(state_key):
        logging.info(f"Journal history already backfilled for {character.name}. Skipping.")
        return True

    logging.warning(f"Starting wallet journal backfill for {character.name}...")
    all_journal_entries = get_wallet_journal(character, fetch_all=True)
    if all_journal_entries is None:
        logging.error(f"Failed to fetch wallet journal during history backfill for {character.name}.")
        return False

    add_wallet_journal_entries_to_db(character.id, all_journal_entries)

    journal_ref_ids = [j['id'] for j in all_journal_entries]
    add_processed_journal_refs(character.id, journal_ref_ids)
    logging.info(f"Stored {len(journal_ref_ids)} journal entries and marked them as processed for {character.name}.")

    set_bot_state(state_key, datetime.now(timezone.utc).isoformat())
    logging.warning(f"Wallet journal backfill for {character.name} is complete.")
    return True


def backfill_all_character_history(character: Character) -> bool:
    """
    Performs a one-time backfill of critical history from ESI.
    This is now a two-phase process for transactions: an initial fast sync,
    followed by a gradual background backfill.
    Returns True on success, False on any ESI failure during the initial sync.
    """
    state_key = f"history_backfilled_{character.id}"
    if get_bot_state(state_key):
        logging.info(f"Full history already backfilled for {character.name}. Skipping.")
        return True

    logging.warning(f"Starting initial fast sync for {character.name}...")

    # --- Phase 1: Initial Fast Transaction Sync ---
    logging.info(f"Fetching most recent transactions for {character.name}...")
    initial_transactions = get_wallet_transactions(character) # No before_id gets the latest
    if initial_transactions is None:
        logging.error(f"Failed to fetch initial transactions during sync for {character.name}.")
        return False

    # --- Phase 2: Kick off Background Backfill ---
    if not initial_transactions:
        logging.info(f"No transaction history found for {character.name}. Marking backfill as complete.")
        set_bot_state(state_key, datetime.now(timezone.utc).isoformat())
    else:
        add_historical_transactions_to_db(character.id, initial_transactions)
        buy_transactions = [tx for tx in initial_transactions if tx.get('is_buy')]
        if buy_transactions:
            for tx in buy_transactions:
                add_purchase_lot(character.id, tx['type_id'], tx['quantity'], tx['unit_price'], purchase_date=tx['date'])
            logging.info(f"Seeded {len(buy_transactions)} initial buy transactions for FIFO tracking for {character.name}.")

        oldest_tx_id = min(tx['transaction_id'] for tx in initial_transactions)
        logging.info(f"Oldest transaction ID from initial sync is {oldest_tx_id}. Kicking off background backfill.")

        update_character_backfill_state(character.id, is_backfilling=True, before_id=oldest_tx_id)
        # Import the task locally to prevent circular import issues
        from tasks import continue_backfill_character_history
        continue_backfill_character_history.delay(character.id)
        logging.info(f"Scheduled background backfill task for character {character.name}.")

    # --- The rest of the original function remains for immediate data seeding ---
    if not backfill_character_journal_history(character):
        return False

    logging.info(f"Seeding order history for {character.name}...")
    all_historical_orders = get_market_orders_history(character, force_revalidate=True)
    if all_historical_orders is None:
        logging.error(f"Failed to fetch order history during backfill for {character.name}.")
        return False
    add_processed_orders(character.id, [o['order_id'] for o in all_historical_orders])
    logging.info(f"Seeded {len(all_historical_orders)} historical orders for {character.name}.")

    logging.info(f"Fetching and caching current open orders for {character.name}...")
    open_orders = get_market_orders(character, force_revalidate=True)
    if open_orders is None:
        logging.error(f"Failed to fetch open market orders during backfill for {character.name}.")
        return False
    all_cached_orders = get_tracked_market_orders(character.id)
    if all_cached_orders:
        remove_tracked_market_orders(character.id, [o['order_id'] for o in all_cached_orders])
    update_tracked_market_orders(character.id, open_orders)
    logging.info(f"Cached {len(open_orders)} open orders for {character.name}.")

    # After caching the open orders, immediately fetch the regional data for them.
    if open_orders:
        _trigger_regional_market_data_fetch(character, open_orders)

    logging.warning(f"Initial fast sync for {character.name} is complete. Full history will be retrieved in the background.")
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

def get_contracts_from_db(character_id: int) -> list:
    """Retrieves all cached contracts for a character from the local database."""
    conn = database.get_db_connection()
    contracts = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT contract_data FROM contracts WHERE character_id = %s", (character_id,))
            contracts = [row[0] for row in cursor.fetchall()]
    finally:
        database.release_db_connection(conn)
    return contracts

def _resolve_location(location_id: int, character: Character) -> dict | None:
    """
    Resolves a location_id (station or structure) to its constituent parts (system_id, region_id).
    Uses the database cache first, then falls back to ESI. Returns a dict.
    """
    # 1. Check our own location_cache first
    cached_location = get_location_from_cache(location_id)
    if cached_location and cached_location.get('system_id') and cached_location.get('region_id'):
        return cached_location

    # 2. If not in cache, resolve via ESI
    system_id = None
    region_id = None

    # Resolve for structures (requires auth)
    if location_id > 10000000000:
        structure_info = get_structure_info(character, location_id)
        if structure_info:
            system_id = structure_info.get('solar_system_id')
    # Resolve for NPC stations (public)
    else:
        station_info = get_station_info(location_id)
        if station_info:
            system_id = station_info.get('system_id')

    # Get region from system to fully populate the cache item
    if system_id:
        system_info = get_system_info(system_id)
        if system_info:
            constellation_id = system_info.get('constellation_id')
            if constellation_id:
                constellation_info = get_constellation_info(constellation_id)
                if constellation_info:
                    region_id = constellation_info.get('region_id')

    # 3. Save to cache if we successfully resolved everything
    if location_id and system_id and region_id:
        save_location_to_cache(location_id, system_id, region_id)
        return {'system_id': system_id, 'region_id': region_id}

    return None


def _resolve_location_to_system_id(location_id: int, character: Character) -> int | None:
    """Wrapper around _resolve_location to get just the system_id."""
    location_details = _resolve_location(location_id, character)
    return location_details.get('system_id') if location_details else None


def _resolve_location_to_region_id(location_id: int, character: Character) -> int | None:
    """Wrapper around _resolve_location to get just the region_id."""
    location_details = _resolve_location(location_id, character)
    return location_details.get('region_id') if location_details else None


def get_jump_distance(origin_location_id: int, destination_location_id: int, character: Character) -> int | None:
    """
    Calculates the jump distance between two locations (stations or structures).
    Uses a database cache to store and retrieve system-to-system jump counts.
    """
    # Step 1: Resolve both locations to their solar system IDs
    origin_system_id = _resolve_location_to_system_id(origin_location_id, character)
    destination_system_id = _resolve_location_to_system_id(destination_location_id, character)

    if not origin_system_id or not destination_system_id:
        logging.warning(f"Could not resolve one or both system IDs for jump calculation: {origin_location_id} -> {destination_location_id}")
        return None

    if origin_system_id == destination_system_id:
        return 0

    # Step 2: Check the database cache for the jump distance
    cached_jumps = get_jump_distance_from_db(origin_system_id, destination_system_id)
    if cached_jumps is not None:
        logging.debug(f"Found cached jump distance from {origin_system_id} to {destination_system_id}: {cached_jumps} jumps.")
        return cached_jumps

    # Step 3: If not cached, calculate it via ESI
    logging.info(f"No cached jump distance found. Calculating route from {origin_system_id} to {destination_system_id} via ESI.")
    route = get_route(origin_system_id, destination_system_id)

    if route is None:
        logging.error(f"Failed to get route from ESI for {origin_system_id} -> {destination_system_id}")
        return None

    # ESI returns a list of system IDs in the route. Number of jumps is len - 1.
    jumps = len(route) - 1

    # Step 4: Save the newly calculated distance to the cache for future use
    save_jump_distance_to_db(origin_system_id, destination_system_id, jumps)
    logging.info(f"Calculated and cached {jumps} jumps from {origin_system_id} to {destination_system_id}.")

    return jumps


def _trigger_regional_market_data_fetch(character: Character, open_orders: list):
    """
    Given a character and their open orders, fetches the regional market data
    for each unique region/type pair to warm up the cache for undercut checks.
    """
    if not open_orders:
        return

    unique_region_types = set()
    for order in open_orders:
        region_id = _resolve_location_to_region_id(order['location_id'], character)
        if region_id:
            unique_region_types.add((region_id, order['type_id']))

    if not unique_region_types:
        return

    logging.info(f"Warming up regional market data cache for {len(unique_region_types)} region/type pairs for character {character.name}.")
    for region_id, type_id in unique_region_types:
        # We call this function for its side effect: caching the data.
        # We force revalidation to ensure the data is fresh.
        get_region_market_orders(region_id, type_id, force_revalidate=True)
        # Small sleep to be polite to ESI, although caching should handle most of this.
        time.sleep(0.1)
    logging.info(f"Regional market data cache warmup complete for character {character.name}.")


def _create_character_info_image(character_id, corporation_id, alliance_id=None):
    """
    Creates a composite image of the character portrait, corp logo, and alliance logo.
    """
    try:
        # URLs for the images
        portrait_url = f"https://images.evetech.net/characters/{character_id}/portrait?size=256"
        corp_logo_url = f"https://images.evetech.net/corporations/{corporation_id}/logo?size=128"
        alliance_logo_url = f"https://images.evetech.net/alliances/{alliance_id}/logo?size=128" if alliance_id else None

        # Download images using the new caching function
        portrait_data = get_cached_image(portrait_url)
        if not portrait_data:
            logging.error(f"Failed to get portrait for character {character_id}. Aborting image creation.")
            return None
        portrait_img = Image.open(io.BytesIO(portrait_data)).convert("RGBA")

        corp_logo_data = get_cached_image(corp_logo_url)
        corp_logo_img = Image.open(io.BytesIO(corp_logo_data)).convert("RGBA") if corp_logo_data else None

        alliance_logo_img = None
        if alliance_logo_url:
            alliance_logo_data = get_cached_image(alliance_logo_url)
            alliance_logo_img = Image.open(io.BytesIO(alliance_logo_data)).convert("RGBA") if alliance_logo_data else None

        # Create composite image
        width = 256
        height = 256 + (10 + 128) if (corp_logo_img or alliance_logo_img) else 256
        composite = Image.new('RGBA', (width, height), (28, 28, 28, 255))

        # Paste portrait
        composite.paste(portrait_img, (0, 0), portrait_img)

        # Paste logos
        if alliance_logo_img and corp_logo_img:
            composite.paste(corp_logo_img, (0, 256 + 10), corp_logo_img)
            composite.paste(alliance_logo_img, (128, 256 + 10), alliance_logo_img)
        elif corp_logo_img: # Only corp exists
            corp_pos = ((width - 128) // 2, 256 + 10)
            composite.paste(corp_logo_img, corp_pos, corp_logo_img)

        # Save to buffer
        buf = io.BytesIO()
        composite.save(buf, format='PNG')
        buf.seek(0)
        return buf

    except Exception as e:
        logging.error(f"Failed to create character info image: {e}", exc_info=True)
        return None


def get_new_and_updated_character_info():
    """
    Fetches information about new and updated characters from the database.
    - A character is 'new' if they don't have a 'history_backfilled' state AND are not currently backfilling.
    - A character is 'updated' if their `needs_update_notification` flag is set.
    Returns a dictionary mapping character_id to their status.
    e.g., {123: {'is_new': True}, 456: {'needs_update': True}}
    """
    conn = database.get_db_connection()
    db_chars_info = {}
    try:
        with conn.cursor() as cursor:
            # Get all characters that aren't marked for deletion, including their backfill status
            cursor.execute(
                "SELECT character_id, needs_update_notification, is_backfilling FROM characters WHERE deletion_scheduled_at IS NULL"
            )
            all_chars = cursor.fetchall()
            if not all_chars:
                return {}

            all_char_ids = [row[0] for row in all_chars]

            # Find which of these characters have the 'history_backfilled' state
            state_keys = [f"history_backfilled_{char_id}" for char_id in all_char_ids]
            placeholders = ','.join(['%s'] * len(state_keys))
            cursor.execute(f"SELECT key FROM bot_state WHERE key IN ({placeholders})", state_keys)
            backfilled_keys = {row[0] for row in cursor.fetchall()}

            for char_id, needs_update, is_backfilling in all_chars:
                info = {}
                # A character is new only if backfill is not complete AND not currently in progress.
                if f"history_backfilled_{char_id}" not in backfilled_keys and not is_backfilling:
                    info['is_new'] = True

                # Check if the character needs a credential update notification
                if needs_update:
                    info['needs_update'] = True

                if info: # Only add to dict if there's something to do
                    db_chars_info[char_id] = info
    finally:
        database.release_db_connection(conn)
    return db_chars_info


def get_characters_to_purge():
    """
    Fetches characters whose deletion grace period has expired.
    Returns a list of tuples with (character_id, character_name, telegram_user_id).
    """
    conn = database.get_db_connection()
    characters_to_purge = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT character_id, character_name, telegram_user_id FROM characters WHERE deletion_scheduled_at IS NOT NULL AND deletion_scheduled_at <= %s",
                (datetime.now(timezone.utc),)
            )
            characters_to_purge = cursor.fetchall()
    finally:
        database.release_db_connection(conn)
    return characters_to_purge


def get_first_telegram_user_id():
    """Retrieves the telegram_id of the first user who registered."""
    conn = database.get_db_connection()
    user_id = None
    try:
        with conn.cursor() as cursor:
            # Assuming the first entry is the first user. A created_at timestamp would be more robust.
            cursor.execute("SELECT telegram_id FROM telegram_users ORDER BY telegram_id ASC LIMIT 1")
            row = cursor.fetchone()
            if row:
                user_id = row[0]
    finally:
        database.release_db_connection(conn)
    return user_id


def get_bot_statistics():
    """Gathers various statistics about the bot's operation."""
    conn = database.get_db_connection()
    stats = {}
    try:
        with conn.cursor() as cursor:
            # Total characters
            cursor.execute("SELECT COUNT(*) FROM characters")
            stats['total_characters'] = cursor.fetchone()[0]

            # Last character registration time
            cursor.execute("SELECT MAX(created_at) FROM characters")
            last_reg = cursor.fetchone()[0]
            stats['last_character_registration'] = last_reg.strftime('%Y-%m-%d %H:%M:%S UTC') if last_reg else "N/A"

            # Last market price update
            cursor.execute("SELECT MAX(expires) FROM esi_cache WHERE cache_key LIKE '%/markets/%/orders/%'")
            last_market_update = cursor.fetchone()[0]
            stats['last_market_price_update'] = last_market_update.strftime('%Y-%m-%d %H:%M:%S UTC') if last_market_update else "N/A"

            # DB Size
            cursor.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
            stats['db_size'] = cursor.fetchone()[0]

            # Last bot start time
            cursor.execute("SELECT value FROM bot_state WHERE key = 'bot_start_time'")
            start_time_str = cursor.fetchone()
            if start_time_str:
                start_time = datetime.fromisoformat(start_time_str[0])
                duration = datetime.now(timezone.utc) - start_time
                stats['last_bot_start_duration'] = str(duration).split('.')[0] # Format as HH:MM:SS
            else:
                stats['last_bot_start_duration'] = "N/A"

            # ESI requests in the last hour
            cursor.execute("SELECT COUNT(*) FROM bot_stats WHERE timestamp > NOW() - INTERVAL '1 hour'")
            stats['esi_requests_last_hour'] = cursor.fetchone()[0]

            # ESI errors since last start
            if start_time_str:
                cursor.execute(
                    "SELECT COUNT(*) FROM bot_stats WHERE timestamp > %s AND status_code NOT IN (200, 304, 404)",
                    (start_time,)
                )
                stats['esi_errors_since_start'] = cursor.fetchone()[0]
            else:
                stats['esi_errors_since_start'] = "N/A"

            # Market activity in the last 24 hours
            twenty_four_hours_ago = datetime.now(timezone.utc) - timedelta(hours=24)
            cursor.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN is_buy = false THEN quantity * unit_price ELSE 0 END), 0) AS total_sales_value,
                    COALESCE(SUM(CASE WHEN is_buy = true THEN quantity * unit_price ELSE 0 END), 0) AS total_buy_value,
                    COUNT(*) AS total_transactions,
                    COUNT(DISTINCT character_id) AS active_characters
                FROM historical_transactions
                WHERE date > %s
            """, (twenty_four_hours_ago,))
            market_stats = cursor.fetchone()
            stats['total_sales_value_24h'] = market_stats[0]
            stats['total_buy_value_24h'] = market_stats[1]
            stats['total_transactions_24h'] = market_stats[2]
            stats['active_characters_24h'] = market_stats[3]


    finally:
        database.release_db_connection(conn)
    return stats


def log_esi_request(status_code: int):
    """Logs an ESI request status to the bot_stats table."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO bot_stats (timestamp, status_code) VALUES (%s, %s)",
                (datetime.now(timezone.utc), status_code)
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Error logging ESI request to bot_stats: {e}")
        conn.rollback()
    finally:
        database.release_db_connection(conn)


# --- Telegram Helpers ---

def send_telegram_message_sync(bot: telegram.Bot, message: str, chat_id: int, reply_markup=None):
    """Synchronously sends a message to a specific chat_id by running the async method in a new event loop."""
    if not chat_id:
        logging.error("No chat_id provided. Cannot send message.")
        return
    try:
        # Since this is called from a sync task (Celery), we need to run the async `send_message`
        # method in its own event loop.
        asyncio.run(bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown', reply_markup=reply_markup))
        logging.info(f"Sent message to chat_id: {chat_id}.")
    except Exception as e:
        # Catching a broad exception here because the async context can raise various errors.
        logging.error(f"Error sending Telegram message to {chat_id}: {e}", exc_info=True)

# --- Celery Task Helpers & Logic ---

def get_all_character_ids():
    """Returns a list of all active character IDs from the database."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT character_id FROM characters WHERE deletion_scheduled_at IS NULL")
            return [row[0] for row in cursor.fetchall()]
    finally:
        database.release_db_connection(conn)

def get_characters_with_daily_overview_enabled():
    """Returns a list of character IDs that have daily overviews enabled."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT character_id FROM characters WHERE enable_daily_overview = TRUE AND deletion_scheduled_at IS NULL")
            return [row[0] for row in cursor.fetchall()]
    finally:
        database.release_db_connection(conn)

def format_paginated_message(header: str, item_lines: list, footer: str, chat_id: int) -> list[dict]:
    """
    Splits a long message into chunks and returns a list of dictionaries,
    each representing a message to be sent by a task.
    """
    CHUNK_SIZE = 30
    notifications = []

    if not item_lines:
        message = header + "\n" + footer
        notifications.append({'message': message, 'chat_id': chat_id})
        return notifications

    # First chunk with header
    first_chunk = item_lines[:CHUNK_SIZE]
    message = header + "\n" + "\n".join(first_chunk)

    if len(item_lines) <= CHUNK_SIZE:
        message += "\n" + footer
        notifications.append({'message': message, 'chat_id': chat_id})
    else:
        notifications.append({'message': message, 'chat_id': chat_id})
        # Intermediate chunks
        remaining_lines = item_lines[CHUNK_SIZE:]
        for i in range(0, len(remaining_lines), CHUNK_SIZE):
            chunk = remaining_lines[i:i + CHUNK_SIZE]
            if (i + CHUNK_SIZE) >= len(remaining_lines):
                # Last chunk with footer
                message = "\n".join(chunk) + "\n" + footer
            else:
                message = "\n".join(chunk)
            notifications.append({'message': message, 'chat_id': chat_id})

    return notifications


def process_character_wallet(character_id: int) -> list[dict]:
    """
    Processes wallet journal and transactions for a single character.
    Returns a list of notification dictionaries to be sent.
    """
    character = get_character_by_id(character_id)
    if not character:
        return []

    notifications = []

    # --- Wallet Journal Processing ---
    history_backfilled_at_str = get_bot_state(f"history_backfilled_{character.id}")
    if history_backfilled_at_str:
        try:
            history_backfilled_at = datetime.fromisoformat(history_backfilled_at_str)
            if (datetime.now(timezone.utc) - history_backfilled_at) > timedelta(hours=grace_period_hours):
                recent_journal, headers = get_wallet_journal(character, return_headers=True)
                if recent_journal:
                    journal_ref_ids = [j['id'] for j in recent_journal]
                    existing_ref_ids = get_ids_from_db('historical_journal', 'ref_id', character.id, journal_ref_ids)
                    new_journal_ref_ids = set(journal_ref_ids) - existing_ref_ids
                    if new_journal_ref_ids:
                        new_entries = [j for j in recent_journal if j['id'] in new_journal_ref_ids]
                        add_wallet_journal_entries_to_db(character.id, new_entries)
                        add_processed_journal_refs(character.id, list(new_journal_ref_ids))
                        logging.info(f"Processed {len(new_entries)} new journal entries for {character.name}.")
        except (ValueError, TypeError):
            pass  # Handle legacy or malformed timestamps

    # --- Wallet Transaction Processing ---
    if not history_backfilled_at_str:
        return []

    try:
        # Check if the character's history has been backfilled
        datetime.fromisoformat(history_backfilled_at_str)
    except (ValueError, TypeError):
        return []

    recent_tx, headers = get_wallet_transactions(character, return_headers=True)
    if not recent_tx:
        return []

    tx_ids_from_esi = [tx['transaction_id'] for tx in recent_tx]
    existing_tx_ids = get_ids_from_db('historical_transactions', 'transaction_id', character.id, tx_ids_from_esi)
    new_tx_ids = set(tx_ids_from_esi) - existing_tx_ids

    if not new_tx_ids:
        return []

    set_bot_state(f"chart_cache_dirty_{character.id}", "true")
    new_transactions = [
        tx for tx in recent_tx
        if tx['transaction_id'] in new_tx_ids and datetime.fromisoformat(tx['date'].replace('Z', '+00:00')) > character.created_at
    ]
    add_historical_transactions_to_db(character.id, new_transactions)

    sales, buys = defaultdict(list), defaultdict(list)
    for tx in new_transactions:
        (buys if tx['is_buy'] else sales)[tx['type_id']].append(tx)

    # --- Essential Data Processing (Always Run) ---
    # Process buys to update purchase lots for COGS tracking
    for type_id, tx_group in buys.items():
        for tx in tx_group:
            add_purchase_lot(character.id, type_id, tx['quantity'], tx['unit_price'], purchase_date=tx['date'])

    # Process sales to consume purchase lots for COGS tracking and store the result
    sales_cogs = {}
    for type_id, tx_group in sales.items():
        total_quantity_sold = sum(t['quantity'] for t in tx_group)
        cogs = calculate_cogs_and_update_lots(character.id, type_id, total_quantity_sold)
        sales_cogs[type_id] = cogs


    # --- Notification Generation (Run only if enabled) ---
    # Suppress notifications for characters pending deletion or with notifications disabled
    if get_character_deletion_status(character.id) or not character.notifications_enabled:
        if get_character_deletion_status(character.id):
            logging.info(f"Character {character.name} ({character.id}) is pending deletion. Suppressing wallet notifications.")
        return []


    # Fetch data needed for notifications
    all_type_ids = list(sales.keys()) + list(buys.keys())
    all_loc_ids = [t['location_id'] for txs in list(sales.values()) + list(buys.values()) for t in txs]
    id_to_name = get_names_from_ids(list(set(all_type_ids + all_loc_ids)), character=character)
    wallet_balance = get_wallet_balance(character, force_revalidate=True)
    full_journal = get_full_wallet_journal_from_db(character.id)
    tx_id_to_journal_map = {entry['context_id']: entry for entry in full_journal if entry.get('ref_type') == 'market_transaction'}
    fee_journal_by_timestamp = defaultdict(list)
    for entry in full_journal:
        if entry['ref_type'] == 'transaction_tax':
            fee_journal_by_timestamp[entry['date']].append(entry)


    # Low Balance Alert
    if wallet_balance is not None and character.wallet_balance_threshold > 0:
        state_key = f"low_balance_alert_sent_at_{character.id}"
        last_alert_str = get_bot_state(state_key)
        try:
            # Check if an alert was sent in the last day
            should_send = not last_alert_str or (datetime.now(timezone.utc) - datetime.fromisoformat(last_alert_str)) > timedelta(days=1)
            if wallet_balance < character.wallet_balance_threshold and should_send:
                alert_message = f"⚠️ *Low Wallet Balance Warning ({character.name})* ⚠️\n\nYour wallet balance has dropped below `{character.wallet_balance_threshold:,.2f}` ISK.\n**Current Balance:** `{wallet_balance:,.2f}` ISK"
                notifications.append({'message': alert_message, 'chat_id': character.telegram_user_id})
                set_bot_state(state_key, datetime.now(timezone.utc).isoformat())
            elif wallet_balance >= character.wallet_balance_threshold and last_alert_str:
                # Reset the alert state if balance is now okay
                set_bot_state(state_key, '')
        except (ValueError, TypeError):
             set_bot_state(state_key, '') # Reset if state is malformed

    # Buy Notifications
    if buys and character.enable_buys_notifications:
        if len(buys) >= character.notification_batch_threshold:
            header = f"🛒 *Multiple Market Buys ({character.name})* 🛒"
            item_lines = [f"  • Bought: `{sum(t['quantity'] for t in tx_group)}` x `{id_to_name.get(type_id, 'Unknown')}`" for type_id, tx_group in buys.items()]
            footer = f"\n**Total Cost:** `{sum(tx['quantity'] * tx['unit_price'] for tx_group in buys.values() for tx in tx_group):,.2f} ISK`\n**Wallet:** `{wallet_balance:,.2f} ISK`"
            notifications.extend(format_paginated_message(header, item_lines, footer, character.telegram_user_id))
        else:
            for type_id, tx_group in buys.items():
                total_quantity = sum(t['quantity'] for t in tx_group)
                total_cost = sum(t['quantity'] * t['unit_price'] for t in tx_group)
                message = f"🛒 *Market Buy ({character.name})* 🛒\n\n**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n**Quantity:** `{total_quantity}`\n**Total Cost:** `{total_cost:,.2f} ISK`\n**Location:** `{id_to_name.get(tx_group[0]['location_id'], 'Unknown')}`\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                notifications.append({'message': message, 'chat_id': character.telegram_user_id})

    # Sale Notifications
    if sales and character.enable_sales_notifications:
        sales_details = []
        for type_id, tx_group in sales.items():
            total_quantity = sum(t['quantity'] for t in tx_group)
            total_value = sum(t['quantity'] * t['unit_price'] for t in tx_group)
            cogs = sales_cogs.get(type_id)

            # Get actual taxes from the journal
            journal_taxes = sum(
                abs(fee['amount'])
                for tx in tx_group
                if tx_id_to_journal_map.get(tx['transaction_id'])
                for fee in fee_journal_by_timestamp.get(tx_id_to_journal_map[tx['transaction_id']]['date'], [])
            )

            estimated_broker_fees = 0
            if cogs is not None:
                # Estimate broker fees based on user settings
                estimated_broker_fees = (cogs * (character.buy_broker_fee / 100)) + (total_value * (character.sell_broker_fee / 100))

            total_fees = journal_taxes + estimated_broker_fees
            net_profit = total_value - cogs - total_fees if cogs is not None else None
            sales_details.append({'type_id': type_id, 'tx_group': tx_group, 'cogs': cogs, 'total_fees': total_fees, 'net_profit': net_profit})

        if len(sales_details) >= character.notification_batch_threshold:
            header = f"✅ *Multiple Market Sales ({character.name})* ✅"
            item_lines = [f"  • Sold: `{sum(t['quantity'] for t in sale_info['tx_group'])}` x `{id_to_name.get(sale_info['type_id'], 'Unknown')}`" for sale_info in sales_details]
            grand_total_value = sum(sum(t['quantity'] * t['unit_price'] for t in sale_info['tx_group']) for sale_info in sales_details)
            grand_total_fees = sum(sale_info['total_fees'] for sale_info in sales_details)
            grand_total_net_profit = sum(sale_info['net_profit'] for sale_info in sales_details if sale_info['net_profit'] is not None)
            profit_line = f"\n**Total Net Profit:** `{grand_total_net_profit:,.2f} ISK`" if grand_total_net_profit > 0 else ""
            footer = f"\n**Total Sale Value:** `{grand_total_value:,.2f} ISK`\n**Total Fees:** `{grand_total_fees:,.2f} ISK`{profit_line}\n**Wallet:** `{wallet_balance:,.2f} ISK`"
            notifications.extend(format_paginated_message(header, item_lines, footer, character.telegram_user_id))
        else:
            for sale_info in sales_details:
                type_id, tx_group, total_fees, net_profit = sale_info['type_id'], sale_info['tx_group'], sale_info['total_fees'], sale_info['net_profit']
                total_quantity = sum(t['quantity'] for t in tx_group)
                avg_price = sum(t['quantity'] * t['unit_price'] for t in tx_group) / total_quantity
                profit_line = f"\n**Net Profit:** `{net_profit:,.2f} ISK`" if net_profit is not None else "\n**Profit:** `N/A (Missing Purchase History)`"
                message = f"✅ *Market Sale ({character.name})* ✅\n\n**Item:** `{id_to_name.get(type_id, 'Unknown')}`\n**Quantity:** `{total_quantity}` @ `{avg_price:,.2f} ISK`\n**Total Fees:** `{total_fees:,.2f} ISK`{profit_line}\n\n**Location:** `{id_to_name.get(tx_group[0]['location_id'], 'Unknown')}`\n**Wallet:** `{wallet_balance:,.2f} ISK`"
                notifications.append({'message': message, 'chat_id': character.telegram_user_id})

    return notifications


def process_character_orders(character_id: int) -> list[dict]:
    """
    Processes open orders, undercuts, and historical orders for a single character.
    Returns a list of notification dictionaries.
    """
    character = get_character_by_id(character_id)
    if not character:
        return []

    notifications = []

    # --- Order History (Cancelled/Expired) ---
    history_backfilled_at_str = get_bot_state(f"history_backfilled_{character.id}")
    if history_backfilled_at_str and character.notifications_enabled:
        try:
            history_backfilled_at = datetime.fromisoformat(history_backfilled_at_str)
            order_history, headers = get_market_orders_history(character, return_headers=True, force_revalidate=True)
            if order_history:
                processed_order_ids = get_processed_orders(character.id)
                new_orders = [o for o in order_history if o['order_id'] not in processed_order_ids and datetime.fromisoformat(o['issued'].replace('Z', '+00:00')) > character.created_at]
                if new_orders:
                    # Trust the ESI state directly for cancelled vs expired status
                    cancelled = [o for o in new_orders if o.get('state') == 'cancelled']
                    # An order is only truly expired if it was not filled.
                    # The ESI history endpoint marks filled orders as "expired" but with volume_remain: 0.
                    expired = [o for o in new_orders if o.get('state') == 'expired' and o.get('volume_remain', 0) > 0]

                    item_ids = [o['type_id'] for o in new_orders]
                    id_to_name = get_names_from_ids(item_ids)

                    for order in cancelled:
                        order_type = "Buy" if order.get('is_buy_order') else "Sell"
                        if (order_type == "Buy" and character.enable_buys_notifications) or (order_type == "Sell" and character.enable_sales_notifications):
                            msg = f"ℹ️ *{order_type} Order Cancelled ({character.name})* ℹ️\nYour order for `{order['volume_total']}` x `{id_to_name.get(order['type_id'], 'Unknown')}` was cancelled."
                            notifications.append({'message': msg, 'chat_id': character.telegram_user_id})

                    for order in expired:
                        order_type = "Buy" if order.get('is_buy_order') else "Sell"
                        if (order_type == "Buy" and character.enable_buys_notifications) or (order_type == "Sell" and character.enable_sales_notifications):
                            msg = f"ℹ️ *{order_type} Order Expired ({character.name})* ℹ️\nYour order for `{order['volume_total']}` x `{id_to_name.get(order['type_id'], 'Unknown')}` has expired."
                            notifications.append({'message': msg, 'chat_id': character.telegram_user_id})

                    add_processed_orders(character.id, [o['order_id'] for o in new_orders])
        except (ValueError, TypeError):
            pass

    # --- Open Orders & Undercut Check ---
    open_orders, headers = get_market_orders(character, return_headers=True, force_revalidate=True)
    if open_orders is None:
        return notifications

    # Cache Management
    esi_order_ids = {o['order_id'] for o in open_orders}
    cached_orders = get_tracked_market_orders(character.id)
    cached_order_ids = {o['order_id'] for o in cached_orders}
    orders_to_remove = cached_order_ids - esi_order_ids
    if orders_to_remove:
        remove_tracked_market_orders(character.id, list(orders_to_remove))
    if open_orders:
        update_tracked_market_orders(character.id, open_orders)
    elif not orders_to_remove and cached_order_ids:
        remove_tracked_market_orders(character.id, list(cached_order_ids))

    # Undercut Notifications
    if character.enable_undercut_notifications and open_orders:
        remove_stale_undercut_statuses(character.id, list(esi_order_ids))
        previous_statuses = get_undercut_statuses(character.id)
        new_statuses, notifications_to_send = [], []
        market_data_cache = {}
        cached_orders_map = {o['order_id']: o for o in cached_orders}

        for order in open_orders:
            region_id = _resolve_location_to_region_id(order['location_id'], character)
            if not region_id:
                logging.warning(f"Could not resolve region for location {order['location_id']} on order {order['order_id']}. Skipping undercut check for this order.")
                continue

            if region_id not in market_data_cache: market_data_cache[region_id] = {}
            if order['type_id'] not in market_data_cache[region_id]:
                regional_orders = get_region_market_orders(region_id, order['type_id'], force_revalidate=True)
                if regional_orders:
                    sell_orders = sorted([o for o in regional_orders if not o.get('is_buy_order')], key=lambda x: x['price'])
                    buy_orders = sorted([o for o in regional_orders if o.get('is_buy_order')], key=lambda x: x['price'], reverse=True)
                    market_data_cache[region_id][order['type_id']] = {'sell': sell_orders, 'buy': buy_orders}

            is_outbid_or_undercut, competitor = False, None
            regional_market_orders = market_data_cache.get(region_id, {}).get(order['type_id'])

            if regional_market_orders:
                if order.get('is_buy_order'):
                    buy_orders = regional_market_orders.get('buy', [])
                    for best_buy in buy_orders:
                        if best_buy['order_id'] != order['order_id']:
                            if best_buy['price'] > order['price']:
                                is_outbid_or_undercut, competitor = True, best_buy
                            break
                else:
                    sell_orders = regional_market_orders.get('sell', [])
                    for best_sell in sell_orders:
                        if best_sell['order_id'] != order['order_id']:
                            if best_sell['price'] < order['price']:
                                is_outbid_or_undercut, competitor = True, best_sell
                            break

            new_statuses.append({
                'order_id': order['order_id'],
                'is_undercut': is_outbid_or_undercut,
                'competitor_price': competitor['price'] if competitor else None,
                'competitor_location_id': competitor['location_id'] if competitor else None,
                'competitor_volume': competitor.get('volume_remain') if competitor else None
            })

            previous_status_info = previous_statuses.get(order['order_id'], {})
            was_undercut = previous_status_info.get('is_undercut', False)
            cached_order = cached_orders_map.get(order['order_id'])
            previous_price = cached_order['price'] if cached_order else None

            if is_outbid_or_undercut and not was_undercut:
                if competitor and 'issued' in competitor and datetime.fromisoformat(competitor['issued'].replace('Z', '+00:00')) > character.created_at:
                    notifications_to_send.append({'type': 'undercut', 'my_order': order, 'competitor': competitor})
            elif not is_outbid_or_undercut and was_undercut:
                if previous_price is not None and order['price'] == previous_price:
                    notifications_to_send.append({'type': 'back_on_top', 'my_order': order})

        if new_statuses:
            update_undercut_statuses(character.id, new_statuses)

        if notifications_to_send:
            all_ids = {n['my_order']['type_id'] for n in notifications_to_send} | \
                      {n['my_order']['location_id'] for n in notifications_to_send} | \
                      {n['competitor']['location_id'] for n in notifications_to_send if n.get('competitor')}
            id_to_name = get_names_from_ids(list(all_ids), character)
            for notif in notifications_to_send:
                my_order = notif['my_order']
                if notif['type'] == 'undercut':
                    competitor = notif['competitor']
                    jumps = get_jump_distance(my_order['location_id'], competitor['location_id'], character)
                    location_line = f"`{id_to_name.get(competitor['location_id'], 'Unknown')}`" + (f" ({jumps} jumps)" if jumps is not None else "")
                    if my_order.get('is_buy_order'):
                        title, body, competitor_price_label, competitor_location_label = f"❗️ *Buy Order Outbid ({character.name})* ❗️", f"Your buy order for **{id_to_name.get(my_order['type_id'])}** has been outbid.", "Highest Bid", "Highest Bid Location"
                    else:
                        title, body, competitor_price_label, competitor_location_label = f"❗️ *Sell Order Undercut ({character.name})* ❗️", f"Your sell order for **{id_to_name.get(my_order['type_id'])}** has been undercut.", "Lowest Price", "Lowest Price Location"
                    msg = (
                        f"{title}\n\n"
                        f"{body}\n\n"
                        f"  *Your Order*\n"
                        f"  • Price: `{my_order['price']:,.2f}` ISK\n"
                        f"  • Qty: `{my_order['volume_remain']:,}` / `{my_order['volume_total']:,}`\n"
                        f"  • Location: `{id_to_name.get(my_order['location_id'])}`\n\n"
                        f"  *Competitor's Order*\n"
                        f"  • {competitor_price_label}: `{competitor['price']:,.2f}` ISK\n"
                        f"  • Qty: `{competitor['volume_remain']:,}`\n"
                        f"  • Location: {location_line}"
                    )
                    notifications.append({'message': msg, 'chat_id': character.telegram_user_id})
                elif notif['type'] == 'back_on_top':
                    order_type_str = "Buy order" if my_order.get('is_buy_order') else "Sell order"
                    msg = (f"✅ *Back on Top ({character.name})* ✅\n\n"
                           f"Your {order_type_str} for **{id_to_name.get(my_order['type_id'])}** is the best price again!\n\n"
                           f"  • **Price:** `{my_order['price']:,.2f}` ISK\n"
                           f"  • **Location:** `{id_to_name.get(my_order['location_id'])}`\n"
                           f"  • **Remaining:** `{my_order['volume_remain']:,}` of `{my_order['volume_total']:,}`")
                    notifications.append({'message': msg, 'chat_id': character.telegram_user_id})

    # --- Prevent notifications for characters pending deletion ---
    if get_character_deletion_status(character.id):
        logging.info(f"Character {character.name} ({character.id}) is pending deletion. Suppressing {len(notifications)} order notifications.")
        return []

    return notifications


def process_character_contracts(character_id: int) -> list[dict]:
    """
    Processes contracts for a single character and returns notifications.
    """
    character = get_character_by_id(character_id)
    if not character or not character.enable_contracts_notifications:
        return []

    notifications = []
    history_backfilled_at_str = get_bot_state(f"history_backfilled_{character.id}")
    if not history_backfilled_at_str:
        return []

    try:
        history_backfilled_at = datetime.fromisoformat(history_backfilled_at_str)
        if (datetime.now(timezone.utc) - history_backfilled_at) < timedelta(hours=1):
            return []
    except (ValueError, TypeError):
        pass

    contracts, headers = get_contracts(character, return_headers=True, force_revalidate=True)
    if contracts is None:
        return []

    current_contract_ids = [c['contract_id'] for c in contracts]
    remove_stale_contracts(character.id, current_contract_ids)
    update_contracts_cache(character.id, contracts)

    processed_contract_ids = get_processed_contracts(character.id)
    new_contracts = [c for c in contracts if c['contract_id'] not in processed_contract_ids and c['status'] == 'outstanding']

    if new_contracts:
        ids_to_resolve = {c['issuer_id'] for c in new_contracts} | {c.get('assignee_id') for c in new_contracts if c.get('assignee_id')} | {c.get('start_location_id') for c in new_contracts if c.get('start_location_id')} | {c.get('end_location_id') for c in new_contracts if c.get('end_location_id')}
        id_to_name = get_names_from_ids(list(ids_to_resolve), character)
        for contract in new_contracts:
            lines = [f"📝 *New Contract ({character.name})* 📝", f"\n*Type:* `{contract['type'].replace('_', ' ').title()}`", f"*From:* `{id_to_name.get(contract['issuer_id'], 'Unknown')}`", f"*Status:* `{contract['status'].replace('_', ' ').title()}`"]
            if contract.get('assignee_id'): lines.append(f"*To:* `{id_to_name.get(contract['assignee_id'], 'Unknown')}`")
            if contract.get('start_location_id'): lines.append(f"*Location:* `{id_to_name.get(contract['start_location_id'], 'Unknown')}`")
            if contract.get('price', 0) > 0: lines.append(f"*Price:* `{contract['price']:,.2f} ISK`")
            try:
                expires_dt = datetime.fromisoformat(contract['date_expired'].replace('Z', '+00:00'))
                time_left = expires_dt - datetime.now(timezone.utc)
                if time_left.total_seconds() > 0:
                    d, h, m = time_left.days, time_left.seconds // 3600, (time_left.seconds % 3600) // 60
                    expires_str = f"{d}d {h}h {m}m" if d > 0 else f"{h}h {m}m"
                    lines.append(f"*Expires In:* `{expires_str.strip()}`")
            except (ValueError, KeyError): pass
            notifications.append({'message': "\n".join(lines), 'chat_id': character.telegram_user_id})

    add_processed_contracts(character.id, current_contract_ids)

    # --- Prevent notifications for characters pending deletion ---
    if get_character_deletion_status(character.id):
        logging.info(f"Character {character.name} ({character.id}) is pending deletion. Suppressing {len(notifications)} contract notifications.")
        return []

    return notifications


def _prepare_chart_data(character_id, start_of_period):
    """
    Prepares all data needed for chart generation.
    1. Fetches all historical financial events.
    2. Builds the inventory state up to the start of the chart period.
    3. Returns the initial inventory state and all events within the period.
    """
    all_transactions = get_historical_transactions_from_db(character_id)
    full_journal = get_full_wallet_journal_from_db(character_id)
    # Use exact transaction and market provider taxes from the journal.
    # Broker's fee will be estimated based on user settings, so we exclude it here.
    fee_ref_types = {'transaction_tax', 'market_provider_tax'}

    all_events = []
    for tx in all_transactions:
        all_events.append({'type': 'tx', 'data': tx, 'date': datetime.fromisoformat(tx['date'].replace('Z', '+00:00'))})
    for entry in full_journal:
        if entry['ref_type'] in fee_ref_types:
            all_events.append({'type': 'fee', 'data': entry, 'date': entry['date']})
    all_events.sort(key=lambda x: x['date'])

    inventory = defaultdict(list)
    events_before_period = [e for e in all_events if e['date'] < start_of_period]

    for event in events_before_period:
        if event['type'] == 'tx':
            tx = event['data']
            if tx.get('is_buy'):
                inventory[tx['type_id']].append({'quantity': tx['quantity'], 'price': tx['unit_price']})
            else: # Sale
                remaining_to_sell = tx['quantity']
                lots = inventory.get(tx['type_id'], [])
                if lots:
                    consumed_count = 0
                    for lot in lots:
                        if remaining_to_sell <= 0: break
                        take = min(remaining_to_sell, lot['quantity'])
                        remaining_to_sell -= take
                        lot['quantity'] -= take
                        if lot['quantity'] == 0: consumed_count += 1
                    inventory[tx['type_id']] = lots[consumed_count:]

    events_in_period = [e for e in all_events if e['date'] >= start_of_period]
    return inventory, events_in_period

def get_character_net_worth(character: Character, force_revalidate: bool = False) -> float | None:
    """
    Calculates a character's total net worth, using a 1-hour cache stored in the database.
    If force_revalidate is True, it bypasses the cache check but still updates the cache.
    """
    if not character:
        return None

    # 1. Check for a fresh, cached net worth
    if not force_revalidate and character.net_worth is not None and character.net_worth_last_updated is not None:
        if (datetime.now(timezone.utc) - character.net_worth_last_updated) < timedelta(hours=1):
            logging.debug(f"Returning cached net worth for {character.name}.")
            return character.net_worth

    # 2. If cache is stale or revalidation is forced, perform the full calculation
    logging.info(f"Calculating net worth from ESI for {character.name}...")
    total_net_worth = 0

    # Get Wallet Balance
    wallet_balance = get_wallet_balance(character, force_revalidate=True)
    if wallet_balance is None:
        logging.error(f"Failed to get wallet balance for {character.name}, cannot calculate net worth.")
        return None # Wallet balance is essential
    total_net_worth += wallet_balance

    # Get Market Prices
    market_prices_raw = get_market_prices(force_revalidate=True)
    if not market_prices_raw:
        logging.error("Failed to get market prices, cannot calculate asset values.")
        return None # Prices are essential
    market_prices = {p['type_id']: p.get('adjusted_price', p.get('average_price', 0)) for p in market_prices_raw}

    # Get Blueprints to exclude them from asset valuation
    blueprints = get_character_blueprints(character)
    blueprint_item_ids = {bp['item_id'] for bp in blueprints} if blueprints else set()

    # Get Assets and Orders
    assets = get_character_assets(character)
    orders = get_market_orders(character)

    asset_value = 0
    if assets is not None:
        assets_by_type = defaultdict(int)
        for asset in assets:
            # Exclude blueprints by their unique item_id
            if asset['item_id'] in blueprint_item_ids:
                continue
            assets_by_type[asset['type_id']] += asset['quantity']

        # Handle active ship hull if not in assets
        ship = get_character_ship(character)
        if ship:
            if not any(a['item_id'] == ship['ship_item_id'] for a in assets):
                assets_by_type[ship['ship_type_id']] += 1

        # Value all assets using adjusted_price
        for type_id, qty in assets_by_type.items():
            asset_value += qty * market_prices.get(type_id, 0)

        # Now, correct the valuation for items in sell orders
        if orders:
            for order in orders:
                if not order.get('is_buy_order'):
                    type_id = order['type_id']
                    quantity = order['volume_remain']
                    order_price = order['price']
                    adjusted_price = market_prices.get(type_id, 0)

                    # The correction is the difference between the order price and adjusted price
                    price_correction = (order_price - adjusted_price) * quantity
                    asset_value += price_correction

    total_net_worth += asset_value

    # Sum Buy Order Escrow
    if orders is not None:
        buy_order_escrow = sum(o.get('escrow', 0) for o in orders if o.get('is_buy_order'))
        total_net_worth += buy_order_escrow

    # Sum Contract Escrows
    contracts = get_contracts(character)
    if contracts is not None:
        contract_escrow = 0
        for contract in contracts:
            if contract.get('status') == 'outstanding':
                # Sum all relevant financial fields in the contract
                contract_escrow += contract.get('price', 0)
                contract_escrow += contract.get('reward', 0)
                contract_escrow += contract.get('collateral', 0)
                contract_escrow += contract.get('buyout', 0)
        total_net_worth += contract_escrow

    # 3. Update the cache
    now = datetime.now(timezone.utc)
    update_character_net_worth(character.id, total_net_worth, now)

    # Update the in-memory object
    character.net_worth = total_net_worth
    character.net_worth_last_updated = now

    return total_net_worth


def _calculate_overview_data(character: Character) -> dict:
    """Fetches all necessary data from the local DB and calculates overview statistics."""
    logging.info(f"Calculating overview data for {character.name} from local database...")

    now = datetime.now(timezone.utc)
    one_day_ago = now - timedelta(days=1)
    thirty_days_ago = now - timedelta(days=30)

    all_transactions = get_historical_transactions_from_db(character.id)
    full_journal = get_full_wallet_journal_from_db(character.id)

    # --- Profit/Loss Calculation ---
    def calculate_profit(start_date):
        inventory, events = _prepare_chart_data(character.id, start_date)
        profit = 0
        sales_value = 0

        # `events` from `_prepare_chart_data` now contains journaled fees like
        # transaction_tax and market_provider_tax. Broker's fees are estimated separately.
        journaled_fees_value = sum(abs(e['data']['amount']) for e in events if e['type'] == 'fee')
        estimated_broker_fees = 0

        for event in events:
            if event['type'] == 'tx':
                tx = event['data']
                if tx.get('is_buy'):
                    inventory[tx['type_id']].append({'quantity': tx['quantity'], 'price': tx['unit_price']})
                else: # Sale
                    sale_value = tx['quantity'] * tx['unit_price']
                    sales_value += sale_value
                    cogs = 0
                    remaining_to_sell = tx['quantity']
                    lots = inventory.get(tx['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[tx['type_id']] = lots[consumed_count:]

                    # Add estimated broker fees for this sale
                    if cogs > 0:
                        estimated_broker_fees += (cogs * (character.buy_broker_fee / 100)) + (sale_value * (character.sell_broker_fee / 100))

                    profit += sale_value - cogs

        # Subtract all fees from the final profit
        total_fees = journaled_fees_value + estimated_broker_fees
        profit -= total_fees

        return profit, sales_value, total_fees

    profit_24h, total_sales_24h, total_fees_24h = calculate_profit(one_day_ago)
    profit_30_days, total_sales_30_days, total_fees_30_days = calculate_profit(thirty_days_ago)

    wallet_balance = get_last_known_wallet_balance(character)
    net_worth = get_character_net_worth(character)
    available_years = sorted(list(set(datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).year for tx in all_transactions))) if all_transactions else []

    return {
        "now": now, "wallet_balance": wallet_balance, "net_worth": net_worth,
        "total_sales_24h": total_sales_24h, "total_fees_24h": total_fees_24h, "profit_24h": profit_24h,
        "total_sales_30_days": total_sales_30_days, "total_fees_30_days": total_fees_30_days, "profit_30_days": profit_30_days,
        "available_years": available_years
    }

def _format_overview_message(overview_data: dict, character: Character) -> tuple[str, InlineKeyboardMarkup]:
    """Formats the overview data into a message string and keyboard."""
    now = overview_data['now']
    net_worth_str = f"`{overview_data['net_worth']:,.2f} ISK`" if overview_data['net_worth'] is not None else "`Calculating...`"

    message = (
        f"📊 *Market Overview ({character.name})*\n"
        f"_{now.strftime('%Y-%m-%d %H:%M UTC')}_\n\n"
        f"*Wallet Balance:* `{overview_data['wallet_balance'] or 0:,.2f} ISK`\n"
        f"*Total Net Worth:* {net_worth_str}\n\n"
        f"*Last Day:*\n"
        f"  - Total Sales Value: `{overview_data['total_sales_24h']:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{overview_data['total_fees_24h']:,.2f} ISK`\n"
        f"  - **Profit (FIFO):** `{overview_data['profit_24h']:,.2f} ISK`\n\n"
        f"---\n\n"
        f"🗓️ *Last 30 Days:*\n"
        f"  - Total Sales Value: `{overview_data['total_sales_30_days']:,.2f} ISK`\n"
        f"  - Total Fees (Broker + Tax): `{overview_data['total_fees_30_days']:,.2f} ISK`\n"
        f"  - **Profit (FIFO):** `{overview_data['profit_30_days']:,.2f} ISK`"
    )
    keyboard = [
        [InlineKeyboardButton("Last Day", callback_data=f"chart_lastday_{character.id}"), InlineKeyboardButton("Last 7 Days", callback_data=f"chart_7days_{character.id}")],
        [InlineKeyboardButton("Last 30 Days", callback_data=f"chart_30days_{character.id}"), InlineKeyboardButton("All Time", callback_data=f"chart_alltime_{character.id}")]
    ]
    return message, InlineKeyboardMarkup(keyboard)


def prepare_paginated_overview_data(user_id: int, page: int = 0):
    """
    Prepares the data for a single page of the multi-character overview.
    This is a synchronous, data-intensive function designed to be called from a Celery task.
    Returns a tuple of (message_text, reply_markup_json, status).
    Status can be 'success', 'no_characters'.
    """
    user_characters = get_characters_for_user(user_id)
    if not user_characters:
        return "You have no characters to display.", None, "no_characters"

    total_pages = len(user_characters)
    # Ensure page index is valid
    page = max(0, min(page, total_pages - 1))

    character = user_characters[page]

    # Calculate and format the overview for the current character
    overview_data = _calculate_overview_data(character)
    # Get the standard message and the base keyboard with chart buttons
    message, base_keyboard = _format_overview_message(overview_data, character)

    # --- Modify the keyboard for pagination ---
    new_keyboard_rows = []

    # 1. Modify the chart buttons to include pagination info for the back button
    for row in base_keyboard.inline_keyboard:
        new_row = []
        for button in row:
            # Original callback data: chart_{type}_{char_id}
            # We append the origin information for the chart's "Back" button.
            # e.g., chart_lastday_12345_page_0
            new_callback_data = f"{button.callback_data}_page_{page}"
            new_row.append(InlineKeyboardButton(button.text, callback_data=new_callback_data))
        new_keyboard_rows.append(new_row)

    # 2. Add navigation buttons
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"overview_page_{page - 1}"))

    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))

    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"overview_page_{page + 1}"))

    if nav_row:
        new_keyboard_rows.append(nav_row)

    # 3. Add a back button to the character selection screen
    new_keyboard_rows.append([InlineKeyboardButton("« Back to Character Selection", callback_data="overview")])

    reply_markup = InlineKeyboardMarkup(new_keyboard_rows)

    return message, json.dumps(reply_markup.to_dict()), "success"


# --- Chart Generation ---

def format_isk(value):
    """Formats a number into a human-readable ISK string."""
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}b"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.2f}k"
    return f"{value:.2f}"


def _calculate_top_profitable_items(events_in_period: list, initial_inventory: dict, character_id: int) -> str:
    """
    Calculates the top 5 most profitable items from a list of events and returns a formatted string.
    This helper function is designed to be called by the various chart generation functions.
    """
    inventory = initial_inventory.copy()
    item_profits = defaultdict(lambda: {'profit': 0, 'sales_value': 0})

    # First, process all buys in the period to update the inventory
    for event in events_in_period:
        if event['type'] == 'tx' and event['data'].get('is_buy'):
            tx = event['data']
            inventory[tx['type_id']].append({'quantity': tx['quantity'], 'price': tx['unit_price']})

    # Then, process all sales and fees
    for event in events_in_period:
        if event['type'] == 'tx' and not event['data'].get('is_buy'):
            tx = event['data']
            sale_value = tx['quantity'] * tx['unit_price']

            cogs = 0
            remaining_to_sell = tx['quantity']
            lots = inventory.get(tx['type_id'], [])
            if lots:
                consumed_count = 0
                for lot in lots:
                    if remaining_to_sell <= 0: break
                    take = min(remaining_to_sell, lot['quantity'])
                    cogs += take * lot['price']
                    remaining_to_sell -= take
                    lot['quantity'] -= take
                    if lot['quantity'] == 0: consumed_count += 1
                inventory[tx['type_id']] = lots[consumed_count:]

            # Only track profit if COGS could be fully determined
            if remaining_to_sell == 0:
                net_profit = sale_value - cogs
                item_profits[tx['type_id']]['profit'] += net_profit
                item_profits[tx['type_id']]['sales_value'] += sale_value

    if not item_profits:
        return ""

    # Sort items by profit in descending order
    sorted_items = sorted(item_profits.items(), key=lambda item: item[1]['profit'], reverse=True)

    # Get the top 5
    top_5_items = sorted_items[:5]

    if not top_5_items:
        return ""

    # Resolve names for the top 5 items
    type_ids = [item_id for item_id, data in top_5_items]
    character = get_character_by_id(character_id)
    id_to_name = get_names_from_ids(type_ids, character)

    # Format the output string
    lines = ["\n\n*Top 5 Profitable Items (Net Profit):*"]
    for item_id, data in top_5_items:
        name = id_to_name.get(item_id, f"Unknown Item ID: {item_id}")
        profit_in_millions = data['profit'] / 1_000_000
        lines.append(f"  - `{name}`: `{profit_in_millions:,.2f}m ISK`")

    return "\n".join(lines)


def generate_last_day_chart(character_id: int):
    """
    Generates a chart for the last 24 hours, processing events chronologically to ensure accuracy.
    Also calculates the top 5 profitable items for the period.
    Returns a tuple: (BytesIO buffer, caption_suffix_string) or None.
    """
    import matplotlib
    matplotlib.use('Agg')  # Use a non-interactive backend
    import matplotlib.pyplot as plt
    character = get_character_by_id(character_id)
    if not character: return None, None

    now = datetime.now(timezone.utc)
    start_of_period = now - timedelta(days=1)

    # Get initial inventory state and all events within the period, sorted chronologically.
    inventory, events_in_period = _prepare_chart_data(character_id, start_of_period)

    # If there are no events, there's nothing to chart.
    if not any(e['type'] == 'tx' and not e['data'].get('is_buy') for e in events_in_period) and \
       not any(e['type'] == 'fee' for e in events_in_period):
        return None, None

    # --- Top Items Calculation ---
    # Create a deep copy of the inventory for the profit calculation to not interfere with the chart calculation
    inventory_for_profit_calc = defaultdict(list)
    for type_id, lots in inventory.items():
        inventory_for_profit_calc[type_id] = [lot.copy() for lot in lots]
    caption_suffix = _calculate_top_profitable_items(events_in_period, inventory_for_profit_calc, character_id)


    # --- Data Preparation for Chart ---
    hour_labels = [(start_of_period + timedelta(hours=i)).strftime('%H') for i in range(24)]
    hourly_sales = {label: 0 for label in hour_labels}
    hourly_fees = {label: 0 for label in hour_labels}

    hourly_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    # --- Chronological Event Processing for Chart ---
    for i in range(24):
        hour_start = start_of_period + timedelta(hours=i)
        hour_end = hour_start + timedelta(hours=1)
        hour_label = hour_start.strftime('%H')

        # Process all events that fall within this hour, in order.
        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < hour_end:
            event = events_in_period[event_idx]
            event_type = event['type']
            data = event['data']

            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else:  # Sale
                    sale_value = data['quantity'] * data['unit_price']
                    hourly_sales[hour_label] += sale_value

                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    # Estimate broker fees for this sale
                    estimated_broker_fee = 0
                    if cogs > 0:
                        estimated_broker_fee = (cogs * (character.buy_broker_fee / 100)) + (sale_value * (character.sell_broker_fee / 100))

                    hourly_fees[hour_label] += estimated_broker_fee
                    accumulated_profit += sale_value - cogs - estimated_broker_fee

            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                hourly_fees[hour_label] += fee_amount
                accumulated_profit -= fee_amount

            event_idx += 1

        hourly_cumulative_profit.append(accumulated_profit)

    # --- Plotting ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(hour_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(hourly_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(hourly_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)

    ax2 = ax.twinx()
    # Prepend the starting profit (0) for the line plot
    final_profit_line = [0] + hourly_cumulative_profit
    ax2.plot(range(25), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(25), final_profit_line, color="lime", alpha=0.3, zorder=1)

    ax.set_title(f'Performance for {character.name} (Last 24 Hours)', color='white', fontsize=16)
    ax.set_xlabel('Hour (UTC)', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], hour_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf, caption_suffix

def _generate_daily_breakdown_chart(character_id: int, days_to_show: int):
    """
    Helper to generate charts with a daily breakdown (last 7/30 days).
    Also calculates the top 5 profitable items for the period.
    Returns a tuple: (BytesIO buffer, caption_suffix_string) or None.
    """
    import matplotlib
    matplotlib.use('Agg')  # Use a non-interactive backend
    import matplotlib.pyplot as plt
    character = get_character_by_id(character_id)
    if not character: return None, None

    now = datetime.now(timezone.utc)
    start_of_period = (now - timedelta(days=days_to_show-1)).replace(hour=0, minute=0, second=0, microsecond=0)

    inventory, events_in_period = _prepare_chart_data(character_id, start_of_period)

    if not any(e['type'] == 'tx' and not e['data'].get('is_buy') for e in events_in_period) and \
       not any(e['type'] == 'fee' for e in events_in_period):
        return None, None

    # --- Top Items Calculation ---
    inventory_for_profit_calc = defaultdict(list)
    for type_id, lots in inventory.items():
        inventory_for_profit_calc[type_id] = [lot.copy() for lot in lots]
    caption_suffix = _calculate_top_profitable_items(events_in_period, inventory_for_profit_calc, character_id)

    # --- Data Preparation for Chart ---
    days = [(start_of_period + timedelta(days=i)) for i in range(days_to_show)]
    label_format = '%d' if days_to_show == 30 else '%m-%d'
    bar_labels = [d.strftime(label_format) for d in days]
    daily_sales = {label: 0 for label in bar_labels}
    daily_fees = {label: 0 for label in bar_labels}

    daily_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    # --- Chronological Event Processing for Chart ---
    for day_start in days:
        day_end = day_start + timedelta(days=1)
        day_label = day_start.strftime(label_format)

        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < day_end:
            event = events_in_period[event_idx]
            event_type = event['type']
            data = event['data']

            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else:  # Sale
                    sale_value = data['quantity'] * data['unit_price']
                    daily_sales[day_label] += sale_value
                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    # Estimate broker fees for this sale
                    estimated_broker_fee = 0
                    if cogs > 0:
                        estimated_broker_fee = (cogs * (character.buy_broker_fee / 100)) + (sale_value * (character.sell_broker_fee / 100))

                    daily_fees[day_label] += estimated_broker_fee
                    accumulated_profit += sale_value - cogs - estimated_broker_fee

            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                daily_fees[day_label] += fee_amount
                accumulated_profit -= fee_amount

            event_idx += 1

        daily_cumulative_profit.append(accumulated_profit)

    # --- Plotting ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(bar_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(daily_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(daily_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)

    ax2 = ax.twinx()
    final_profit_line = [0] + daily_cumulative_profit
    ax2.plot(range(days_to_show + 1), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(days_to_show + 1), final_profit_line, color="lime", alpha=0.3, zorder=1)

    ax.set_title(f'Performance for {character.name} (Last {days_to_show} Days)', color='white', fontsize=16)
    ax.set_xlabel('Date', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], bar_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf, caption_suffix

def generate_last_7_days_chart(character_id: int):
    """Generates a chart for the last 7 days."""
    return _generate_daily_breakdown_chart(character_id, 7)

def generate_last_30_days_chart(character_id: int):
    """Generates a chart for the last 30 days."""
    return _generate_daily_breakdown_chart(character_id, 30)

def generate_all_time_chart(character_id: int):
    """
    Generates a monthly breakdown chart for the character's entire history.
    Also calculates the top 5 profitable items for the period.
    Returns a tuple: (BytesIO buffer, caption_suffix_string) or None.
    """
    import matplotlib
    matplotlib.use('Agg')  # Use a non-interactive backend
    import matplotlib.pyplot as plt
    character = get_character_by_id(character_id)
    if not character: return None, None

    inventory, events_in_period = _prepare_chart_data(character_id, datetime.min.replace(tzinfo=timezone.utc))
    if not events_in_period: return None, None

    # --- Top Items Calculation ---
    inventory_for_profit_calc = defaultdict(list)
    for type_id, lots in inventory.items():
        inventory_for_profit_calc[type_id] = [lot.copy() for lot in lots]
    caption_suffix = _calculate_top_profitable_items(events_in_period, inventory_for_profit_calc, character_id)

    # --- Data Preparation for Chart ---
    start_date = events_in_period[0]['date']
    end_date = datetime.now(timezone.utc)
    months = []
    current_month = start_date.replace(day=1)
    while current_month <= end_date:
        months.append(current_month)
        next_month_val = current_month.month + 1
        next_year_val = current_month.year
        if next_month_val > 12:
            next_month_val = 1
            next_year_val += 1
        current_month = current_month.replace(year=next_year_val, month=next_month_val)

    bar_labels = [m.strftime('%Y-%m') for m in months]
    monthly_sales = {label: 0 for label in bar_labels}
    monthly_fees = {label: 0 for label in bar_labels}
    monthly_cumulative_profit = []
    accumulated_profit = 0
    event_idx = 0

    # --- Chronological Event Processing ---
    for month_start in months:
        month_label = month_start.strftime('%Y-%m')
        next_month_start = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)

        while event_idx < len(events_in_period) and events_in_period[event_idx]['date'] < next_month_start:
            event = events_in_period[event_idx]
            event_type = event['type']
            data = event['data']

            if event_type == 'tx':
                if data.get('is_buy'):
                    inventory[data['type_id']].append({'quantity': data['quantity'], 'price': data['unit_price']})
                else: # Sale
                    sale_value = data['quantity'] * data['unit_price']
                    monthly_sales[month_label] += sale_value
                    cogs = 0
                    remaining_to_sell = data['quantity']
                    lots = inventory.get(data['type_id'], [])
                    if lots:
                        consumed_count = 0
                        for lot in lots:
                            if remaining_to_sell <= 0: break
                            take = min(remaining_to_sell, lot['quantity'])
                            cogs += take * lot['price']
                            remaining_to_sell -= take
                            lot['quantity'] -= take
                            if lot['quantity'] == 0: consumed_count += 1
                        inventory[data['type_id']] = lots[consumed_count:]
                    # Estimate broker fees for this sale
                    estimated_broker_fee = 0
                    if cogs > 0:
                        estimated_broker_fee = (cogs * (character.buy_broker_fee / 100)) + (sale_value * (character.sell_broker_fee / 100))

                    monthly_fees[month_label] += estimated_broker_fee
                    accumulated_profit += sale_value - cogs - estimated_broker_fee
            elif event_type == 'fee':
                fee_amount = abs(data['amount'])
                monthly_fees[month_label] += fee_amount
                accumulated_profit -= fee_amount

            event_idx += 1

        monthly_cumulative_profit.append(accumulated_profit)

    # --- Plotting ---
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('#1c1c1c')
    ax.set_facecolor('#282828')
    bar_width = 0.4
    r1 = range(len(bar_labels))
    r2 = [x + bar_width for x in r1]
    ax.bar(r1, list(monthly_sales.values()), color='cyan', width=bar_width, edgecolor='black', label='Sales', zorder=2)
    ax.bar(r2, list(monthly_fees.values()), color='red', width=bar_width, edgecolor='black', label='Fees', zorder=2)

    ax2 = ax.twinx()
    final_profit_line = [0] + monthly_cumulative_profit
    ax2.plot(range(len(months) + 1), final_profit_line, label='Accumulated Profit', color='lime', linestyle='-', zorder=3)
    ax2.fill_between(range(len(months) + 1), final_profit_line, color="lime", alpha=0.3, zorder=1)

    ax.set_title(f'Performance for {character.name} (All Time)', color='white', fontsize=16)
    ax.set_xlabel('Month', color='white', fontsize=12)
    ax.set_ylabel('Sales / Fees (ISK)', color='white', fontsize=12)
    ax2.set_ylabel('Accumulated Profit (ISK)', color='white', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', zorder=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0)
    plt.xticks([r + bar_width/2 for r in r1], bar_labels, rotation=45, ha='right')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax2.tick_params(axis='y', colors='white')
    plt.setp(ax.spines.values(), color='gray')
    plt.setp(ax2.spines.values(), color='gray')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format_isk(x)))
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor=fig.get_facecolor(), bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    buf.seek(0)
    return buf, caption_suffix


def send_main_menu_sync(bot: telegram.Bot, telegram_user_id: int, top_message: str = None):
    """Constructs and sends the main menu to a user, callable from a sync task."""
    user_characters = get_characters_for_user(telegram_user_id)
    if not user_characters:
        # This function should only be called for users with characters,
        # but as a safeguard, we'll log a warning and do nothing.
        logging.warning(f"send_main_menu_sync called for user {telegram_user_id} with no characters. Aborting.")
        return

    base_message = (
        f"You have {len(user_characters)} character(s) registered. "
        "Please choose an option from the main menu:"
    )
    message = f"{top_message}\n\n{base_message}" if top_message else base_message
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
            InlineKeyboardButton("📝 View Contracts", callback_data="contracts"),
            InlineKeyboardButton("📊 Request Overview", callback_data="overview")
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
            InlineKeyboardButton("➕ Add Character", callback_data="add_character"),
            InlineKeyboardButton("🗑️ Remove", callback_data="remove")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    send_telegram_message_sync(bot, message, telegram_user_id, reply_markup=reply_markup)


async def send_main_menu_async(bot: telegram.Bot, telegram_user_id: int, top_message: str = None):
    """Async version of sending the main menu, for use within an event loop."""
    user_characters = get_characters_for_user(telegram_user_id)
    if not user_characters:
        logging.warning(f"send_main_menu_async called for user {telegram_user_id} with no characters. Aborting.")
        return

    base_message = (
        f"You have {len(user_characters)} character(s) registered. "
        "Please choose an option from the main menu:"
    )
    message = f"{top_message}\n\n{base_message}" if top_message else base_message
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
            InlineKeyboardButton("📝 View Contracts", callback_data="contracts"),
            InlineKeyboardButton("📊 Request Overview", callback_data="overview")
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
            InlineKeyboardButton("➕ Add Character", callback_data="add_character"),
            InlineKeyboardButton("🗑️ Remove", callback_data="remove")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # Call the bot's async method directly instead of the sync wrapper
    await bot.send_message(chat_id=telegram_user_id, text=message, parse_mode='Markdown', reply_markup=reply_markup)


def send_daily_overview_for_character(character_id: int, bot):
    """Generates and sends the daily overview for a single character."""
    character = get_character_by_id(character_id)
    if not character or get_character_deletion_status(character.id):
        return

    logging.info(f"Running scheduled daily overview for {character.name}...")
    try:
        overview_data = _calculate_overview_data(character)
        message, reply_markup = _format_overview_message(overview_data, character)

        new_keyboard = list(reply_markup.inline_keyboard)
        new_keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
        new_reply_markup = InlineKeyboardMarkup(new_keyboard)

        from tasks import send_telegram_message_sync
        send_telegram_message_sync(bot, message, chat_id=character.telegram_user_id, reply_markup=new_reply_markup)
        logging.info(f"Daily overview sent for {character.name}.")
    except Exception as e:
        logging.error(f"Failed to send daily overview for {character.name}: {e}", exc_info=True)


def prepare_historical_sales_data(character_id: int, user_id: int, page: int = 0):
    """
    Fetches and prepares a paginated list of historical sales transactions,
    with detailed profit and loss analysis using FIFO for COGS and
    wallet journal entries for accurate tax and fee calculations.
    This is a synchronous, data-intensive function designed to be called from a Celery task.
    Returns a tuple of (message_text, reply_markup_json, status).
    Status can be 'success', 'no_character', 'backfill_failed', 'no_sales'.
    """
    character = get_character_by_id(character_id)
    if not character:
        return None, None, "no_character"

    # --- Data Integrity Check & Backfill ---
    journal_state_key = f"journal_history_backfilled_{character.id}"
    if not get_bot_state(journal_state_key):
        logging.info(f"Performing one-time sync of wallet journal history for {character.name}...")
        backfill_success = backfill_character_journal_history(character)
        if not backfill_success:
            logging.error(f"Failed to sync journal history for {character.name}.")
            return f"❌ Failed to sync journal history for {character.name}. Please try again later.", None, "backfill_failed"

    # --- Data Fetching & On-Demand Refresh ---
    all_transactions = get_historical_transactions_from_db(character.id)
    full_journal = get_full_wallet_journal_from_db(character.id)

    try:
        logging.info(f"Performing on-demand transaction refresh for {character.name}...")
        recent_transactions_from_esi = get_wallet_transactions(character)
        if recent_transactions_from_esi:
            existing_tx_ids = {tx['transaction_id'] for tx in all_transactions}
            new_transactions = [
                tx for tx in recent_transactions_from_esi
                if tx['transaction_id'] not in existing_tx_ids
            ]
            if new_transactions:
                logging.info(f"On-demand refresh found {len(new_transactions)} new transactions for {character.name}.")
                add_historical_transactions_to_db(character.id, new_transactions)
                all_transactions.extend(new_transactions)

        logging.info(f"Performing on-demand journal refresh for {character.name}...")
        recent_journal_entries_from_esi = get_wallet_journal(character)
        if recent_journal_entries_from_esi:
            existing_journal_ids = {entry['id'] for entry in full_journal}
            new_journal_entries = [
                entry for entry in recent_journal_entries_from_esi
                if entry['id'] not in existing_journal_ids
            ]
            if new_journal_entries:
                logging.info(f"On-demand refresh found {len(new_journal_entries)} new journal entries for {character.name}.")
                add_wallet_journal_entries_to_db(character.id, new_journal_entries)
                for entry in new_journal_entries:
                    entry['date'] = datetime.fromisoformat(entry['date'].replace('Z', '+00:00'))
                full_journal.extend(new_journal_entries)
                full_journal.sort(key=lambda x: x['date'], reverse=True)
    except Exception as e:
        logging.error(f"On-demand data refresh failed for {character.name}: {e}", exc_info=True)

    # --- COGS Calculation (In-Memory FIFO Simulation) ---
    inventory = defaultdict(list)
    sorted_transactions = sorted(all_transactions, key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')))
    sale_cogs_data = {}

    for tx in sorted_transactions:
        type_id = tx['type_id']
        if tx.get('is_buy'):
            inventory[type_id].append({'quantity': tx['quantity'], 'price': tx['unit_price']})
        else:
            cogs = 0
            remaining_to_sell = tx['quantity']
            lots_to_consume_from = inventory.get(type_id, [])
            cogs_calculable = True
            if not lots_to_consume_from:
                cogs_calculable = False
            else:
                lots_consumed_count = 0
                for lot in lots_to_consume_from:
                    if remaining_to_sell <= 0: break
                    quantity_from_lot = min(remaining_to_sell, lot['quantity'])
                    cogs += quantity_from_lot * lot['price']
                    remaining_to_sell -= quantity_from_lot
                    lot['quantity'] -= quantity_from_lot
                    if lot['quantity'] == 0:
                        lots_consumed_count += 1
                inventory[type_id] = lots_to_consume_from[lots_consumed_count:]
                if remaining_to_sell > 0:
                    cogs_calculable = False
            sale_cogs_data[tx['transaction_id']] = cogs if cogs_calculable else None

    # --- Data Filtering and Annotation ---
    sale_journal_entries = [
        entry for entry in full_journal
        if entry.get('ref_type') == 'market_transaction' and entry.get('amount', 0) > 0
    ]
    sale_transaction_ids = {entry['context_id'] for entry in sale_journal_entries}
    sales_transactions = [
        tx for tx in all_transactions
        if tx['transaction_id'] in sale_transaction_ids
    ]

    tx_id_to_journal_map = {
        entry['context_id']: entry for entry in full_journal
        if entry.get('ref_type') == 'market_transaction'
    }
    fee_journal_by_timestamp = defaultdict(list)
    tax_ref_types = {'transaction_tax'}
    for entry in full_journal:
        if entry['ref_type'] in tax_ref_types:
            fee_journal_by_timestamp[entry['date']].append(entry)

    for sale in sales_transactions:
        sale['cogs'] = sale_cogs_data.get(sale['transaction_id'])
        sale_value = sale['quantity'] * sale['unit_price']
        main_journal_entry = tx_id_to_journal_map.get(sale['transaction_id'])
        taxes = 0
        if main_journal_entry:
            precise_timestamp = main_journal_entry['date']
            related_fees = fee_journal_by_timestamp.get(precise_timestamp, [])
            taxes = sum(abs(fee['amount']) for fee in related_fees)
        else:
            logging.warning(f"Could not find matching journal entry for sale transaction_id {sale['transaction_id']}")

        sale['taxes'] = taxes
        if sale.get('cogs') is not None:
            estimated_broker_fees = (sale['cogs'] * (character.buy_broker_fee / 100)) + (sale_value * (character.sell_broker_fee / 100))
            sale['total_fees'] = taxes + estimated_broker_fees
            sale['net_profit'] = sale_value - sale['cogs'] - sale['total_fees']
        else:
            sale['net_profit'] = None

    if not sales_transactions:
        user_characters = get_characters_for_user(user_id)
        back_callback = "sales" if len(user_characters) > 1 else "start_command"
        keyboard = [[InlineKeyboardButton("« Back", callback_data=back_callback)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"🧾 *Historical Sales for {character.name}*\n\nNo historical sales found."
        return message, json.dumps(reply_markup.to_dict()), "no_sales"

    sales_transactions.sort(key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 5
    total_items = len(sales_transactions)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_tx = sales_transactions[start_index:end_index]

    page_broker_fees = 0
    if paginated_tx:
        start_date = datetime.fromisoformat(paginated_tx[-1]['date'].replace('Z', '+00:00'))
        end_date = datetime.fromisoformat(paginated_tx[0]['date'].replace('Z', '+00:00'))
        page_broker_fees = sum(
            abs(entry['amount']) for entry in full_journal
            if entry['ref_type'] == 'brokers_fee' and start_date <= entry['date'] <= end_date
        )

    # --- Name Resolution ---
    type_ids = [tx['type_id'] for tx in paginated_tx]
    location_ids = [tx['location_id'] for tx in paginated_tx]
    id_to_name = get_names_from_ids(list(set(type_ids + location_ids)), character)

    # --- Message Formatting ---
    header = f"🧾 *Historical Sales for {character.name}*\n"
    message_lines = []
    for tx in paginated_tx:
        item_name = id_to_name.get(tx['type_id'], f"Type ID {tx['type_id']}")
        date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
        sale_value = tx['quantity'] * tx['unit_price']
        line = (
            f"*{item_name}*\n"
            f"  *Date:* `{date_str}`\n"
            f"  *Qty:* `{tx['quantity']:,}` @ `{tx['unit_price']:,.2f}`\n"
            f"  *Sale Value:* `{sale_value:,.2f}` ISK\n"
        )
        if tx.get('cogs') is not None:
            line += f"  *Cost (FIFO):* `{tx['cogs']:,.2f}` ISK\n"
            line += f"  *Total Fees (Est.):* `{tx.get('total_fees', 0):,.2f}` ISK\n"
            line += f"  *Net Profit (Est.):* `{tx['net_profit']:,.2f}` ISK"
        else:
            line += f"  *Net Profit:* `N/A (Missing Purchase History)`"
        message_lines.append(line)

    footer = (
        f"\n---\n*Broker Fees for this page's period:* `{page_broker_fees:,.2f}` ISK\n"
        f"_(Note: Net Profit is an estimate including sales tax and broker fees.)_"
    )

    # --- Keyboard ---
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"history_list_sale_{character_id}_{page - 1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"history_list_sale_{character_id}_{page + 1}"))

    if nav_row: keyboard.append(nav_row)
    user_characters = get_characters_for_user(user_id)
    back_callback = "sales" if len(user_characters) > 1 else "start_command"
    keyboard.append([InlineKeyboardButton("« Back", callback_data=back_callback)])
    reply_markup = InlineKeyboardMarkup(keyboard)

    full_message = header + "\n".join(message_lines) + footer
    return full_message, json.dumps(reply_markup.to_dict()), "success"


def prepare_open_orders_data(character_id: int, user_id: int, is_buy: bool, page: int = 0):
    """
    Fetches and prepares a paginated list of open orders from the local cache.
    This is a synchronous, data-intensive function designed to be called from a Celery task.
    Returns a tuple of (message_text, reply_markup_json, status).
    Status can be 'success', 'no_character', 'no_orders', 'error'.
    """
    character = get_character_by_id(character_id)
    if not character:
        return None, None, "no_character"

    # --- Data Fetching ---
    all_orders = get_tracked_market_orders(character.id)
    skills_data = get_character_skills(character)

    if all_orders is None:
        return f"❌ Could not fetch market orders for {character.name}. The database might be unavailable.", None, "error"

    # --- Order Capacity Calculation ---
    order_capacity_str = ""
    if skills_data and 'skills' in skills_data:
        skill_map = {s['skill_id']: s['active_skill_level'] for s in skills_data['skills']}
        trade_level = skill_map.get(3443, 0)
        retail_level = skill_map.get(3444, 0)
        wholesale_level = skill_map.get(16596, 0)
        tycoon_level = skill_map.get(18580, 0)
        max_orders = 5 + (trade_level * 4) + (retail_level * 8) + (wholesale_level * 16) + (tycoon_level * 32)
        order_capacity_str = f"({len(all_orders)} / {max_orders} orders)"

    # Filter for buy or sell orders
    filtered_orders = [order for order in all_orders if bool(order.get('is_buy_order')) == is_buy]
    order_type_str = "Buy" if is_buy else "Sale"

    # --- Message Formatting ---
    header = f"📄 *Open {order_type_str} Orders for {character.name}* {order_capacity_str}\n\n"

    if not filtered_orders:
        keyboard = [[InlineKeyboardButton("« Back", callback_data="open_orders")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = header + f"✅ No open {order_type_str.lower()} orders found."
        return message, json.dumps(reply_markup.to_dict()), "no_orders"

    filtered_orders.sort(key=lambda x: datetime.fromisoformat(x['issued'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 10
    total_items = len(filtered_orders)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_orders = filtered_orders[start_index:end_index]

    # --- Data Resolution ---
    type_ids_on_page = list(set(order['type_id'] for order in paginated_orders))
    location_ids = [order['location_id'] for order in paginated_orders]
    all_undercut_statuses = get_undercut_statuses(character.id)
    competitor_location_ids = [
        status.get('competitor_location_id')
        for status in all_undercut_statuses.values()
        if status.get('competitor_location_id')
    ]
    ids_to_resolve = list(set(type_ids_on_page + location_ids + competitor_location_ids))
    id_to_name = get_names_from_ids(ids_to_resolve, character)

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

        undercut_status = all_undercut_statuses.get(order['order_id'])
        if undercut_status and undercut_status['is_undercut']:
            competitor_price = undercut_status.get('competitor_price', 0.0)
            competitor_location_id = undercut_status.get('competitor_location_id')
            if competitor_price and competitor_location_id:
                competitor_loc_name = id_to_name.get(competitor_location_id, "Unknown Location")
                jumps = get_jump_distance(order['location_id'], competitor_location_id, character)
                jumps_str = f" ({jumps}j)" if jumps is not None else ""
                alert_text = "Outbid" if is_buy else "Undercut"
                price_text = "Highest bid" if is_buy else "Lowest price"
                competitor_volume = undercut_status.get('competitor_volume')
                volume_str = f" (Qty: {competitor_volume:,})" if competitor_volume is not None else ""
                alert_line = f"❗️ {alert_text}! {price_text}: {competitor_price:,.2f} in {competitor_loc_name}{jumps_str}{volume_str}"
                line += f"\n  `> {alert_line}`"
        message_lines.append(line)

    summary_footer = "\n\n---\n_Undercut status is updated periodically._"

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
    keyboard.append([InlineKeyboardButton("« Back", callback_data="open_orders")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    full_message = header + "\n\n".join(message_lines) + summary_footer
    return full_message, json.dumps(reply_markup.to_dict()), "success"


def prepare_historical_buys_data(character_id: int, user_id: int, page: int = 0):
    """
    Fetches and prepares a paginated list of historical buy transactions.
    This is a synchronous, data-intensive function designed to be called from a Celery task.
    Returns a tuple of (message_text, reply_markup_json, status).
    Status can be 'success', 'no_character', 'no_buys'.
    """
    character = get_character_by_id(character_id)
    if not character:
        return None, None, "no_character"

    # --- Data Fetching & Filtering ---
    all_transactions = get_historical_transactions_from_db(character.id)
    buy_transactions = [tx for tx in all_transactions if tx.get('is_buy')]

    if not buy_transactions:
        user_characters = get_characters_for_user(user_id)
        back_callback = "buys" if len(user_characters) > 1 else "start_command"
        keyboard = [[InlineKeyboardButton("« Back", callback_data=back_callback)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"🧾 *Historical Buys for {character.name}*\n\nNo historical buys found."
        return message, json.dumps(reply_markup.to_dict()), "no_buys"

    # Sort by date, newest first for display
    buy_transactions.sort(key=lambda x: datetime.fromisoformat(x['date'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 5
    total_items = len(buy_transactions)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_tx = buy_transactions[start_index:end_index]

    # --- Name Resolution ---
    type_ids = [tx['type_id'] for tx in paginated_tx]
    location_ids = [tx['location_id'] for tx in paginated_tx]
    id_to_name = get_names_from_ids(list(set(type_ids + location_ids)), character)

    # --- Message Formatting ---
    header = f"🧾 *Historical Buys for {character.name}*\n"
    message_lines = []
    for tx in paginated_tx:
        item_name = id_to_name.get(tx['type_id'], f"Type ID {tx['type_id']}")
        location_name = id_to_name.get(tx['location_id'], f"Location ID {tx['location_id']}")
        date_str = datetime.fromisoformat(tx['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
        total_value = tx['quantity'] * tx['unit_price']

        line = (
            f"*{item_name}*\n"
            f"  *Date:* `{date_str}`\n"
            f"  *Qty:* `{tx['quantity']:,}` @ `{tx['unit_price']:,.2f}`\n"
            f"  *Total Cost:* `{total_value:,.2f}` ISK\n"
            f"  *Location:* `{location_name}`"
        )
        message_lines.append(line)

    # --- Keyboard ---
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"history_list_buy_{character_id}_{page - 1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"history_list_buy_{character_id}_{page + 1}"))

    if nav_row: keyboard.append(nav_row)

    user_characters = get_characters_for_user(user_id)
    back_callback = "buys" if len(user_characters) > 1 else "start_command"
    keyboard.append([InlineKeyboardButton("« Back", callback_data=back_callback)])
    reply_markup = InlineKeyboardMarkup(keyboard)

    full_message = header + "\n".join(message_lines)
    return full_message, json.dumps(reply_markup.to_dict()), "success"


def prepare_character_info_data(character_id: int):
    """
    Fetches and prepares all data and the composite image for the character info display.
    This is a synchronous, data-intensive function designed to be called from a Celery task.
    Returns a tuple of (caption, image_bytes, reply_markup_json, status).
    Status can be 'success', 'no_character', 'error'.
    """
    character = get_character_by_id(character_id)
    if not character:
        return None, None, None, "no_character"

    # --- ESI Calls ---
    public_info = get_character_public_info(character.id)
    online_status = get_character_online_status(character)

    if not public_info:
        return f"❌ Could not fetch public info for {character.name}.", None, None, "error"

    corp_info = get_corporation_info(public_info['corporation_id'])
    alliance_info = None
    if 'alliance_id' in public_info:
        alliance_info = get_alliance_info(public_info['alliance_id'])

    # --- Formatting ---
    char_name = public_info.get('name', character.name)
    try:
        birthday = datetime.fromisoformat(public_info['birthday'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
    except (ValueError, KeyError):
        birthday = "Unknown"
    security_status = f"{public_info.get('security_status', 0):.2f}"
    corp_name = corp_info.get('name', 'Unknown Corporation') if corp_info else 'Unknown Corporation'

    caption_lines = [
        f"*{char_name}*",
        f"`Character ID: {character.id}`",
        f"Birthday: {birthday}",
        f"Security Status: {security_status}",
        f"Corporation: {corp_name}",
    ]

    if alliance_info:
        alliance_name = alliance_info.get('name', 'Unknown Alliance')
        caption_lines.append(f"Alliance: {alliance_name}")

    if online_status:
        status_text = "🟢 Online" if online_status.get('online') else "🔴 Offline"
        login_count = online_status.get('logins', 'N/A')
        caption_lines.append(f"Status: {status_text}")
        caption_lines.append(f"Total Logins: {login_count:,}")

    caption = "\n".join(caption_lines)

    # --- Image Composition ---
    image_buffer = _create_character_info_image(
        character.id,
        public_info['corporation_id'],
        public_info.get('alliance_id')
    )
    image_bytes = image_buffer.getvalue() if image_buffer else None

    # --- Keyboard ---
    back_callback = f"settings_char_{character.id}"
    keyboard = [[InlineKeyboardButton("« Back", callback_data=back_callback)]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    return caption, image_bytes, json.dumps(reply_markup.to_dict()), "success"


def prepare_contracts_data(character_id: int, user_id: int, page: int = 0):
    """
    Fetches and prepares a paginated list of outstanding contracts from the local cache.
    This is a synchronous, data-intensive function designed to be called from a Celery task.
    Returns a tuple of (message_text, reply_markup_json, status).
    Status can be 'success', 'no_character', 'no_contracts'.
    """
    character = get_character_by_id(character_id)
    if not character:
        return None, None, "no_character"

    # --- Data Fetching & Filtering ---
    all_contracts = get_contracts_from_db(character.id)
    outstanding_contracts = [c for c in all_contracts if c.get('status') == 'outstanding']

    if not outstanding_contracts:
        back_callback = "start_command"
        keyboard = [[InlineKeyboardButton("« Back", callback_data=back_callback)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"📝 *Outstanding Contracts for {character.name}*\n\nNo outstanding contracts found."
        return message, json.dumps(reply_markup.to_dict()), "no_contracts"

    outstanding_contracts.sort(key=lambda x: datetime.fromisoformat(x['date_issued'].replace('Z', '+00:00')), reverse=True)

    # --- Pagination ---
    items_per_page = 5
    total_items = len(outstanding_contracts)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))
    start_index = page * items_per_page
    end_index = start_index + items_per_page
    paginated_contracts = outstanding_contracts[start_index:end_index]

    # --- Name Resolution ---
    ids_to_resolve = set()
    for c in paginated_contracts:
        ids_to_resolve.add(c['issuer_id'])
        if c.get('assignee_id'):
            ids_to_resolve.add(c['assignee_id'])
        if c.get('start_location_id'):
            ids_to_resolve.add(c['start_location_id'])
        if c.get('end_location_id'):
            ids_to_resolve.add(c['end_location_id'])
    id_to_name = get_names_from_ids(list(ids_to_resolve), character)

    # --- Message Formatting ---
    header = f"📝 *Outstanding Contracts for {character.name}*\n"
    message_lines = []
    for contract in paginated_contracts:
        contract_type = contract['type'].replace('_', ' ').title()
        issuer_name = id_to_name.get(contract['issuer_id'], 'Unknown')

        line = f"*{contract_type}* from `{issuer_name}`"
        if contract.get('title'):
            line += f"\n  *Title:* `{contract['title']}`"
        if contract.get('assignee_id'):
            assignee_name = id_to_name.get(contract['assignee_id'], 'Unknown')
            line += f"\n  *To:* `{assignee_name}`"
        if contract.get('start_location_id'):
            location_name = id_to_name.get(contract['start_location_id'], 'Unknown')
            line += f"\n  *Location:* `{location_name}`"
        if contract.get('reward', 0) > 0:
            line += f"\n  *Reward:* `{contract['reward']:,.2f}` ISK"
        if contract.get('collateral', 0) > 0:
            line += f"\n  *Collateral:* `{contract['collateral']:,.2f}` ISK"
        try:
            expires_dt = datetime.fromisoformat(contract['date_expired'].replace('Z', '+00:00'))
            time_left = expires_dt - datetime.now(timezone.utc)
            if time_left.total_seconds() <= 0:
                expires_str = "Expired"
            else:
                days = time_left.days
                hours = time_left.seconds // 3600
                expires_str = f"{days}d {hours}h"
            line += f"\n  *Expires in:* `{expires_str}`"
        except (ValueError, KeyError):
            pass
        message_lines.append(line)

    # --- Keyboard ---
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("« Prev", callback_data=f"contracts_list_{character_id}_{page - 1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next »", callback_data=f"contracts_list_{character_id}_{page + 1}"))
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton("« Back", callback_data="start_command")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    full_message = header + "\n\n".join(message_lines)
    return full_message, json.dumps(reply_markup.to_dict()), "success"