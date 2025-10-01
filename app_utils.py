import logging
import os
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import asyncio
import requests
import io
from PIL import Image

import database
from tasks import continue_backfill_character_history

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
    enable_buy_notifications: bool
    enable_daily_overview: bool
    enable_undercut_notifications: bool
    enable_contract_notifications: bool
    notification_batch_threshold: int
    created_at: datetime
    is_backfilling: bool
    backfill_before_id: int | None

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
                    enable_sales_notifications, enable_buy_notifications,
                    enable_daily_overview, enable_undercut_notifications,
                    enable_contract_notifications, notification_batch_threshold, created_at,
                    is_backfilling, backfill_before_id
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
            is_backfilling, backfill_before_id
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
            enable_buy_notifications=bool(enable_buys),
            enable_daily_overview=bool(enable_overview),
            enable_undercut_notifications=bool(enable_undercut),
            enable_contract_notifications=bool(enable_contracts),
            notification_batch_threshold=batch_threshold,
            created_at=created_at,
            is_backfilling=bool(is_backfilling),
            backfill_before_id=backfill_before_id
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
                    wallet_balance_threshold BIGINT DEFAULT 0,
                    enable_sales_notifications BOOLEAN DEFAULT TRUE,
                    enable_buy_notifications BOOLEAN DEFAULT TRUE,
                    enable_daily_overview BOOLEAN DEFAULT TRUE,
                    enable_undercut_notifications BOOLEAN DEFAULT TRUE,
                    notification_batch_threshold INTEGER DEFAULT 3,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('UTC', now()),
                    needs_update_notification BOOLEAN DEFAULT FALSE,
                    deletion_scheduled_at TIMESTAMP WITH TIME ZONE
                )
            """)

            # Migration: Add enable_undercut_notifications column if it doesn't exist
            # Migration: Rename enable_daily_summary to enable_daily_overview
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'characters' AND column_name = 'enable_daily_summary'")
            if cursor.fetchone():
                logging.info("Applying migration: Renaming 'enable_daily_summary' to 'enable_daily_overview'...")
                cursor.execute("ALTER TABLE characters RENAME COLUMN enable_daily_summary TO enable_daily_overview;")
                logging.info("Migration for 'enable_daily_overview' complete.")

            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'characters' AND column_name = 'enable_undercut_notifications'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'enable_undercut_notifications' column to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN enable_undercut_notifications BOOLEAN DEFAULT TRUE;")
                logging.info("Migration for 'enable_undercut_notifications' complete.")

            # Migration: Add deletion_scheduled_at column if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'characters' AND column_name = 'deletion_scheduled_at'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'deletion_scheduled_at' column to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN deletion_scheduled_at TIMESTAMP WITH TIME ZONE;")
                logging.info("Migration for 'deletion_scheduled_at' complete.")

            # Migration: Add needs_update_notification column if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'characters' AND column_name = 'needs_update_notification'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'needs_update_notification' column to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN needs_update_notification BOOLEAN DEFAULT FALSE;")
                logging.info("Migration for 'needs_update_notification' complete.")

            # Migration: Add enable_contract_notifications column if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'characters' AND column_name = 'enable_contract_notifications'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'enable_contract_notifications' column to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN enable_contract_notifications BOOLEAN DEFAULT TRUE;")
                logging.info("Migration for 'enable_contract_notifications' complete.")

            # Migration: Add is_backfilling and backfill_before_id columns for gradual history backfill
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'characters' AND column_name = 'is_backfilling'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'is_backfilling' column to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN is_backfilling BOOLEAN DEFAULT FALSE;")
                logging.info("Migration for 'is_backfilling' complete.")

            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'characters' AND column_name = 'backfill_before_id'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'backfill_before_id' column to characters table...")
                cursor.execute("ALTER TABLE characters ADD COLUMN backfill_before_id BIGINT;")
                logging.info("Migration for 'backfill_before_id' complete.")


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
            # Migration: Add price column to market_orders if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'market_orders' AND column_name = 'price'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'price' column to market_orders table...")
                # Add the column and set a default that will be updated on the next poll
                cursor.execute("ALTER TABLE market_orders ADD COLUMN price NUMERIC(17, 2) DEFAULT 0.0;")
                logging.info("Migration for 'price' complete.")

            # Migration: Add order_data column to market_orders if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'market_orders' AND column_name = 'order_data'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'order_data' column to market_orders table...")
                cursor.execute("ALTER TABLE market_orders ADD COLUMN order_data JSONB;")
                logging.info("Migration for 'order_data' complete.")

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
                    generated_at TIMESTAMP WITH TIME ZONE NOT NULL
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

            # Migration: Add competitor_price column to undercut_statuses if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'undercut_statuses' AND column_name = 'competitor_price'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'competitor_price' column to undercut_statuses table...")
                cursor.execute("ALTER TABLE undercut_statuses ADD COLUMN competitor_price NUMERIC(17, 2);")
                logging.info("Migration for 'competitor_price' complete.")

            # Migration: Add competitor_location_id column to undercut_statuses if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'undercut_statuses' AND column_name = 'competitor_location_id'")
            if not cursor.fetchone():
                logging.info("Applying migration: Adding 'competitor_location_id' column to undercut_statuses table...")
                cursor.execute("ALTER TABLE undercut_statuses ADD COLUMN competitor_location_id BIGINT;")
                logging.info("Migration for 'competitor_location_id' complete.")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chart_cache (
                    chart_key TEXT PRIMARY KEY,
                    character_id INTEGER NOT NULL,
                    chart_data BYTEA NOT NULL,
                    generated_at TIMESTAMP WITH TIME ZONE NOT NULL
                )
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


def get_processed_journal_refs(character_id):
    """Retrieves all processed journal ref IDs for a character from the database."""
    conn = database.get_db_connection()
    processed_ids = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ref_id FROM historical_journal WHERE character_id = %s", (character_id,))
            processed_ids = {row[0] for row in cursor.fetchall()}
    finally:
        database.release_db_connection(conn)
    return processed_ids


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
                    enable_sales_notifications, enable_buy_notifications,
                    enable_daily_overview, enable_undercut_notifications,
                    enable_contract_notifications, notification_batch_threshold, created_at,
                    is_backfilling, backfill_before_id
                FROM characters WHERE telegram_user_id = %s AND deletion_scheduled_at IS NULL
            """, (telegram_user_id,))
            rows = cursor.fetchall()
            for row in rows:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    wallet_balance_threshold,
                    enable_sales, enable_buys,
                    enable_overview, enable_undercut, enable_contracts, batch_threshold, created_at,
                    is_backfilling, backfill_before_id
                ) = row

                user_characters.append(Character(
                    id=char_id, name=name, refresh_token=refresh_token,
                    telegram_user_id=telegram_user_id,
                    notifications_enabled=bool(notifications_enabled),
                    wallet_balance_threshold=wallet_balance_threshold,
                    enable_sales_notifications=bool(enable_sales),
                    enable_buy_notifications=bool(enable_buys),
                    enable_daily_overview=bool(enable_overview),
                    enable_undercut_notifications=bool(enable_undercut),
                    enable_contract_notifications=bool(enable_contracts),
                    notification_batch_threshold=batch_threshold,
                    created_at=created_at,
                    is_backfilling=bool(is_backfilling),
                    backfill_before_id=backfill_before_id
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
                    enable_sales_notifications, enable_buy_notifications,
                    enable_daily_overview, enable_undercut_notifications,
                    enable_contract_notifications, notification_batch_threshold, created_at,
                    is_backfilling, backfill_before_id
                FROM characters WHERE character_id = %s
            """, (character_id,))
            row = cursor.fetchone()

            if row:
                (
                    char_id, name, refresh_token, telegram_user_id, notifications_enabled,
                    wallet_balance_threshold,
                    enable_sales, enable_buys,
                    enable_overview, enable_undercut, enable_contracts, batch_threshold, created_at,
                    is_backfilling, backfill_before_id
                ) = row

                character = Character(
                    id=char_id, name=name, refresh_token=refresh_token,
                    telegram_user_id=telegram_user_id,
                    notifications_enabled=bool(notifications_enabled),
                    wallet_balance_threshold=wallet_balance_threshold,
                    enable_sales_notifications=bool(enable_sales),
                    enable_buy_notifications=bool(enable_buys),
                    enable_daily_overview=bool(enable_overview),
                    enable_undercut_notifications=bool(enable_undercut),
                    enable_contract_notifications=bool(enable_contracts),
                    notification_batch_threshold=batch_threshold,
                    created_at=created_at,
                    is_backfilling=bool(is_backfilling),
                    backfill_before_id=backfill_before_id
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
        "buys": "enable_buy_notifications",
        "overview": "enable_daily_overview",
        "undercut": "enable_undercut_notifications",
        "contracts": "enable_contract_notifications"
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


def get_characters_needing_update():
    """Retrieves all characters that have been newly added or updated and need a notification."""
    conn = database.get_db_connection()
    character_ids = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT character_id FROM characters WHERE needs_update_notification = TRUE"
            )
            character_ids = [row[0] for row in cursor.fetchall()]
    finally:
        database.release_db_connection(conn)
    return character_ids


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


def get_contracts_from_db(character_id: int) -> list:
    """Retrieves all cached contracts for a character from the database."""
    conn = database.get_db_connection()
    contracts = []
    try:
        with conn.cursor() as cursor:
            # The contract_data column is of type JSONB, so psycopg2 will automatically parse it into a dict.
            cursor.execute("SELECT contract_data FROM contracts WHERE character_id = %s", (character_id,))
            contracts = [row[0] for row in cursor.fetchall()]
    finally:
        database.release_db_connection(conn)
    return contracts


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
    import psycopg2
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
    """Retrieves a cached chart from the database."""
    conn = database.get_db_connection()
    chart_data = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT chart_data FROM chart_cache WHERE chart_key = %s", (chart_key,))
            row = cursor.fetchone()
            if row:
                chart_data = row[0]
    finally:
        database.release_db_connection(conn)
    return chart_data


def save_chart_to_cache(chart_key: str, character_id: int, chart_data: bytes):
    """Saves or updates a chart in the database cache."""
    import psycopg2
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            binary_data = psycopg2.Binary(chart_data)
            cursor.execute(
                """
                INSERT INTO chart_cache (chart_key, character_id, chart_data, generated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (chart_key) DO UPDATE SET
                    chart_data = EXCLUDED.chart_data,
                    generated_at = EXCLUDED.generated_at;
                """,
                (chart_key, character_id, binary_data, datetime.now(timezone.utc))
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


def get_undercut_statuses(character_id: int) -> dict[int, dict]:
    """
    Retrieves the last known undercut status, competitor price, and competitor location for all of a character's orders.
    Returns a dict mapping order_id to {'is_undercut': bool, 'competitor_price': float|None, 'competitor_location_id': int|None}.
    """
    conn = database.get_db_connection()
    statuses = {}
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT order_id, is_undercut, competitor_price, competitor_location_id FROM undercut_statuses WHERE character_id = %s",
                (character_id,)
            )
            for row in cursor.fetchall():
                order_id, is_undercut, competitor_price, competitor_location_id = row
                statuses[order_id] = {
                    'is_undercut': is_undercut,
                    # Convert Decimal from DB to float, or keep it as None
                    'competitor_price': float(competitor_price) if competitor_price is not None else None,
                    'competitor_location_id': competitor_location_id
                }
    finally:
        database.release_db_connection(conn)
    return statuses

def update_undercut_statuses(character_id: int, statuses: list[dict]):
    """Inserts or updates the undercut status, competitor price, and location for a list of orders."""
    if not statuses:
        return
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            # `statuses` is a list of dicts: [{'order_id': X, 'is_undercut': Y, 'competitor_price': Z, 'competitor_location_id': A}, ...]
            data_to_insert = [
                (s['order_id'], character_id, s['is_undercut'], s.get('competitor_price'), s.get('competitor_location_id')) for s in statuses
            ]
            upsert_query = """
                INSERT INTO undercut_statuses (order_id, character_id, is_undercut, competitor_price, competitor_location_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id, character_id) DO UPDATE
                SET is_undercut = EXCLUDED.is_undercut,
                    competitor_price = EXCLUDED.competitor_price,
                    competitor_location_id = EXCLUDED.competitor_location_id;
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
ACCESS_TOKEN_CACHE = {}

def get_access_token(character_id, refresh_token):
    """
    Retrieves a valid access token for a character, using an in-memory cache
    to avoid redundant requests.
    """
    if character_id in ACCESS_TOKEN_CACHE:
        token_info = ACCESS_TOKEN_CACHE[character_id]
        # Check if the token is still valid (with a 60-second buffer)
        if token_info['expires_at'] > time.time() + 60:
            logging.debug(f"Returning cached access token for character {character_id}")
            return token_info['access_token']

    logging.info(f"No valid cached token for character {character_id}. Requesting a new one.")
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
        expires_in = token_data.get("expires_in", 1200) # Default to 20 minutes

        # Cache the new token with its expiration time
        ACCESS_TOKEN_CACHE[character_id] = {
            'access_token': access_token,
            'expires_at': time.time() + expires_in
        }
        logging.info(f"Successfully obtained and cached new access token for character {character_id}")
        return access_token
    except requests.exceptions.RequestException as e:
        logging.error(f"Error refreshing access token for character {character_id}: {e}")
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

def get_character_skills(character, force_revalidate=False):
    """Fetches a character's skills from ESI."""
    if not character: return None
    url = f"https://esi.evetech.net/v4/characters/{character.id}/skills/"
    return make_esi_request(url, character=character, force_revalidate=force_revalidate)

def get_wallet_balance(character, return_headers=False, force_revalidate=False):
    if not character: return None
    url = f"https://esi.evetech.net/v1/characters/{character.id}/wallet/"
    return make_esi_request(url, character=character, return_headers=return_headers, force_revalidate=force_revalidate)

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


async def _resolve_location_to_system_id(location_id: int, character: Character) -> int | None:
    """
    Resolves a location_id (station or structure) to a solar_system_id.
    Uses the database cache first, then falls back to ESI.
    """
    # 1. Check our own location_cache first
    cached_location = await asyncio.to_thread(get_location_from_cache, location_id)
    if cached_location and cached_location.get('system_id'):
        return cached_location['system_id']

    # 2. If not in cache, resolve via ESI
    system_id = None
    region_id = None # We'll try to get this to populate the cache fully

    # Resolve for structures (requires auth)
    if location_id > 10000000000:
        structure_info = await asyncio.to_thread(get_structure_info, character, location_id)
        if structure_info:
            system_id = structure_info.get('solar_system_id')
    # Resolve for NPC stations (public)
    else:
        station_info = await asyncio.to_thread(get_station_info, location_id)
        if station_info:
            system_id = station_info.get('system_id')

    # Get region from system to fully populate the cache item
    if system_id:
        system_info = await asyncio.to_thread(get_system_info, system_id)
        if system_info:
            constellation_id = system_info.get('constellation_id')
            if constellation_id:
                constellation_info = await asyncio.to_thread(get_constellation_info, constellation_id)
                if constellation_info:
                    region_id = constellation_info.get('region_id')

    # 3. Save to cache if we successfully resolved everything
    if location_id and system_id and region_id:
        await asyncio.to_thread(save_location_to_cache, location_id, system_id, region_id)

    return system_id


async def get_jump_distance(origin_location_id: int, destination_location_id: int, character: Character) -> int | None:
    """
    Calculates the jump distance between two locations (stations or structures).
    Uses a database cache to store and retrieve system-to-system jump counts.
    """
    # Step 1: Resolve both locations to their solar system IDs
    origin_system_id = await _resolve_location_to_system_id(origin_location_id, character)
    destination_system_id = await _resolve_location_to_system_id(destination_location_id, character)

    if not origin_system_id or not destination_system_id:
        logging.warning(f"Could not resolve one or both system IDs for jump calculation: {origin_location_id} -> {destination_location_id}")
        return None

    if origin_system_id == destination_system_id:
        return 0

    # Step 2: Check the database cache for the jump distance
    cached_jumps = await asyncio.to_thread(get_jump_distance_from_db, origin_system_id, destination_system_id)
    if cached_jumps is not None:
        logging.debug(f"Found cached jump distance from {origin_system_id} to {destination_system_id}: {cached_jumps} jumps.")
        return cached_jumps

    # Step 3: If not cached, calculate it via ESI
    logging.info(f"No cached jump distance found. Calculating route from {origin_system_id} to {destination_system_id} via ESI.")
    route = await asyncio.to_thread(get_route, origin_system_id, destination_system_id)

    if route is None:
        logging.error(f"Failed to get route from ESI for {origin_system_id} -> {destination_system_id}")
        return None

    # ESI returns a list of system IDs in the route. Number of jumps is len - 1.
    jumps = len(route) - 1

    # Step 4: Save the newly calculated distance to the cache for future use
    await asyncio.to_thread(save_jump_distance_to_db, origin_system_id, destination_system_id, jumps)
    logging.info(f"Calculated and cached {jumps} jumps from {origin_system_id} to {destination_system_id}.")

    return jumps


def get_cached_chart(chart_key: str):
    """Retrieves a cached chart from the database."""
    conn = database.get_db_connection()
    chart_data = None
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT chart_data FROM chart_cache WHERE chart_key = %s", (chart_key,))
            row = cursor.fetchone()
            if row:
                chart_data = row[0]
    finally:
        database.release_db_connection(conn)
    return chart_data


def save_chart_to_cache(chart_key: str, character_id: int, chart_data: bytes):
    """Saves or updates a chart in the database cache."""
    import psycopg2
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            binary_data = psycopg2.Binary(chart_data)
            cursor.execute(
                """
                INSERT INTO chart_cache (chart_key, character_id, chart_data, generated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (chart_key) DO UPDATE SET
                    chart_data = EXCLUDED.chart_data,
                    generated_at = EXCLUDED.generated_at;
                """,
                (chart_key, character_id, binary_data, datetime.now(timezone.utc))
            )
            conn.commit()
    finally:
        database.release_db_connection(conn)


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
    import psycopg2
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

def _create_character_info_image(character_id: int, corporation_id: int, alliance_id: int | None) -> io.BytesIO | None:
    """
    Creates a composite image with character, corp, and alliance portraits.
    Returns a BytesIO buffer with the PNG image, or None on failure.
    """
    try:
        # Define image URLs
        char_portrait_url = f"https://images.evetech.net/characters/{character_id}/portrait?size=256"
        corp_logo_url = f"https://images.evetech.net/corporations/{corporation_id}/logo?size=128"
        alliance_logo_url = f"https://images.evetech.net/alliances/{alliance_id}/logo?size=128" if alliance_id else None

        # Download images
        char_img_data = get_cached_image(char_portrait_url)
        corp_img_data = get_cached_image(corp_logo_url)
        alliance_img_data = get_cached_image(alliance_logo_url) if alliance_logo_url else None

        if not char_img_data:
            logging.error(f"Could not download character portrait for {character_id}.")
            return None

        # Open images
        char_img = Image.open(io.BytesIO(char_img_data)).convert("RGBA")
        corp_img = Image.open(io.BytesIO(corp_img_data)).convert("RGBA") if corp_img_data else None
        alliance_img = Image.open(io.BytesIO(alliance_img_data)).convert("RGBA") if alliance_img_data else None

        # Create base image
        base_width = 256
        base_height = 256
        base_img = Image.new('RGBA', (base_width, base_height), (0, 0, 0, 0))

        # Paste character portrait
        base_img.paste(char_img, (0, 0))

        # Paste corporation logo (bottom left)
        if corp_img:
            corp_img.thumbnail((64, 64))
            base_img.paste(corp_img, (5, base_height - 64 - 5), corp_img)

        # Paste alliance logo (bottom right)
        if alliance_img:
            alliance_img.thumbnail((96, 96))
            base_img.paste(alliance_img, (base_width - 96 - 5, base_height - 96 - 5), alliance_img)

        # Save to buffer
        img_buffer = io.BytesIO()
        base_img.save(img_buffer, format='PNG')
        img_buffer.seek(0)
        return img_buffer

    except Exception as e:
        logging.error(f"Error creating character info image for {character_id}: {e}", exc_info=True)
        return None


# --- Data Polling and Processing ---

async def poll_wallet_journal(character: Character, bot):
    """Polls the wallet journal for a character and sends notifications for new entries."""
    if not character.notifications_enabled:
        return None

    journal_entries, headers = get_wallet_journal(character, return_headers=True)
    if journal_entries is None:
        return get_next_run_delay(headers)

    processed_refs = get_processed_journal_refs(character.id)
    new_entries = [entry for entry in journal_entries if entry['id'] not in processed_refs]

    if new_entries:
        logging.info(f"Found {len(new_entries)} new journal entries for {character.name}.")
        # Further logic to format and send messages would go here.
        add_processed_journal_refs(character.id, [e['id'] for e in new_entries])

    return get_next_run_delay(headers)

async def poll_wallet_transactions(character: Character, bot):
    """Polls wallet transactions for a character, processing sales and buys."""
    transactions, headers = get_wallet_transactions(character, return_headers=True)
    if transactions is None:
        return get_next_run_delay(headers)

    # Logic to process new transactions would go here.
    # e.g., calculate profit on sales, add buys to purchase lots.

    return get_next_run_delay(headers)

async def poll_order_history(character: Character, bot):
    """Polls historical market orders to keep the 'processed_orders' cache up to date."""
    history, headers = get_market_orders_history(character, return_headers=True)
    if history is None:
        return get_next_run_delay(headers)

    processed_ids = get_processed_orders(character.id)
    new_historical_orders = [o for o in history if o['order_id'] not in processed_ids]
    if new_historical_orders:
        add_processed_orders(character.id, [o['order_id'] for o in new_historical_orders])
        logging.info(f"Added {len(new_historical_orders)} new historical orders to cache for {character.name}")

    return get_next_run_delay(headers)


async def poll_market_orders(character: Character, bot):
    """Polls current market orders, compares with cache, and sends notifications."""
    open_orders, headers = get_market_orders(character, return_headers=True, force_revalidate=True)
    if open_orders is None:
        return get_next_run_delay(headers)

    cached_orders = {o['order_id']: o for o in get_tracked_market_orders(character.id)}
    live_order_ids = {o['order_id'] for o in open_orders}

    # Logic to compare and find filled/cancelled orders would go here.

    update_tracked_market_orders(character.id, open_orders)
    stale_order_ids = list(cached_orders.keys() - live_order_ids)
    if stale_order_ids:
        remove_tracked_market_orders(character.id, stale_order_ids)


    return get_next_run_delay(headers)

async def poll_contracts(character: Character, bot):
    """Polls contracts, compares with cache, and sends notifications."""
    contracts, headers = get_contracts(character, return_headers=True)
    if contracts is None:
        return get_next_run_delay(headers)

    processed_ids = get_processed_contracts(character.id)
    new_contracts = [c for c in contracts if c['contract_id'] not in processed_ids]

    if new_contracts:
         # Logic to send notifications for new contracts would go here.
        add_processed_contracts(character.id, [c['contract_id'] for c in new_contracts])

    update_contracts_cache(character.id, contracts)
    current_contract_ids = [c['contract_id'] for c in contracts]
    remove_stale_contracts(character.id, current_contract_ids)

    return get_next_run_delay(headers)


async def master_wallet_journal_poll(application):
    """Master polling loop for wallet journals."""
    await asyncio.sleep(10) # Initial delay
    while True:
        delay = 600 # Default delay
        try:
            if CHARACTERS:
                # Get delay from the first character, assume it's similar for others
                _, headers = get_wallet_journal(CHARACTERS[0], return_headers=True)
                delay = get_next_run_delay(headers)
                for character in CHARACTERS:
                    await poll_wallet_journal(character, application.bot)
        except Exception as e:
            logging.error(f"Error in master_wallet_journal_poll: {e}", exc_info=True)
        await asyncio.sleep(delay)

async def master_wallet_transaction_poll(application):
    """Master polling loop for wallet transactions."""
    await asyncio.sleep(15) # Initial delay
    while True:
        delay = 600 # Default delay
        try:
            if CHARACTERS:
                _, headers = get_wallet_transactions(CHARACTERS[0], return_headers=True)
                delay = get_next_run_delay(headers)
                for character in CHARACTERS:
                    await poll_wallet_transactions(character, application.bot)
        except Exception as e:
            logging.error(f"Error in master_wallet_transaction_poll: {e}", exc_info=True)
        await asyncio.sleep(delay)

async def master_order_history_poll(application):
    """Master polling loop for order history."""
    await asyncio.sleep(20) # Initial delay
    while True:
        delay = 600 # Default delay
        try:
            if CHARACTERS:
                _, headers = get_market_orders_history(CHARACTERS[0], return_headers=True)
                delay = get_next_run_delay(headers)
                for character in CHARACTERS:
                    await poll_order_history(character, application.bot)
        except Exception as e:
            logging.error(f"Error in master_order_history_poll: {e}", exc_info=True)
        await asyncio.sleep(delay)

async def master_orders_poll(application):
    """Master polling loop for current market orders."""
    await asyncio.sleep(5) # Initial delay
    while True:
        delay = 300 # Default delay
        try:
            if CHARACTERS:
                _, headers = get_market_orders(CHARACTERS[0], return_headers=True)
                delay = get_next_run_delay(headers)
                for character in CHARACTERS:
                    await poll_market_orders(character, application.bot)
        except Exception as e:
            logging.error(f"Error in master_orders_poll: {e}", exc_info=True)
        await asyncio.sleep(delay)


async def master_check_new_characters_poll(application):
    """Periodically checks for newly added characters and sends them a welcome message."""
    while True:
        await asyncio.sleep(5)  # Check every 5 seconds
        try:
            new_character_ids = get_characters_needing_update()
            if not new_character_ids:
                continue

            logging.info(f"Found {len(new_character_ids)} new character(s) to process.")
            load_characters_from_db()  # Reload all characters to get the new ones

            for char_id in new_character_ids:
                character = get_character_by_id(char_id)
                if not character:
                    logging.error(f"Could not find new character {char_id} in memory after reloading.")
                    continue

                # Perform initial data seeding in a separate thread to avoid blocking
                seed_success = await asyncio.to_thread(seed_data_for_character, character)

                # Send welcome message
                if seed_success:
                    welcome_message = (
                        f"🎉 Welcome, {character.name}! 🎉\n\n"
                        "I've successfully added your character and performed an initial sync of your market data. "
                        "You can now use the main menu to get started. Try /start"
                    )
                else:
                    welcome_message = (
                        f"⚠️ Welcome, {character.name}! ⚠️\n\n"
                        "I've added your character, but encountered an error during the initial data sync. "
                        "Some features may not work correctly. Please try again later or contact support."
                    )

                # Find the original message and edit it, or send a new one
                prompt_state = get_bot_state(f"add_character_prompt_{character.telegram_user_id}")
                if prompt_state:
                    chat_id, message_id = map(int, prompt_state.split(':'))
                    try:
                        await application.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=welcome_message,
                            parse_mode='Markdown'
                        )
                    except Exception:
                         await application.bot.send_message(
                            chat_id=character.telegram_user_id,
                            text=welcome_message,
                            parse_mode='Markdown'
                        )
                else:
                    await application.bot.send_message(
                        chat_id=character.telegram_user_id,
                        text=welcome_message,
                        parse_mode='Markdown'
                    )

                # Reset the flag so we don't process them again
                reset_update_notification_flag(character.id)
                logging.info(f"Successfully processed and welcomed new character {character.name}.")

        except Exception as e:
            logging.error(f"Error in master_check_new_characters_poll: {e}", exc_info=True)


async def master_contracts_poll(application):
    """Master polling loop for contracts."""
    await asyncio.sleep(25) # Initial delay
    while True:
        delay = 600 # Default delay
        try:
            if CHARACTERS:
                _, headers = get_contracts(CHARACTERS[0], return_headers=True)
                delay = get_next_run_delay(headers)
                for character in CHARACTERS:
                    await poll_contracts(character, application.bot)
        except Exception as e:
            logging.error(f"Error in master_contracts_poll: {e}", exc_info=True)
        await asyncio.sleep(delay)


def _calculate_overview_data(character: Character):
    """Calculates the data needed for the daily overview."""
    now = datetime.now(timezone.utc)
    one_day_ago = now - timedelta(days=1)

    balance = get_last_known_wallet_balance(character)
    transactions = get_historical_transactions_from_db(character.id)

    sales_24h = [tx for tx in transactions if not tx['is_buy'] and datetime.fromisoformat(tx['date']) > one_day_ago]
    buys_24h = [tx for tx in transactions if tx['is_buy'] and datetime.fromisoformat(tx['date']) > one_day_ago]

    total_sales = sum(tx['quantity'] * tx['unit_price'] for tx in sales_24h)
    total_buys = sum(tx['quantity'] * tx['unit_price'] for tx in buys_24h)

    # Simplified profit calculation for the overview
    profit = total_sales - total_buys

    return {
        "balance": balance,
        "total_sales": total_sales,
        "total_buys": total_buys,
        "profit": profit,
        "sales_count": len(sales_24h),
        "buys_count": len(buys_24h)
    }

def _format_overview_message(overview_data, character):
    """Formats the overview data into a message and keyboard."""
    message = (
        f"📊 *Daily Overview for {character.name}*\n\n"
        f"💰 Wallet Balance: `{overview_data['balance']:,.2f} ISK`\n\n"
        f"*Last 24 Hours:*\n"
        f"📈 Total Sales: `{overview_data['total_sales']:,.2f} ISK` ({overview_data['sales_count']} transactions)\n"
        f"🛒 Total Buys: `{overview_data['total_buys']:,.2f} ISK` ({overview_data['buys_count']} transactions)\n"
        f"💸 Est. Profit: `{overview_data['profit']:,.2f} ISK`"
    )

    keyboard = [
        [
            InlineKeyboardButton("Sales Chart (24h)", callback_data=f"chart_sales_24h_{character.id}"),
            InlineKeyboardButton("Profit Chart (7d)", callback_data=f"chart_profit_7d_{character.id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    return message, reply_markup


def resolve_location_to_region(location_id, character):
    return 10000002 # The Forge, for now