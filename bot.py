import requests
import telegram
import time
import schedule
import logging
import os
from collections import defaultdict
from config import ESI_CLIENT_ID, ESI_SECRET_KEY, ESI_REFRESH_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- File-based Persistence ---

DATA_DIR = "data"
PROCESSED_TRANSACTIONS_FILE = os.path.join(DATA_DIR, "processed_transactions.dat")

def load_processed_transactions():
    """Loads the set of processed transaction IDs from a file."""
    try:
        with open(PROCESSED_TRANSACTIONS_FILE, "r") as f:
            return {int(line.strip()) for line in f}
    except FileNotFoundError:
        return set()

def save_processed_transactions(processed_ids):
    """Saves the set of processed transaction IDs to a file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PROCESSED_TRANSACTIONS_FILE, "w") as f:
        for transaction_id in processed_ids:
            f.write(f"{transaction_id}\n")

# --- ESI API Functions ---

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

def get_filled_orders(access_token, character_id):
    """Fetches the character's filled sales orders."""
    if not character_id:
        return []
    url = f"https://esi.evetech.net/v1/characters/{character_id}/wallet/transactions/"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"datasource": "tranquility"}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return [tx for tx in response.json() if not tx.get('is_buy')]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching filled orders: {e}")
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

    filled_orders = get_filled_orders(access_token, character_id)
    processed_transactions = load_processed_transactions()

    new_orders = [o for o in filled_orders if o['transaction_id'] not in processed_transactions]

    if not new_orders:
        logging.info("No new sales orders found."); return

    logging.info(f"Found {len(new_orders)} new sales orders. Grouping and processing...")

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
    save_processed_transactions(processed_transactions.union(all_new_transaction_ids))

    logging.info(f"Check complete. Processed {len(all_new_transaction_ids)} new orders in {len(grouped_sales)} groups.")

if __name__ == "__main__":
    logging.info("Bot starting up...")

    logging.info("Performing initial check...")
    import asyncio
    asyncio.run(check_for_new_orders())

    schedule.every(5).minutes.do(lambda: asyncio.run(check_for_new_orders()))
    logging.info("Scheduler started. Will check for new orders every 5 minutes.")

    while True:
        schedule.run_pending()
        time.sleep(1)