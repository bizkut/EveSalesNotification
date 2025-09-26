import requests
import telegram
import time
import schedule
import logging
from config import ESI_CLIENT_ID, ESI_SECRET_KEY, ESI_REFRESH_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- ESI API Functions ---

def get_access_token():
    """
    Refreshes the ESI access token using the refresh token.
    """
    url = "https://login.eveonline.com/v2/oauth/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "login.eveonline.com"
    }
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
    """
    Gets the character ID from the access token.
    """
    url = "https://login.eveonline.com/oauth/verify"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("CharacterID")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting character ID: {e}")
        return None

def get_filled_orders(access_token, character_id):
    """
    Fetches the character's filled sales orders from the ESI API.
    """
    if not character_id:
        return []
    url = f"https://esi.evetech.net/v1/characters/{character_id}/wallet/transactions/"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "datasource": "tranquility"
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        # We are interested in sales, which are positive transactions
        return [tx for tx in response.json() if tx.get('is_buy') == False]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching filled orders: {e}")
        return []

# --- Telegram Bot Functions ---

async def send_telegram_message(message):
    """
    Sends a message to the configured Telegram channel.
    """
    try:
        bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=message, parse_mode='Markdown')
        logging.info(f"Sent message to Telegram channel {TELEGRAM_CHANNEL_ID}")
    except Exception as e:
        logging.error(f"Error sending Telegram message: {e}")


import os

# --- Main Application Logic ---

DATA_DIR = "data"
PROCESSED_TRANSACTIONS_FILE = os.path.join(DATA_DIR, "processed_transactions.dat")

def load_processed_transactions():
    """Loads the set of processed transaction IDs from a file."""
    try:
        with open(PROCESSED_TRANSACTIONS_FILE, "r") as f:
            return set(int(line.strip()) for line in f)
    except FileNotFoundError:
        return set()

def save_processed_transactions(processed_ids):
    """Saves the set of processed transaction IDs to a file."""
    # Ensure the data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PROCESSED_TRANSACTIONS_FILE, "w") as f:
        for transaction_id in processed_ids:
            f.write(f"{transaction_id}\n")

processed_transactions = load_processed_transactions()

async def check_for_new_orders():
    """
    Main job function to check for and notify about new sales orders.
    """
    logging.info("Checking for new sales orders...")
    access_token = get_access_token()
    if not access_token:
        logging.error("Could not obtain access token. Skipping check.")
        return

    character_id = get_character_id(access_token)
    if not character_id:
        logging.error("Could not obtain character ID. Skipping check.")
        return

    filled_orders = get_filled_orders(access_token, character_id)

    # Sort orders by transaction_id to process them in order
    filled_orders.sort(key=lambda x: x['transaction_id'])

    for order in filled_orders:
        transaction_id = order['transaction_id']
        if transaction_id not in processed_transactions:
            # This is a new order, process and notify
            item_id = order['type_id']
            quantity = order['quantity']
            price = order['unit_price']
            client_id = order['client_id']

            # You would typically have a way to resolve these IDs to names
            # For now, we'll just use the IDs.
            message = (
                f"*New Sale!* 🎉\n\n"
                f"Item ID: `{item_id}`\n"
                f"Quantity: `{quantity}`\n"
                f"Price per unit: `{price:,.2f} ISK`\n"
                f"Total: `{(quantity * price):,.2f} ISK`\n"
                f"Buyer ID: `{client_id}`"
            )

            await send_telegram_message(message)
            processed_transactions.add(transaction_id)
            # To avoid spamming on first run, let's add a small delay
            time.sleep(1)

    save_processed_transactions(processed_transactions)
    logging.info(f"Check complete. Total processed transactions: {len(processed_transactions)}")


if __name__ == "__main__":
    logging.info("Bot started. Performing initial check...")
    # Run once on startup
    import asyncio
    asyncio.run(check_for_new_orders())

    # Schedule the job to run every 5 minutes
    schedule.every(5).minutes.do(lambda: asyncio.run(check_for_new_orders()))

    logging.info("Scheduler started. Will check for new orders every 5 minutes.")

    while True:
        schedule.run_pending()
        time.sleep(1)