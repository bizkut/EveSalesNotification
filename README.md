# EVE Online Market Notification Telegram Bot

This is a comprehensive Telegram bot designed to provide EVE Online players with detailed, real-time notifications and daily summaries of their market activities. The bot is fully containerized with Docker for easy deployment and uses a persistent SQLite database to ensure data integrity across restarts.

## Features

- **Near Real-Time Market Notifications**: Checks for activity on your active market orders every 60 seconds for both sales and buys.
- **Intelligent Filtering**: Notifications are only sent for non-immediate orders (1-90 day duration), ignoring instant transactions.
- **Activity Grouping**: Multiple fills of the same item within a 60-second window are grouped into a single, summarized notification.
- **Rich Contextual Data**:
  - **Market Price Context**: Sale notifications include the average price for that item in your chosen trade hub, showing you how your sale compares to the market.
  - **Trade Location**: All notifications include the name of the station or Citadel where the trade occurred.
  - **Wallet Balance**: All notifications display your current wallet balance for at-a-glance financial awareness.
- **Low Wallet Balance Alert**: Sends a one-time warning if your wallet drops below a configurable threshold, helping you avoid running out of ISK for new orders.
- **Comprehensive Daily Summary**: At a user-defined time, the bot sends a detailed financial report, including:
  - **24-Hour Stats**: Total sales value, total fees (broker's + tax), and an estimated profit.
  - **Monthly Stats**: A summary for the current calendar month, including total sales value, total fees, and gross revenue.
- **Accurate Fee & Profit Reporting**:
  - The daily summary uses your wallet journal to provide **100% accurate** fee totals.
  - Profit is estimated by comparing sales revenue against the 30-day average purchase price of the items sold.
- **Highly Configurable**:
  - The daily summary time, trade region, and wallet balance threshold can be easily set in the configuration file.
  - Sales notifications, buy notifications, and the daily summary can all be independently enabled or disabled.
- **Robust & Persistent**:
  - Uses a SQLite database to track market order states, processed journal entries, and alert states, preventing duplicate notifications.
  - Intelligently seeds its history on the first run to ignore all past transactions and only report on new activity.

---

## Requirements

- Docker
- Docker Compose
- A Telegram Bot Token and Channel ID
- An EVE Online Account

---

## Setup Instructions

Follow these steps to get your bot up and running.

### Step 1: Create an EVE Online Application

1.  Go to the [EVE Online Developers Portal](https://developers.eveonline.com/applications) and log in.
2.  Create a new application.
3.  Fill in the details. For the **Callback URL**, you **must** use `https://localhost/callback`.
4.  Once the application is created, click on it to view its details. Under the "Scopes" section, add the following required scopes:
    -   `esi-wallet.read_character_wallet.v1`
    -   `esi-wallet.read_character_journal.v1`
    -   `esi-markets.read_character_orders.v1`
    -   `esi-universe.read_structures.v1`
5.  Keep the **Client ID** and **Secret Key** handy for the next step.

### Step 2: Configure the Bot

1.  In the project directory, copy the example configuration file:
    ```bash
    cp config.py.example config.py
    ```
2.  Open `config.py` with a text editor and fill in the following details:
    -   `ESI_CLIENT_ID` & `ESI_SECRET_KEY`: From your EVE application.
    -   `TELEGRAM_BOT_TOKEN` & `TELEGRAM_CHANNEL_ID`: From your Telegram bot setup.
    -   `DAILY_SUMMARY_TIME`: The "HH:MM" UTC time for the daily report.
    -   `REGION_ID`: The region ID for your main trade hub (e.g., `10000002` for The Forge/Jita).
    -   `WALLET_BALANCE_THRESHOLD`: The ISK amount for the low-balance alert (set to `0` to disable).
    -   `ENABLE_SALES_NOTIFICATIONS`, `ENABLE_BUY_NOTIFICATIONS`, `ENABLE_DAILY_SUMMARY`: Set to `"true"` or `"false"`.

### Step 3: Generate Your ESI Refresh Token

This script will guide you through the EVE Online authentication process to grant the bot access to your character's data. **If you are upgrading, you must run this again to grant the new permissions.**

1.  Run the token generation script:
    ```bash
    python get_refresh_token.py
    ```
2.  The script will print a long URL. **Copy this entire URL.**
3.  Paste the URL into your local web browser and authorize the application.
4.  You will be redirected to a `localhost` page. Copy the **full URL** from your browser's address bar.
5.  Paste the full callback URL back into the script prompt and press Enter.
6.  The script will generate your `ESI_REFRESH_TOKEN`. Copy the entire line and paste it into your `config.py` file.

### Step 4: Run the Bot

With the configuration complete, you can now build and run the bot using Docker Compose.

```bash
docker-compose up --build -d
```

The bot will now start. On its first run, it will seed its database with your historical data and then begin its normal schedule.

---

## Example Notifications

**Market Sale Notification:**
```
✅ Market Sale! ✅

**Item:** `Tritanium` (`34`)
**Quantity Sold:** `1000`
**Your Price:** `10.50 ISK`
**Jita Avg Price:** `10.25 ISK` (+2.4%)
**Location:** `Jita 4-4 - Caldari Navy Assembly Plant`
**Wallet Balance:** `1,234,567,890.12 ISK`
```

**Daily Summary:**
```
📊 Daily Market Summary (2025-09-26)

**Wallet Balance:** `1,234,567,890.12 ISK`

**Past 24 Hours:**
  - Total Sales Value: `15,000,000.00 ISK`
  - Total Fees (Broker + Tax): `750,000.00 ISK`
  - **Estimated Profit:** `3,500,000.00 ISK`*

---

🗓️ Current Month Summary (September 2025):
  - Total Sales Value: `120,000,000.00 ISK`
  - Total Fees (Broker + Tax): `6,000,000.00 ISK`
  - **Gross Revenue (Sales - Fees):** `114,000,000.00 ISK`

_*Profit is estimated based on the average purchase price of items over the last 30 days._
```

---

## Resetting the Bot

If you ever need to completely reset the bot's history (e.g., to re-seed or after a major update), follow these steps:

1.  Stop and remove the container: `docker-compose down`
2.  Remove the persistent data volume: `docker volume rm <project_name>_processed_data`
3.  Restart the bot: `docker-compose up --build -d`