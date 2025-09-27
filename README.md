# EVE Online Market Notification Telegram Bot

This is a comprehensive Telegram bot designed to provide EVE Online players with detailed, real-time notifications and daily summaries of their market activities. The bot is fully containerized with Docker for easy deployment and uses a persistent SQLite database to ensure data integrity across restarts.

## Features

- **Near Real-Time Market Notifications**: Checks for market activity every 60 seconds. Uses efficient ETag-based polling to get data as soon as it's updated by the EVE Online servers, often much faster than the standard cache timer.
- **Intelligent Filtering**: Notifications are only sent for non-immediate orders (1-90 day duration), ignoring instant transactions.
- **Activity Grouping**: Multiple fills of the same item within a 60-second window are grouped into a single, summarized notification.
- **Rich Contextual Data**:
  - **Real-Time Profit Tracking**: Sales notifications include the gross profit for the transaction, calculated using the First-In, First-Out (FIFO) method against your historical purchases.
  - **Live Market Price Context**: Sales notifications compare your price against the current best buy order in your main trade hub, giving you a real-time view of how your sale stacks up against the competition.
  - **Trade Location**: All notifications include the name of the station or Citadel where the trade occurred.
  - **Wallet Balance**: All notifications display your current wallet balance for at-a-glance financial awareness.
- **Low Wallet Balance Alert**: Sends a one-time warning if your wallet drops below a configurable threshold, helping you avoid running out of ISK for new orders.
- **Comprehensive Daily Summary**: At a user-defined time, the bot sends a detailed financial report. The calculations are stateless, meaning the report is always accurate for the requested period, regardless of how many times it's run. It includes:
  - **24-Hour Stats**: Total sales value, total fees (broker's + tax), and a precise FIFO-based profit.
  - **Monthly Stats**: A summary for the current calendar month, including total sales value, total fees, and gross revenue.
- **Multi-Character Support**: Monitor and receive notifications for multiple EVE characters from a single instance of the bot.
- **Accurate & Consistent Reporting**:
  - The summary calculations use the most reliable data sources (wallet transactions for sales, wallet journal for fees) to provide consistent and accurate reports.
  - Profit is calculated using the First-In, First-Out (FIFO) method, giving you a true reflection of performance.
- **Highly Configurable**:
  - The daily summary time, trade region, and wallet balance threshold can be easily set in the configuration file.
  - Sales notifications, buy notifications, and the daily summary can all be independently enabled or disabled.
- **Robust & Persistent**:
  - **Multi-Layer Caching**: Uses a combination of an in-memory cache for speed and a persistent SQLite database to store item names, market data, and processed transaction IDs, minimizing API calls and preventing duplicate notifications.
  - **Intelligent Seeding**: On the first run, the bot intelligently seeds its history to ignore all past transactions and only report on new activity.
  - **Historical Backfill**: For new characters, the bot automatically backfills its database with the entire transaction history to ensure profit calculations are accurate from day one.

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
-   `LOG_LEVEL`: The desired logging verbosity. Can be `'DEBUG'`, `'INFO'`, `'WARNING'`, or `'ERROR'`. Defaults to `'INFO'`.
    -   `DAILY_SUMMARY_TIME`: The "HH:MM" UTC time for the daily report.
    -   `REGION_ID`: The region ID for your main trade hub (e.g., `10000002` for The Forge/Jita).
    -   `WALLET_BALANCE_THRESHOLD`: The ISK amount for the low-balance alert (set to `0` to disable).
    -   `ENABLE_SALES_NOTIFICATIONS`, `ENABLE_BUY_NOTIFICATIONS`, `ENABLE_DAILY_SUMMARY`: Set to `"true"` or `"false"`.

### Step 3: Generate Your ESI Refresh Token(s)

This script guides you through the EVE Online authentication process to grant the bot access to your character's data.

1.  Run the token generation script:
    ```bash
    python get_refresh_token.py
    ```
2.  The script will print a long URL. **Copy this entire URL.**
3.  Paste the URL into your local web browser. **Important**: Make sure you are logged into the correct EVE Online account for the character you wish to add.
4.  Authorize the application. You will be redirected to a `localhost` page that likely won't load.
5.  Copy the **full URL** from your browser's address bar.
6.  Paste the full callback URL back into the script prompt and press Enter.
7.  The script will provide you with a line to add to your `config.py`. It will look like `ESI_REFRESH_TOKEN_X = "..."`.
8.  Add this line to your `config.py` file.

**To add multiple characters, simply run the `python get_refresh_token.py` script again for each character.** The script will tell you the correct number to use for the variable name (e.g., `ESI_REFRESH_TOKEN_2`, `ESI_REFRESH_TOKEN_3`, etc.).

### Step 4: Run the Bot

With the configuration complete, you can now build and run the bot using Docker Compose.

```bash
docker-compose up --build -d
```

The bot will now start. On its first run, it will seed its database with your historical data and then begin its normal schedule.

---

## Usage & Commands

You can interact with the bot by sending it commands in a private chat or in a channel it's a member of.

**Note on Commands:**
- The command menu (the `/` button in the text input field) will only be visible when you are in a **private chat** with the bot. In channels, you must type the commands manually.
- If you have multiple characters configured, commands like `/balance`, `/summary`, `/sales`, and `/buys` will present a menu to select a character (or "All Characters").
- If you have only **one** character configured, these commands will execute directly for that character, bypassing the selection menu for a faster experience.

### Available Commands

-   `/balance` - Fetches the wallet balance. Shows a character selection menu if multiple characters are configured.
-   `/summary` - Triggers the daily summary. Shows a character selection menu if multiple characters are configured.
-   `/sales` - Shows the 5 most recent sales. Shows a character selection menu if multiple characters are configured.
-   `/buys` - Shows the 5 most recent buys. Shows a character selection menu if multiple characters are configured.

---

## Example Notifications

**Market Sale Notification:**
```
✅ Market Sale! ✅

**Item:** `Tritanium` (`34`)
**Quantity Sold:** `1000`
**Avg. Your Price:** `10.50 ISK`
**Jita Best Buy:** `10.45 ISK` (+0.48%)
**Gross Profit (before fees):** `500.00 ISK`

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
  - **Profit (FIFO):** `3,500,000.00 ISK`

---

🗓️ Current Month Summary (September 2025):
  - Total Sales Value: `120,000,000.00 ISK`
  - Total Fees (Broker + Tax): `6,000,000.00 ISK`
  - **Gross Revenue (Sales - Fees):** `114,000,000.00 ISK`
```

---

## Resetting the Bot

If you ever need to completely reset the bot's history (e.g., to re-seed or after a major update), follow these steps:

1.  Stop and remove the container: `docker-compose down`
2.  Remove the persistent data volume: `docker volume rm <project_name>_processed_data`
3.  Restart the bot: `docker-compose up --build -d`