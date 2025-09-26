# EVE Online Market Notification Telegram Bot

This is a comprehensive Telegram bot designed to provide EVE Online players with detailed notifications about their market activities. The bot is fully containerized with Docker for easy deployment and uses a persistent SQLite database to ensure data integrity across restarts.

## Features

- **Near Real-Time Sales Notifications**: Checks for sales of your active market orders every 60 seconds.
- **Intelligent Filtering**: Sales notifications are only sent for non-immediate sell orders (1-90 day duration), ignoring instant sales to buy orders.
- **Sales Grouping**: Multiple sales of the same item within a 60-second window are grouped into a single, summarized notification to reduce spam.
- **Comprehensive Daily Summary**: At a user-defined time, the bot sends a detailed financial report, including:
  - **24-Hour Stats**: Total sales value, total fees (broker's + tax), and an estimated profit.
  - **Monthly Stats**: A summary for the current calendar month, including total sales value, total fees, and gross revenue.
- **Accurate Fee & Profit Reporting**:
  - The daily summary uses your wallet journal to provide **100% accurate** fee totals.
  - Profit is estimated by comparing sales revenue against the 30-day average purchase price of the items sold.
- **Highly Configurable**:
  - The time of the daily summary can be easily set in the configuration file.
  - Both the 60-second sales notifications and the daily summary can be independently enabled or disabled.
- **Robust & Persistent**:
  - Uses a SQLite database to track market order states and processed journal entries, preventing duplicate notifications.
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
5.  Keep the **Client ID** and **Secret Key** handy for the next step.

### Step 2: Configure the Bot

1.  In the project directory, copy the example configuration file:
    ```bash
    cp config.py.example config.py
    ```
2.  Open `config.py` with a text editor and fill in the following details:
    -   `ESI_CLIENT_ID`: Your application's Client ID from Step 1.
    -   `ESI_SECRET_KEY`: Your application's Secret Key from Step 1.
    -   `TELEGRAM_BOT_TOKEN`: Your Telegram bot's API token (obtained from BotFather).
    -   `TELEGRAM_CHANNEL_ID`: The ID of the channel you want the bot to post in.
    -   You can also customize `DAILY_SUMMARY_TIME`, `ENABLE_SALES_NOTIFICATIONS`, and `ENABLE_DAILY_SUMMARY` here.

### Step 3: Generate Your ESI Refresh Token

This script will guide you through the EVE Online authentication process to grant the bot access to your character's data.

1.  Run the token generation script:
    ```bash
    python get_refresh_token.py
    ```
2.  The script will print a long URL to your console. **Copy this entire URL.**
3.  Paste the URL into your local web browser.
4.  Log in with your EVE Online account and authorize the application for the character you want to monitor.
5.  You will be redirected to a `localhost` page that shows an error. **This is expected.** Copy the **full URL** from your browser's address bar.
6.  Go back to your terminal and **paste the full callback URL** into the script prompt, then press Enter.
7.  The script will generate your `ESI_REFRESH_TOKEN`. Copy the entire line it provides (e.g., `ESI_REFRESH_TOKEN = "a_very_long_string"`).
8.  Open `config.py` again and **paste the refresh token line** into the file, replacing the placeholder.

### Step 4: Run the Bot

With the configuration complete, you can now build and run the bot using Docker Compose.

```bash
docker-compose up --build -d
```

The bot will now start. On its first run, it will seed its database with your historical data (this may take a moment) and then begin its normal schedule.

---

## Usage

Once running, the bot will automatically perform its tasks:
- **Sales Notifications**: Every 60 seconds, it checks for filled market orders and sends a notification if any are found.
- **Daily Summary**: At the time specified in your `config.py`, it will post the daily financial summary.

### Example Notifications

**Market Sale Notification:**
```
✅ Market Sale! ✅

**Item:** `Tritanium` (`34`)
**Quantity Sold:** `1000`
**Total Value:** `10,000.00 ISK`
```

**Daily Summary:**
```
📊 Daily Market Summary (2025-09-26)

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

If you ever need to completely reset the bot's history (e.g., to re-seed the historical data), follow these steps:

1.  Stop and remove the container:
    ```bash
    docker-compose down
    ```
2.  Remove the persistent data volume:
    ```bash
    docker volume rm <project_name>_processed_data
    ```
    *(You can find the exact volume name by running `docker volume ls`)*
3.  Restart the bot:
    ```bash
    docker-compose up --build -d
    ```