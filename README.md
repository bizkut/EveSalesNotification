# EVE Online Market Notification Telegram Bot

> **Public Bot Available!**
>
> Don't want to host the bot yourself? You can use the public instance for free by sending a message to **[@Evegametraderbot](https://t.me/Evegametraderbot)** on Telegram!

This is a comprehensive, multi-user Telegram bot designed to provide EVE Online players with detailed, real-time notifications and daily overviews of their market activities. The bot is fully containerized with Docker for easy deployment and uses a persistent PostgreSQL database to ensure data integrity across restarts.

## Features

- **Multi-User & Character Support**: Any user can add multiple characters by interacting with the bot in a private message. All data is stored securely in a database.
- **Seamless Character Updates**: Re-authorizing an existing character (e.g., to update API scopes) is handled gracefully. The bot sends a confirmation and immediately starts using the new permissions.
- **Private & Secure**: Notifications and command responses are sent directly to the user who owns the character.
- **New Contract Notifications**: Get real-time alerts for any new contracts that require your attention.
- **Near Real-Time Market Notifications**: Checks for market activity every 60 seconds using efficient ETag-based polling.
- **Intelligent Grouping**: Multiple transactions of the same type are grouped into a single, summarized notification to reduce spam.
- **Rich Contextual Data & Undercut Alerts**:
  - **Accurate Profit Tracking (FIFO & Journal-Based)**: The bot uses the First-In, First-Out (FIFO) method to calculate the Cost of Goods Sold (COGS) for all sales. It then uses your character's actual wallet journal data for 100% accurate tax and broker fee calculations, providing a true financial record in all views.
  - **Automatic & Accurate Market Context**: Sales notifications are compared against the best buy order in the *exact location* of the sale. The bot automatically determines if the sale was in an NPC station or a player-owned structure and fetches the correct market data. For stations, it finds the correct region by walking the ESI hierarchy (`station -> system -> region`), ensuring pinpoint accuracy without any user configuration.
  - **Accurate, Multi-Region Undercut Alerts**: When viewing open orders, the bot checks for undercuts in the *actual region of each order*, making it highly accurate for players who trade in multiple hubs simultaneously.
  - **Live Undercut Notifications**: Get notified the moment one of your orders is undercut. The bot monitors both regional markets and player-owned structures. To prevent spam, notifications are only sent once when an order's status changes from competitive to undercut. This can be toggled in the settings.
  - **Jump Distance Calculation**: Undercut alerts now include the number of jumps from your order's location (for notifications) or your character's current location (for the interactive view) to the competitor's location, giving you immediate context on how far away the best price is.
  - **Wallet Balance**: All notifications include your character's current wallet balance.
- **Low Wallet Balance Alert**: Sends a one-time warning if a character's wallet drops below a configurable threshold.
- **Comprehensive Daily Overview**: At a user-defined time, the bot sends a detailed, private financial report for each character (if enabled).
- **View Open Orders**: Interactively browse through all open buy and sell orders in a paginated view. The bot displays your character's current order capacity (e.g., "152 / 305 orders") and provides undercut alerts.
- **Public Character Info**: View an overview of any character's public information, including their portrait, corporation and alliance logos, security status, and birthday, all presented in a clean composite image.
- **Modern Inline Menu**: All bot commands are handled through a clean, interactive inline menu system directly within the chat.
- **Interactive On-Demand Charts**: Generate detailed performance charts directly within Telegram with a single button press. The bot offers several timeframes: "Last Day," "Last 7 Days," "Last 30 Days," and "All Time."
  - **Advanced Visualization**: Charts display profit as a cumulative area graph (showing gains and losses over the period) while sales and fees are shown as non-cumulative bar graphs for easy comparison.
  - **Intelligent Caching**: Charts are cached intelligently to ensure fast delivery. "Last Day" charts are cached hourly, daily charts are cached for the day, and the "All Time" chart is only regenerated when new transaction data is detected.
- **Highly Configurable**: All major settings (wallet alerts, notification types, etc.) are configurable on a per-character basis via the bot's menu.
- **Robust & Persistent**: Employs a sophisticated caching strategy using a PostgreSQL database. Background polling tasks continuously fetch data from ESI, and user-facing commands read from this fast, local cache. This minimizes API calls, prevents duplicate notifications, and ensures the bot remains responsive even during ESI slowdowns.
- **Non-Blocking History Backfill**: To provide an excellent user experience, the bot uses a two-phase backfill process for characters with large transaction histories.
  - **Phase 1: Fast Sync**: When a character is first added, the bot fetches only the most recent page of transactions. This makes the bot interactive almost immediately, allowing the user to use commands without a long wait.
  - **Phase 2: Gradual Background Backfill**: After the fast sync, a background job is scheduled. Using Celery and Redis, this job fetches the rest of the character's transaction history, one page at a time, without blocking the main bot. This process is resilient and will automatically retry if it encounters temporary ESI API errors.
- **Notification Grace Period**: To prevent a flood of old alerts after a character is added, a 1-hour grace period begins after the *background backfill* is fully complete. During this time, no new notifications (sales, buys, cancellations, or expirations) will be sent. This ensures that only market activity occurring after the full history has been retrieved will trigger an alert.
- **Graceful Deletion with 1-Hour Grace Period**: To prevent accidental data loss, character deletion is a two-step process. When you remove a character, they are "soft-deleted" and scheduled for permanent deletion in one hour. If you re-add the character within this grace period, the deletion is cancelled, and monitoring resumes instantly without needing to re-fetch all historical data. If you do nothing, all data is permanently wiped after the hour is up.

---

## Architecture

The bot is designed for efficiency and resilience, relying on a combination of background polling tasks and a database cache to provide a responsive user experience while minimizing direct ESI API calls.

-   **Background Polling**: The bot runs several continuous, asynchronous background tasks:
    -   **`master_orders_poll`**: Fetches a character's current open market orders. This data is used for undercut notifications and to provide a cached view for the "Open Orders" command.
    -   **`master_wallet_transaction_poll`**: Fetches the latest wallet transactions to identify new sales and buys for notifications and historical logging.
    -   **`master_wallet_journal_poll`**: Fetches the latest wallet journal entries, which are crucial for accurately calculating taxes and broker's fees.
    -   **`master_order_history_poll`**: Fetches historical order data to detect and notify users about cancelled or expired orders.
    -   **`master_contracts_poll`**: Fetches a character's contracts and notifies the user about any new, outstanding contracts requiring their attention.
-   **Database Caching**: All data fetched from the ESI API is stored in a PostgreSQL database.
    -   **Historical Data**: Wallet transactions and journal entries are stored permanently, creating a complete financial history for each character.
    -   **Snapshot Data**: Open market orders are stored as a snapshot. The `master_orders_poll` task ensures this table is always a direct reflection of the character's current open orders on the ESI.
-   **User Commands**: When a user requests data (e.g., "View Sales" or "Open Orders"), the bot reads directly from the fast, local database cache instead of making a new ESI API call. This makes the bot highly responsive and less susceptible to ESI API latency or downtime.

---

## Requirements

- A server with Docker and Docker Compose.
- An EVE Online Account.
- A Telegram Bot Token.

---

## Setup Instructions

Follow these steps to deploy your own instance of the bot.

### Step 1: Create an EVE Online Application

1.  Go to the [EVE Online Developers Portal](https://developers.eveonline.com/applications) and log in.
2.  Create a new application.
3.  For the **Callback URL**, you will need a public-facing URL. We will generate this in the next step, but for now, you can use a placeholder like `https://example.com/callback`. You will need to come back and update this later.
4.  Under the "Scopes" section, add the following required scopes:
    -   `esi-wallet.read_character_wallet.v1`
    -   `esi-contracts.read_character_contracts.v1`
    -   `esi-markets.read_character_orders.v1`
    -   `esi-universe.read_structures.v1`
    -   `esi-markets.structure_markets.v1`
    -   `esi-skills.read_skills.v1`
    -   `esi-location.read_online.v1`
5.  Keep the **Client ID** and **Secret Key** handy for the next step.

### Step 2: Configure and Run the Bot

All configuration is handled through a `.env` file. The bot uses the included Cloudflare Tunnel service to easily and securely expose the web app to the internet, which is required for EVE's authentication flow.

1.  Create your environment file by copying the example:
    ```bash
    cp .env.example .env
    ```
2.  Open `.env` with a text editor and fill in the required values:
    -   `ESI_CLIENT_ID` & `ESI_SECRET_KEY`: From your EVE application in Step 1.
    -   `TELEGRAM_BOT_TOKEN`: The token for your bot from BotFather on Telegram.
    -   `POSTGRES_PASSWORD`: Choose a secure password for the database.
    -   `TUNNEL_TOKEN`: **Leave this blank for now.**

3.  Start the bot for the first time to generate your tunnel credentials:
    ```bash
    docker-compose up --build -d
    ```
4.  The `cloudflared` service will authenticate and create a tunnel. View its logs to get your public URL:
    ```bash
    docker-compose logs cloudflared
    ```
    Look for a line similar to `INF | url=https://something-random.trycloudflare.com`. This is your public URL.

5.  Now, update your configuration with the public URL:
    -   **Update EVE Application**: Go back to the EVE Developer Portal and update your application's **Callback URL** to `https://your-public-url.trycloudflare.com/callback`.
    -   **Update `.env` file**: Fill in the `WEBAPP_URL` with your public URL (`https://your-public-url.trycloudflare.com`).

6.  Restart the bot to apply the final configuration:
    ```bash
    docker-compose restart
    ```

Your bot is now fully configured and running.

---

## Usage

All interaction with the bot is handled through a clean, inline button-based menu in your private chat with it.

1.  **Start the Bot**: Send the `/start` command to the bot. It will welcome you and display the main menu.
2.  **Use the Menu**: Simply press the buttons in the chat to perform actions.
    -   **💰 View Balances**: Fetches the current wallet balance for your character(s).
    -   **📊 Open Orders**: Shows a paginated list of your open buy or sell orders and your current order capacity.
    -   **📈 View Sales**: Displays a detailed, paginated history of all sales, including profit and fee calculations.
    -   **🛒 View Buys**: Displays a detailed, paginated history of all buy transactions.
    -   **📝 View Contracts**: Shows a paginated list of all outstanding contracts.
    -   **📊 Request Overview**: Manually triggers the daily overview report, which includes on-demand performance charts.
    -   **⚙️ Settings**: Configure per-character settings like wallet balance alerts, notification preferences, and view public character info.
    -   **➕ Add Character**: Starts the process of adding a new character.
    -   **🗑️ Remove Character**: Schedules a character and all their associated data for permanent deletion after a one-hour grace period. This action can be cancelled by re-adding the character within the hour.
3.  **Character Selection**: If you have multiple characters, the bot will present you with a new inline menu to choose which character you want to interact with after you select an action.

---

## Example Notifications

**Market Sale Notification:**
```
✅ *Market Sale (Character Name)* ✅

**Item:** `Tritanium`
**Quantity:** `1,000` @ `10.00 ISK`
**The Forge Best Buy:** `9.95 ISK` (+0.50%)
**Total Fees:** `1,050.00 ISK`
**Net Profit:** `950.00 ISK`

**Location:** `Jita 4-4 - Caldari Navy Assembly Plant`
**Wallet:** `1,234,567,890.12 ISK`
```

**Open Order Undercut:**
```
📄 *Open Buy Orders for Character Name* (152 / 305 orders)

*Tritanium*
  `1,000,000` of `5,000,000` @ `9.90` ISK
  *Location:* `Jita 4-4 - Caldari Navy Assembly Plant`
  `> ❗️ Undercut! Best buy: 9.95`
```

**Daily Overview:**
```
📊 *Market Overview (Character Name)*
_2025-09-26 18:00 UTC_

*Wallet Balance:* `1,234,567,890.12 ISK`

*Last Day:*
  - Total Sales Value: `15,000,000.00 ISK`
  - Total Fees (Broker + Tax): `750,000.00 ISK`
  - **Profit (FIFO):** `3,500,000.00 ISK`

---

🗓️ *Last 30 Days:*
  - Total Sales Value: `120,000,000.00 ISK`
  - Total Fees (Broker + Tax): `6,000,000.00 ISK`
  - **Profit (FIFO):** `25,000,000.00 ISK`
```

---

## Resetting the Bot

If you ever need to completely reset the bot's history (e.g., to re-seed or after a major update), follow these steps:

1.  Stop and remove the container: `docker-compose down`
2.  Remove the persistent data volume: `docker volume rm <project_name>_processed_data`
3.  Restart the bot: `docker-compose up --build -d`