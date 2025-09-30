# EVE Online Market Notification Telegram Bot

> **Public Bot Available!**
>
> Don't want to host the bot yourself? You can use the public instance for free by sending a message to **[@Evegametraderbot](https://t.me/Evegametraderbot)** on Telegram!

This is a comprehensive, multi-user Telegram bot designed to provide EVE Online players with detailed, real-time notifications and daily summaries of their market activities. The bot is fully containerized with Docker for easy deployment and uses a persistent PostgreSQL database to ensure data integrity across restarts.

## Features

- **Multi-User & Character Support**: Any user can add multiple characters by interacting with the bot in a private message. All data is stored securely in a database.
- **Seamless Character Updates**: Re-authorizing an existing character (e.g., to update API scopes) is handled gracefully. The bot sends a confirmation and immediately starts using the new permissions.
- **Private & Secure**: Notifications and command responses are sent directly to the user who owns the character.
- **Near Real-Time Market Notifications**: Checks for market activity every 60 seconds using efficient ETag-based polling.
- **Intelligent Grouping**: Multiple transactions of the same type are grouped into a single, summarized notification to reduce spam.
- **Rich Contextual Data & Undercut Alerts**:
  - **Accurate Profit Tracking (FIFO & Journal-Based)**: The bot uses the First-In, First-Out (FIFO) method to calculate the Cost of Goods Sold (COGS) for all sales. It then uses your character's actual wallet journal data for 100% accurate tax and broker fee calculations, providing a true financial record in all views.
  - **Live Market Price Context**: Sales notifications are compared against the current best buy order in your character's configured main trade hub.
  - **Open Order Undercut Alerts**: When viewing open orders, the bot checks for undercuts in your configured region. It will warn you if a higher buy order or a lower sell order exists, helping you stay competitive.
  - **Wallet Balance**: All notifications include your character's current wallet balance.
- **Low Wallet Balance Alert**: Sends a one-time warning if a character's wallet drops below a configurable threshold.
- **Comprehensive Daily Summary**: At a user-defined time, the bot sends a detailed, private financial report for each character (if enabled).
- **View Open Orders**: Interactively browse through all open buy and sell orders in a paginated view. The bot displays your character's current order capacity (e.g., "152 / 305 orders") and provides undercut alerts.
- **Public Character Info**: View a summary of any character's public information, including their portrait, corporation and alliance logos, security status, and birthday, all presented in a clean composite image.
- **Modern Inline Menu**: All bot commands are handled through a clean, interactive inline menu system directly within the chat.
- **Interactive On-Demand Charts**: Generate detailed performance charts directly within Telegram. The summary now includes inline buttons to create an hourly (last 24h), daily (current month), and monthly (for all historical years) performance chart. After viewing a chart, you can easily return to the summary view using the "Back to Summary" button.
- **Highly Configurable**: All major settings (wallet alerts, notification types, etc.) are configurable on a per-character basis via the bot's menu.
- **Robust & Persistent**: Uses a combination of an in-memory cache and a persistent PostgreSQL database to minimize API calls and prevent duplicate notifications.
- **Intelligent Seeding & Backfill**: On first add, the bot intelligently seeds a character's entire transaction history to ensure profit calculations are accurate from day one. To prevent a flood of old alerts, a 1-hour grace period begins after the initial historical data sync is complete. During this time, no new notifications (sales, buys, cancellations, or expirations) will be sent. This ensures that only market activity occurring after the sync and grace period will trigger an alert.
- **Graceful Deletion with 1-Hour Grace Period**: To prevent accidental data loss and API abuse, character deletion is now a two-step process. When you remove a character, they are "soft-deleted" and scheduled for permanent deletion in one hour. If you re-add the character within this grace period, the deletion is cancelled, and monitoring resumes instantly without needing to re-fetch all historical data. If you do nothing, all data is permanently wiped after the hour is up.

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
    -   **📊 Request Summary**: Manually triggers the daily summary report, which includes on-demand performance charts.
    -   **⚙️ Settings**: Configure per-character settings like your preferred trading region, wallet balance alerts, notification preferences, and view public character info.
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

**Daily Summary:**
```
📊 *Daily Market Summary (Character Name)*
_2025-09-26 18:00 UTC_

*Wallet Balance:* `1,234,567,890.12 ISK`

*Past 24 Hours:*
  - Total Sales Value: `15,000,000.00 ISK`
  - Total Fees (Broker + Tax): `750,000.00 ISK`
  - **Profit (FIFO):** `3,500,000.00 ISK`

---

🗓️ *Current Month Summary (September 2025):*
  - Total Sales Value: `120,000,000.00 ISK`
  - Total Fees (Broker + Tax): `6,000,000.00 ISK`
  - *Gross Revenue (Sales - Fees):* `114,000,000.00 ISK`
```

---

## Resetting the Bot

If you ever need to completely reset the bot's history (e.g., to re-seed or after a major update), follow these steps:

1.  Stop and remove the container: `docker-compose down`
2.  Remove the persistent data volume: `docker volume rm <project_name>_processed_data`
3.  Restart the bot: `docker-compose up --build -d`