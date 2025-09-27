# EVE Online Market Notification Telegram Bot

> **Public Bot Available!**
>
> Don't want to host the bot yourself? You can use the public instance for free by sending a message to **[@Evegametraderbot](https://t.me/Evegametraderbot)** on Telegram!

This is a comprehensive, multi-user Telegram bot designed to provide EVE Online players with detailed, real-time notifications and daily summaries of their market activities. The bot is fully containerized with Docker for easy deployment and uses a persistent PostgreSQL database to ensure data integrity across restarts.

## Features

- **Multi-User & Character Support**: Any user can add multiple characters by interacting with the bot in a private message. All data is stored securely in a database.
- **Private & Secure**: Notifications and command responses are sent directly to the user who owns the character.
- **Near Real-Time Market Notifications**: Checks for market activity every 60 seconds using efficient ETag-based polling.
- **Intelligent Filtering & Grouping**: Notifications are sent for non-immediate orders, and multiple fills are grouped into a single, summarized notification.
- **Rich Contextual Data**:
  - **Real-Time Profit Tracking**: Sales notifications include FIFO-based gross profit calculations.
  - **Live Market Price Context**: Sales are compared against the current best buy order in your main trade hub.
  - **Trade Location & Wallet Balance**: All notifications include the trade location and your current wallet balance.
- **Low Wallet Balance Alert**: Sends a one-time warning if a character's wallet drops below a configurable threshold.
- **Comprehensive Daily Summary**: At a user-defined time, the bot sends a detailed, private financial report for each character (if enabled).
- **Highly Configurable**: All major settings (trade region, summary time, wallet alerts, etc.) are configurable on a per-character basis via the bot's "Settings" menu.
- **Robust & Persistent**: Uses a combination of an in-memory cache and a persistent PostgreSQL database to minimize API calls and prevent duplicate notifications.
- **Intelligent Seeding & Backfill**: On first add, the bot intelligently seeds a character's history to report only on new activity and ensure profit calculations are accurate from day one.

---

## Requirements

- A server with Docker and Docker Compose.
- An EVE Online Account.
- A Telegram Bot Token.

---

## Setup Instructions

Follow these steps to deploy your own instance of the bot.

### Step 1: Get a Public URL

The bot's web component needs to be accessible from the internet for the EVE Online authentication to work. The easiest way to achieve this is by using the included Cloudflare Tunnel service.

1.  Follow the [Cloudflare guide](https://developers.cloudflare.com/zerotrust/get-started/create-tunnel/) to create a new tunnel.
2.  In your tunnel's dashboard, configure a **Public Hostname** (e.g., `eve-bot.yourdomain.com`).
3.  Set the service type to **HTTP** and the URL to `webapp:5000`.
4.  Note down your public hostname and the **tunnel token**. You will need these for the next steps.

Alternatively, you can use your own reverse proxy (like Nginx) to expose the `webapp` service on port 5000.

### Step 2: Create an EVE Online Application

1.  Go to the [EVE Online Developers Portal](https://developers.eveonline.com/applications) and log in.
2.  Create a new application.
3.  For the **Callback URL**, enter your public-facing URL from Step 1, followed by `/callback`. For example: `https://eve-bot.yourdomain.com/callback`.
4.  Under the "Scopes" section, add the following required scopes:
    -   `esi-wallet.read_character_wallet.v1`
    -   `esi-markets.read_character_orders.v1`
    -   `esi-universe.read_structures.v1`
    -   `esi-markets.structure_markets.v1`
5.  Keep the **Client ID** and **Secret Key** handy for the next step.

### Step 3: Configure the Environment

All configuration is handled through a `.env` file.

1.  Create your environment file by copying the example:
    ```bash
    cp .env.example .env
    ```
2.  Open `.env` with a text editor and fill in the required values:
    -   `ESI_CLIENT_ID` & `ESI_SECRET_KEY`: From your EVE application in Step 2.
    -   `CALLBACK_URL`: The **exact same** callback URL you used in Step 2.
    -   `TELEGRAM_BOT_TOKEN`: The token for your bot from BotFather on Telegram.
    -   `WEBAPP_URL`: The public base URL for the webapp component (e.g., `https://eve-bot.yourdomain.com`).
    -   `POSTGRES_PASSWORD`: Choose a secure password for the database.
    -   `TUNNEL_TOKEN`: Your Cloudflare tunnel token from Step 1 (if you are using the tunnel).

### Step 4: Run the Bot

With the configuration complete, you can now build and run the bot using Docker Compose.

```bash
docker-compose up --build -d
```

The bot and its companion web app will now start.

### Step 3: (Optional) Configure Cloudflare Tunnel

This bot includes an integrated [Cloudflare Tunnel](https://www.cloudflare.com/products/tunnel/) service to easily and securely expose the web app to the internet.

1.  Follow the [Cloudflare guide](https://developers.cloudflare.com/zerotrust/get-started/create-tunnel/) to create a new tunnel. Note down the **tunnel name** and the **tunnel token**.
2.  In your tunnel's dashboard, configure the **Public Hostname**. Set the hostname you want (e.g., `eve.gametrader.my`) to point to the internal `webapp` service at `http://webapp:5000`.
3.  Create a `.env` file by copying the example: `cp .env.example .env`
4.  Open the new `.env` file and paste your **tunnel name** and **token** into the corresponding variables.
5.  Finally, ensure your `CALLBACK_URL` and `WEBAPP_URL` in `config.py` use your public `https` hostname (e.g., `https://eve.gametrader.my`).

If you choose not to use the tunnel, you can safely ignore the `.env` file and will need to set up your own reverse proxy.

---

## Usage

All interaction with the bot is handled through a simple, button-based menu in your private chat with it.

1.  **Start the Bot**: Send the `/start` command to the bot. It will welcome you and display the main menu keyboard.
2.  **Use the Menu**: Simply press the buttons on the keyboard to perform actions.
    -   **➕ Add Character**: Starts the process of adding a new character.
    -   **🔔 Manage Notifications**: View and toggle master notification settings for each of your characters.
    -   **⚙️ Settings**: Configure per-character settings like your preferred trading region, daily summary time, and wallet balance alerts.
    -   **💰 View Balances**: Fetches the current wallet balance for your character(s).
    -   **📊 Request Summary**: Manually triggers the daily summary report for your character(s).
    -   **📈 View Sales**: Shows the 5 most recent sales for a selected character.
    -   **🛒 View Buys**: Shows the 5 most recent buys for a selected character.
    -   **🗑️ Remove Character**: Starts the process of removing a character and all of their associated data.
3.  **Character Selection**: If you have multiple characters, the bot will present you with an inline menu to choose which character you want to interact with after you select an action.

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