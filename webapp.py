import os
import database
import logging
import requests
from flask import Flask, request, redirect, render_template_string

# Configure logging
log_level_str = os.getenv('LOG_LEVEL', 'WARNING').upper()
log_level = getattr(logging, log_level_str, logging.WARNING)
# Get the root logger
logger = logging.getLogger()
logger.setLevel(log_level)
# Remove any existing handlers to prevent conflicts
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
# Create a new stream handler
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# Add the new handler to the root logger
logger.addHandler(handler)

# --- Database Functions ---
def add_character_to_db(character_id, character_name, refresh_token, telegram_user_id):
    """Adds or updates a character in the PostgreSQL database."""
    conn = database.get_db_connection()
    try:
        with conn.cursor() as cursor:
            # First, ensure the telegram user exists.
            cursor.execute(
                "INSERT INTO telegram_users (telegram_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (telegram_user_id,)
            )

            # Now, "upsert" the character.
            cursor.execute(
                """
                INSERT INTO characters (character_id, character_name, refresh_token, telegram_user_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (character_id) DO UPDATE SET
                    character_name = EXCLUDED.character_name,
                    refresh_token = EXCLUDED.refresh_token,
                    telegram_user_id = EXCLUDED.telegram_user_id,
                    needs_update_notification = TRUE
                """,
                (character_id, character_name, refresh_token, telegram_user_id)
            )
            conn.commit()
        logging.info(f"Successfully added/updated character {character_name} ({character_id}) for user {telegram_user_id}.")
        return True
    except Exception as e:
        logging.error(f"Database error while adding character: {e}", exc_info=True)
        conn.rollback() # Rollback on error
        return False
    finally:
        database.release_db_connection(conn)


# --- ESI/OAuth Functions ---
def get_token_from_code(auth_code):
    """Exchanges an authorization code for a refresh token and access token."""
    url = "https://login.eveonline.com/v2/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Host": "login.eveonline.com"}
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "client_id": os.getenv("ESI_CLIENT_ID"),
        "client_secret": os.getenv("ESI_SECRET_KEY")
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting token from auth code: {e}")
        return None

def get_character_details_from_token(access_token):
    """Gets character details from an access token."""
    url = "https://login.eveonline.com/oauth/verify"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("CharacterID"), data.get("CharacterName")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting character details from access token: {e}")
        return None, None


# --- Flask Web Application ---
app = Flask(__name__)

# Basic HTML templates for user feedback
SUCCESS_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Success</title>
    <style>body { font-family: sans-serif; text-align: center; background-color: #282c34; color: white; padding-top: 50px; }</style>
</head>
<body>
    <h1>✅ Success!</h1>
    <p>Character <strong>{{ character_name }}</strong> has been successfully added.</p>
    <p>You can now close this window and return to Telegram.</p>
</body>
</html>
"""

# Template with automatic redirect to the Telegram bot
SUCCESS_TEMPLATE_REDIRECT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Success</title>
    <style>
        body { font-family: sans-serif; text-align: center; background-color: #282c34; color: white; padding-top: 50px; }
        a { color: #61dafb; }
    </style>
    <script>
        setTimeout(function() {
            window.location.href = "{{ telegram_url }}";
        }, 3000); // 3-second delay
    </script>
</head>
<body>
    <h1>✅ Success!</h1>
    <p>Character <strong>{{ character_name }}</strong> has been successfully added.</p>
    <p>You will be redirected back to Telegram shortly.</p>
    <p>If you are not redirected, <a href="{{ telegram_url }}">click here to return to the bot</a>.</p>
</body>
</html>
"""

ERROR_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Error</title>
    <style>body { font-family: sans-serif; text-align: center; background-color: #282c34; color: white; padding-top: 50px; }</style>
</head>
<body>
    <h1>❌ Error</h1>
    <p>{{ message }}</p>
    <p>Please try again or contact the bot administrator.</p>
</body>
</html>
"""

@app.route('/login')
def login():
    """
    Redirects the user to the EVE Online SSO login page.
    The telegram_user_id is passed in the 'state' parameter.
    """
    telegram_user_id = request.args.get('user')
    if not telegram_user_id:
        return render_template_string(ERROR_TEMPLATE, message="Missing user information. Please start the process from the bot again."), 400

    # These are the scopes the bot needs to function.
    scopes = [
        "esi-wallet.read_character_wallet.v1",
        "esi-markets.read_character_orders.v1",
        "esi-universe.read_structures.v1",
        # Required to fetch market data from player-owned structures.
        "esi-markets.structure_markets.v1",
        "esi-skills.read_skills.v1",
        "esi-location.read_online.v1",
        "esi-contracts.read_character_contracts.v1"
    ]
    scope_string = " ".join(scopes)

    # Note: The `CALLBACK_URL` must be configured in your EVE Dev Application
    # to point to where this webapp is hosted, e.g., http://yourdomain.com/callback
    # We read it from the config file to ensure consistency.
    callback_url = os.getenv('CALLBACK_URL', 'http://localhost:5000/callback')

    params = {
        "response_type": "code",
        "redirect_uri": callback_url,
        "client_id": os.getenv("ESI_CLIENT_ID"),
        "scope": scope_string,
        "state": telegram_user_id # Pass the user's telegram ID through the state param
    }
    auth_url = "https://login.eveonline.com/v2/oauth/authorize/?" + requests.compat.urlencode(params)
    return redirect(auth_url)


@app.route('/callback')
def callback():
    """
    Handles the callback from EVE SSO after the user authenticates.
    """
    auth_code = request.args.get('code')
    telegram_user_id = request.args.get('state')

    if not auth_code or not telegram_user_id:
        return render_template_string(ERROR_TEMPLATE, message="Authentication failed or was cancelled."), 400

    # 1. Exchange authorization code for tokens
    token_data = get_token_from_code(auth_code)
    if not token_data:
        return render_template_string(ERROR_TEMPLATE, message="Failed to exchange authorization code for a token."), 500

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")

    # 2. Get character details
    character_id, character_name = get_character_details_from_token(access_token)
    if not character_id or not character_name:
        return render_template_string(ERROR_TEMPLATE, message="Failed to verify token and get character details."), 500

    # 3. Save the character to the database
    if add_character_to_db(character_id, character_name, refresh_token, telegram_user_id):
        # Check if the bot username is configured for automatic redirection
        bot_username = os.getenv("TELEGRAM_BOT_USERNAME")
        if bot_username:
            telegram_url = f"https://t.me/{bot_username}"
            return render_template_string(
                SUCCESS_TEMPLATE_REDIRECT,
                character_name=character_name,
                telegram_url=telegram_url
            )
        else:
            logging.warning("TELEGRAM_BOT_USERNAME is not set. Falling back to standard success page without redirection.")
            return render_template_string(SUCCESS_TEMPLATE, character_name=character_name)
    else:
        return render_template_string(ERROR_TEMPLATE, message="An internal error occurred while saving your character."), 500

if __name__ == '__main__':
    # Note: For production, this should be run with a proper WSGI server like Gunicorn.
    app.run(host='0.0.0.0', port=5000)