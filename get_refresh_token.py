import requests
import base64
import webbrowser
import secrets
import string
import os
import re

try:
    # We only import these to check if they are set, not to use them directly.
    from config import ESI_CLIENT_ID, ESI_SECRET_KEY
except ImportError:
    if not os.path.exists("config.py"):
        print("Error: config.py not found.")
        print("Please copy config.py.example to config.py and fill in your ESI_CLIENT_ID and ESI_SECRET_KEY.")
        exit()
    # If config.py exists but the import fails, it's likely a user error we can ignore for now.
    ESI_CLIENT_ID = None
    ESI_SECRET_KEY = None

def generate_state_token(length=16):
    """Generates a secure random string for the state parameter."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for i in range(length))

def get_next_token_number():
    """Finds the next available number for ESI_REFRESH_TOKEN_X in config.py."""
    if not os.path.exists("config.py"):
        return 1

    try:
        with open("config.py", "r") as f:
            content = f.read()

        # Find all numbers from variables like ESI_REFRESH_TOKEN_1, ESI_REFRESH_TOKEN_2, etc.
        token_numbers = re.findall(r"^\s*ESI_REFRESH_TOKEN_(\d+)\s*=", content, re.MULTILINE)

        if not token_numbers:
            return 1

        return max(int(n) for n in token_numbers) + 1
    except Exception as e:
        print(f"Warning: Could not read config.py to determine next token number: {e}")
        # Default to 1, user can manually change it.
        return 1

def main():
    """
    Guides the user through the ESI OAuth2 flow to get a refresh token.
    """
    # Since we might not have imported them, we need to get them from the config module dynamically
    import config

    if not hasattr(config, 'ESI_CLIENT_ID') or config.ESI_CLIENT_ID == "your_client_id_here":
        print("Error: ESI_CLIENT_ID is not set in config.py.")
        print("Please add your application's Client ID to config.py.")
        return

    scopes = [
        "esi-wallet.read_character_wallet.v1",
        "esi-markets.read_character_orders.v1",
        "esi-universe.read_structures.v1",
        "esi-characters.read_notifications.v1" # Required for character name resolution
    ]
    scopes_string = " ".join(scopes)
    callback_url = "https://localhost/callback"

    # 1. Generate the authorization URL with a state parameter
    state = generate_state_token()
    auth_url = (
        f"https://login.eveonline.com/v2/oauth/authorize/?"
        f"response_type=code&"
        f"redirect_uri={callback_url}&"
        f"client_id={config.ESI_CLIENT_ID}&"
        f"scope={scopes_string}&"
        f"state={state}"
    )

    print("--- EVE Online API Authorization ---")
    print("\n1. A browser window will now open with the EVE Online authorization page.")
    print("   If it does not open, please copy and paste the following URL into your browser:")
    print(f"\n   {auth_url}\n")

    try:
        webbrowser.open(auth_url)
    except webbrowser.Error:
        print("Could not open a web browser automatically.")

    # 2. Get the authorization code from the user
    print("2. Log in, authorize the application, and you will be redirected to a non-functional page.")
    print("   Copy the ENTIRE URL from your browser's address bar and paste it here:\n")

    redirected_url = input("Callback URL: ")

    try:
        # Extract the authorization code and state from the URL
        auth_code = redirected_url.split("code=")[1].split("&")[0]
        returned_state = redirected_url.split("state=")[1].split("&")[0]
    except IndexError:
        print("\nError: Could not find 'code=' or 'state=' in the provided URL.")
        print("Please make sure you copied the full URL after being redirected.")
        return

    # 3. Verify the state parameter to prevent CSRF attacks
    if returned_state != state:
        print("\nError: State parameter mismatch. This could indicate a security issue.")
        print("Please try running the script again.")
        return

    # 4. Exchange the authorization code for a refresh token
    print("\n4. Exchanging authorization code for a refresh token...")

    auth_header = base64.b64encode(f"{config.ESI_CLIENT_ID}:{config.ESI_SECRET_KEY}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "login.eveonline.com"
    }
    data = {
        "grant_type": "authorization_code",
        "code": auth_code
    }

    try:
        response = requests.post("https://login.eveonline.com/v2/oauth/token", headers=headers, data=data)
        response.raise_for_status()
        token_data = response.json()
        refresh_token = token_data.get("refresh_token")

        if refresh_token:
            token_number = get_next_token_number()
            print("\n--- SUCCESS! ---")
            print("Your refresh token has been generated.")
            if token_number > 1:
                print(f"This appears to be for character number {token_number}. If this is wrong, you can manually change the number.")
            print("\nCopy the following line and paste it into your config.py file:\n")
            print(f'ESI_REFRESH_TOKEN_{token_number} = "{refresh_token}"\n')
        else:
            print("\nError: Could not retrieve refresh token from the response.")
            print("Response:", token_data)

    except requests.exceptions.RequestException as e:
        print(f"\nError during token exchange: {e}")
        print("Response content:", e.response.text if e.response else "No response")

if __name__ == "__main__":
    main()