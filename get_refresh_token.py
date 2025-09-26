import requests
import base64
import webbrowser

try:
    from config import ESI_CLIENT_ID, ESI_SECRET_KEY
except ImportError:
    print("Error: config.py not found.")
    print("Please copy config.py.example to config.py and fill in your ESI_CLIENT_ID and ESI_SECRET_KEY.")
    exit()

def main():
    """
    Guides the user through the ESI OAuth2 flow to get a refresh token.
    """
    if not ESI_CLIENT_ID or ESI_CLIENT_ID == "your_client_id_here":
        print("Error: ESI_CLIENT_ID is not set in config.py.")
        print("Please add your application's Client ID to config.py.")
        return

    # Scopes needed for the bot to function
    scopes = "esi-wallet.read_character_wallet.v1"

    # The callback URL you configured in your EVE application
    callback_url = "https://localhost/callback"

    # 1. Generate the authorization URL
    auth_url = (
        f"https://login.eveonline.com/v2/oauth/authorize/?"
        f"response_type=code&"
        f"redirect_uri={callback_url}&"
        f"client_id={ESI_CLIENT_ID}&"
        f"scope={scopes}"
    )

    print("--- EVE Online API Authorization ---")
    print("\n1. A browser window will now open with the EVE Online authorization page.")
    print("   If it does not open, please copy and paste the following URL into your browser:")
    print(f"\n   {auth_url}\n")

    webbrowser.open(auth_url)

    # 2. Get the authorization code from the user
    print("2. Log in, authorize the application, and you will be redirected to a non-functional page.")
    print("   Copy the ENTIRE URL from your browser's address bar and paste it here:\n")

    redirected_url = input("Callback URL: ")

    try:
        # Extract the authorization code from the URL
        auth_code = redirected_url.split("code=")[1].split("&")[0]
    except IndexError:
        print("\nError: Could not find 'code=' in the provided URL.")
        print("Please make sure you copied the full URL after being redirected.")
        return

    # 3. Exchange the authorization code for a refresh token
    print("\n3. Exchanging authorization code for a refresh token...")

    auth_header = base64.b64encode(f"{ESI_CLIENT_ID}:{ESI_SECRET_KEY}".encode()).decode()
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
            print("\n--- SUCCESS! ---")
            print("Your refresh token has been generated.")
            print("Copy the following line and paste it into your config.py file:\n")
            print(f'ESI_REFRESH_TOKEN = "{refresh_token}"\n')
        else:
            print("\nError: Could not retrieve refresh token from the response.")
            print("Response:", token_data)

    except requests.exceptions.RequestException as e:
        print(f"\nError during token exchange: {e}")
        print("Response content:", e.response.text if e.response else "No response")

if __name__ == "__main__":
    main()