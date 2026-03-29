from __future__ import annotations

import os
import secrets
import webbrowser
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from src.clients.whoop_client import WhoopClient, WhoopClientConfig
from src.shared.logger import get_logger
from src.shared.whoop_tokens import save_whoop_tokens_from_response


logger = get_logger(__name__)


def extract_code_and_state(redirected_url: str) -> tuple[str | None, str | None]:
    """
    Extract the OAuth code and state from a redirected URL.

    Args:
        redirected_url: Full URL from the browser after WHOOP redirects.

    Returns:
        Tuple of (code, state).
    """
    parsed = urlparse(redirected_url)
    query_params = parse_qs(parsed.query)

    code = query_params.get("code", [None])[0]
    state = query_params.get("state", [None])[0]
    return code, state


def main() -> int:
    """
    Run the one-time WHOOP OAuth flow and print access/refresh tokens.
    """
    load_dotenv()

    client_id = os.environ["WHOOP_CLIENT_ID"]
    client_secret = os.environ["WHOOP_CLIENT_SECRET"]
    redirect_uri = os.environ["WHOOP_REDIRECT_URI"]

    state = secrets.token_urlsafe(16)

    client = WhoopClient(
        WhoopClientConfig(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            token_update_callback=save_whoop_tokens_from_response,
        )
    )

    try:
        auth_url = client.build_authorization_url(state=state)

        logger.info("Opening WHOOP authorization URL in browser")
        print("\nOpen this URL if your browser does not launch automatically:\n")
        print(auth_url)
        print("")

        try:
            webbrowser.open(auth_url)
        except Exception:
            logger.warning("Could not open browser automatically")

        print("After approving access, WHOOP will redirect to your redirect URI.")
        print("It may show a browser error page at localhost, which is expected.")
        print("Copy the FULL redirected URL from your browser and paste it below.\n")

        redirected_url = input("Paste redirected URL: ").strip()
        if not redirected_url:
            logger.error("No redirected URL was provided.")
            return 1

        code, returned_state = extract_code_and_state(redirected_url)

        if not code:
            logger.error("Could not find `code` in the redirected URL.")
            return 1

        if returned_state != state:
            logger.error("State mismatch. Expected %s but got %s", state, returned_state)
            return 1

        token_response = client.exchange_code_for_tokens(code)

        access_token = token_response.get("access_token")
        refresh_token = token_response.get("refresh_token")
        expires_in = token_response.get("expires_in")
        scope = token_response.get("scope")

        print("\nWHOOP token exchange succeeded.\n")
        print(f"access_token: {access_token}")
        print(f"refresh_token: {refresh_token}")
        print(f"expires_in: {expires_in}")
        print(f"scope: {scope}")

        print("\nAdd these to your .env:\n")
        print(f"WHOOP_ACCESS_TOKEN={access_token}")
        print(f"WHOOP_REFRESH_TOKEN={refresh_token}")
        print("\nTokens were also persisted to Postgres in app.oauth_tokens.\n")

        return 0

    except Exception:
        logger.exception("WHOOP OAuth test failed")
        return 1

    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
