from __future__ import annotations

import os

from dotenv import load_dotenv

from src.clients.whoop_client import WhoopClient, WhoopClientConfig
from src.shared.logger import get_logger
from src.shared.whoop_tokens import load_whoop_tokens, save_whoop_tokens_from_response


logger = get_logger(__name__)
logger.setLevel("DEBUG")


def resolve_whoop_tokens() -> dict[str, str | None]:
    """
    Resolve WHOOP tokens for connectivity testing, preferring persisted Postgres tokens.
    """
    stored_tokens = load_whoop_tokens()
    if stored_tokens:
        logger.info(
            "Loaded WHOOP OAuth tokens from Postgres updated_at=%s expires_at=%s",
            stored_tokens.get("updated_at"),
            stored_tokens.get("expires_at"),
        )
        return {
            "access_token": stored_tokens.get("access_token"),
            "refresh_token": stored_tokens.get("refresh_token"),
        }

    access_token = os.environ.get("WHOOP_ACCESS_TOKEN")
    refresh_token = os.environ.get("WHOOP_REFRESH_TOKEN")

    if access_token and refresh_token:
        logger.info("Seeding WHOOP OAuth tokens from environment into Postgres")
        save_whoop_tokens_from_response(
            {
                "access_token": access_token,
                "refresh_token": refresh_token,
            }
        )

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
    }


def test_collection(client: WhoopClient, collection_name: str) -> None:
    """
    Test a single WHOOP collection by fetching one page of records.

    Args:
        client (WhoopClient): Initialized WHOOP client.
        collection_name (str): Collection name to test.
    """
    logger.info(f"Testing WHOOP collection: {collection_name}")

    records = list(
        client.iter_collection_records(
            collection_name=collection_name,
            max_pages=1,
            limit=10,
        )
    )

    logger.info(
        f"WHOOP collection '{collection_name}' returned {len(records)} records."
    )

    if records:
        sample_record = records[0]
        logger.debug(f"Sample {collection_name} record: {sample_record}")
    else:
        logger.warning(f"No records returned for WHOOP collection '{collection_name}'.")


def main() -> int:
    """
    Test WHOOP API connectivity across supported collections.

    Returns:
        int: Exit code.
    """
    load_dotenv()

    logger.info("Starting WHOOP connection test")
    whoop_tokens = resolve_whoop_tokens()

    client = WhoopClient(
        WhoopClientConfig(
            client_id=os.environ["WHOOP_CLIENT_ID"],
            client_secret=os.environ["WHOOP_CLIENT_SECRET"],
            redirect_uri=os.environ["WHOOP_REDIRECT_URI"],
            access_token=whoop_tokens.get("access_token"),
            refresh_token=whoop_tokens.get("refresh_token"),
            token_update_callback=save_whoop_tokens_from_response,
        )
    )

    try:
        test_collection(client, "cycle")
        test_collection(client, "recovery")
        test_collection(client, "sleep")
        test_collection(client, "workout")

        logger.info("Finished WHOOP connection test successfully")
        return 0

    except Exception:
        logger.exception("WHOOP connection test failed")
        return 1

    finally:
        client.close()
        logger.debug("Closed WHOOP client")
        
        
if __name__ == "__main__":
    raise SystemExit(main())
