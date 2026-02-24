import os
import sys
from dotenv import load_dotenv
from typing import Dict, Any

from src.clients.notion_client import NotionClient, NotionClientConfig, NotionAPIError
from src.shared.logger import get_logger

logger = get_logger("test_notion_client")
logger.setLevel('DEBUG')

def _safe_get_property_names(page: Dict[str, Any]) -> list[str]:
    """Return the property names present on a Notion page object."""
    # Extract properties from page
    properties = page.get("properties") or {}
    
    # Validate properties
    if isinstance(properties, dict):
        return sorted(list(properties.keys()))
    return []


def main() -> int:
    """
    Test connectivity to the Notion API by querying a database and printing basic info.

    Expected env vars:
        NOTION_TOKEN
        NOTION_DATABASE_ID
    """
    logger.info("Starting Notion client integration test script")
    
    # Load and store environment variables
    load_dotenv()
    token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_DATABASE_ID")

    # Validate token
    if not token:
        logger.error("NOTION_TOKEN is missing")
        return 1

    # Validate Database ID
    if not database_id:
        logger.error("NOTION_DATABASE_ID is missing")
        return 1

    # Create Notion Client with config
    notion_client = NotionClient(NotionClientConfig(token=token))
    logger.debug("Initialized NotionClient for connectivity test")

    try:
        # Pull only 1 API page (up to 100 rows) to validate access
        logger.info("Querying Notion database_id=%s page_size=10", database_id)
        response = notion_client.query_database(database_id, page_size=10)

        # Extract results from response
        results = response.get("results", [])
        has_more = bool(response.get("has_more"))
        next_cursor = response.get("next_cursor")

        logger.info(
            "Notion API connectivity check succeeded rows=%s has_more=%s next_cursor_present=%s",
            len(results),
            has_more,
            bool(next_cursor),
        )

        # Display first page, if results are returned
        if results:
            first = results[0]
            logger.debug("First page id=%s", first.get("id"))
            
            properties = _safe_get_property_names(first)
            logger.debug(f'Properties: {properties}')
        else:
            logger.debug("No rows returned. (Database may be empty, or filters/views may restrict results.)")

        return 0

    except NotionAPIError as e:
        logger.exception("Notion API error during connectivity test")
        return 2
    except Exception as e:
        logger.exception("Unexpected error during connectivity test")
        return 3
    finally:
        notion_client.close()
        logger.debug("Notion client closed")


if __name__ == "__main__":
    raise SystemExit(main()) 
