from typing import Iterable, Dict, Any
from dotenv import load_dotenv
import os
from psycopg.types.json import Jsonb

from src.shared.logger import get_logger
from src.shared.db import get_conn
from src.clients.notion_client import NotionClient, NotionClientConfig

# Create global logger
logger = get_logger(__name__)

# SQL statement to upsert Notion entry into the database
UPSERT_NOTION_ENTRY_SQL = '''
INSERT INTO raw.notion_entries (
    id,
    source_updated_at,
    payload
)
VALUES (
    %(id)s,
    %(source_updated_at)s,
    %(payload)s::jsonb
)
ON CONFLICT (id) DO UPDATE
SET
    source_updated_at = EXCLUDED.source_updated_at,
    payload = EXCLUDED.payload,
    ingested_at = NOW();
'''


def upsert_pages(pages: Iterable[Dict[str, Any]]) -> int:
    '''
    Takes pages from Notion API and upserts them into Postgres
    Database. Uses page ID as primary key to enforce updates.
    Returns the number of rows upserted for logging purposes.

    Args:
        pages (Iterable[Dict[str, Any]]): Notion Database pages

    Returns:
        int: number of rows upserted
    '''
    # Store the number of rows upserted
    row_count = 0
    
    # Create connection and cursor
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Iterate through pages and yield each page
            for page in pages:
                # Store fields from page
                page_id = page.get('id')
                source_updated_at = page.get('last_edited_time')
                
                # Validate page ID
                if not page_id:
                    logger.warning(f'Page missing ID, skipping: {page}')
                    continue
                
                # Execute upsert statement
                cur.execute(UPSERT_NOTION_ENTRY_SQL, 
                    {
                        'id': page_id,
                        'source_updated_at': source_updated_at,
                        'payload': Jsonb(page)
                    }
                )
                row_count += 1
    return row_count
    
def main() -> int:
    '''
    Driver function for Notion Sync Job. Fetches pages from Notion
    Database and upserts them into Postgres database.
    Returns 0 on success and 1 on failure for logging purposes.

    Returns:
        int: 0 on success, 1 on failure
    '''
    # Load environment variables
    load_dotenv()
    
    # Store Notion environment variables
    notion_token = os.environ["NOTION_TOKEN"]
    notion_database_id = os.environ["NOTION_DATABASE_ID"]
    
    logger.info('Starting Notion Sync Job')
    
    # Create NotionAPI client
    notion_client_config = NotionClientConfig(token=notion_token)
    notion_client = NotionClient(notion_client_config)
    
    # Fetch pages from Notion API and upsert into Postgres
    try:
        logger.info(f'Fetching pages from Notion database {notion_database_id}')
        pages = notion_client.iter_database_pages(notion_database_id)
        
        logger.info('Upserting pages into Postgres database')
        rows_upserted = upsert_pages(pages)
        logger.info(
            'Fetched pages from Notion API and upserted into Postgres database.',
            extra={
                'pages': rows_upserted
            }
        )
        return 0
    except Exception as e:
        logger.error(f'Notion Sync Job failed. Error: {str(e)}')
        return 1
    finally:
        # Close Notion client session
        notion_client.close()
        logger.info('Finished Notion Sync Job')
        
if __name__ == "__main__":
    raise SystemExit(main())
    
