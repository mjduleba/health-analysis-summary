from __future__ import annotations

import os

from dotenv import load_dotenv

from src.clients.whoop_client import WhoopClient, WhoopClientConfig
from src.jobs.whoop_sync import WHOOP_COLLECTIONS, get_sync_window, sync_collection
from src.shared.db import get_conn
from src.shared.logger import get_logger

# Initialize global logger
logger = get_logger(__name__)
logger.setLevel('DEBUG')


def validate_env() -> bool:
    '''
    Validate the environment variables required for the WHOOP sync integration test.
    '''
    # Create environment variables expected
    required_env_vars = (
        'WHOOP_CLIENT_ID',
        'WHOOP_CLIENT_SECRET',
        'WHOOP_REDIRECT_URI',
        'WHOOP_ACCESS_TOKEN',
        'REPORT_LOOKBACK_DAYS',
        'POSTGRES_HOST',
        'POSTGRES_DB',
        'POSTGRES_USER',
        'POSTGRES_PASSWORD',
    )

    # Create list using missing variables mask
    missing = [env_var for env_var in required_env_vars if not os.getenv(env_var)]
    
    # Log any missing variables
    if missing:
        logger.error('Missing required env vars: %s', ', '.join(sorted(missing)))
        return False

    return True


def get_table_row_count(table_name: str) -> int:
    '''
    Return the current row count for a raw WHOOP table.
    '''
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM {table_name}')
            return int(cur.fetchone()[0])


def log_table_snapshot(table_name: str, *, label: str) -> None:
    '''
    Log a compact snapshot of the current raw WHOOP table contents.
    '''
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'''
                SELECT
                    COUNT(*) AS row_count,
                    MAX(source_updated_at) AS max_source_updated_at,
                    MAX(ingested_at) AS max_ingested_at
                FROM {table_name}
                '''
            )
            row_count, max_source_updated_at, max_ingested_at = cur.fetchone()

    logger.info(
        '%s table=%s rows=%s latest_source_updated_at=%s latest_ingested_at=%s',
        label,
        table_name,
        row_count,
        max_source_updated_at,
        max_ingested_at,
    )


def main() -> int:
    '''
    Run an end-to-end integration test for the WHOOP sync job.
    '''
    # Load environment variables into memory
    load_dotenv()

    # Validate environment variables
    if not validate_env():
        return 1

    # Find start and end of sync window
    start, end = get_sync_window()
    logger.info('Starting WHOOP sync integration test start=%s end=%s', start, end)

    # Create Whoop API Client wrapper
    client = WhoopClient(
        WhoopClientConfig(
            client_id=os.environ['WHOOP_CLIENT_ID'],
            client_secret=os.environ['WHOOP_CLIENT_SECRET'],
            redirect_uri=os.environ['WHOOP_REDIRECT_URI'],
            access_token=os.environ.get('WHOOP_ACCESS_TOKEN'),
            refresh_token=os.environ.get('WHOOP_REFRESH_TOKEN'),
        )
    )

    try:
        total_rows_upserted = 0

        # Iterate through each colletion
        for collection_config in WHOOP_COLLECTIONS:
            # Store collection and table
            collection_name = collection_config['collection_name']
            table_name = collection_config['table_name']

            logger.info('Testing WHOOP sync collection=%s table=%s', collection_name, table_name)

            # Log before sync has been executed
            rows_before = get_table_row_count(table_name)
            log_table_snapshot(table_name, label='Before sync')

            # Execute collection sync
            rows_upserted = sync_collection(client, **collection_config)
            rows_after = get_table_row_count(table_name)

            # Log sync results
            logger.info(
                'Sync result collection=%s upserted=%s rows_before=%s rows_after=%s delta=%s',
                collection_name,
                rows_upserted,
                rows_before,
                rows_after,
                rows_after - rows_before,
            )
            log_table_snapshot(table_name, label='After sync')

            total_rows_upserted += rows_upserted

        logger.info(
            'Finished WHOOP sync integration test total_rows_upserted=%s collections=%s',
            total_rows_upserted,
            len(WHOOP_COLLECTIONS),
        )
        return 0

    except Exception:
        logger.exception('WHOOP sync integration test failed')
        return 1

    finally:
        client.close()
        logger.debug('Closed WHOOP client')


if __name__ == '__main__':
    raise SystemExit(main())
