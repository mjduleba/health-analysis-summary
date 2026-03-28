from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from src.clients.whoop_client import WhoopClient, WhoopClientConfig
from src.shared.db import get_conn
from src.shared.logger import get_logger

# Initialize global logger
logger = get_logger(__name__)

# Initialize global collections storage
WHOOP_COLLECTIONS = (
    {
        'collection_name': 'cycle',
        'table_name': 'raw.whoop_cycles',
        'id_field': 'id',
    },
    {
        'collection_name': 'recovery',
        'table_name': 'raw.whoop_recoveries',
        'id_field': 'cycle_id',
    },
    {
        'collection_name': 'sleep',
        'table_name': 'raw.whoop_sleeps',
        'id_field': 'id',
    },
    {
        'collection_name': 'workout',
        'table_name': 'raw.whoop_workouts',
        'id_field': 'id',
    },
)


def get_sync_window() -> tuple[str, str]:
    '''
    Build the WHOOP sync lookback window using REPORT_LOOKBACK_DAYS.

    Returns:
        tuple[str, str]: ISO 8601 UTC start and end timestamps.
    '''
    # Store environment variable for lookback days
    lookback_days = int(os.environ['REPORT_LOOKBACK_DAYS'])
    
    # Store start and end of window
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=lookback_days)
    
    return start.isoformat(), now.isoformat()


def build_upsert_sql(table_name: str) -> str:
    '''
    Build the per-table upsert statement for raw WHOOP ingestion.
    '''
    return f'''
    INSERT INTO {table_name} (
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


def normalize_record(
    record: dict[str, Any],
    *,
    id_field: str,
) -> dict[str, Any] | None:
    '''
    Normalizes record into raw upsert payload.

    Args:
        record (dict[str, Any]): record from Whoop API
        id_field (str): id

    Returns:
        dict[str, Any] | None: upsert data
    '''
    # Extract ID from record
    record_id = record.get(id_field)
    
    # Validate record
    if record_id is None:
        logger.warning(
            'Skipping WHOOP record missing %s: %s',
            id_field,
            record,
        )
        return None

    return {
        'id': str(record_id),
        'source_updated_at': record.get('updated_at'),
        'payload': Jsonb(record),
    }


def fetch_collection_records(client: WhoopClient, collection_name: str) -> list[dict[str, Any]]:
    '''
    Fetch records for given collection.

    Args:
        client (WhoopClient): Whoop API Client
        collection_name (str): collection being extracted

    Returns:
        list[dict[str, Any]]: list of records
    '''
    
    # Store start and end of data window
    start, end = get_sync_window()
    logger.info(
        'Fetching WHOOP collection=%s start=%s end=%s',
        collection_name,
        start,
        end,
    )
    
    # Fetch records for given collection, inside date window
    records = list(
        client.iter_collection_records(
            collection_name=collection_name,
            start=start,
            end=end,
        )
    )
    logger.info('Fetched %s WHOOP %s records', len(records), collection_name)
    return records


def upsert_collection_records(
    table_name: str,
    records: list[dict[str, Any]],
    *,
    id_field: str,
) -> int:
    '''
    Upserts all records from a collection into its Postgres data table.

    Args:
        table_name (str): name of table for upsert
        records (list[dict[str, Any]]): records being upserted into table
        id_field (str): id

    Returns:
        int: number of rows upserted
    '''
    # Check if records exist
    if not records:
        logger.info('No WHOOP records to upsert for table=%s', table_name)
        return 0

    # Initialize unique upsert for table
    upsert_sql = build_upsert_sql(table_name)
    row_count = 0

    logger.debug('Opening Postgres transaction for WHOOP upsert table=%s', table_name)

    with get_conn() as conn:
        with conn.cursor() as cur:
            for record in records:
                # Get normalized record for upsert payload
                normalized_record = normalize_record(record, id_field=id_field)
                
                # Validate record return
                if normalized_record is None:
                    continue
                
                # Exectute upsert statement
                cur.execute(upsert_sql, normalized_record)
                row_count += 1

    logger.info('Upserted %s rows into %s', row_count, table_name)
    return row_count


def sync_collection(
    client: WhoopClient,
    *,
    collection_name: str,
    table_name: str,
    id_field: str,
) -> int:
    '''
    Fetch and upsert one collection from Whoop into Postgres.

    Args:
        client (WhoopClient): Whoop Client 
        collection_name (str): name of collection being fetched/upserted
        table_name (str): name of table
        id_field (str): id

    Returns:
        int: number of rows upserted
    '''
    logger.info(
        'Processing WHOOP collection=%s table=%s id_field=%s',
        collection_name,
        table_name,
        id_field,
    )
    
    # Fetch and store collection records
    records = fetch_collection_records(client, collection_name)
    
    # Upsert records into Postgres
    rows_upserted = upsert_collection_records(
        table_name,
        records,
        id_field=id_field,
    )
    
    logger.info(
        'Completed WHOOP collection=%s fetched=%s upserted=%s',
        collection_name,
        len(records),
        rows_upserted,
    )
    return rows_upserted


def main() -> int:
    '''
    Driver function for the WHOOP raw ingestion job.

    Returns:
        int: 0 on success, 1 on failure.
    '''
    # Load in environment variables
    load_dotenv()

    logger.info('Starting WHOOP Sync Job')

    # Create WhoopClient using WhoopClientConfig
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
        # Initialize upsert row counter
        total_rows_upserted = 0
        
        # Fetch and upsert each collection
        for collection_config in WHOOP_COLLECTIONS:
            total_rows_upserted += sync_collection(client, **collection_config)

        logger.info(
            'Completed WHOOP Sync Job total_rows_upserted=%s collections=%s',
            total_rows_upserted,
            len(WHOOP_COLLECTIONS),
        )
        return 0
    except Exception:
        logger.exception('WHOOP Sync Job failed')
        return 1
    finally:
        client.close()
        logger.info('Finished WHOOP Sync Job')


if __name__ == '__main__':
    raise SystemExit(main())
