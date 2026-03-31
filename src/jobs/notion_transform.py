from __future__ import annotations

import os
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from src.shared.db import get_conn
from src.shared.logger import get_logger

# Create global logger
logger = get_logger(__name__)


RAW_NOTION_TABLE = 'raw.notion_entries'


def get_transform_window() -> tuple[pd.Timestamp, pd.Timestamp]:
    '''
    Build the Notion transform window from REPORT_LOOKBACK_DAYS.

    Returns:
        tuple[pd.Timestamp, pd.Timestamp]: window start/end in UTC.
    '''
    # Store lookback window using environment variable
    lookback_days = int(os.environ['REPORT_LOOKBACK_DAYS'])
    end_date = pd.Timestamp.now(tz='UTC')
    start_date = end_date - pd.Timedelta(days=lookback_days)
    return start_date, end_date


def fetch_raw_notion_entries() -> pd.DataFrame:
    '''
    Fetch raw Notion rows from Postgres.

    Returns:
        pd.DataFrame: raw notion rows with payload JSON.
    '''
    # Create SQL query to fetch raw Notion records
    query = f'''
    SELECT
        id,
        ingested_at,
        source_updated_at,
        payload
    FROM {RAW_NOTION_TABLE}
    ORDER BY source_updated_at ASC NULLS LAST, ingested_at ASC;
    '''

    # Execute query and load results into DataFrame
    with get_conn() as conn:
        df = pd.read_sql_query(query, conn)

    logger.info('Fetched %s raw rows from %s', len(df), RAW_NOTION_TABLE)
    return df


def extract_property(
    properties: dict[str, Any],
    property_name: str,
    property_type: str,
    default: Any = None,
) -> Any:
    '''
    Safely extract a Notion property value by type.

    Args:
        properties (dict[str, Any]): Notion page properties object.
        property_name (str): name of property to extract.
        property_type (str): Notion property type.
        default (Any, optional): fallback value.

    Returns:
        Any: extracted property value or default.
    '''
    # Retrieve requested property from Notion page properties
    property_value = properties.get(property_name, {})
    if not isinstance(property_value, dict):
        return default

    # Extract date property values
    if property_type == 'date':
        date_value = property_value.get('date') or {}
        if not isinstance(date_value, dict):
            return default
        return date_value.get('start', default)

    # Extract number and checkbox values
    if property_type in {'number', 'checkbox'}:
        return property_value.get(property_type, default)

    # Extract select option names
    if property_type == 'select':
        select_value = property_value.get('select') or {}
        if not isinstance(select_value, dict):
            return default
        return select_value.get('name', default)

    # Extract title and rich text content
    if property_type in {'title', 'rich_text'}:
        items = property_value.get(property_type) or []
        if not items:
            return default
        return ''.join(item.get('plain_text', '') for item in items) or default

    return default


def parse_notion_date(value: Any) -> pd.Timestamp:
    '''
    Parse a Notion date string into a pandas timestamp.

    Args:
        value (Any): raw Notion date value.

    Returns:
        pd.Timestamp: parsed timestamp or NaT.
    '''
    if value in (None, ''):
        return pd.NaT
    return pd.to_datetime(value, utc=True, errors='coerce')


def timestamp_to_date(timestamp: pd.Timestamp) -> pd.Timestamp:
    '''
    Convert a timestamp to normalized UTC date.

    Args:
        timestamp (pd.Timestamp): timestamp value.

    Returns:
        pd.Timestamp: normalized date or NaT.
    '''
    if pd.isna(timestamp):
        return pd.NaT
    return timestamp.tz_convert('UTC').normalize()


def ensure_output_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    '''
    Ensure an output DataFrame contains the expected columns in order.

    Args:
        df (pd.DataFrame): candidate DataFrame.
        columns (list[str]): expected columns.

    Returns:
        pd.DataFrame: DataFrame with all expected columns.
    '''
    # Check DataFrame is not empty
    if df.empty:
        return pd.DataFrame(columns=columns)

    # Backfill missing columns so downstream joins have a stable schema
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df.loc[:, columns]


def transform_notion_entries(raw_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Transform raw Notion payloads into a normalized entry DataFrame.

    Args:
        raw_df (pd.DataFrame): raw Notion rows.

    Returns:
        pd.DataFrame: normalized entry rows.
    '''
    # Store expected Notion entry columns
    columns = [
        'page_id',
        'date',
        'weight_lbs',
        'calories',
        'protein_g',
        'notes',
        'source_updated_at',
    ]

    # Check if DataFrame is empty, backfill missing columns
    if raw_df.empty:
        return ensure_output_columns(pd.DataFrame(), columns)

    # Build normalized entry records from raw payloads
    records: list[dict[str, Any]] = []
    for _, row in raw_df.iterrows():
        payload = row['payload'] or {}
        properties = payload.get('properties', {})
        notion_date = parse_notion_date(extract_property(properties, 'Date', 'date'))

        records.append(
            {
                'page_id': payload.get('id') or row.get('id'),
                'date': timestamp_to_date(notion_date),
                'weight_lbs': extract_property(properties, 'Weight (lbs)', 'number'),
                'calories': extract_property(properties, 'Caloric Intake', 'number'),
                'protein_g': extract_property(properties, 'Protein (g)', 'number'),
                'notes': extract_property(properties, 'Notes', 'rich_text'),
                'source_updated_at': parse_notion_date(payload.get('last_edited_time') or row.get('source_updated_at')),
            }
        )

    # Deduplicate by page ID and keep the latest version
    df = pd.DataFrame(records)
    df = df.dropna(subset=['date']).drop_duplicates(subset=['page_id'], keep='last')
    return ensure_output_columns(df, columns)


def build_daily_notion_dataframe(entries_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Aggregate normalized Notion entry rows into one daily DataFrame.

    Args:
        entries_df (pd.DataFrame): transformed Notion entries.

    Returns:
        pd.DataFrame: one daily Notion summary row per date.
    '''
    # Store expected Notion daily columns
    columns = [
        'date',
        'page_count',
        'weight_lbs',
        'calories',
        'protein_g',
        'notes',
    ]

    # Check if DataFrame is empty, backfill missing columns
    if entries_df.empty:
        return ensure_output_columns(pd.DataFrame(), columns)

    # Sort rows so the latest page is selected for non-additive fields
    entries_df = entries_df.copy().sort_values(['date', 'source_updated_at'])

    # Aggregate multiple pages into one daily summary row
    daily_records: list[dict[str, Any]] = []
    for date, group in entries_df.groupby('date', dropna=True):
        latest_row = group.iloc[-1]
        daily_records.append(
            {
                'date': date,
                'page_count': int(len(group)),
                'weight_lbs': latest_row['weight_lbs'],
                'calories': group['calories'].sum(min_count=1),
                'protein_g': group['protein_g'].sum(min_count=1),
                'notes': latest_row['notes'],
            }
        )

    return ensure_output_columns(pd.DataFrame(daily_records), columns)


def backfill_weight_lbs_nearest(daily_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Fill missing daily weight values using the nearest observed weight by date.

    Args:
        daily_df (pd.DataFrame): daily Notion DataFrame.

    Returns:
        pd.DataFrame: daily DataFrame with nearest weight fill applied.
    '''
    # Check if DataFrame is empty or no weight data exists
    if daily_df.empty or daily_df['weight_lbs'].notna().sum() == 0:
        return daily_df.copy()

    # Split rows into missing and observed weight sets
    daily_df = daily_df.copy().sort_values('date').reset_index(drop=True)
    observed_df = daily_df.loc[daily_df['weight_lbs'].notna(), ['date', 'weight_lbs']]
    missing_df = daily_df.loc[daily_df['weight_lbs'].isna(), ['date']].copy()
    if missing_df.empty:
        return daily_df

    # Find the nearest observed weight on each side of the missing date
    previous_df = pd.merge_asof(
        missing_df,
        observed_df.rename(columns={'date': 'previous_date', 'weight_lbs': 'previous_weight'}),
        left_on='date',
        right_on='previous_date',
        direction='backward',
    )
    next_df = pd.merge_asof(
        missing_df,
        observed_df.rename(columns={'date': 'next_date', 'weight_lbs': 'next_weight'}),
        left_on='date',
        right_on='next_date',
        direction='forward',
    )

    # Fill using whichever observed weight is closest in time
    fill_df = missing_df.copy()
    fill_df['previous_date'] = previous_df['previous_date']
    fill_df['previous_weight'] = previous_df['previous_weight']
    fill_df['next_date'] = next_df['next_date']
    fill_df['next_weight'] = next_df['next_weight']
    fill_df['previous_gap'] = (fill_df['date'] - fill_df['previous_date']).abs()
    fill_df['next_gap'] = (fill_df['next_date'] - fill_df['date']).abs()

    fill_df['weight_lbs'] = fill_df['previous_weight']
    use_next_mask = fill_df['previous_weight'].isna() | (
        fill_df['next_weight'].notna() & fill_df['next_gap'].lt(fill_df['previous_gap'])
    )
    fill_df.loc[use_next_mask, 'weight_lbs'] = fill_df.loc[use_next_mask, 'next_weight']

    # Apply filled weights back onto the daily DataFrame
    daily_df = daily_df.merge(fill_df[['date', 'weight_lbs']], on='date', how='left', suffixes=('', '_filled'))
    daily_df['weight_lbs'] = daily_df['weight_lbs'].fillna(daily_df['weight_lbs_filled'])
    return daily_df.drop(columns=['weight_lbs_filled'])


def filter_to_window(
    df: pd.DataFrame,
    *,
    date_column: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    '''
    Filter a transformed DataFrame to the configured lookback window.

    Args:
        df (pd.DataFrame): transformed DataFrame.
        date_column (str): name of date column.
        start_date (pd.Timestamp): inclusive start.
        end_date (pd.Timestamp): inclusive end.

    Returns:
        pd.DataFrame: filtered DataFrame.
    '''
    if df.empty:
        return df.copy()

    # Filter using normalized date boundaries
    mask = (df[date_column] >= start_date.normalize()) & (df[date_column] <= end_date.normalize())
    return df.loc[mask].sort_values(date_column).reset_index(drop=True)


def transform_all_notion_data() -> dict[str, pd.DataFrame]:
    '''
    Fetch, transform, and aggregate all Notion raw data.

    Returns:
        dict[str, pd.DataFrame]: transformed per-entry and daily outputs.
    '''
    # Build the transform window used across all Notion outputs
    start_date, end_date = get_transform_window()

    # Fetch raw Notion pages from Postgres
    raw_entries_df = fetch_raw_notion_entries()

    # Transform raw Notion pages into normalized rows
    entries_df = filter_to_window(
        transform_notion_entries(raw_entries_df),
        date_column='date',
        start_date=start_date,
        end_date=end_date,
    )

    # Aggregate normalized rows into a daily Notion DataFrame
    daily_df = build_daily_notion_dataframe(entries_df)
    daily_df = backfill_weight_lbs_nearest(daily_df)
    daily_df = filter_to_window(
        daily_df,
        date_column='date',
        start_date=start_date,
        end_date=end_date,
    )

    return {
        'entries': entries_df,
        'daily': daily_df,
    }


def main() -> int:
    '''
    Driver function for the Notion transform job.

    Returns:
        int: 0 on success, 1 on failure.
    '''
    load_dotenv()
    logger.info('Starting Notion Transform Job')

    try:
        # Execute Notion transform pipeline and log output details
        outputs = transform_all_notion_data()
        for name, df in outputs.items():
            logger.info('Notion transform output=%s rows=%s columns=%s', name, len(df), list(df.columns))
            logger.debug('Notion transform sample output=%s\n%s', name, df.head())
        logger.info('Finished Notion Transform Job successfully')
        return 0
    except Exception:
        logger.exception('Notion Transform Job failed')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
