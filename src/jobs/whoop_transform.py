from __future__ import annotations

import os
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from src.shared.db import get_conn
from src.shared.logger import get_logger

# Create global logger
logger = get_logger(__name__)


RAW_WHOOP_TABLES = {
    'cycles': 'raw.whoop_cycles',
    'recoveries': 'raw.whoop_recoveries',
    'sleeps': 'raw.whoop_sleeps',
    'workouts': 'raw.whoop_workouts',
}


def get_transform_window() -> tuple[pd.Timestamp, pd.Timestamp]:
    '''
    Build the WHOOP transform window from REPORT_LOOKBACK_DAYS.

    Returns:
        tuple[pd.Timestamp, pd.Timestamp]: window start/end in UTC.
    '''
    # Store lookback window using environment variable
    lookback_days = int(os.environ['REPORT_LOOKBACK_DAYS'])
    end_date = pd.Timestamp.now(tz='UTC')
    start_date = end_date - pd.Timedelta(days=lookback_days)
    return start_date, end_date


def fetch_raw_collection(table_name: str) -> pd.DataFrame:
    '''
    Fetch raw WHOOP rows from Postgres for a single collection table.

    Args:
        table_name (str): raw WHOOP table name.

    Returns:
        pd.DataFrame: raw table rows with payload JSON.
    '''
    # Create SQL query to fetch raw WHOOP records
    query = f'''
    SELECT
        id,
        ingested_at,
        source_updated_at,
        payload
    FROM {table_name}
    ORDER BY source_updated_at ASC NULLS LAST, ingested_at ASC;
    '''

    # Execute query and load results into DataFrame
    with get_conn() as conn:
        df = pd.read_sql_query(query, conn)

    logger.info('Fetched %s raw rows from %s', len(df), table_name)
    return df


def extract_payload_field(payload: dict[str, Any], path: list[str], default: Any = None) -> Any:
    '''
    Safely extract a nested field from a WHOOP payload.

    Args:
        payload (dict[str, Any]): WHOOP payload.
        path (list[str]): nested key path.
        default (Any, optional): fallback value.

    Returns:
        Any: nested value or default.
    '''
    # Walk the nested payload path one key at a time
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def parse_timestamp(value: Any) -> pd.Timestamp:
    '''
    Parse an incoming WHOOP timestamp into a pandas timestamp.

    Args:
        value (Any): raw timestamp value.

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


def milli_to_hours(value: Any) -> float | None:
    '''
    Convert milliseconds to hours.

    Args:
        value (Any): duration in milliseconds.

    Returns:
        float | None: duration in hours.
    '''
    if value in (None, '') or pd.isna(value):
        return None
    return float(value) / 3_600_000


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


def transform_cycles(raw_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Transform raw WHOOP cycle payloads into a normalized cycle DataFrame.

    Args:
        raw_df (pd.DataFrame): raw WHOOP cycles.

    Returns:
        pd.DataFrame: normalized cycle rows.
    '''
    # Store expected columns
    columns = [
        'cycle_id',
        'cycle_start',
        'cycle_end',
        'date',
        'cycle_strain',
        'cycle_kilojoule',
        'cycle_average_heart_rate',
        'cycle_max_heart_rate',
        'score_state',
        'source_updated_at',
    ]
    
    # Check if DataFrame is empty, backfill empty columnns
    if raw_df.empty:
        return ensure_output_columns(pd.DataFrame(), columns)

    # Build normalized cycle records from raw payloads
    records: list[dict[str, Any]] = []
    for _, row in raw_df.iterrows():
        payload = row['payload'] or {}
        cycle_start = parse_timestamp(payload.get('start'))
        cycle_end = parse_timestamp(payload.get('end'))
        date_anchor = cycle_end if not pd.isna(cycle_end) else cycle_start

        records.append(
            {
                'cycle_id': str(payload.get('id')) if payload.get('id') is not None else None,
                'cycle_start': cycle_start,
                'cycle_end': cycle_end,
                'date': timestamp_to_date(date_anchor),
                'cycle_strain': extract_payload_field(payload, ['score', 'strain']),
                'cycle_kilojoule': extract_payload_field(payload, ['score', 'kilojoule']),
                'cycle_average_heart_rate': extract_payload_field(payload, ['score', 'average_heart_rate']),
                'cycle_max_heart_rate': extract_payload_field(payload, ['score', 'max_heart_rate']),
                'score_state': payload.get('score_state'),
                'source_updated_at': parse_timestamp(payload.get('updated_at') or row.get('source_updated_at')),
            }
        )

    # Deduplicate by cycle ID and keep the latest version
    df = pd.DataFrame(records)
    df = df.dropna(subset=['date']).drop_duplicates(subset=['cycle_id'], keep='last')
    return ensure_output_columns(df, columns)


def transform_recoveries(raw_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Transform raw WHOOP recovery payloads into a normalized recovery DataFrame.

    Args:
        raw_df (pd.DataFrame): raw WHOOP recoveries.

    Returns:
        pd.DataFrame: normalized recovery rows.
    '''
    # Store expected Recovery columns
    columns = [
        'cycle_id',
        'sleep_id',
        'date',
        'recovery_score',
        'resting_heart_rate',
        'heart_rate_variability_rmssd',
        'spo2_percentage',
        'skin_temp_celsius',
        'user_calibrating',
        'score_state',
        'source_updated_at',
    ]
    
    # Check if DataFrame is empty, backfill missing columns
    if raw_df.empty:
        return ensure_output_columns(pd.DataFrame(), columns)

    # Build normalized recovery records from raw payloads
    records: list[dict[str, Any]] = []
    for _, row in raw_df.iterrows():
        payload = row['payload'] or {}
        created_at = parse_timestamp(payload.get('created_at'))

        records.append(
            {
                'cycle_id': str(payload.get('cycle_id')) if payload.get('cycle_id') is not None else None,
                'sleep_id': payload.get('sleep_id'),
                'date': timestamp_to_date(created_at),
                'recovery_score': extract_payload_field(payload, ['score', 'recovery_score']),
                'resting_heart_rate': extract_payload_field(payload, ['score', 'resting_heart_rate']),
                'heart_rate_variability_rmssd': extract_payload_field(payload, ['score', 'hrv_rmssd_milli']),
                'spo2_percentage': extract_payload_field(payload, ['score', 'spo2_percentage']),
                'skin_temp_celsius': extract_payload_field(payload, ['score', 'skin_temp_celsius']),
                'user_calibrating': extract_payload_field(payload, ['score', 'user_calibrating']),
                'score_state': payload.get('score_state'),
                'source_updated_at': parse_timestamp(payload.get('updated_at') or row.get('source_updated_at')),
            }
        )

    # Deduplicate by cycle ID and keep the latest version
    df = pd.DataFrame(records)
    df = df.dropna(subset=['date']).drop_duplicates(subset=['cycle_id'], keep='last')
    return ensure_output_columns(df, columns)


def transform_sleeps(raw_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Transform raw WHOOP sleep payloads into a normalized sleep DataFrame.

    Args:
        raw_df (pd.DataFrame): raw WHOOP sleeps.

    Returns:
        pd.DataFrame: normalized sleep rows.
    '''
    # Store expected Sleep columns
    columns = [
        'sleep_id',
        'cycle_id',
        'sleep_start',
        'sleep_end',
        'date',
        'is_nap',
        'sleep_duration_hours',
        'sleep_in_bed_hours',
        'sleep_need_hours',
        'sleep_efficiency',
        'sleep_performance_pct',
        'sleep_consistency_pct',
        'respiratory_rate',
        'disturbance_count',
        'sleep_cycle_count',
        'score_state',
        'source_updated_at',
    ]
    
    # Check if Dataframe is empty, backfill missing columns
    if raw_df.empty:
        return ensure_output_columns(pd.DataFrame(), columns)

    # Build normalized sleep records from raw payloads
    records: list[dict[str, Any]] = []
    for _, row in raw_df.iterrows():
        payload = row['payload'] or {}
        stage_summary = extract_payload_field(payload, ['score', 'stage_summary'], {})
        sleep_needed = extract_payload_field(payload, ['score', 'sleep_needed'], {})

        # Convert WHOOP stage and sleep-need values into aggregate hour metrics
        total_sleep_milli = sum(
            value or 0
            for value in (
                stage_summary.get('total_light_sleep_time_milli'),
                stage_summary.get('total_slow_wave_sleep_time_milli'),
                stage_summary.get('total_rem_sleep_time_milli'),
            )
        )
        total_sleep_need_milli = sum(
            value or 0
            for value in (
                sleep_needed.get('baseline_milli'),
                sleep_needed.get('need_from_sleep_debt_milli'),
                sleep_needed.get('need_from_recent_strain_milli'),
                sleep_needed.get('need_from_recent_nap_milli'),
            )
        )

        sleep_start = parse_timestamp(payload.get('start'))
        sleep_end = parse_timestamp(payload.get('end'))

        records.append(
            {
                'sleep_id': payload.get('id'),
                'cycle_id': str(payload.get('cycle_id')) if payload.get('cycle_id') is not None else None,
                'sleep_start': sleep_start,
                'sleep_end': sleep_end,
                'date': timestamp_to_date(sleep_end if not pd.isna(sleep_end) else sleep_start),
                'is_nap': bool(payload.get('nap')),
                'sleep_duration_hours': milli_to_hours(total_sleep_milli),
                'sleep_in_bed_hours': milli_to_hours(stage_summary.get('total_in_bed_time_milli')),
                'sleep_need_hours': milli_to_hours(total_sleep_need_milli),
                'sleep_efficiency': extract_payload_field(payload, ['score', 'sleep_efficiency_percentage']),
                'sleep_performance_pct': extract_payload_field(payload, ['score', 'sleep_performance_percentage']),
                'sleep_consistency_pct': extract_payload_field(payload, ['score', 'sleep_consistency_percentage']),
                'respiratory_rate': extract_payload_field(payload, ['score', 'respiratory_rate']),
                'disturbance_count': stage_summary.get('disturbance_count'),
                'sleep_cycle_count': stage_summary.get('sleep_cycle_count'),
                'score_state': payload.get('score_state'),
                'source_updated_at': parse_timestamp(payload.get('updated_at') or row.get('source_updated_at')),
            }
        )

    # Deduplicate by sleep ID and keep the latest version
    df = pd.DataFrame(records)
    df = df.dropna(subset=['date']).drop_duplicates(subset=['sleep_id'], keep='last')
    return ensure_output_columns(df, columns)


def transform_workouts(raw_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Transform raw WHOOP workout payloads into a normalized workout DataFrame.

    Args:
        raw_df (pd.DataFrame): raw WHOOP workouts.

    Returns:
        pd.DataFrame: normalized workout rows.
    '''
    # Store expected Workout columns
    columns = [
        'workout_id',
        'workout_start',
        'workout_end',
        'date',
        'sport_id',
        'sport_name',
        'strain',
        'kilojoule',
        'average_heart_rate',
        'max_heart_rate',
        'distance_meter',
        'percent_recorded',
        'score_state',
        'source_updated_at',
    ]
    
    # Check if DataFrame is empty, backfill missing columns
    if raw_df.empty:
        return ensure_output_columns(pd.DataFrame(), columns)

    # Build normalized workout records from raw payloads
    records: list[dict[str, Any]] = []
    for _, row in raw_df.iterrows():
        payload = row['payload'] or {}
        workout_start = parse_timestamp(payload.get('start'))
        workout_end = parse_timestamp(payload.get('end'))

        records.append(
            {
                'workout_id': payload.get('id'),
                'workout_start': workout_start,
                'workout_end': workout_end,
                'date': timestamp_to_date(workout_start if not pd.isna(workout_start) else workout_end),
                'sport_id': payload.get('sport_id'),
                'sport_name': payload.get('sport_name'),
                'strain': extract_payload_field(payload, ['score', 'strain']),
                'kilojoule': extract_payload_field(payload, ['score', 'kilojoule']),
                'average_heart_rate': extract_payload_field(payload, ['score', 'average_heart_rate']),
                'max_heart_rate': extract_payload_field(payload, ['score', 'max_heart_rate']),
                'distance_meter': extract_payload_field(payload, ['score', 'distance_meter']),
                'percent_recorded': extract_payload_field(payload, ['score', 'percent_recorded']),
                'score_state': payload.get('score_state'),
                'source_updated_at': parse_timestamp(payload.get('updated_at') or row.get('source_updated_at')),
            }
        )

    # Deduplicate by workout ID and keep the latest version
    df = pd.DataFrame(records)
    df = df.dropna(subset=['date']).drop_duplicates(subset=['workout_id'], keep='last')
    return ensure_output_columns(df, columns)


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
    # Create copy of DataFrame if empty 
    if df.empty:
        return df.copy()

    # Filter using normalized date boundaries
    mask = (df[date_column] >= start_date.normalize()) & (df[date_column] <= end_date.normalize())
    return df.loc[mask].sort_values(date_column).reset_index(drop=True)


def build_daily_sleep_summary(sleeps_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Aggregate sleep rows to one daily summary row.

    Args:
        sleeps_df (pd.DataFrame): transformed sleeps.

    Returns:
        pd.DataFrame: daily sleep summary.
    '''
    # Store expected Sleep columns
    columns = [
        'date',
        'sleep_id',
        'cycle_id',
        'total_sleep_duration_hours',
        'total_sleep_in_bed_hours',
        'sleep_need_hours',
        'main_sleep_efficiency',
        'main_sleep_performance_pct',
        'main_sleep_consistency_pct',
        'main_respiratory_rate',
        'sleep_event_count',
        'nap_count',
    ]
    
    # Check if DataFrame is empty, backfill missing columns
    if sleeps_df.empty:
        return ensure_output_columns(pd.DataFrame(), columns)

    # Use the longest sleep event as the primary sleep record for the day
    sleeps_df = sleeps_df.copy()
    sleeps_df['sort_duration'] = sleeps_df['sleep_duration_hours'].fillna(0.0)

    daily_records: list[dict[str, Any]] = []
    for date, group in sleeps_df.groupby('date', dropna=True):
        main_sleep = group.sort_values('sort_duration', ascending=False).iloc[0]
        daily_records.append(
            {
                'date': date,
                'sleep_id': main_sleep['sleep_id'],
                'cycle_id': main_sleep['cycle_id'],
                'total_sleep_duration_hours': group['sleep_duration_hours'].sum(min_count=1),
                'total_sleep_in_bed_hours': group['sleep_in_bed_hours'].sum(min_count=1),
                'sleep_need_hours': main_sleep['sleep_need_hours'],
                'main_sleep_efficiency': main_sleep['sleep_efficiency'],
                'main_sleep_performance_pct': main_sleep['sleep_performance_pct'],
                'main_sleep_consistency_pct': main_sleep['sleep_consistency_pct'],
                'main_respiratory_rate': main_sleep['respiratory_rate'],
                'sleep_event_count': int(len(group)),
                'nap_count': int(group['is_nap'].fillna(False).sum()),
            }
        )

    return ensure_output_columns(pd.DataFrame(daily_records), columns)


def build_daily_workout_summary(workouts_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Aggregate workout rows to one daily summary row.

    Args:
        workouts_df (pd.DataFrame): transformed workouts.

    Returns:
        pd.DataFrame: daily workout summary.
    '''
    # Store expected for Daily Workout
    columns = [
        'date',
        'workout_count',
        'total_strain',
        'avg_workout_strain',
        'total_kilojoule',
        'avg_workout_heart_rate',
        'max_workout_heart_rate',
    ]
    
    # Check if DataFrame is empty, backfill missing columns
    if workouts_df.empty:
        return ensure_output_columns(pd.DataFrame(), columns)

    # Aggregate multiple workout events into one daily summary row
    summary = (
        workouts_df.groupby('date', dropna=True)
        .agg(
            workout_count=('workout_id', 'count'),
            total_strain=('strain', 'sum'),
            avg_workout_strain=('strain', 'mean'),
            total_kilojoule=('kilojoule', 'sum'),
            avg_workout_heart_rate=('average_heart_rate', 'mean'),
            max_workout_heart_rate=('max_heart_rate', 'max'),
        )
        .reset_index()
    )
    return ensure_output_columns(summary, columns)


def build_daily_whoop_dataframe(
    cycles_df: pd.DataFrame,
    recoveries_df: pd.DataFrame,
    sleeps_df: pd.DataFrame,
    workouts_df: pd.DataFrame,
) -> pd.DataFrame:
    '''
    Join transformed WHOOP collection outputs into one daily DataFrame.

    Args:
        cycles_df (pd.DataFrame): transformed cycles.
        recoveries_df (pd.DataFrame): transformed recoveries.
        sleeps_df (pd.DataFrame): transformed sleeps.
        workouts_df (pd.DataFrame): transformed workouts.

    Returns:
        pd.DataFrame: one WHOOP summary row per date.
    '''
    # Reduce sleep and workout events to one row per date before joining
    sleep_daily_df = build_daily_sleep_summary(sleeps_df)
    workout_daily_df = build_daily_workout_summary(workouts_df)

    # Select the daily fields needed from cycle and recovery transforms
    cycle_daily_df = cycles_df[
        [
            'date',
            'cycle_id',
            'cycle_strain',
            'cycle_kilojoule',
            'cycle_average_heart_rate',
            'cycle_max_heart_rate',
        ]
    ].drop_duplicates(subset=['date'], keep='last')

    recovery_daily_df = recoveries_df[
        [
            'date',
            'cycle_id',
            'sleep_id',
            'recovery_score',
            'resting_heart_rate',
            'heart_rate_variability_rmssd',
            'spo2_percentage',
            'skin_temp_celsius',
            'user_calibrating',
        ]
    ].drop_duplicates(subset=['date'], keep='last')

    # Build a union of all WHOOP dates so missing collection data does not drop a day
    frames = [cycle_daily_df, recovery_daily_df, sleep_daily_df, workout_daily_df]
    date_frames = [frame[['date']] for frame in frames if not frame.empty]

    if not date_frames:
        return pd.DataFrame(
            columns=[
                'date',
                'cycle_id',
                'sleep_id',
                'recovery_score',
                'resting_heart_rate',
                'heart_rate_variability_rmssd',
                'spo2_percentage',
                'skin_temp_celsius',
                'user_calibrating',
                'cycle_strain',
                'cycle_kilojoule',
                'cycle_average_heart_rate',
                'cycle_max_heart_rate',
                'total_sleep_duration_hours',
                'total_sleep_in_bed_hours',
                'sleep_need_hours',
                'main_sleep_efficiency',
                'main_sleep_performance_pct',
                'main_sleep_consistency_pct',
                'main_respiratory_rate',
                'sleep_event_count',
                'nap_count',
                'workout_count',
                'total_strain',
                'avg_workout_strain',
                'total_kilojoule',
                'avg_workout_heart_rate',
                'max_workout_heart_rate',
            ]
        )

    all_dates = pd.concat(date_frames, ignore_index=True).drop_duplicates().sort_values('date')
    daily_df = all_dates.reset_index(drop=True)

    # Left join each collection summary into the daily WHOOP frame
    for frame in frames:
        if not frame.empty:
            daily_df = daily_df.merge(frame, on='date', how='left')

    return daily_df.sort_values('date').reset_index(drop=True)


def transform_all_whoop_data() -> dict[str, pd.DataFrame]:
    '''
    Fetch, transform, and join all WHOOP raw datasets.

    Returns:
        dict[str, pd.DataFrame]: transformed per-collection and daily outputs.
    '''
    # Build the transform window used across all WHOOP outputs
    start_date, end_date = get_transform_window()

    # Fetch raw WHOOP collections from Postgres
    raw_cycles_df = fetch_raw_collection(RAW_WHOOP_TABLES['cycles'])
    raw_recoveries_df = fetch_raw_collection(RAW_WHOOP_TABLES['recoveries'])
    raw_sleeps_df = fetch_raw_collection(RAW_WHOOP_TABLES['sleeps'])
    raw_workouts_df = fetch_raw_collection(RAW_WHOOP_TABLES['workouts'])

    # Transform and filter each WHOOP collection independently
    cycles_df = filter_to_window(
        transform_cycles(raw_cycles_df),
        date_column='date',
        start_date=start_date,
        end_date=end_date,
    )
    recoveries_df = filter_to_window(
        transform_recoveries(raw_recoveries_df),
        date_column='date',
        start_date=start_date,
        end_date=end_date,
    )
    sleeps_df = filter_to_window(
        transform_sleeps(raw_sleeps_df),
        date_column='date',
        start_date=start_date,
        end_date=end_date,
    )
    workouts_df = filter_to_window(
        transform_workouts(raw_workouts_df),
        date_column='date',
        start_date=start_date,
        end_date=end_date,
    )

    # Join all transformed WHOOP collections into one daily DataFrame
    daily_df = build_daily_whoop_dataframe(
        cycles_df=cycles_df,
        recoveries_df=recoveries_df,
        sleeps_df=sleeps_df,
        workouts_df=workouts_df,
    )
    daily_df = filter_to_window(
        daily_df,
        date_column='date',
        start_date=start_date,
        end_date=end_date,
    )

    return {
        'cycles': cycles_df,
        'recoveries': recoveries_df,
        'sleeps': sleeps_df,
        'workouts': workouts_df,
        'daily': daily_df,
    }


def main() -> int:
    '''
    Driver function for the WHOOP transform job.

    Returns:
        int: 0 on success, 1 on failure.
    '''
    # Load environment variables into memory
    load_dotenv()
    
    logger.info('Starting WHOOP Transform Job')

    try:
        # Execute WHOOP transform pipeline and log output details
        outputs = transform_all_whoop_data()
        for name, df in outputs.items():
            logger.info('WHOOP transform output=%s rows=%s columns=%s', name, len(df), list(df.columns))
            logger.debug('WHOOP transform sample output=%s\n%s', name, df.head())
        logger.info('Finished WHOOP Transform Job successfully')
        return 0
    except Exception:
        logger.exception('WHOOP Transform Job failed')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
