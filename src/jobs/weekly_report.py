import pandas as pd
from dotenv import load_dotenv

from src.shared.db import get_conn
from src.shared.logger import get_logger

# Create global logger
logger = get_logger(__name__)
logger.setLevel('DEBUG')

# SQL query to fetch data from the database
FETCH_DATA_SQL = '''
SELECT
    id,
    ingested_at,
    source_updated_at,
    payload
FROM raw.notion_entries
ORDER BY source_updated_at ASC;
'''

def fetch_data() -> pd.DataFrame:
    '''
    Fetches data from Postgres database and returns it
    as a Pandas DataFrame.

    Returns:
        pd.DataFrame: Postgres query result
    '''
    try:
        # Execute SQL query and load results into a DataFrame
        with get_conn() as conn:
            df = pd.read_sql_query(FETCH_DATA_SQL, conn)
        logger.info(f'Fetched {len(df)} records from the Postgres.')
        return df
    except Exception as e:
        logger.error(f'Error fetching data from Postgres: {e}')
        return pd.DataFrame()
    

def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Flattens the raw DataFrame by extracting relevant properties from the
    payload column and creating a new DataFrame with those properties as columns.

    Args:
        df (pd.DataFrame): Raw DataFrame with payload column

    Returns:
        pd.DataFrame: Flattened DataFrame with properties as columns
    '''
    # Record storage
    records = []

    # Iterate through all payloads in DataFrame
    for payload in df["payload"]:
        # Extract relevant properties from the payload
        properties = payload.get("properties", {})

        # Append extracted data to records list
        records.append(
            {
                "date": properties.get("Date", {}).get("date", {}).get("start"),
                "weight_lbs": properties.get("Weight (lbs)", {}).get("number"),
                "calories": properties.get("Caloric Intake", {}).get("number"),
                "protein_g": properties.get("Protein (g)", {}).get("number"),
            }
        )
    
    # Store refined records in a new DataFrame
    flattened_df = pd.DataFrame(records)
    
    return flattened_df
    
    
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Cleans the raw data fetched from Postgres by extracting relevant
    properties from the payload and refining it to the last 7 days.

    Args:
        df (pd.DataFrame): Raw data DataFrame fetched from Postgres

    Returns:
        pd.DataFrame: Cleaned and refined DataFrame
    '''
    logger.info('Cleaning data from Postgres...')

    if df is None or df.empty:
        logger.warning('No raw data available to clean.')
        return pd.DataFrame()
    
    # Flatten DataFrame to extract each property into its own column
    logger.info('Flattening DataFrame to extract properties from payload...')
    flattened_df = flatten_df(df)
    logger.info('DataFrame flattened successfully.')
    
    # Convert date column to datetime format
    flattened_df['date'] = pd.to_datetime(flattened_df['date'])
    flattened_df.sort_values('date', inplace=True)
    
    # Refine to last 7 days
    # TODO: Swap days parameter, not enough recent data to test
    last_week_date = pd.Timestamp.now() - pd.Timedelta(days=21)
    refined_df = flattened_df[flattened_df['date'] >= last_week_date]
    
    logger.info('Postgres data cleaning complete.')
    logger.debug(f'Cleaned DataFrame: {refined_df}')
    
    return refined_df


def main() -> int:
    """

    Returns:
        int: Exit code.
    """
    # Load env variables to memory
    load_dotenv()
    
    logger.info("Starting weekly report job")

    try:
        raw_df = fetch_data()
        cleaned_df = clean_data(raw_df)

        logger.info("Weekly report dataset prepared with %s rows", len(cleaned_df))
        logger.debug("Cleaned weekly dataset:\n%s", cleaned_df)

        return 0

    except Exception:
        logger.exception("Weekly report job failed")
        return 1
    