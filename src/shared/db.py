import os
from contextlib import contextmanager
from typing import Iterator

import psycopg

from src.shared.logger import get_logger

logger = get_logger(__name__)

def build_dsn():
    '''
    Builds a DSN (Data Source Name) string for connecting
    to a PostgreSQL database using environment variables.
    '''
    host = os.environ["POSTGRES_HOST"]
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    
    return f"host={host} port={port} dbname={db} user={user} password={password}"

@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    '''
    Context manager that yields a connection to the PostgreSQL database.
    Ensures that the connection is properly closed after use.
    '''
    # Build DSN string
    dsn = build_dsn()
    
    # Create connection to Postgres Database
    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    inet_server_addr()::text AS server_addr,
                    inet_server_port() AS server_port,
                    current_database() AS database_name,
                    current_user AS database_user,
                    current_schema() AS current_schema
                """
            )
            row = cur.fetchone()
            logger.debug(
                "Connected to Postgres "
                "server=%s:%s db=%s user=%s schema=%s autocommit=%s",
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                conn.autocommit,
            )
        yield conn
        conn.commit()
        logger.debug("Committed Postgres transaction")
    except Exception:
        conn.rollback()
        logger.exception("Rolled back Postgres transaction due to error")
        raise
    finally:
        conn.close()
        logger.debug("Closed Postgres connection")
