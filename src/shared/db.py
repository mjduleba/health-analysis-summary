import os
from contextlib import contextmanager
from typing import Iterator

import psycopg

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
        yield conn
    finally:
        conn.close()