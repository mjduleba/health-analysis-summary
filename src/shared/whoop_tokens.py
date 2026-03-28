from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from src.shared.db import get_conn
from src.shared.logger import get_logger


logger = get_logger(__name__)

WHOOP_PROVIDER = 'whoop'

CREATE_WHOOP_TOKEN_TABLE_SQL = '''
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.oauth_tokens (
    provider TEXT PRIMARY KEY,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    expires_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
'''

UPSERT_WHOOP_TOKENS_SQL = '''
INSERT INTO app.oauth_tokens (
    provider,
    access_token,
    refresh_token,
    expires_at
)
VALUES (
    %(provider)s,
    %(access_token)s,
    %(refresh_token)s,
    %(expires_at)s
)
ON CONFLICT (provider) DO UPDATE
SET
    access_token = EXCLUDED.access_token,
    refresh_token = EXCLUDED.refresh_token,
    expires_at = EXCLUDED.expires_at,
    updated_at = NOW();
'''


def calculate_expires_at(expires_in: Any) -> datetime | None:
    '''
    Convert an expires_in seconds value to an absolute UTC expiration timestamp.
    '''
    if expires_in in (None, ''):
        return None

    return datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))


def ensure_whoop_token_table() -> None:
    '''
    Ensure the WHOOP OAuth token storage table exists.
    '''
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_WHOOP_TOKEN_TABLE_SQL)


def load_whoop_tokens() -> dict[str, Any] | None:
    '''
    Load the latest WHOOP OAuth token pair from Postgres.
    '''
    ensure_whoop_token_table()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                SELECT
                    access_token,
                    refresh_token,
                    expires_at,
                    updated_at
                FROM app.oauth_tokens
                WHERE provider = %s
                ''',
                (WHOOP_PROVIDER,),
            )
            row = cur.fetchone()

    if row is None:
        return None

    return {
        'access_token': row[0],
        'refresh_token': row[1],
        'expires_at': row[2],
        'updated_at': row[3],
    }


def save_whoop_tokens_from_response(token_response: dict[str, Any]) -> None:
    '''
    Persist the WHOOP OAuth token response to Postgres.
    '''
    access_token = token_response.get('access_token')
    refresh_token = token_response.get('refresh_token')

    if not access_token or not refresh_token:
        raise ValueError('WHOOP token response is missing access_token or refresh_token')

    expires_at = calculate_expires_at(token_response.get('expires_in'))

    ensure_whoop_token_table()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                UPSERT_WHOOP_TOKENS_SQL,
                {
                    'provider': WHOOP_PROVIDER,
                    'access_token': access_token,
                    'refresh_token': refresh_token,
                    'expires_at': expires_at,
                },
            )

    logger.info(
        'Persisted WHOOP OAuth tokens expires_at=%s',
        expires_at,
    )

