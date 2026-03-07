CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.notion_entries (
    id TEXT PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_updated_at TIMESTAMPTZ NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_notion_entries_source_updated_at
  ON raw.notion_entries (source_updated_at);