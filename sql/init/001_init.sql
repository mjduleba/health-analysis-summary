CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS raw.notion_entries (
    id TEXT PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_updated_at TIMESTAMPTZ NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_notion_entries_source_updated_at
  ON raw.notion_entries (source_updated_at);

CREATE TABLE IF NOT EXISTS raw.whoop_cycles (
    id TEXT PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_updated_at TIMESTAMPTZ NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_whoop_cycles_source_updated_at
  ON raw.whoop_cycles (source_updated_at);

CREATE TABLE IF NOT EXISTS raw.whoop_recoveries (
    id TEXT PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_updated_at TIMESTAMPTZ NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_whoop_recoveries_source_updated_at
  ON raw.whoop_recoveries (source_updated_at);

CREATE TABLE IF NOT EXISTS raw.whoop_sleeps (
    id TEXT PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_updated_at TIMESTAMPTZ NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_whoop_sleeps_source_updated_at
  ON raw.whoop_sleeps (source_updated_at);

CREATE TABLE IF NOT EXISTS raw.whoop_workouts (
    id TEXT PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_updated_at TIMESTAMPTZ NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_whoop_workouts_source_updated_at
  ON raw.whoop_workouts (source_updated_at);

CREATE TABLE IF NOT EXISTS app.oauth_tokens (
    provider TEXT PRIMARY KEY,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    expires_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
  
