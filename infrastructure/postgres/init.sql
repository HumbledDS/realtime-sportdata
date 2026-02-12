-- ============================================================
-- PostgreSQL Schema — Warm Tier Storage
-- ============================================================
-- Match events persisted for 90 days (warm tier).
-- Optimized for match event queries by match_id + time range.

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- === Matches ===
CREATE TABLE IF NOT EXISTS matches (
    match_id        TEXT PRIMARY KEY,
    competition_id  TEXT NOT NULL,
    competition_name TEXT,
    home_team_id    TEXT NOT NULL,
    home_team_name  TEXT NOT NULL,
    away_team_id    TEXT NOT NULL,
    away_team_name  TEXT NOT NULL,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    status          TEXT DEFAULT 'SCHEDULED' CHECK (status IN (
        'SCHEDULED', 'LIVE', 'HALF_TIME', 'FINISHED', 'ABANDONED', 'POSTPONED'
    )),
    home_score      INT DEFAULT 0,
    away_score      INT DEFAULT 0,
    matchday        INT,
    venue           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_matches_status ON matches (status);
CREATE INDEX idx_matches_scheduled ON matches (scheduled_at);
CREATE INDEX idx_matches_competition ON matches (competition_id);

-- === Match Events ===
CREATE TABLE IF NOT EXISTS match_events (
    event_id            TEXT PRIMARY KEY,
    event_type          TEXT NOT NULL,
    event_version       INT NOT NULL DEFAULT 1,
    match_id            TEXT NOT NULL REFERENCES matches(match_id),
    occurred_at         TIMESTAMPTZ NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL,
    published_at        TIMESTAMPTZ,
    sequence_number     INT NOT NULL,
    data                JSONB NOT NULL,
    metadata            JSONB,
    source_system       TEXT,
    source_confidence   DOUBLE PRECISION,
    idempotency_key     TEXT UNIQUE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Query pattern: "all events for match X, ordered by time"
CREATE INDEX idx_events_match_time ON match_events (match_id, occurred_at);
-- Query pattern: "all goals across all matches today"
CREATE INDEX idx_events_type_time ON match_events (event_type, occurred_at);
-- Deduplication lookup
CREATE INDEX idx_events_idempotency ON match_events (idempotency_key);
-- Full-text search on event data
CREATE INDEX idx_events_data_gin ON match_events USING GIN (data);

-- === Players ===
CREATE TABLE IF NOT EXISTS players (
    player_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    team_id         TEXT,
    jersey_number   INT,
    position        TEXT,
    nationality     TEXT,
    photo_url       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- === Teams ===
CREATE TABLE IF NOT EXISTS teams (
    team_id         TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    short_name      TEXT,
    logo_url        TEXT,
    primary_color   TEXT,
    secondary_color TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- === User Notification Preferences ===
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id             TEXT PRIMARY KEY,
    followed_teams      TEXT[] DEFAULT '{}',
    followed_players    TEXT[] DEFAULT '{}',
    followed_competitions TEXT[] DEFAULT '{}',
    notification_level  TEXT DEFAULT 'ALL' CHECK (notification_level IN (
        'ALL', 'GOALS_ONLY', 'CRITICAL_ONLY', 'NONE'
    )),
    language            TEXT DEFAULT 'fr',
    timezone            TEXT DEFAULT 'Europe/Paris',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_user_prefs_teams ON user_preferences USING GIN (followed_teams);

-- === Audit Log ===
CREATE TABLE IF NOT EXISTS event_processing_log (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL,
    stage           TEXT NOT NULL, -- 'INGESTED', 'ENRICHED', 'NOTIFIED', 'STORED'
    status          TEXT NOT NULL, -- 'SUCCESS', 'FAILURE', 'RETRY'
    processing_time_ms INT,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_processing_log_event ON event_processing_log (event_id);
CREATE INDEX idx_processing_log_stage ON event_processing_log (stage, status);

-- === Partitioning (90-day retention) ===
-- In production, use pg_partman for automatic partition management.
-- Monthly partitions on match_events by occurred_at.
