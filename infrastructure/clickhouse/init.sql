-- ============================================================
-- ClickHouse Schema — Analytics Cold/Hot Tier
-- ============================================================
-- Optimized for analytical queries: aggregations, time-series, OLAP.
-- Data ingested directly from match-events-analytics Kafka topic.

-- === Raw Events (Kafka Engine — auto-ingest from Kafka) ===
CREATE TABLE IF NOT EXISTS sportdata_analytics.match_events_queue (
    event_id        String,
    event_type      String,
    match_id        String,
    competition_id  String,
    occurred_at     DateTime64(3, 'UTC'),
    received_at     DateTime64(3, 'UTC'),
    published_at    Nullable(DateTime64(3, 'UTC')),
    sequence_number UInt32,
    data            String,  -- JSON payload
    source_system   String,
    source_confidence Float64,
    idempotency_key String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:29092,kafka-2:29092,kafka-3:29092',
    kafka_topic_list = 'match-events-analytics',
    kafka_group_name = 'clickhouse-analytics-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4;

-- === Materialized View: Persistent Events Table ===
CREATE TABLE IF NOT EXISTS sportdata_analytics.match_events (
    event_id        String,
    event_type      LowCardinality(String),
    match_id        String,
    competition_id  LowCardinality(String),
    occurred_at     DateTime64(3, 'UTC'),
    received_at     DateTime64(3, 'UTC'),
    published_at    Nullable(DateTime64(3, 'UTC')),
    sequence_number UInt32,
    data            String,
    source_system   LowCardinality(String),
    source_confidence Float64,

    -- Derived columns for fast analytics
    occurred_date   Date MATERIALIZED toDate(occurred_at),
    processing_latency_ms Int64 MATERIALIZED dateDiff('millisecond', occurred_at, received_at),
    hour_of_day     UInt8 MATERIALIZED toHour(occurred_at)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(occurred_date)
ORDER BY (match_id, occurred_at, event_type)
TTL occurred_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Auto-ingest from Kafka
CREATE MATERIALIZED VIEW IF NOT EXISTS sportdata_analytics.match_events_mv
TO sportdata_analytics.match_events AS
SELECT * FROM sportdata_analytics.match_events_queue;

-- === Aggregation Tables ===

-- Per-match summary (updated on each event)
CREATE TABLE IF NOT EXISTS sportdata_analytics.match_summary (
    match_id        String,
    competition_id  LowCardinality(String),
    match_date      Date,
    total_events    UInt32,
    goals_home      UInt16,
    goals_away      UInt16,
    yellow_cards    UInt16,
    red_cards       UInt16,
    substitutions   UInt16,
    penalties       UInt16,
    avg_latency_ms  Float64,
    p99_latency_ms  Float64,
    last_updated    DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (match_id)
PARTITION BY toYYYYMM(match_date);

-- Per-minute event counts (for timeline visualization)
CREATE TABLE IF NOT EXISTS sportdata_analytics.match_timeline (
    match_id        String,
    match_minute    UInt16,
    event_type      LowCardinality(String),
    event_count     UInt32,
    data_json       String  -- Aggregated data for the minute
) ENGINE = SummingMergeTree()
ORDER BY (match_id, match_minute, event_type);

-- === Latency Monitoring ===
CREATE TABLE IF NOT EXISTS sportdata_analytics.event_latency (
    occurred_at         DateTime64(3, 'UTC'),
    event_type          LowCardinality(String),
    pipeline_stage      LowCardinality(String),
    latency_ms          Int64,
    gateway_id          LowCardinality(String),
    match_id            String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(occurred_at)
ORDER BY (occurred_at, event_type)
TTL occurred_at + INTERVAL 30 DAY;
