-- ClickHouse initialization script for CDC engagement analytics

-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;

-- Create enriched_events table (dedupe-safe)
CREATE TABLE IF NOT EXISTS analytics.enriched_events (
    event_id Int64,
    content_id String,
    user_id String,
    event_type String,
    event_ts DateTime,
    duration_ms Nullable(Int32),
    device String,
    engagement_score Float64,
    content_type Nullable(String),
    length_seconds Nullable(Int32),
    engagement_seconds Nullable(Float64),
    engagement_pct Nullable(Float64)
)
ENGINE = ReplacingMergeTree(event_ts)
ORDER BY (event_id)
SETTINGS index_granularity = 8192;

-- Materialized views analogs (example rollups)
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.engagement_hourly_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour_ts)
ORDER BY (hour_ts, content_type, event_type)
POPULATE AS
SELECT
    toStartOfHour(event_ts) AS hour_ts,
    content_type,
    event_type,
    count() AS event_count,
    sumState(engagement_score) AS total_engagement_score
FROM analytics.enriched_events
GROUP BY hour_ts, content_type, event_type; 