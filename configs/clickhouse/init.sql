-- =============================================================================
-- ClickHouse Initialization Script for Lambda Architecture
-- =============================================================================

CREATE DATABASE IF NOT EXISTS lakehouse;

-- 1. SPEED VIEW: Near-realtime aggregates from Speed Layer (Spark Streaming)
-- State: Provisional (tức thời, gần đúng)
CREATE TABLE IF NOT EXISTS lakehouse.speed_agg
(
    symbol          LowCardinality(String),
    window_start    DateTime64(3, 'UTC'),
    window_end      DateTime64(3, 'UTC'),
    open_price      Float64,
    high_price      Float64,
    low_price       Float64,
    close_price     Float64,
    volume          Float64,
    trade_count     UInt64,
    vwap            Float64,
    is_spike        UInt8 DEFAULT 0,
    created_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start);

-- 2. BATCH VIEW: Ground Truth aggregates from Batch Layer (Spark Batch)
-- State: Reconciled (chuẩn xác tuyệt đối, đã qua Data Quality Gate & Deduplication)
CREATE TABLE IF NOT EXISTS lakehouse.batch_agg
(
    symbol          LowCardinality(String),
    window_start    DateTime64(3, 'UTC'),
    window_end      DateTime64(3, 'UTC'),
    open_price      Float64,
    high_price      Float64,
    low_price       Float64,
    close_price     Float64,
    volume          Float64,
    trade_count     UInt64,
    vwap            Float64,
    is_spike        UInt8 DEFAULT 0,
    batch_run_id    String,
    created_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start);

-- 3. SYSTEM WATERMARK: Theo dõi mốc Batch Watermark để Query Merger đối soát
CREATE TABLE IF NOT EXISTS lakehouse.system_watermark
(
    layer           String,
    watermark_time  DateTime64(3, 'UTC'),
    updated_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (layer);

-- Seed initial watermark
INSERT INTO lakehouse.system_watermark (layer, watermark_time) VALUES ('batch_layer', toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC'));
