-- Table 1: Raw telemetry events (stores all incoming data)
CREATE TABLE raw_events (
    event_id UInt64 CODEC(ZSTD(19)),
    source_id String CODEC(ZSTD(19)),
    metric String,
    value Float64,
    event_time DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (source_id, metric, event_time);

-- Table 2: Baseline statistics (computed from historical data)
CREATE TABLE baseline_statistics (
    source_id String CODEC(ZSTD(19)),
    metric String,
    baseline_mean Float64,
    baseline_stddev Float64,
    sample_count UInt32,
    window_start DateTime64(3),
    window_end DateTime64(3),
    computed_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (source_id, metric);

-- Table 3: Drift events (when drift starts/ends)
CREATE TABLE drift_events (
    source_id String CODEC(ZSTD(19)),
    metric String,
    drift_status Enum8('STARTED' = 1, 'ENDED' = 2),
    detected_at DateTime64(3),
    z_score Float64
) ENGINE = MergeTree()
ORDER BY (source_id, metric, detected_at);

-- Table 4: Current drift status (for API queries)
CREATE TABLE current_drift_status (
    source_id String CODEC(ZSTD(19)),
    metric String,
    status Enum8('NORMAL' = 0, 'DRIFTING' = 1),
    drift_started_at Nullable(DateTime64(3)),
    last_z_score Float64,
    last_updated DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (source_id, metric);


DROP TABLE baseline_statistics;
DROP TABLE current_drift_status;
DROP TABLE  drift_events;
DROP TABLE raw_events;