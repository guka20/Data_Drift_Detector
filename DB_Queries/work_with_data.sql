-- Check how many events in raw_events
SELECT count() FROM raw_events;

-- Check baselines
SELECT * FROM baseline_statistics;

-- Check drift events
SELECT * FROM drift_events ORDER BY detected_at;

-- Check current drift status
SELECT * FROM current_drift_status;

-- Count drift events by type
SELECT
    drift_status,
    count() as count
FROM drift_events
GROUP BY drift_status;

-- See which sources are currently drifting
SELECT * FROM current_drift_status WHERE status = 'DRIFTING';



SELECT
    source_id,
    metric,
    drift_status,
    detected_at,
    z_score
FROM drift_events
ORDER BY detected_at
LIMIT 10;

SELECT
    source_id,
    metric,
    drift_status,
    detected_at,
    z_score
FROM drift_events
ORDER BY detected_at DESC
LIMIT 10;

SELECT
    toStartOfHour(detected_at) as hour,
    drift_status,
    count() as events
FROM drift_events
WHERE source_id = 'room_admin_indoor'
GROUP BY hour, drift_status
ORDER BY hour
LIMIT 50