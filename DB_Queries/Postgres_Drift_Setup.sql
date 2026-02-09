CREATE TABLE drift_config (
    id SERIAL PRIMARY KEY,
    config_key VARCHAR(100) UNIQUE NOT NULL,
    config_value TEXT NOT NULL,
    description TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

TRUNCATE TABLE drift_config;

INSERT INTO drift_config (config_key, config_value) VALUES
('rolling_window_size', '10'),
('consecutive_violations', '3'),
('z_score_threshold', '2.0')
ON CONFLICT (config_key) DO NOTHING;

INSERT INTO drift_config (config_key, config_value, description) VALUES
('baseline_window_hours', '24', 'Hours of historical data for baseline'),
('rolling_window_size', '100', 'Number of events in rolling window'),
('z_score_threshold', '3.0', 'Z-score threshold for drift detection'),
('consecutive_violations', '3', 'Consecutive violations to trigger drift');