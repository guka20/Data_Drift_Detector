from clickhouse_driver import Client
from datetime import datetime, timedelta
import sys

# Clickhouse-ის კონფიგურაცია
CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 9000,
    "database": "default",
    "user": "default",
    "password": "1234",
}


MIN_SAMPLES = 10 

def compute_baseline():
    client = Client(**CLICKHOUSE_CONFIG)
    
    print("Computing baseline statistics...")
    
    # baseline სტატიკისთვის ვიყენებთ პირველ 60 დღეს
    # რადგან ეს არის პერიოდი როცა მონაცემები ნორმალურია
    window_start = datetime(2024, 1, 1, 0, 0, 0)
    window_end = datetime(2024, 3, 1, 0, 0, 0)  # Day 60
    
    print(f"Baseline window: {window_start} to {window_end} (Days 1-60: Normal period)")
    
    query = """
        SELECT
            source_id,
            metric,
            avg(value) as baseline_mean,
            stddevPop(value) as baseline_stddev,
            count() as sample_count,
            toDateTime64(%(window_start)s, 3) as window_start,
            toDateTime64(%(window_end)s, 3) as window_end,
            now64(3) as computed_at
        FROM raw_events
        WHERE event_time >= %(window_start)s
          AND event_time <= %(window_end)s
        GROUP BY source_id, metric
        HAVING sample_count >= %(min_samples)s
    """
    
    print("Calculating statistics...")
    
    results = client.execute(
        query,
        {
            'window_start': window_start,
            'window_end': window_end,
            'min_samples': MIN_SAMPLES
        }
    )
    
    if not results:
        print("No valid baseline data (not enough samples)")
        return
    
    print(f"Computed baseline for {len(results)} (source_id, metric) pairs")
    
    insert_query = """
        INSERT INTO baseline_statistics (
            source_id,
            metric,
            baseline_mean,
            baseline_stddev,
            sample_count,
            window_start,
            window_end,
            computed_at
        ) VALUES
    """
    
    print("Inserting into baseline_statistics table...")
    client.execute(insert_query, results)
    
    print(f"Successfully inserted {len(results)} baseline records")
    
    print("\nSample baseline statistics:")
    print("-" * 80)
    print(f"{'Source ID':<20} {'Metric':<20} {'Mean':<12} {'StdDev':<12} {'Samples':<10}")
    print("-" * 80)
    
    for row in results[:10]:  
        source_id, metric, mean, stddev, count, _, _, _ = row
        print(f"{source_id:<20} {metric:<20} {mean:<12.2f} {stddev:<12.2f} {count:<10}")
    
    if len(results) > 10:
        print(f"... and {len(results) - 10} more")
    
    print("\n" + "=" * 80)
    print("Baseline computation complete!")

if __name__ == "__main__":
    try:
        compute_baseline()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)