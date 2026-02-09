from confluent_kafka import Consumer, KafkaError
from clickhouse_driver import Client
import psycopg2
from collections import defaultdict, deque
from dateutil import parser
import json
import math
import time
from datetime import datetime  

# Kafka Config
KAFKA_CONFIG = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'drift_detector_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# ClickHouse Config
CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 9000,
    "database": "default",
    "user": "default",
    "password": "1234",
}

# PostgreSQL Config
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "drift_db",
    "user": "postgres",
    "password": "1234"
}

KAFKA_TOPIC = 'telemetry.events'

# Global state
rolling_windows = defaultdict(lambda: deque())
drift_state = {}
baseline_cache = {}
config = {}

def load_config():
    """Load configuration from PostgreSQL"""
    global config
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SELECT config_key, config_value FROM drift_config")
    rows = cursor.fetchall()
    
    for key, value in rows:
        if key in ['rolling_window_size', 'consecutive_violations']:
            config[key] = int(value)
        elif key in ['z_score_threshold']:
            config[key] = float(value)
        else:
            config[key] = value
    
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded configuration: {config}")

def load_baselines(clickhouse_client):
    """Load all baseline statistics from ClickHouse into memory"""
    global baseline_cache
    
    query = """
        SELECT 
            source_id,
            metric,
            baseline_mean,
            baseline_stddev,
            sample_count
        FROM baseline_statistics
        FINAL
    """
    
    results = clickhouse_client.execute(query)
    
    for source_id, metric, mean, stddev, sample_count in results:
        key = (source_id, metric)
        baseline_cache[key] = {
            'mean': mean,
            'stddev': stddev,
            'sample_count': sample_count
        }
    
    print(f"✅ Loaded {len(baseline_cache)} baseline statistics")

def calculate_z_score(rolling_mean, baseline_mean, baseline_stddev):
    """Calculate Z-score"""
    if baseline_stddev == 0 or math.isnan(baseline_stddev):
        return 0
    
    return abs(rolling_mean - baseline_mean) / baseline_stddev


def insert_drift_event(clickhouse_client, source_id, metric, drift_status, z_score):
    """Insert drift event (STARTED or ENDED)"""
    query = """
        INSERT INTO drift_events (
            source_id,
            metric,
            drift_status,
            detected_at,
            z_score
        ) VALUES
    """
    
    data = [(source_id, metric, drift_status, datetime.now(), z_score)]  # ✅ datetime object
    clickhouse_client.execute(query, data)
    
    print(f"🚨 DRIFT {drift_status}: {source_id} / {metric} (z-score: {z_score:.2f})")

def process_event(clickhouse_client, source_id, metric, value, event_time):
    """Process a single event and check for drift"""
    key = (source_id, metric)
    
    # Add to rolling window
    window = rolling_windows[key]
    window.append(value)
    
    # Maintain window size
    max_size = config['rolling_window_size']
    if len(window) > max_size:
        window.popleft()
    
    # Need enough data in rolling window
    if len(window) < max_size:
        return
    
    # Calculate rolling statistics
    rolling_mean = sum(window) / len(window)
    
    # Get baseline
    baseline = baseline_cache.get(key)
    if not baseline:
        return
    
    # Calculate Z-score
    z_score = calculate_z_score(rolling_mean, baseline['mean'], baseline['stddev'])
    
    # Get current state
    if key not in drift_state:
        drift_state[key] = {
            'is_drifting': False,
            'consecutive_violations': 0,
            'consecutive_normal': 0
        }
    
    current_state = drift_state[key]
    threshold = config['z_score_threshold']
    
    # Check for violation
    if z_score > threshold:
        current_state['consecutive_violations'] += 1
        current_state['consecutive_normal'] = 0
        
        # Check if we should transition to DRIFTING
        if not current_state['is_drifting']:
            if current_state['consecutive_violations'] >= config['consecutive_violations']:
                # TRANSITION: NORMAL → DRIFTING
                insert_drift_event(clickhouse_client, source_id, metric, 'STARTED', z_score)
                current_state['is_drifting'] = True
    else:
        current_state['consecutive_normal'] += 1
        current_state['consecutive_violations'] = 0
        
        # Check if we should transition to NORMAL
        if current_state['is_drifting']:
            if current_state['consecutive_normal'] >= config['consecutive_violations']:
                # TRANSITION: DRIFTING → NORMAL
                insert_drift_event(clickhouse_client, source_id, metric, 'ENDED', z_score)
                current_state['is_drifting'] = False

def main():
    print("🚀 Starting Drift Detector...")
    
    # Load configuration
    load_config()
    
    # Setup ClickHouse client
    clickhouse_client = Client(**CLICKHOUSE_CONFIG)
    
    # Load baselines
    load_baselines(clickhouse_client)
    
    # Setup Kafka consumer
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([KAFKA_TOPIC])
    
    print(f"📡 Listening to Kafka topic: {KAFKA_TOPIC}")
    print(f"⚙️  Configuration: rolling_window={config['rolling_window_size']}, "
          f"z_threshold={config['z_score_threshold']}, "
          f"consecutive={config['consecutive_violations']}")
    
    event_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Kafka error: {msg.error()}")
                continue
            
            try:
                # Parse message
                data = json.loads(msg.value().decode('utf-8'))
                source_id = data['source_id']
                metric = data['metric']
                value = data['value']
                event_time = parser.isoparse(data['event_time'])
                
                # Process event
                process_event(clickhouse_client, source_id, metric, value, event_time)
                
                event_count += 1
                
                if event_count % 10000 == 0:
                    print(f"📊 Processed {event_count:,} events")
                
                # Commit every 1000 messages
                if event_count % 1000 == 0:
                    consumer.commit(asynchronous=False)
                
            except (json.JSONDecodeError, KeyError) as e:
                print(f"⚠️  Parse error: {e}")
                continue
    
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        consumer.close()
        print(f"✅ Processed total {event_count:,} events")

if __name__ == "__main__":
    main()