from confluent_kafka import Consumer, KafkaError
from clickhouse_driver import Client
import psycopg2
from collections import defaultdict, deque
from dateutil import parser
import json
import math
import time
from datetime import datetime  

# kafka-ს კონფიგურაცია
KAFKA_CONFIG = {
    # პორტი რომელზეც იმუშავებს kafka
    'bootstrap.servers': '127.0.0.1:9092',
    # უნიქალური იდენტიფიკატორი 
    'group.id': 'drift_detector_group',
    # ვუთითებს საიდან დაიწყოს წაკითხვა მონაცემების
    # earliest - უძველესი latest - უახლესი
    'auto.offset.reset': 'earliest',
    # უნდა დააკომიტოს/შეინახოს თუ არა consumer-ის მიერ წაკითხული offset-ები 
    'enable.auto.commit': False
}

# Clickhouse კონფიგურაცია
CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 9000,
    "database": "default",
    "user": "default",
    "password": "1234",
}

# postgresql-ის კონფიგურაცია
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "drift_db",
    "user": "postgres",
    "password": "1234"
}

# kafka ტოპიკის სახელი
KAFKA_TOPIC = 'telemetry.events'

rolling_windows = defaultdict(lambda: deque())
drift_state = {}
baseline_cache = {}
config = {}

def load_config():
    """postgresql-ის კონფიგურაციების ჩატვირთვა"""
    global config
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SELECT config_key, config_value FROM drift_config")
    rows = cursor.fetchall()
    # გარდავქმნათ მონაცემთა ტიპები ჩვენთვის საჭიროებისამებრ
    for key, value in rows:
        if key in ['rolling_window_size', 'consecutive_violations']:
            config[key] = int(value)
        elif key in ['z_score_threshold']:
            config[key] = float(value)
        else:
            config[key] = value
    
    cursor.close()
    conn.close()
    
    print(f"Loaded configuration: {config}")

def load_baselines(clickhouse_client):
    # ყველა baseline–ის სტატისტიკის ჩამოტვირთვა clickhouse-დან
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
    
    print(f"Loaded {len(baseline_cache)} baseline statistics")

def calculate_z_score(rolling_mean, baseline_mean, baseline_stddev):
    # z-score-ს გამოთვლა
    if baseline_stddev == 0 or math.isnan(baseline_stddev):
        return 0
    
    return abs(rolling_mean - baseline_mean) / baseline_stddev


def insert_drift_event(clickhouse_client, source_id, metric, drift_status, z_score):
    """drift event-ის შენახვა (STARTED ან ENDED) """
    query = """
        INSERT INTO drift_events (
            source_id,
            metric,
            drift_status,
            detected_at,
            z_score
        ) VALUES
    """
    
    data = [(source_id, metric, drift_status, datetime.now(), z_score)] 
    clickhouse_client.execute(query, data)
    
    print(f"DRIFT {drift_status}: {source_id} / {metric} (z-score: {z_score:.2f})")

def process_event(clickhouse_client, source_id, metric, value, event_time):
    # თითოეული ევენთის შემოწმება
    key = (source_id, metric)
    
    window = rolling_windows[key]
    window.append(value)
    
    max_size = config['rolling_window_size']
    if len(window) > max_size:
        window.popleft()
    
    if len(window) < max_size:
        return
    
    rolling_mean = sum(window) / len(window)
    
    baseline = baseline_cache.get(key)
    if not baseline:
        return
    
    z_score = calculate_z_score(rolling_mean, baseline['mean'], baseline['stddev'])
    
    # მიმდინარე გარემოება. დრიფტია თუ არ არის დრიფტი.
    if key not in drift_state:
        drift_state[key] = {
            'is_drifting': False,
            'consecutive_violations': 0,
            'consecutive_normal': 0
        }
    
    current_state = drift_state[key]
    threshold = config['z_score_threshold']
    
    # შევამოწმოთ დარღვევა
    if z_score > threshold:
        current_state['consecutive_violations'] += 1
        current_state['consecutive_normal'] = 0
        
        # შევამოწმოთ გვჭირდება თუ არა drift რეჟიმში გადასვლა
        if not current_state['is_drifting']:
            if current_state['consecutive_violations'] >= config['consecutive_violations']:
                # გადასვლა normal-დან drifting-ზე
                insert_drift_event(clickhouse_client, source_id, metric, 'STARTED', z_score)
                current_state['is_drifting'] = True
    else:
        current_state['consecutive_normal'] += 1
        current_state['consecutive_violations'] = 0
        
        # შევამოწმოთ გვჭირდება თუ არა NORMAL-ზე გადასვლა
        if current_state['is_drifting']:
            if current_state['consecutive_normal'] >= config['consecutive_violations']:
                # DRIFTING–დან NORMAL-ზე გადასვლა
                insert_drift_event(clickhouse_client, source_id, metric, 'ENDED', z_score)
                current_state['is_drifting'] = False

def main():
    print("Starting Drift Detector...")
    
    load_config()
    
    clickhouse_client = Client(**CLICKHOUSE_CONFIG)
    
    load_baselines(clickhouse_client)
    
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([KAFKA_TOPIC])
    
    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    print(f"Configuration: rolling_window={config['rolling_window_size']}, "
          f"z_threshold={config['z_score_threshold']}, "
          f"consecutive={config['consecutive_violations']}")
    
    event_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            # ვამოწმებთ kafka-დან წამოღებული მონაცემი არის თუ არა წამოსული
            # თუ ერორია ვაკეთებთ რეაგირებას
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}")
                continue
            
            try:
                # შეტყობინების გარდაქმნა საჭიროებისამებრ
                data = json.loads(msg.value().decode('utf-8'))
                source_id = data['source_id']
                metric = data['metric']
                value = data['value']
                event_time = parser.isoparse(data['event_time'])
                
                process_event(clickhouse_client, source_id, metric, value, event_time)
                
                event_count += 1
                
                if event_count % 10000 == 0:
                    print(f"Processed {event_count:,} events")
                
                # ვინახავთ ყოველ 1000 ევენთს
                if event_count % 1000 == 0:
                    consumer.commit(asynchronous=False)
                
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Parse error: {e}")
                continue
    
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()
        print(f"Processed total {event_count:,} events")

if __name__ == "__main__":
    main()