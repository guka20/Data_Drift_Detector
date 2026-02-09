import pandas as pd
from confluent_kafka import Producer
import json
from datetime import datetime

# Kafka config
conf = {'bootstrap.servers': '127.0.0.1:9092'}
producer = Producer(conf)

# Load dataset
print("📁 Loading IoT temperature dataset...")
df = pd.read_csv('./dataset/IOT-temp.csv')

print(f"📊 Loaded {len(df)} records")
print(f"📅 Date range: {df['noted_date'].min()} to {df['noted_date'].max()}")

# Clean and prepare data
events_sent = 0
failed = 0

print("🔄 Transforming and sending to Kafka...")

for idx, row in df.iterrows():
    try:
        # Parse timestamp (format: "08-12-2018 09:30")
        timestamp = datetime.strptime(row['noted_date'], '%d-%m-%Y %H:%M')
        
        # Create source_id based on indoor/outdoor
        location = row['out/in'].strip().lower()
        source_id = f"room_admin_{location}door"  # "room_admin_indoor" or "room_admin_outdoor"
        
        # Create event
        event = {
            "source_id": source_id,
            "metric": "temperature",
            "value": float(row['temp']),
            "event_time": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }
        
        # Send to Kafka
        producer.produce('telemetry.events', json.dumps(event).encode('utf-8'))
        events_sent += 1
        
        # Flush periodically
        if events_sent % 5000 == 0:
            producer.flush()
            progress = (events_sent / len(df)) * 100
            print(f"📤 Sent {events_sent:,} / {len(df):,} events ({progress:.1f}%)")
        
    except Exception as e:
        failed += 1
        if failed < 10:  # Only print first 10 errors
            print(f"⚠️  Error processing row {idx}: {e}")

# Final flush
producer.flush()

print(f"\n✅ Complete!")
print(f"📊 Total events sent: {events_sent:,}")
print(f"❌ Failed: {failed}")
print(f"\n📋 Next steps:")
print("1. Run: python3 kafka_to_clickhouse.py (to ingest to ClickHouse)")
print("2. Run: python3 compute_baseline.py")
print("3. Run: python3 drift_detector.py")