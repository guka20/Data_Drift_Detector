import pandas as pd
from confluent_kafka import Producer
import json
from datetime import datetime

# Kafka-ს კონფიგურაცია
conf = {'bootstrap.servers': '127.0.0.1:9092'}
producer = Producer(conf)

# მონაცემების წამოღება IOT-temp.csv-დან
print("Loading IoT temperature dataset...")
df = pd.read_csv('./dataset/IOT-temp.csv')

print(f"Loaded {len(df)} records")
print(f"Date range: {df['noted_date'].min()} to {df['noted_date'].max()}")

events_sent = 0
failed = 0

print("Transforming and sending to Kafka...")

for idx, row in df.iterrows():
    try:
        
        timestamp = datetime.strptime(row['noted_date'], '%d-%m-%Y %H:%M')

        #შექვმნად აიდი indoor ან outdoor 
        location = row['out/in'].strip().lower()
        source_id = f"room_admin_{location}door" 
        
        # event-ის შექმნა
        event = {
            "source_id": source_id,
            "metric": "temperature",
            "value": float(row['temp']),
            "event_time": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }
        
        # kafka-ში გადაგზავნა
        producer.produce('telemetry.events', json.dumps(event).encode('utf-8'))
        events_sent += 1
        
        # ჩატვირთვა პერიოდულად
        # თითო ევენთის გასაგზავნად დასჭირდება დიდი დრო
        # ამიტომ ვაჯგუფებთ და ვაგზავნით ყოველთ 5000 event-ს ერთად
        if events_sent % 5000 == 0:
            producer.flush()
            progress = (events_sent / len(df)) * 100
            print(f"Sent {events_sent:,} / {len(df):,} events ({progress:.1f}%)")
        
    except Exception as e:
        failed += 1
        if failed < 10: 
            print(f"Error processing row {idx}: {e}")

producer.flush()