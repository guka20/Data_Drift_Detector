import random
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # your Kafka broker address and port
    'client.id': 'data-drift-producer',
    'acks': 'all',
    'retries': 3,
    'linger.ms': 100,
    'compression.type': 'snappy',
}

producer = Producer(conf)

TOPIC = 'telemetry.events'
METRICS = ['cpu_usage', 'memory_usage', 'request_latency']
SOURCE_IDS = [f'device_{i}' for i in range(1, 6)]  # Example devices

def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def generate_event():
    return {
        "source_id": random.choice(SOURCE_IDS),
        "metric": random.choice(METRICS),
        "value": round(random.uniform(0, 100), 2),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

try:
    count = 0
    while True:
        event = generate_event()
        producer.produce(
            topic=TOPIC,
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_callback
        )
        producer.poll(0) 
        count = count + 1
        if(count%10000 == 0):
            
            print('${count} Data Saved')
       
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.flush()
