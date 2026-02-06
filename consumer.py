from confluent_kafka import Consumer, KafkaError
from multiprocessing import Process, cpu_count
from clickhouse_driver import Client
import json
import time

# Consumer Config

conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'data_drift_detect_1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Clickhouse config
CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 9000,
    "database": "default",
    "user": "default",
    "password": "1234",
}

consumer = Consumer(conf)
consumer.subscribe(['telemetry.events'])

#   source_id String CODEC(ZSTD(19)),
#   metric String,
#   value Float64,
#   event_time DateTime64(3)

def insert_data_to_clickhouse(worker_id):
    client = Client(**CLICKHOUSE_CONFIG)

    insert_sql = """ 
        INSERT INTO telemetry_raw_events(
            event_id,
            source_id,
            metric,
            value,
            event_time
        ) VALUES
    """
    tx_id_base = worker_id * 10_000_000_000
    tx_id = tx_id_base
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print(msg)
                continue

            if msg.error():

                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f'Error: {msg.error()}')
                continue
            data = json.loads(msg.value().decode('utf-8'))
            data_list = list(data.values())
            print(f"Processing: {data_list}")
            row = (tx_id, *data_list)
            client.execute(insert_sql,[row])
            tx_id+=1
            consumer.commit(asynchronous=False)
            time.sleep(1)

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()

# MAIN

if __name__ == "__main__":
    workers = cpu_count()
    print(f"Starting {workers} ClickHouse processes")

    processes = []
    for i in range(workers):
        p = Process(target=insert_data_to_clickhouse, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print("🔥 Data import into Clickhouse is completed")