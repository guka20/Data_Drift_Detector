from confluent_kafka import Consumer, KafkaError
from multiprocessing import Process, cpu_count
from clickhouse_driver import Client
from dateutil import parser
import json
import signal
import sys
import time

# kafka-ს 
KAFKA_CONFIG = {
    # პორტი რომელზეც იმუშავებს kafka
    'bootstrap.servers': '127.0.0.1:9092',
    # უნიკალური იდენტიფიკატორი 
    'group.id': 'data_drift_detect_1',
    # ვუთითებს საიდან დაიწყოს წაკითხვა მონაცემების
    # earliest - უძველესი latest - უახლესი
    'auto.offset.reset': 'earliest',
    # უნდა დააკომიტოს/შეინახოს თუ არა consumer-ის მიერ წაკითხული offset-ები 
    'enable.auto.commit': False
}


# ClickHouse-ის კონფიგურაცია
CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "port": 9000,
    "database": "default",
    "user": "default",
    "password": "1234",
}

KAFKA_TOPIC = 'telemetry.events'
ROWS_PER_BATCH = 10_000


shutdown_flag = False

def signal_handler(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    print("\nShutdown signal received, finishing current batches...")

def clickhouse_worker(worker_id):
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([KAFKA_TOPIC])
    client = Client(**CLICKHOUSE_CONFIG)

    insert_sql = """
        INSERT INTO raw_events (
            event_id,
            source_id,
            metric,
            value,
            event_time
        ) VALUES
    """
    
    # უნიკალური id–ის შექმნა
    tx_id = int(time.time() * 1_000_000) * 1000 + worker_id * 100_000_000_000
    
    rows = []
    total_inserted = 0
    
    try:
        while not shutdown_flag:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # არანაირი შეტყობუინება არაა, ვამოწმებთ გვაქვს თუ არა რომელიმე row მოლოდინის რეჟიმში
                if rows:
                    try:
                        client.execute(insert_sql, rows)
                        consumer.commit(asynchronous=False)
                        total_inserted += len(rows)
                        print(f"[Worker {worker_id}] Flushed {len(rows)} rows | Total: {total_inserted:,}")
                        rows.clear()
                    except Exception as e:
                        print(f"[Worker {worker_id}] ClickHouse insert failed: {e}")
                        raise
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f'[Worker {worker_id}] Kafka error: {msg.error()}')
                    continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                event_time = parser.isoparse(data['event_time'])
        
                row = (
                    tx_id,
                    data['source_id'],
                    data['metric'],
                    data['value'],
                    event_time
                )
                rows.append(row)
                tx_id += 1

                # BATCH-ების მიხედვით დაყოფა და შესაბამისი რაოდენობის შენახვის შემთხვევაში შეინახოს consumer–მა
                if len(rows) >= ROWS_PER_BATCH:
                    try:
                        client.execute(insert_sql, rows)
                        consumer.commit(asynchronous=False)
                        total_inserted += len(rows)
                        print(f"[Worker {worker_id}] Inserted {len(rows)} rows | Total: {total_inserted:,}")
                        rows.clear()
                    except Exception as e:
                        print(f"[Worker {worker_id}] ClickHouse insert failed: {e}")
                        raise
                        
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                print(f"[Worker {worker_id}] Message parse error: {e}")
                continue

    except KeyboardInterrupt:
        print(f"[Worker {worker_id}] Keyboard interrupt received")
    except Exception as e:
        print(f"[Worker {worker_id}] Fatal error: {e}")
    finally:
        # თუ კი გავთიშავთ პროცესს ავტომატურად დარჩენილი მონაცემების შენახვა/ჩარეცხვა
        if rows:
            try:
                print(f"[Worker {worker_id}] Flushing {len(rows)} remaining rows...")
                client.execute(insert_sql, rows)
                consumer.commit(asynchronous=False)
                total_inserted += len(rows)
                print(f"[Worker {worker_id}] Final flush complete | Total: {total_inserted:,}")
            except Exception as e:
                print(f"[Worker {worker_id}] Final flush failed: {e}")
        
        consumer.close()
        print(f"[Worker {worker_id}] Shutdown complete. Total rows inserted: {total_inserted:,}")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    workers = cpu_count()
    print(f"Starting {workers} worker processes...")

    processes = []
    for i in range(workers):
        p = Process(target=clickhouse_worker, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print("Data import into ClickHouse is completed")