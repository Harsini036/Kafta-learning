import multiprocessing
import json
import csv
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

load_dotenv()
bootstrap_server = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
topic_name = os.getenv('KAFKA_TOPIC', 'test1')
group_id = 'parallel_group'
NUM_CONSUMERS = 3
WAIT_TIME = 90  # seconds

def consume_partition(proc_num, latest_records):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[bootstrap_server],
        group_id=group_id,
        client_id=f'consumer_{proc_num}',  # Unique per process
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )
    print(f"Process-{proc_num} started.")

    last_received = time.time()
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        got_message = False
        for tp, messages in msg_pack.items():
            for message in messages:
                record = message.value
                try:
                    mtn = record['mtn']
                    timestamp = record['timestamp']
                except Exception as e:
                    print(f"Process-{proc_num}: Skipped malformed record: {e}")
                    continue
                # Manager dict is shared safely across processes
                if (mtn not in latest_records or timestamp > latest_records[mtn]['timestamp']):
                    latest_records[mtn] = record
                print(f"Process-{proc_num}: Consumed Key={mtn}, Partition={message.partition}, Timestamp={timestamp}")
                last_received = time.time()
                got_message = True
        if not got_message and (time.time() - last_received > WAIT_TIME):
            print(f"Process-{proc_num}: No new messages for {WAIT_TIME}s, exiting.")
            break
    consumer.close()
    print(f"Process-{proc_num} stopped.")

def main():
    manager = multiprocessing.Manager()
    latest_records = manager.dict()
    processes = []
    for i in range(NUM_CONSUMERS):
        p = multiprocessing.Process(target=consume_partition, args=(i+1, latest_records))
        p.start()
        processes.append(p)
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Stopping consumers...")
    finally:
        with open('latest_mtn_records.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['mtn', 'enodeb', 'gnodeb', 'status', 'timestamp'])
            writer.writeheader()
            for rec in latest_records.values():
                writer.writerow(rec)
        print("Saved latest records to latest_mtn_records.csv")

if __name__ == "__main__":
    main()
