import threading
import json
import csv
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

load_dotenv()
bootstrap_server = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
topic_name = os.getenv('KAFKA_TOPIC', 'test3')
group_id = 'parallel_group2'

NUM_CONSUMERS = 3
latest_records = {}
lock = threading.Lock()
WAIT_TIME = 90  # seconds

def consume_partition(thread_num):
    group_id = f"diff_grp_test_{thread_num}_{int(time.time())}"
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[bootstrap_server],
        group_id=group_id,  # Fresh group for every run!
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )
    consumer.subscribe([topic_name])
    consumer.poll(timeout_ms=1000)  # Metadata refresh

    print(f"Thread-{thread_num} started.")
    last_received = time.time()
    msg_count = 0

    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        got_message = False
        for tp, messages in msg_pack.items():
            for message in messages:
                record = message.value
                mtn = record['mtn']
                timestamp = record['timestamp']
                with lock:
                    if (mtn not in latest_records or timestamp > latest_records[mtn]['timestamp']):
                        latest_records[mtn] = record
                print(f"Thread-{thread_num}: Consumed Key={mtn}, Partition={message.partition}, Timestamp={timestamp}")
                last_received = time.time()
                got_message = True
                msg_count += 1
        if not got_message and (time.time() - last_received > WAIT_TIME):
            print(f"Thread-{thread_num} read {msg_count} messages in total.")
            print(f"Thread-{thread_num}: No new messages for {WAIT_TIME}s, exiting.")
            break
    consumer.close()
    print(f"Thread-{thread_num} stopped.")

def main():
    threads = []
    for i in range(NUM_CONSUMERS):
        t = threading.Thread(target=consume_partition, args=(i+1,))
        t.daemon = True
        t.start()
        threads.append(t)

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping consumers...")
    finally:
        with open('diff_consumer_latest_record.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['mtn', 'enodeb', 'gnodeb', 'status', 'timestamp'])
            writer.writeheader()
            with lock:
                for rec in latest_records.values():
                    writer.writerow(rec)
        print("Saved latest records to diff_consumer_latest_record.csv")

if __name__ == "__main__":
    main()
