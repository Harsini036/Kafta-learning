import threading
import json
import csv
import time
import logging
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
from flask import Flask, jsonify

LOG_FILENAME = 'consumer.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler(LOG_FILENAME), logging.StreamHandler()]
)

load_dotenv()
bootstrap_server = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
topic_name = os.getenv('KAFKA_TOPIC', 'test2')
group_id = 'parallel_group1'

NUM_CONSUMERS = 3
latest_records = {}
lock = threading.Lock()
WAIT_TIME = 90

def consume_partition(thread_num):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=[bootstrap_server],
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )
        logging.info(f"Thread-{thread_num} started.")
        last_received = time.time()
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)
            got_message = False
            for tp, messages in msg_pack.items():
                for message in messages:
                    try:
                        record = message.value
                        mtn = record['mtn']
                        timestamp = record['timestamp']
                        with lock:
                            if (mtn not in latest_records or timestamp > latest_records[mtn]['timestamp']):
                                latest_records[mtn] = record
                        logging.info(f"Thread-{thread_num}: Consumed Key={mtn}, Partition={message.partition}, Timestamp={timestamp}")
                        last_received = time.time()
                        got_message = True
                    except Exception as e:
                        logging.error(f"Thread-{thread_num}: Error processing message: {str(e)}", exc_info=True)
            if not got_message and (time.time() - last_received > WAIT_TIME):
                logging.warning(f"Thread-{thread_num}: No new messages for {WAIT_TIME}s, exiting.")
                break
        consumer.close()
        logging.info(f"Thread-{thread_num} stopped.")
    except Exception as e:
        logging.error(f"Thread-{thread_num}: Error in consumer startup: {str(e)}", exc_info=True)

def health_check():
    latest_error = None
    if os.path.exists(LOG_FILENAME):
        with open(LOG_FILENAME, 'r') as logf:
            for line in logf:
                if "ERROR" in line:
                    latest_error = line.strip()
    if latest_error:
        return {"status": "ERRO", "latest_error": latest_error}
    else:
        return {"status": "OK", "latest_error": None}

app = Flask(__name__)

@app.route("/healthpoint")
def healthpoint():
    return jsonify(health_check())

def run_flask():
    app.run(host="0.0.0.0", port=5000)

def main():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    threads = []
    for i in range(NUM_CONSUMERS):
        t = threading.Thread(target=consume_partition, args=(i+1,), daemon=True)
        t.start()
        threads.append(t)

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping consumers...")
    except Exception as e:
        logging.error(f"Main thread error: {str(e)}", exc_info=True)
    finally:
        try:
            with open('threads_latest_record.csv', 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['mtn', 'enodeb', 'gnodeb', 'status', 'timestamp'])
                writer.writeheader()
                with lock:
                    for rec in latest_records.values():
                        writer.writerow(rec)
            logging.info("Saved latest records to threads_latest_record.csv")
        except Exception as e:
            logging.error(f"Error writing CSV: {str(e)}", exc_info=True)

    # KEEP Flask health API running after all consumers stop
    logging.info("Consumers may be stopped. Health endpoint still running. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Exiting health check service.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Uncaught exception: {str(e)}", exc_info=True)
