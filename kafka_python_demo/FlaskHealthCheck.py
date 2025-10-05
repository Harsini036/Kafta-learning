import threading
import json
import csv
import time
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from dotenv import load_dotenv
import os
from collections import deque
from flask import Flask, jsonify, request

load_dotenv()
bootstrap_server = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
# Default topic
topic_name = os.getenv('KAFKA_TOPIC', 'test2')
group_id = 'parallel_group1'

NUM_CONSUMERS = 3
latest_records = {}
lock = threading.Lock()
WAIT_TIME = 90  # seconds

errors = deque(maxlen=5)
errors_lock = threading.Lock()

health_status = {
    "status": "Unknown",
    "error": [],
    "latest_records": {},
    "topic_exists": False,
    "last_refreshed": "",
    "current_topic": topic_name
}
status_lock = threading.Lock()

def topic_exists_check(topic_name, bootstrap_server):
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_server)
        topics = admin.list_topics()
        admin.close()
        return topic_name in topics
    except Exception as e:
        with errors_lock:
            errors.append(f"Admin client error: {str(e)}")
        return False

def health_updater():
    global topic_name
    while True:
        detail = {}
        current_topic = topic_name
        topic_exist = topic_exists_check(current_topic, bootstrap_server)
        detail['topic_exists'] = topic_exist
        detail['current_topic'] = current_topic
        with errors_lock:
            err_list = list(errors)
            detail['error'] = err_list
            # Topic absent overrides status
            detail['status'] = "OK" if topic_exist and len(err_list) == 0 else "ERRO"
            # Add explicit error for missing topic
            if not topic_exist:
                not_exist_msg = f"Topic '{current_topic}' does not exist on the broker."
                if not_exist_msg not in err_list:
                    errors.append(not_exist_msg)
            else:
                # If topic now exists, remove that error
                for err in err_list:
                    if f"Topic '{current_topic}' does not exist on the broker." in err:
                        errors.remove(err)
        with lock:
            detail['latest_records'] = latest_records.copy()
        detail['last_refreshed'] = time.strftime("%Y-%m-%d %H:%M:%S")
        with status_lock:
            health_status.clear()
            health_status.update(detail)
        time.sleep(30)

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
        print(f"Thread-{thread_num} started.")

        last_received = time.time()
        while True:
            try:
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
                            print(f"Thread-{thread_num}: Consumed Key={mtn}, Partition={message.partition}, Timestamp={timestamp}")
                            last_received = time.time()
                            got_message = True
                        except Exception as e:
                            with errors_lock:
                                errors.append(f"Thread-{thread_num}: error processing message: {str(e)}")
                # Clear errors when recovery happens
                if got_message:
                    with errors_lock:
                        errors.clear()
                if not got_message and (time.time() - last_received > WAIT_TIME):
                    print(f"Thread-{thread_num}: No new messages for {WAIT_TIME}s, exiting.")
                    break
            except Exception as e:
                with errors_lock:
                    errors.append(f"Thread-{thread_num}: error in poll {str(e)}")
        consumer.close()
    except Exception as e:
        with errors_lock:
            errors.append(f"Thread-{thread_num}: startup error {str(e)}")
    finally:
        print(f"Thread-{thread_num} stopped.")

app = Flask(__name__)

@app.route("/health")
def health():
    with status_lock:
        return jsonify(health_status)

@app.route('/select_topic', methods=['POST'])
def select_topic():
    global topic_name
    data = request.get_json()
    if data and 'topic_name' in data:
        topic_name = data['topic_name']
        with errors_lock:
            errors.clear()
        return jsonify({'msg': f'topic_name changed to {topic_name}'}), 200
    else:
        return jsonify({'error': 'No topic_name supplied'}), 400

def run_flask():
    app.run(host="0.0.0.0", port=5000)

def main():
    # Start health updater
    updater_thread = threading.Thread(target=health_updater)
    updater_thread.daemon = True
    updater_thread.start()

    # Start Flask
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # Always check topic existence before starting consumers
    if topic_exists_check(topic_name, bootstrap_server):
        threads = []
        for i in range(NUM_CONSUMERS):
            try:
                t = threading.Thread(target=consume_partition, args=(i+1,))
                t.daemon = True
                t.start()
                threads.append(t)
            except Exception as e:
                with errors_lock:
                    errors.append(f"Main thread error: {str(e)}")

        try:
            while any(t.is_alive() for t in threads):
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping consumers...")
        except Exception as e:
            with errors_lock:
                errors.append(f"Main loop error: {str(e)}")
        finally:
            try:
                with open('threads_latest_record.csv', 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['mtn', 'enodeb', 'gnodeb', 'status', 'timestamp'])
                    writer.writeheader()
                    with lock:
                        for rec in latest_records.values():
                            writer.writerow(rec)
                print("Saved latest records to threads_latest_record.csv")
            except Exception as e:
                with errors_lock:
                    errors.append(f"Error writing CSV: {str(e)}")

    print("Consumers may be stopped. Health endpoint still running. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("Exiting health check service.")

if __name__ == "__main__":
    main()
