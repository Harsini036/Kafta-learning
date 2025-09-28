import csv
import json
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv
import os
load_dotenv()

boostrap_server = os.getenv('KAFKA_BOOTSTRAP')
topic_name = 'test3'

producer = KafkaProducer(
     bootstrap_servers=[boostrap_server],
     key_serializer=lambda k: k.encode('utf-8'),
     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

print("Type your messages. Press Ctrl+C to exit.")
try:
    with open('mock_kafka_test.csv', newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        reader.fieldnames = [name.strip().lower() for name in reader.fieldnames]
        for row in reader:
            key = row['mtn']
            value = {
                "mtn": row["mtn"],
                "enodeb": row["enodeb"],
                "gnodeb": row["gnodeb"],
                "status": row["status"],
                "timestamp": row["timestamp"]
            }
            future = producer.send(topic_name, key=key, value=value)
            record_metadata = future.get(timeout=10)
            print(f"Sent: {key}, {value} --> Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

    #producer.flush()
except Exception as e:
    print(f"Error occurred: {e}")
finally:
    producer.close()
