import csv
import json
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv
import os
load_dotenv()

boostrap_server = os.getenv('KAFKA_BOOTSTRAP')
topic_name = os.getenv('KAFKA_TOPIC')

producer = KafkaProducer(
     bootstrap_servers=[boostrap_server],
     key_serializer=lambda k: k.encode('utf-8'),
     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

print("Type your messages. Press Ctrl+C to exit.")
try:
    with open('test.csv', newline='', encoding='utf-8-sig') as csvfile:
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
            print(f"Sent: {key}, {value}")
            producer.send(topic_name, key=key, value=value)

    producer.flush()
except Exception as e:
    print(f"Error occurred: {e}")
finally:
    producer.close()
