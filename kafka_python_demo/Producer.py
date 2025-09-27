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
     value_serializer=lambda v: v.encode('utf-8')
)

print("Type your messages. Press Ctrl+C to exit.")
try:
    while True:
        user_input = input("Enter message: ")
        producer.send(topic_name, user_input)
        producer.flush()
        print(f"Sent: {user_input}")
except KeyboardInterrupt:
    print("\nExiting...")
finally:
    producer.close()
