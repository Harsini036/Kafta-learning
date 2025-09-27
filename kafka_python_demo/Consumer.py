from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

load_dotenv()

bootstrap_server = os.getenv('KAFKA_BOOTSTRAP')
topic = os.getenv('KAFKA_TOPIC')

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[bootstrap_server],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Listening for messages. Press Ctrl+C to exit.")
try:
    for message in consumer:
        print(f"Received: {message.value}")
except KeyboardInterrupt:
    print("\nExiting...")

