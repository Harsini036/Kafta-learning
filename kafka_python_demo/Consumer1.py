from kafka import KafkaConsumer
from dotenv import load_dotenv
import os, json
from datetime import datetime

load_dotenv()

bootstrap_server = os.getenv('KAFKA_BOOTSTRAP')
topic = os.getenv('KAFKA_TOPIC')

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[bootstrap_server],
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # don’t commit offsets
    consumer_timeout_ms=5000,  # ⏳ stop after 5s of no new messages
    value_deserializer=lambda x: x.decode('utf-8')
)

def parse_timestamp(ts: str):
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")

latest_record = None

print("Reading all messages...")
for message in consumer:
    raw_value = message.value

    # ✅ Try JSON parse
    try:
        record = json.loads(raw_value)
    except json.JSONDecodeError:
        continue

    # ✅ Must contain required fields
    if not isinstance(record, dict) or "mtn" not in record or "timestamp" not in record:
        continue

    ts = parse_timestamp(record["timestamp"])
    print(latest_record)
    if latest_record is None or ts > parse_timestamp(latest_record["timestamp"]):
        latest_record = record

# ✅ Print only the final latest record
print("\n📌 Final Latest Record:")
print(latest_record)
