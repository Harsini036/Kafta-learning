import csv
from collections import defaultdict

filename = 'mock_kafka_test.csv'  # Make sure this is in your current folder!

# Dictionary to count occurrences and track latest records
counts = defaultdict(int)
latest = {}

with open(filename, 'r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        mtn = row['mtn']
        ts = row['timestamp']
        counts[mtn] += 1
        # If unseen OR this record has a later timestamp, store it
        if mtn not in latest or ts > latest[mtn]['timestamp']:
            latest[mtn] = row

# Find and print duplicate MTNs and their latest timestamp
print("Duplicate MTNs and their latest record's timestamp:")
for mtn in counts:
    if counts[mtn] > 1:
        print(f"MTN: {mtn}, Count: {counts[mtn]}, Latest Timestamp: {latest[mtn]['timestamp']}")
