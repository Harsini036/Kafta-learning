import csv
import random
from datetime import datetime

def random_timestamp(start_year=2016, end_year=2025):
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    if month == 2:
        day = random.randint(1, 29) if year % 4 == 0 else random.randint(1, 28)
    elif month in [4, 6, 9, 11]:
        day = random.randint(1, 30)
    else:
        day = random.randint(1, 31)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(year, month, day, hour, minute, second).strftime('%Y-%m-%dT%H:%M:%SZ')

statuses = ['OK', 'WARN', 'ERROR', 'DOWN']
data = []
for i in range(100):
    mtn = random.randint(1000000000, 9999999999)
    enodeb = f'ENB{random.randint(1, 20):03d}'
    gnodeb = f'GN{random.randint(1, 20):03d}'
    status = random.choice(statuses)
    timestamp = random_timestamp()
    data.append([mtn, enodeb, gnodeb, status, timestamp])
with open('mock_kafka_test.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['mtn', 'enodeb', 'gnodeb', 'status', 'timestamp'])
    writer.writerows(data)
