from kafka import KafkaConsumer, TopicPartition

bootstrap_server = 'localhost:9092'
topic_name = 'test3'
consumer = KafkaConsumer(
    bootstrap_servers=[bootstrap_server],
    client_id="partition_offset_checker"
)
# Explicitly subscribe to the topic (metadata fetched)
consumer.subscribe([topic_name])

consumer.poll(timeout_ms=1000)  # Force metadata refresh

partition_counts = {}
for partition in range(3):
    tp = TopicPartition(topic_name, partition)
    end_offset = consumer.end_offsets([tp])[tp]
    partition_counts[partition] = end_offset

print("Records per partition:", partition_counts)
consumer.close()
