from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'demo_testing_kafka',  # Replace with your actual topic
    bootstrap_servers='34.126.117.10:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

try:
    for message in consumer:
        raw_data = message.value
        print(f"Received message: {raw_data}")
except KeyboardInterrupt:
        print("Stopping consumer.")