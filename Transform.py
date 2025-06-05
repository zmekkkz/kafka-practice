from kafka import KafkaConsumer
import json
from datetime import datetime
import psycopg2

# Kafka consumer configuration
consumer = KafkaConsumer(
    'demo_testing_kafka',  # Replace with your actual topic
    bootstrap_servers='34.126.117.10:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Transform the incoming Kafka message
def transform_message(msg:dict) -> dict:
    """Clean and transform the incoming message."""
    name_parts = msg.get('name', '').split()
    first_name = name_parts[0] if len(name_parts) > 0 else ''
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ''

    address = msg.get('address', '').replace('\n', ', ')
    text = msg.get('text', '')
    word_count = len(text.split())

    return {
        'first_name': first_name,
        'last_name': last_name,
        'address': address,
        'text': text,
        'word_count': word_count,
        'ingested_at': datetime.utcnow().isoformat()
    }

# Insert transformed data into PostgreSQL
def insert_to_postgres(conn:psycopg2.extensions.connection, transformed:dict) -> None:
    """Insert transformed data into PostgreSQL database."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO user_messages (first_name, last_name, address, text, word_count, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed['first_name'],
            transformed['last_name'],
            transformed['address'],
            transformed['text'],
            transformed['word_count'],
            transformed['ingested_at']
        ))
        conn.commit()

# PostgreSQL connection setup
def get_postgres_connection() -> psycopg2.extensions.connection:
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        dbname='mek',
        user='airflow',
        password='airflow',
        host='localhost',
        port='5433'
    )

# Main pipeline
def main() -> None:
    """Main function to consume messages and insert into PostgreSQL."""
    conn = get_postgres_connection()
    print("Started consuming messages...")

    try:
        for message in consumer:
            raw_data = message.value
            transformed = transform_message(raw_data)
            insert_to_postgres(conn, transformed)
            print("Inserted:", transformed)
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
