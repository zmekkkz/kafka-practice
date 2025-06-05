
"""
consumer.py

Kafka consumer that reads messages from Kafka and stores them in a PostgreSQL database using SQLAlchemy.
"""

import time
from json import loads
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# SQLAlchemy setup
DATABASE_URL = "postgresql+psycopg2://airflow:airflow@localhost:5433/mek"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class KafkaData(Base):
    """SQLAlchemy model for Kafka data."""
    __tablename__ = 'kafka_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    address = Column(String)
    text = Column(String)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

consumer = KafkaConsumer(
    'demo_testing_kafka',
    bootstrap_servers=['34.126.117.10:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

_batch = []
_last_flush = time.time()

for c in consumer:
    data = c.value
    print(data)
    record = KafkaData(
        name=data.get('name'),
        address=data.get('address'),
        text=data.get('text')
    )
    _batch.append(record)
    now = time.time()
    # Commit every 500 records or every 5 seconds
    if len(_batch) >= 500 or (now - _last_flush) >= 5:
        session.add_all(_batch)
        session.commit()
        print(f"Saved batch of {len(_batch)} records to DB.")
        _batch = []
        _last_flush = now
    else:
        print("Queued record for batch insert:", data)

# After the loop, commit any remaining records
if _batch:
    session.add_all(_batch)
    session.commit()
    print(f"Saved final batch of {len(_batch)} records to DB.")