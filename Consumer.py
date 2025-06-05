from kafka import KafkaConsumer
from json import loads

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
# SQLAlchemy setup
DATABASE_URL = "postgresql+psycopg2://airflow:airflow@localhost:5433/mek"
engine = create_engine(DATABASE_URL)

Base = declarative_base()

# Define your table/model
class KafkaData(Base):
    __tablename__ = 'kafka_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    address = Column(String)
    text = Column(String)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Kafka consumer
consumer = KafkaConsumer(
    'demo_testing_kafka',
    bootstrap_servers=['34.126.117.10:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for c in consumer:
    data = c.value
    print(data)
    record = KafkaData(
        name=data.get('name'),
        address=data.get('address'),
        text=data.get('text')
    )
    import time
    batch = getattr(session, '_batch', None)
    last_flush = getattr(session, '_last_flush', None)
    if batch is None:
        batch = []
        session._batch = batch
        last_flush = time.time()
        session._last_flush = last_flush
    batch.append(record)
    now = time.time()
    # Commit every 500 records or every 5 seconds
    if len(batch) >= 500 or (now - session._last_flush) >= 5:
        session.add_all(batch)
        session.commit()
        print(f"Saved batch of {len(batch)} records to DB.")
        session._batch = []
        session._last_flush = now
    else:
        print("Queued record for batch insert:", data)

# After the loop, commit any remaining records
if hasattr(session, '_batch') and session._batch:
    session.add_all(session._batch)
    session.commit()
    print(f"Saved final batch of {len(session._batch)} records to DB.")