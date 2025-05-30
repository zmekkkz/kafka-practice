# Kafka Practice - Quick Start

This project demonstrates basic usage of Apache Kafka with Python producers and consumers.

## Prerequisites

- Apache Kafka and Zookeeper installed
- Python 3 with required packages (`kafka-python`, `sqlalchemy`, `psycopg2`, `faker`)

## Starting Kafka and Zookeeper

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

## Creating the Kafka Topic

```bash
bin/kafka-topics.sh --create --topic demo_testing_kafka --bootstrap-server 34.126.117.10:9092 --replication-factor 1 --partitions 1
```

## Using the Console Producer

```bash
bin/kafka-console-producer.sh --topic demo_testing_kafka --bootstrap-server 34.126.117.10:9092
```

## Using the Console Consumer

```bash
bin/kafka-console-consumer.sh --topic demo_testing_kafka --bootstrap-server 34.126.117.10:9092
```

---

**Note:**  
- Adjust the `--bootstrap-server` address as needed for your environment.
- For Python producer/consumer, see `Producer.py` and `Consumer.py` in this repository.
