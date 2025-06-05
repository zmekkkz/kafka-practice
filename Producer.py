import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json
from faker import Faker

producer = KafkaProducer(bootstrap_servers=['34.126.117.10:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))
fake = Faker()
while True:
    dict_stock = {'name':fake.name(),'address':fake.address(),'text':fake.text()}
    producer.send('demo_testing_kafka',value=dict_stock)
    sleep(1)