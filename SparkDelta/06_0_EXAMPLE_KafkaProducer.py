import json
import random
import time
import datetime

from kafka import KafkaProducer

broker_address = "localhost:29092"
topic = "demo"


class Event:
    def __init__(self, event_time, word, count ):
        self.event_time = event_time
        self.word = word
        self.count = count

def generate_event(event_time):
    count = int(random.gauss(16, 4))
    word = random.choice(['spark', 'hadoop', 'cassandra', 'kafka', 'python', 'scala'])
    return Event(event_time, word, count)

producer = KafkaProducer(
    bootstrap_servers=broker_address,
    key_serializer=str.encode,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    message = generate_event(str(datetime.datetime.now()))
    producer.send(topic, key=str(message.word), value=message.__dict__)
    producer.flush()
    time.sleep(1)
