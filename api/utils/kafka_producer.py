from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'kafka:9092'})

def send_purchase(data):
    producer.produce('purchases', json.dumps(data).encode('utf-8'))
    producer.flush()
