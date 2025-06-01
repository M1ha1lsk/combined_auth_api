from confluent_kafka import Producer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('kafka-producer')

class KafkaProducer:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'message.timeout.ms': 5000,
            'retries': 5
        })
        
    def delivery_report(self, err, msg):
        """Callback для обработки результатов отправки"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def send_purchase(self, data):
        """
        Отправка данных о покупке в Kafka
        :param data: словарь с данными о покупке
        """
        try:
            self.producer.produce(
                topic='purchases',
                value=json.dumps(data).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.flush()
            logger.info(f"Successfully sent purchase data: {data}")
        except Exception as e:
            logger.error(f"Failed to send purchase data: {e}")
            raise

producer = KafkaProducer()