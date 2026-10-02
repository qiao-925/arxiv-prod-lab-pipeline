"""Kafka Producer 封装"""
import json
from kafka import KafkaProducer

from .config import Config


class KafkaClient:
    def __init__(self, logger):
        self.log = logger
        self.log.info(f"连接 Kafka: {Config.KAFKA_BOOTSTRAP}")
        self.producer = KafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=50,
            batch_size=65536,
            max_request_size=Config.KAFKA_MAX_REQUEST_SIZE,
            compression_type="gzip",
            enable_idempotence=True,
        )
        self.log.info("Kafka Producer 就绪")

    def send(self, topic: str, value: dict):
        self.producer.send(topic, value=value)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()
