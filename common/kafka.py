"""Kafka Producer 封装（无业务语义，各业务包共享）

连接端点（bootstrap 地址、最大请求体积）统一取自 common.config，
本模块只封装 Producer 的构造与发送，不含任何论文 / 批次业务概念。
"""
import json

from kafka import KafkaProducer

from common import config as cfg


class KafkaClient:
    def __init__(self, logger):
        self.log = logger
        self.log.info(f"连接 Kafka: {cfg.KAFKA_BOOTSTRAP}")
        self.producer = KafkaProducer(
            bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=50,
            batch_size=65536,
            max_request_size=cfg.KAFKA_MAX_REQUEST_SIZE,
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
