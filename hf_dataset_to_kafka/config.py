"""集中配置管理（数据接入侧）

数据源身份（HF 数据集名 / 镜像端点）与环境命名（资源名后缀）均来自
顶层共享 config.py，与「数据准备」包 (pipeline) 保持单一事实来源，避免两端漂移。

资源名（Kafka Topic、PG 库名、Job 名）默认由 ENV 派生（test→-dev / _dev，
prod→-prod / _prod）；如需显式指定，仍可用同名环境变量覆盖。
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import ENV as _ENV
from config import HF_DATASET_NAME as _HF_DATASET_NAME
from config import HF_ENDPOINT as _HF_ENDPOINT
from config import MINIO_ACCESS_KEY as _MINIO_ACCESS_KEY
from config import MINIO_ENDPOINT as _MINIO_ENDPOINT
from config import MINIO_SECRET_KEY as _MINIO_SECRET_KEY
from config import get_job_name as _get_job_name
from config import get_kafka_group as _get_kafka_group
from config import get_kafka_topic as _get_kafka_topic
from config import get_minio_bucket as _get_minio_bucket
from config import get_pg_db as _get_pg_db


class Config:
    # 环境：test（dev）/ prod —— 决定默认资源名后缀
    ENV = _ENV

    # 数据源类型：local / huggingface
    DATA_SOURCE = os.getenv("DATA_SOURCE", "local")

    # HuggingFace（继承自顶层共享 config）
    HF_DATASET_NAME = _HF_DATASET_NAME
    HF_ENDPOINT = _HF_ENDPOINT

    # 本地数据源（默认 tests/data；也可指向「数据准备」的产出，
    # 例如 data/raw_test/test_2000.jsonl）
    LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "tests/data/test_2000.jsonl")

    # Kafka（Topic / Group 默认按 ENV 派生）
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "arxiv-cluster-kafka-bootstrap.kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", _get_kafka_topic())
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", _get_kafka_group())
    KAFKA_MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", "10485760"))

    # PostgreSQL（库名默认按 ENV 派生）
    PG_HOST = os.getenv("PG_HOST", "postgres.database")
    PG_PORT = int(os.getenv("PG_PORT", "5432"))
    PG_DB = os.getenv("PG_DB", _get_pg_db())
    PG_USER = os.getenv("PG_USER", "postgres")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "arxiv2024")

    # MinIO（Bucket 默认按 ENV 派生；当前仅预留，供 Spark 落地区使用）
    MINIO_ENDPOINT = _MINIO_ENDPOINT
    MINIO_ACCESS_KEY = _MINIO_ACCESS_KEY
    MINIO_SECRET_KEY = _MINIO_SECRET_KEY
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", _get_minio_bucket())

    # Ingestor（Job 名默认按 ENV 派生 → dev/prod 断点续传 checkpoint 分离）
    JOB_NAME = os.getenv("JOB_NAME", _get_job_name())
    RUN_LIMIT = int(os.getenv("RUN_LIMIT", "0"))
    SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "0"))
    MAX_MSG_SIZE = int(os.getenv("MAX_MSG_SIZE", str(10 * 1024 * 1024)))

    # 启动后是否要求人工确认（默认关闭，避免调度器任务卡住）
    CONFIRM_STARTUP = os.getenv("CONFIRM_STARTUP", "0") == "1"

    @classmethod
    def validate(cls):
        errors = []
        if cls.ENV not in ("test", "prod"):
            errors.append(f"ENV 必须是 test 或 prod，当前: {cls.ENV}")
        if cls.DATA_SOURCE not in ("local", "huggingface"):
            errors.append(f"DATA_SOURCE 必须是 local 或 huggingface，当前: {cls.DATA_SOURCE}")
        if not cls.KAFKA_BOOTSTRAP:
            errors.append("KAFKA_BOOTSTRAP 未设置")
        if not cls.KAFKA_TOPIC:
            errors.append("KAFKA_TOPIC 未设置")
        if not cls.PG_HOST:
            errors.append("PG_HOST 未设置")
        if not cls.PG_DB:
            errors.append("PG_DB 未设置")
        if cls.MAX_MSG_SIZE <= 0:
            errors.append("MAX_MSG_SIZE 必须大于 0")
        return errors
