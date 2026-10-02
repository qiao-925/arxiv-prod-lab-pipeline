"""数据接入侧业务配置

连接端点（HF / Kafka / PostgreSQL / MinIO）与资源名派生统一来自
common.config（单一事实来源），本模块只保留**数据接入业务参数**：
数据源类型、本地路径、批次 / 限流 / 消息大小、启动确认等。

环境必须显式声明：入口先调用 `init_env("test" | "prod")`，再 import 本模块。
本模块在 import 时即解析资源名（Class 属性），若未声明环境会立即
RuntimeError（见 common.config 的 _env()），绝不静默落到某个环境。
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import HF_DATASET_NAME as _HF_DATASET_NAME
from common.config import HF_ENDPOINT as _HF_ENDPOINT
from common.config import KAFKA_BOOTSTRAP as _KAFKA_BOOTSTRAP
from common.config import MINIO_ACCESS_KEY as _MINIO_ACCESS_KEY
from common.config import MINIO_BUCKET as _MINIO_BUCKET
from common.config import MINIO_ENDPOINT as _MINIO_ENDPOINT
from common.config import MINIO_SECRET_KEY as _MINIO_SECRET_KEY
from common.config import PG_HOST as _PG_HOST
from common.config import PG_PASSWORD as _PG_PASSWORD
from common.config import PG_PORT as _PG_PORT
from common.config import PG_USER as _PG_USER
from common.config import get_env as _get_env
from common.config import get_job_name as _get_job_name
from common.config import get_kafka_group as _get_kafka_group
from common.config import get_kafka_topic as _get_kafka_topic
from common.config import get_minio_prefix as _get_minio_prefix
from common.config import get_pg_db as _get_pg_db


class Config:
    # 环境：test / prod —— 决定资源名后缀（须先 init_env）
    ENV = _get_env()

    # 数据源类型：local / huggingface
    DATA_SOURCE = os.getenv("DATA_SOURCE", "local")

    # HuggingFace（继承自 common.config）
    HF_DATASET_NAME = _HF_DATASET_NAME
    HF_ENDPOINT = _HF_ENDPOINT

    # 本地数据源（默认 tests/data；也可指向「数据准备」的产出，
    # 例如 data/raw_test/test_2000.jsonl）
    LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "tests/data/test_2000.jsonl")

    # Kafka（Bootstrap 端点自 common.config；Topic / Group 默认按 ENV 派生）
    KAFKA_BOOTSTRAP = _KAFKA_BOOTSTRAP
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", _get_kafka_topic())
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", _get_kafka_group())

    # PostgreSQL（端点自 common.config；库名默认按 ENV 派生）
    PG_HOST = _PG_HOST
    PG_PORT = _PG_PORT
    PG_DB = os.getenv("PG_DB", _get_pg_db())
    PG_USER = _PG_USER
    PG_PASSWORD = _PG_PASSWORD

    # MinIO：bucket 全环境共用，环境靠对象前缀区分（端点自 common.config）
    MINIO_ENDPOINT = _MINIO_ENDPOINT
    MINIO_ACCESS_KEY = _MINIO_ACCESS_KEY
    MINIO_SECRET_KEY = _MINIO_SECRET_KEY
    MINIO_BUCKET = _MINIO_BUCKET
    MINIO_PREFIX = os.getenv("MINIO_PREFIX", _get_minio_prefix())

    # Ingestor（Job 名默认按 ENV 派生 → test/prod 断点续传 checkpoint 分离）
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
