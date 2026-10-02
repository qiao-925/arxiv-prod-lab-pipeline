"""全局共享配置：数据源身份 + 数据路径 + 环境命名

被「数据准备」(pipeline) 与「数据接入」(hf_dataset_to_kafka) 两个业务包共享，
保证二者看到同一个数据集、同一套数据目录、同一套资源命名，避免各写各的漂移。

逻辑隔离：资源有限时不做物理隔离（双命名空间），而是同一套基础设施
（Kafka / PostgreSQL / MinIO 各一份），通过 ENV 派生的资源名后缀区分环境。
所有资源名统一在此派生，代码不写死名字。
"""
import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent

# ---- HuggingFace 数据源身份（两个包共用）----
HF_DATASET_NAME = os.getenv("HF_DATASET_NAME", "ines-besrour/unarxive_2024")
HF_ENDPOINT = os.getenv("HF_ENDPOINT", "https://hf-mirror.com")

# ---- 环境（逻辑隔离）----
# ENV 取值：test（开发/dev）或 prod（生产）。资源名后缀由此派生。
ENV = os.getenv("ENV", "test")
SUFFIX = "-dev" if ENV == "test" else "-prod"      # 短横线：Kafka / MinIO / Job
DB_SUFFIX = "_dev" if ENV == "test" else "_prod"   # 下划线：PG 库名

# ---- 数据目录契约 ----
# pipeline 产出 → ingest 读取；测试 / 生产隔离。
DATA_ROOT = Path(os.getenv("DATA_ROOT", str(PROJECT_ROOT / "data")))
DATA_RAW = DATA_ROOT / "raw"                  # 生产：下载的原始 JSONL
DATA_RAW_TEST = DATA_ROOT / "raw_test"        # 测试：下载的原始 JSONL
DATA_SORTED = DATA_ROOT / "sorted"            # 生产：按日期分区 Parquet
DATA_SORTED_TEST = DATA_ROOT / "sorted_test"  # 测试：按日期分区 Parquet


def get_kafka_topic() -> str:
    """Kafka 论文 Topic：arxiv-papers-dev / arxiv-papers-prod"""
    return f"arxiv-papers{SUFFIX}"


def get_kafka_group() -> str:
    """Kafka 消费者组（供 Flink 等下游使用）：flink-dev / flink-prod"""
    return f"flink{SUFFIX}"


def get_pg_db() -> str:
    """PostgreSQL 库名：arxiv_dev / arxiv_prod"""
    return f"arxiv{DB_SUFFIX}"


def get_minio_bucket() -> str:
    """MinIO Bucket：arxiv-dev / arxiv-prod"""
    return f"arxiv{SUFFIX}"


def get_job_name() -> str:
    """Job 名（批次记账 / 断点续传按 Job 分隔）：hf-ingestor-dev / hf-ingestor-prod"""
    return f"hf-ingestor{SUFFIX}"


# ---- MinIO 连接（资源名见 get_minio_bucket）----
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
