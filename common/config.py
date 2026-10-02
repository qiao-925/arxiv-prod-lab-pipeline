"""common 连接配置：数据源身份 + 连接端点 + 数据路径 + 环境命名

被「数据准备」(tools) 与「数据接入」(jobs) 两个业务包共享，
保证二者看到同一个数据集、同一套数据目录、同一套资源命名，避免各写各的漂移。
所有连接端点（HF / Kafka / PostgreSQL / MinIO）统一在此声明，业务包不各自写默认值。

逻辑隔离：资源有限时不做物理隔离（双命名空间），而是同一套基础设施
（Kafka / PostgreSQL / MinIO 各一份），通过 ENV 派生的资源名后缀区分环境。
所有资源名统一在此派生，代码不写死名字。

环境必须「显式声明」：在任何 get_xxx() 之前调用 init_env("test" | "prod")。
未声明时调用 get_xxx() 立即 RuntimeError，绝不静默落到某个环境。
"""
import os
from pathlib import Path

# 项目根目录（common/ 的父目录）
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---- HuggingFace 数据源身份（两个包共用）----
HF_DATASET_NAME = os.getenv("HF_DATASET_NAME", "ines-besrour/unarxive_2024")
HF_ENDPOINT = os.getenv("HF_ENDPOINT", "https://hf-mirror.com")

# ---- 环境（逻辑隔离，显式声明）----
# None 表示尚未声明；任何 get_*() 都会强制要求先声明。
_ENV = None
_VALID_ENVS = ("test", "prod")


def init_env(env: str):
    """显式初始化环境。必须在任何 get_xxx() 之前调用。

        test → 后缀 -test / _test，前缀 test/
        prod → 后缀 -prod / _prod，前缀 prod/
    """
    global _ENV
    if env not in _VALID_ENVS:
        raise ValueError(
            f"非法环境: {env!r}；请调用 init_env('test') 或 init_env('prod')"
        )
    _ENV = env
    print(f"[CONFIG] 环境: {env}")


def _env() -> str:
    """返回已声明的环境；未声明则报错（强制显式声明）。"""
    if _ENV is None:
        raise RuntimeError("请先调用 init_env('test' 或 'prod')")
    return _ENV


def get_env() -> str:
    """当前环境：test / prod"""
    return _env()


# ---- 资源名后缀派生 ----
# 短横线后缀：Kafka Topic / Group、MinIO、Job 名；下划线后缀：PG 库名。
# test 与 prod 对称带后缀，一眼可辨，不会把命名错当成"默认名"。
def _suffix() -> str:
    return "-test" if _env() == "test" else "-prod"


def _db_suffix() -> str:
    return "_test" if _env() == "test" else "_prod"


def get_kafka_topic() -> str:
    """Kafka 论文 Topic：arxiv-papers-test / arxiv-papers-prod"""
    return f"arxiv-papers{_suffix()}"


def get_kafka_group() -> str:
    """Kafka 消费者组（供 Flink 等下游使用）：flink-test / flink-prod"""
    return f"flink{_suffix()}"


def get_pg_db() -> str:
    """PostgreSQL 库名：arxiv_test / arxiv_prod"""
    return f"arxiv{_db_suffix()}"


def get_minio_prefix() -> str:
    """MinIO 对象前缀（同一 bucket 内的逻辑目录）：test/ 或 prod/"""
    return f"{_env()}/"


def get_job_name() -> str:
    """Job 名（批次记账 / 断点续传按 Job 分隔）：hf-ingestor-test / hf-ingestor-prod"""
    return f"hf-ingestor{_suffix()}"


# ---- 数据目录契约 ----
# pipeline 产出 → ingest 读取；测试 / 生产隔离。
DATA_ROOT = Path(os.getenv("DATA_ROOT", str(PROJECT_ROOT / "data")))
DATA_RAW = DATA_ROOT / "raw"                  # 生产：下载的原始 JSONL
DATA_RAW_TEST = DATA_ROOT / "raw_test"        # 测试：下载的原始 JSONL
DATA_SORTED = DATA_ROOT / "sorted"            # 生产：按日期分区 Parquet
DATA_SORTED_TEST = DATA_ROOT / "sorted_test"  # 测试：按日期分区 Parquet


def get_data_raw() -> Path:
    """按环境返回原始 JSONL 目录（test→data/raw_test，prod→data/raw）"""
    return DATA_RAW_TEST if _env() == "test" else DATA_RAW


def get_data_sorted() -> Path:
    """按环境返回分区 Parquet 目录（test→data/sorted_test，prod→data/sorted）"""
    return DATA_SORTED_TEST if _env() == "test" else DATA_SORTED


# ---- 连接端点（统一在此声明，业务包不各自写默认值）----

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "arxiv-cluster-kafka-bootstrap.kafka:9092")
KAFKA_MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", "10485760"))

# PostgreSQL
PG_HOST = os.getenv("PG_HOST", "postgres.database")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "arxiv2024")

# MinIO（bucket 全环境共用，环境靠对象前缀 get_minio_prefix() 区分）
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "arxiv")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio.minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
