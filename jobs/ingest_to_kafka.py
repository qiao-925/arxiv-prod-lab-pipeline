#!/usr/bin/env python3
"""ingest_to_kafka.py - 数据接入 Job

读取数据源（本地 JSONL / HuggingFace 流式）→ Kafka → PG 批次记账。
由 DolphinScheduler 触发，跑完一批即退出，支持断点续传
（批次与 offset 逻辑见 hf_dataset_to_kafka/ingestor.py）。

用法：
    python jobs/ingest_to_kafka.py --run-limit 1000
    DATA_SOURCE=huggingface python jobs/ingest_to_kafka.py --run-limit 1000

配置经环境变量注入（KAFKA_BOOTSTRAP / PG_HOST / LOCAL_DATA_PATH / DATA_SOURCE ...），
详见 README。本 Job 是 hf_dataset_to_kafka 的薄入口。

DolphinScheduler Shell 任务示例：
    cd /home/qiao/arxiv-prod-lab-pipeline
    source .venv/bin/activate
    python jobs/ingest_to_kafka.py --run-limit 10000
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hf_dataset_to_kafka.__main__ import main


if __name__ == "__main__":
    main()
