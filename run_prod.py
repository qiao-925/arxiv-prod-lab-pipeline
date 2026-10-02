#!/usr/bin/env python3
"""run_prod.py - 生产入口（显式声明 prod 环境）

入口**显式** init_env("prod")，业务代码只调用 get_xxx() 派生资源名
（Topic: arxiv-papers-prod，库: arxiv_prod，Job: hf-ingestor-prod）。
绝不静默落到 test。

用法：
    python run_prod.py --run-limit 10000
    DATA_SOURCE=huggingface python run_prod.py --run-limit 10000
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.config import init_env

init_env("prod")  # ← 显式声明；必须在导入业务模块之前

from jobs.ingest_to_kafka import main  # noqa: E402  （导入顺序有意为之）

if __name__ == "__main__":
    main()
