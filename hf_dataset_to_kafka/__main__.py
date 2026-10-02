#!/usr/bin/env python3
"""
hf_dataset_to_kafka - 从 HuggingFace/本地读取 arXiv 数据，推送到 Kafka

用法：
    # 使用本地测试数据（默认）
    python -m hf_dataset_to_kafka

    # 使用 HuggingFace 流式
    DATA_SOURCE=huggingface python -m hf_dataset_to_kafka

    # 限制条数
    python -m hf_dataset_to_kafka --run-limit 500
"""
import argparse

from .config import Config
from .ingestor import Ingestor


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-limit", type=int, default=Config.RUN_LIMIT,
                        help="本次运行处理条数，0 表示不限制")
    parser.add_argument("--data-source", type=str, default=Config.DATA_SOURCE,
                        choices=["local", "huggingface"],
                        help="数据源类型")
    args = parser.parse_args(argv)

    Config.RUN_LIMIT = args.run_limit
    Config.DATA_SOURCE = args.data_source

    # 重新导入 Reader（因为配置变了）
    import importlib
    from . import ingestor as ingestor_module
    importlib.reload(ingestor_module)

    ingestor_module.Ingestor().run()


if __name__ == "__main__":
    main()
