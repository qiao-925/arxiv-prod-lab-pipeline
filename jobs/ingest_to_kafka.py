#!/usr/bin/env python3
"""ingest_to_kafka.py - 数据接入 Job 入口（显式声明环境）

读取数据源（本地 JSONL / HuggingFace 流式）→ Kafka → PG 批次记账。
业务实现平铺在 jobs/ 包内（config / ingestor / pg_client / reader），
连接端点与 Kafka 客户端来自 common。

环境由入口显式声明：本入口提供 `--env` 便捷参数，或由上层入口
（tests/run_test.py → test，run_prod.py → prod）先 init_env() 后再导入。
未声明时会立即报错「请先调用 init_env('test' 或 'prod')」——有意为之，防误连环境。

用法：
    python jobs/ingest_to_kafka.py --env test  --run-limit 1000
    python jobs/ingest_to_kafka.py --env prod  --run-limit 1000
    DATA_SOURCE=huggingface python jobs/ingest_to_kafka.py --env test --run-limit 1000

DolphinScheduler Shell 任务（生产）：
    cd /home/qiao/arxiv-prod-lab-pipeline
    source .venv/bin/activate
    python run_prod.py --run-limit 10000
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["test", "prod"], default=None,
                        help="环境（test/prod）。未提供时要求入口已 init_env()")
    parser.add_argument("--run-limit", type=int, default=None,
                        help="本次运行处理条数，0 表示不限制")
    parser.add_argument("--data-source", type=str, default=None,
                        choices=["local", "huggingface"],
                        help="数据源类型")
    args = parser.parse_args(argv)

    # 若显式传了 --env 则在此声明；否则假定入口已 init_env()。
    # 两者都没有时，下面的 import 会因未声明环境而立即 RuntimeError。
    if args.env:
        from common.config import init_env
        init_env(args.env)

    # 在声明环境之后才导入配置。
    from jobs.config import Config

    if args.run_limit is not None:
        Config.RUN_LIMIT = args.run_limit
    if args.data_source is not None:
        Config.DATA_SOURCE = args.data_source

    # Ingestor 在 import 时按 Config.DATA_SOURCE 选定 Reader，
    # 故必须在覆盖 DATA_SOURCE 之后再导入。
    from jobs.ingestor import Ingestor

    Ingestor().run()


if __name__ == "__main__":
    main()
