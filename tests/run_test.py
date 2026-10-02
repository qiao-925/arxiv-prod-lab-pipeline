#!/usr/bin/env python3
"""run_test.py - 端到端测试

流程：
    1. 下载 2000 条论文     → data/raw_test
    2. 预处理为分区 Parquet  → data/sorted_test
    3. （可选）接入 Kafka / PG

用法：
    python tests/run_test.py                 # 1 + 2
    python tests/run_test.py --with-ingest   # 1 + 2 + 3（需 Kafka/PG 可用）

以子进程调用各 CLI 入口，等价于手工逐条执行，验证真实运行路径。
"""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable


def run(cmd):
    print("\n>>> " + " ".join(str(c) for c in cmd) + "\n", flush=True)
    subprocess.run([str(c) for c in cmd], cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description="端到端测试（下载 → 预处理 → 可选接入）")
    parser.add_argument("--with-ingest", action="store_true",
                        help="额外执行接入步骤（需 Kafka/PG 可用）")
    parser.add_argument("--limit", type=int, default=2000,
                        help="下载条数（默认 2000）")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print(f"端到端测试（{args.limit} 条）")
    print("=" * 70)

    # 1. 下载
    run([PY, "tools/download.py",
         "--output", "data/raw_test",
         "--limit", str(args.limit)])

    # 2. 预处理（小资源）
    run([PY, "tools/preprocess.py",
         "--input", "data/raw_test",
         "--output", "data/sorted_test",
         "--driver-memory", "2g",
         "--executor-memory", "4g",
         "--shuffle-partitions", "8"])

    # 3. 可选：接入
    if args.with_ingest:
        run([PY, "jobs/ingest_to_kafka.py", "--run-limit", "1000"])

    print("\n" + "=" * 70)
    print("端到端测试完成")
    print("=" * 70)


if __name__ == "__main__":
    main()
