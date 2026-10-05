#!/usr/bin/env python3
"""run_e2e_test.py - 端到端测试（test 环境）

流程：
    1. 下载 2000 条论文     → data/raw_test
    2. 预处理为分区 Parquet  → data/sorted_test
    3. 逐日推送到 Kafka     → jobs/job_daily_push.py

有缓存则跳过（download / preprocess 自带 _completed 标记）。

用法：
    python tests/run_e2e_test.py                 # 全链路
    python tests/run_e2e_test.py --limit 500     # 只下载 500 条

环境变量：
    KAFKA_BOOTSTRAP / PG_* / MINIO_*   指向远程
    SIM_DAY_SECONDS                     时间模拟，默认 5
"""
import os

from common.paths import DATA_RAW, DATA_SORTED

os.environ["ENV"] = "test"


import argparse
import json




import subprocess
import sys
import time
from jobs.job_daily_push import main as job_daily_push
from email.utils import parsedate_to_datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable

sys.path.insert(0, str(ROOT))

# ─── 1) ENV 必须在导入 common.config 之前固定为 test ───
os.environ["ENV"] = "test"

# ─── 2) 常量与路径（内联，不走 common.constants）───
HF_DATASET_NAME = os.getenv("HF_DATASET_NAME", "ines-besrour/unarxive_2024")
HF_ENDPOINT = os.getenv("HF_ENDPOINT", "https://hf-mirror.com")
os.environ.setdefault("HF_ENDPOINT", HF_ENDPOINT)

# ═══════════════════════════════════════════════════════════════════════
# 工具
# ═══════════════════════════════════════════════════════════════════════
def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _run(cmd) -> None:
    print("\n>>> " + " ".join(str(c) for c in cmd) + "\n", flush=True)
    env = os.environ.copy()
    env["ENV"] = "test"
    subprocess.run([str(c) for c in cmd], cwd=ROOT, check=True, env=env)


# ═══════════════════════════════════════════════════════════════════════
# Part 1: 下载
# ═══════════════════════════════════════════════════════════════════════
def _is_download_completed(output_dir: Path, limit: int) -> bool:
    if not (output_dir / "_download_completed").exists():
        return False
    progress_file = output_dir / "_progress.json"
    if progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
        if limit > 0:
            return progress.get("total_count", 0) >= limit
    return True


def _save_progress(output_dir: Path, total_count: int) -> None:
    with open(output_dir / "_progress.json", "w") as f:
        json.dump({
            "total_count": total_count,
            "completed": True,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }, f, indent=2)
    (output_dir / "_download_completed").touch()


def download(output_dir: Path, limit: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("HuggingFace 数据下载")
    print("=" * 70)
    print(f"数据集: {HF_DATASET_NAME}")
    print(f"镜像:   {HF_ENDPOINT}")
    print(f"输出:   {output_dir}")

    if _is_download_completed(output_dir, limit):
        _log("✅ 下载已完成，跳过")
        return

    from datasets import load_dataset

    output_file = output_dir / (f"test_{limit}.jsonl" if limit > 0 else "full.jsonl")
    _log(f"输出文件: {output_file}")
    _log("开始下载...")

    t0 = time.time()
    dataset = load_dataset(HF_DATASET_NAME, split="train", streaming=True)

    count = 0
    total_bytes = 0
    done = False

    with open(output_file, "w", encoding="utf-8") as f:
        for example in dataset:
            raw = example.get("jsonl")
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")

            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    json.loads(line)
                except json.JSONDecodeError:
                    continue

                f.write(line + "\n")
                total_bytes += len(line.encode("utf-8"))
                count += 1

                if count % 500 == 0:
                    _log(f"  进度: {count} 条, {total_bytes/1024/1024:.1f} MB")

                if limit > 0 and count >= limit:
                    done = True
                    break
            if done:
                break

    _save_progress(output_dir, count)
    _log(f"✅ 下载完成: {count} 条, {total_bytes/1024/1024:.2f} MB, "
         f"耗时 {time.time() - t0:.0f}s")


# ═══════════════════════════════════════════════════════════════════════
# Part 2: 预处理
# ═══════════════════════════════════════════════════════════════════════
VERSION_SCHEMA = None
METADATA_SCHEMA = None
RECORD_SCHEMA = None


def _init_schemas():
    global VERSION_SCHEMA, METADATA_SCHEMA, RECORD_SCHEMA
    if VERSION_SCHEMA is not None:
        return
    from pyspark.sql.types import StructType, StructField, StringType, ArrayType

    VERSION_SCHEMA = StructType([
        StructField("version", StringType()),
        StructField("created", StringType()),
    ])
    METADATA_SCHEMA = StructType([
        StructField("versions", ArrayType(VERSION_SCHEMA)),
    ])
    RECORD_SCHEMA = StructType([
        StructField("metadata", METADATA_SCHEMA),
    ])


def _parse_first_published(versions):
    if not versions or len(versions) == 0:
        return None
    try:
        created = versions[0]["created"]
        if not created:
            return None
        return parsedate_to_datetime(created).strftime("%Y-%m-%d")
    except Exception:
        return None


def _is_batch_completed(output_batch_dir: Path) -> bool:
    return (output_batch_dir / "_completed").exists()


def preprocess(input_dir: Path, output_dir: Path) -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, udf
    from pyspark.sql.types import StringType

    _init_schemas()

    print("=" * 70)
    print("arXiv 数据预处理")
    print("=" * 70)
    print(f"输入: {input_dir}")
    print(f"输出: {output_dir}")

    input_files = sorted(input_dir.rglob("*.jsonl"))
    if not input_files:
        _log(f"[ERROR] 未找到 JSONL: {input_dir}")
        return

    pending = [
        f for f in input_files
        if not _is_batch_completed(output_dir / f.stem)
    ]
    if not pending:
        _log("✅ 预处理已完成，跳过")
        return

    _log(f"待处理 {len(pending)}/{len(input_files)} 个文件")

    spark = SparkSession.builder \
        .appName("arxiv-preprocess") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.memoryOverhead", "1g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    parse_udf = udf(_parse_first_published, StringType())

    for i, batch_file in enumerate(pending, 1):
        output_batch_dir = output_dir / batch_file.stem
        _log(f"[{i}/{len(pending)}] 处理 {batch_file.name}")

        df = spark.read.text(str(batch_file))
        df_with_date = df.select(
            col("value").alias("raw_json"),
            parse_udf(
                from_json(col("value"), RECORD_SCHEMA)
                .getField("metadata")
                .getField("versions")
            ).alias("publish_date"),
        )
        df_with_date \
            .filter(col("publish_date").isNotNull()) \
            .select("raw_json", "publish_date") \
            .write \
            .partitionBy("publish_date") \
            .mode("overwrite") \
            .parquet(str(output_batch_dir))

        output_batch_dir.mkdir(parents=True, exist_ok=True)
        (output_batch_dir / "_completed").touch()

    spark.stop()
    _log("✅ 预处理完成")


# ═══════════════════════════════════════════════════════════════════════
# Part 3: 主流程
# ═══════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="端到端测试")
    parser.add_argument("--limit", type=int, default=2000,
                        help="下载条数（默认 2000）")
    args = parser.parse_args()

    # 延迟导入 config：触发 Infisical 加载
    from common import config

    print("\n" + "=" * 70)
    print(f"端到端测试（limit={args.limit}）")
    print("=" * 70)
    print(f"ENV             = {os.environ['ENV']}")
    print(f"KAFKA_BOOTSTRAP = {config.KAFKA_BOOTSTRAP}")
    print(f"PG_HOST         = {config.PG_HOST}")
    print(f"PG_PORT         = {config.PG_PORT}")
    print(f"PG_USER         = {config.PG_USER}")
    print(f"MINIO_ENDPOINT  = {config.MINIO_ENDPOINT}")
    print(f"MINIO_BUCKET    = {config.MINIO_BUCKET}")
    print("=" * 70)

    # 1. 下载
    download(DATA_RAW, limit=args.limit)

    # 2. 预处理
    preprocess(DATA_RAW, DATA_SORTED)

    # 3. 逐日推送（子进程调用 job，独立进程运行）
    job_daily_push();

    print("\n" + "=" * 70)
    print("端到端测试完成")
    print("=" * 70)


if __name__ == "__main__":
    main()