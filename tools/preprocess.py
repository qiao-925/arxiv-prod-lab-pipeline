#!/usr/bin/env python3
"""preprocess.py - 预处理：JSONL → 按发布日期分区 Parquet

从本地 JSONL 读取（保留原始 JSON 文本），提取 metadata.versions[0].created
作为 publish_date，按日期分区写入 Parquet。测试与生产共用。

用法：
    python tools/preprocess.py --input data/raw --output data/sorted
    python tools/preprocess.py --input data/raw_test --output data/sorted_test \
        --driver-memory 2g --executor-memory 4g --shuffle-partitions 8

断点续传：每个输入文件 = 一个批次，完成写 <output>/<文件名>/_completed；
重跑自动跳过已完成批次（--force 强制重跑）。

注：UDF（日期解析）在本文件内定义，避免跨模块序列化问题。
"""
import argparse
import sys
import time
from email.utils import parsedate_to_datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import ENV, DATA_RAW, DATA_RAW_TEST, DATA_SORTED, DATA_SORTED_TEST

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType
)


VERSION_SCHEMA = StructType([
    StructField("version", StringType()),
    StructField("created", StringType())
])

METADATA_SCHEMA = StructType([
    StructField("versions", ArrayType(VERSION_SCHEMA))
])

RECORD_SCHEMA = StructType([
    StructField("metadata", METADATA_SCHEMA)
])


def _parse_first_published(versions):
    """从 versions[0].created 解析首次发布日期（YYYY-MM-DD）"""
    if not versions or len(versions) == 0:
        return None
    try:
        created = versions[0]["created"]
        if not created:
            return None
        dt = parsedate_to_datetime(created)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _discover_input_files(input_path: Path, pattern: str) -> list:
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        return sorted(input_path.rglob(pattern))
    raise FileNotFoundError(f"输入路径不存在: {input_path}")


def _is_batch_completed(output_batch_dir: Path) -> bool:
    return (output_batch_dir / "_completed").exists()


def _mark_batch_completed(output_batch_dir: Path):
    output_batch_dir.mkdir(parents=True, exist_ok=True)
    (output_batch_dir / "_completed").touch()


def _create_spark(driver_memory, executor_memory, shuffle_partitions):
    return SparkSession.builder \
        .appName("arxiv-preprocess") \
        .master("local[*]") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.driver.memoryOverhead", "1g") \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m") \
        .getOrCreate()


def _process_batch(spark, batch_file, output_batch_dir, parse_udf, log):
    t0 = time.time()

    df = spark.read.text(str(batch_file))
    count = df.count()

    if count == 0:
        log(f"  {batch_file.name}: 空文件，跳过")
        return 0, 0

    df_with_date = df.select(
        col("value").alias("raw_json"),
        parse_udf(
            from_json(col("value"), RECORD_SCHEMA)
            .getField("metadata")
            .getField("versions")
        ).alias("publish_date")
    )

    valid = df_with_date.filter(col("publish_date").isNotNull()).count()
    invalid = count - valid

    df_with_date \
        .filter(col("publish_date").isNotNull()) \
        .select("raw_json", "publish_date") \
        .write \
        .partitionBy("publish_date") \
        .mode("overwrite") \
        .parquet(str(output_batch_dir))

    _mark_batch_completed(output_batch_dir)

    elapsed = time.time() - t0
    log(f"  {batch_file.name}: {count} 条 (有效 {valid}, 失败 {invalid}), "
        f"耗时 {elapsed:.1f}s")

    return valid, invalid


def preprocess(
    input_path: str,
    output_path: str,
    pattern: str = "*.jsonl",
    driver_memory: str = "4g",
    executor_memory: str = "8g",
    shuffle_partitions: int = 200,
    resume: bool = True,
    force: bool = False,
):
    """按发布日期分区预处理 JSONL → Parquet。

    Args:
        input_path: 输入文件或目录
        output_path: 输出目录（每个输入文件写到 <output_path>/<file_stem>/）
        pattern: 目录模式下匹配输入文件（默认 *.jsonl）
        driver_memory / executor_memory: Spark 内存配置
        shuffle_partitions: shuffle 分区数
        resume: 为 True 时跳过已完成批次
        force: 为 True 时忽略 _completed 标记，强制重跑
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    log = lambda msg: print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    print("=" * 70)
    print("arXiv 数据预处理")
    print("=" * 70)
    print(f"输入:           {input_path}")
    print(f"输出:           {output_path}")
    print()

    input_files = _discover_input_files(input_path, pattern)

    if not input_files:
        log(f"[ERROR] 未找到匹配的文件: {input_path} / {pattern}")
        return

    total_input_size = sum(f.stat().st_size for f in input_files)
    log(f"找到 {len(input_files)} 个文件，"
        f"总大小 {total_input_size/1024/1024/1024:.2f} GB")
    print()

    log("初始化 Spark...")
    t0 = time.time()
    spark = _create_spark(driver_memory, executor_memory, shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")
    log(f"Spark 初始化完成: {time.time() - t0:.1f}s")
    print()

    parse_udf = udf(_parse_first_published, StringType())

    log("开始处理...")
    t_start = time.time()

    total_valid = 0
    total_invalid = 0
    processed_count = 0
    skipped_count = 0

    for i, input_file in enumerate(input_files, 1):
        batch_name = input_file.stem
        output_batch_dir = output_path / batch_name

        if resume and not force and _is_batch_completed(output_batch_dir):
            log(f"[{i}/{len(input_files)}] {input_file.name}: 已完成，跳过")
            skipped_count += 1
            continue

        log(f"[{i}/{len(input_files)}] 处理 {input_file.name}")

        try:
            valid, invalid = _process_batch(
                spark, input_file, output_batch_dir, parse_udf, log
            )
            total_valid += valid
            total_invalid += invalid
            processed_count += 1
        except Exception as e:
            log(f"  [ERROR] 处理失败: {e}")
            continue

    total_elapsed = time.time() - t_start

    print()
    print("=" * 70)
    print("处理完成")
    print("=" * 70)
    print(f"  输入文件数:     {len(input_files)}")
    print(f"  已处理:         {processed_count}")
    print(f"  已跳过:         {skipped_count}")
    print(f"  有效记录:       {total_valid}")
    print(f"  解析失败:       {total_invalid}")
    print(f"  总耗时:         {total_elapsed:.1f}s ({total_elapsed/60:.1f} 分钟)")
    print("=" * 70)

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="JSONL → 按发布日期分区 Parquet")
    parser.add_argument("--input", type=str,
                        default=str(DATA_RAW_TEST if ENV == "test" else DATA_RAW),
                        help="输入文件或目录（默认按 ENV：test→data/raw_test, prod→data/raw）")
    parser.add_argument("--output", type=str,
                        default=str(DATA_SORTED_TEST if ENV == "test" else DATA_SORTED),
                        help="输出目录（默认按 ENV：test→data/sorted_test, prod→data/sorted）")
    parser.add_argument("--pattern", type=str, default="*.jsonl",
                        help="目录模式下匹配输入文件（默认 *.jsonl）")
    parser.add_argument("--driver-memory", type=str, default="4g",
                        help="Spark driver 内存（默认 4g）")
    parser.add_argument("--executor-memory", type=str, default="8g",
                        help="Spark executor 内存（默认 8g）")
    parser.add_argument("--shuffle-partitions", type=int, default=200,
                        help="shuffle 分区数（默认 200）")
    parser.add_argument("--force", action="store_true",
                        help="忽略 _completed 标记，强制重跑")
    args = parser.parse_args()

    preprocess(
        input_path=args.input,
        output_path=args.output,
        pattern=args.pattern,
        driver_memory=args.driver_memory,
        executor_memory=args.executor_memory,
        shuffle_partitions=args.shuffle_partitions,
        force=args.force,
    )


if __name__ == "__main__":
    main()
