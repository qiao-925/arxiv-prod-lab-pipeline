#!/usr/bin/env python3
"""
spark_release_date_preprocess_fs_test.py
    - 按论文发布日期预处理的可行性验证脚本（无损版）
    - 包含数据完整性验证（3 层 + 抽样）
"""

import argparse
import hashlib
import json
import time
from pathlib import Path
from email.utils import parsedate_to_datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, udf
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType
)


DEFAULT_INPUT = "/home/qiao/arxiv-prod-lab-pipeline//data/raw_test/test_2000.jsonl"
DEFAULT_OUTPUT = "/home/qiao/arxiv-prod-lab-pipeline/tests/data/sorted"


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


def parse_first_published(versions):
    """从 versions[0].created 解析首次发布日期"""
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


def md5(text):
    """计算字符串的 MD5 哈希"""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def verify_integrity(spark, input_path, output_path, raw_count, valid_count):
    """
    数据完整性验证（3 层 + 抽样）
    
    第 1 层：行数对比
    第 2 层：paper_id 集合对比
    第 3 层：raw_json 内容哈希对比
    抽样：取 5 条详细对比
    """
    print()
    print("=" * 70)
    print("阶段 6：数据完整性验证")
    print("=" * 70)

    t_start = time.time()

    # ============================================================
    # 第 1 层：行数对比
    # ============================================================
    print()
    print("[第 1 层] 行数对比")
    print("-" * 70)

    df_raw = spark.read.text(str(input_path))
    df_sorted = spark.read.parquet(str(output_path))

    raw_count_check = df_raw.count()
    sorted_count = df_sorted.count()

    print(f"  原始行数:    {raw_count_check}")
    print(f"  处理后行数:  {sorted_count}")

    level1_pass = (raw_count_check == sorted_count)
    if level1_pass:
        print(f"  ✅ 通过")
    else:
        print(f"  ❌ 失败: 差异 {raw_count_check - sorted_count} 条")

    # ============================================================
    # 第 2 层：paper_id 集合对比
    # ============================================================
    print()
    print("[第 2 层] paper_id 集合对比")
    print("-" * 70)

    # 从原始数据提取 paper_id
    raw_ids = df_raw.select(
        get_json_object(col("value"), "$.paper_id").alias("paper_id")
    ).filter(col("paper_id").isNotNull()) \
     .distinct() \
     .collect()
    raw_id_set = {row["paper_id"] for row in raw_ids}

    # 从处理后数据提取 paper_id
    sorted_ids = df_sorted.select(
        get_json_object(col("raw_json"), "$.paper_id").alias("paper_id")
    ).filter(col("paper_id").isNotNull()) \
     .distinct() \
     .collect()
    sorted_id_set = {row["paper_id"] for row in sorted_ids}

    print(f"  原始唯一 paper_id 数:    {len(raw_id_set)}")
    print(f"  处理后唯一 paper_id 数:  {len(sorted_id_set)}")

    only_in_raw = raw_id_set - sorted_id_set
    only_in_sorted = sorted_id_set - raw_id_set

    level2_pass = (not only_in_raw and not only_in_sorted)
    if level2_pass:
        print(f"  ✅ 通过: 集合完全相同")
    else:
        print(f"  ❌ 失败:")
        print(f"    仅原始有: {len(only_in_raw)} 个")
        print(f"    仅处理后有: {len(only_in_sorted)} 个")
        if only_in_raw:
            print(f"    示例: {list(only_in_raw)[:3]}")
        if only_in_sorted:
            print(f"    示例: {list(only_in_sorted)[:3]}")

    # ============================================================
    # 第 3 层：raw_json 内容哈希对比
    # ============================================================
    print()
    print("[第 3 层] raw_json 内容哈希对比")
    print("-" * 70)

    t0 = time.time()
    raw_hashes = set()
    for row in df_raw.collect():
        raw_hashes.add(md5(row["value"]))
    t_hash_raw = time.time() - t0

    t0 = time.time()
    sorted_hashes = set()
    for row in df_sorted.collect():
        sorted_hashes.add(md5(row["raw_json"]))
    t_hash_sorted = time.time() - t0

    print(f"  原始唯一哈希数:    {len(raw_hashes)}")
    print(f"  处理后唯一哈希数:  {len(sorted_hashes)}")

    only_raw_hash = raw_hashes - sorted_hashes
    only_sorted_hash = sorted_hashes - raw_hashes

    level3_pass = (not only_raw_hash and not only_sorted_hash)
    if level3_pass:
        print(f"  ✅ 通过: 内容 100% 一致（字节级相同）")
        print(f"    哈希计算耗时: raw={t_hash_raw:.2f}s, sorted={t_hash_sorted:.2f}s")
    else:
        print(f"  ❌ 失败:")
        print(f"    仅原始有: {len(only_raw_hash)} 个")
        print(f"    仅处理后有: {len(only_sorted_hash)} 个")

    # ============================================================
    # 抽样验证：取 5 条详细对比
    # ============================================================
    print()
    print("[抽样验证] 取 5 条详细对比")
    print("-" * 70)

    sample_rows = df_sorted.limit(5).collect()

    raw_df_indexed = df_raw.select(
        col("value").alias("raw_json"),
        get_json_object(col("value"), "$.paper_id").alias("paper_id")
    )

    sample_pass_count = 0
    for i, row in enumerate(sample_rows):
        sorted_json = row["raw_json"]
        sorted_paper_id = json.loads(sorted_json).get("paper_id")

        raw_match = raw_df_indexed.filter(col("paper_id") == sorted_paper_id).first()

        print(f"\n  [{i+1}] paper_id: {sorted_paper_id}")

        if raw_match:
            raw_json = raw_match["raw_json"]
            raw_hash = md5(raw_json)
            sorted_hash = md5(sorted_json)

            print(f"      原始哈希:   {raw_hash[:16]}...")
            print(f"      处理后哈希: {sorted_hash[:16]}...")

            if raw_hash == sorted_hash:
                print(f"      ✅ 字节级一致")
                sample_pass_count += 1
            else:
                print(f"      ❌ 哈希不一致")

            raw_parsed = json.loads(raw_json)
            sorted_parsed = json.loads(sorted_json)

            raw_keys = set(raw_parsed.keys())
            sorted_keys = set(sorted_parsed.keys())

            if raw_keys == sorted_keys:
                print(f"      ✅ 字段集合一致: {sorted(raw_keys)}")
            else:
                print(f"      ❌ 字段差异: raw-only={raw_keys - sorted_keys}, "
                      f"sorted-only={sorted_keys - raw_keys}")
        else:
            print(f"      ❌ 在原始数据中未找到")

    sample_pass = (sample_pass_count == len(sample_rows))

    # ============================================================
    # 验证汇总
    # ============================================================
    t_total = time.time() - t_start

    print()
    print("=" * 70)
    print("数据完整性验证 —— 结果汇总")
    print("=" * 70)
    print(f"  第 1 层（行数）:           {'✅ 通过' if level1_pass else '❌ 失败'}")
    print(f"  第 2 层（paper_id 集合）:  {'✅ 通过' if level2_pass else '❌ 失败'}")
    print(f"  第 3 层（内容哈希）:        {'✅ 通过' if level3_pass else '❌ 失败'}")
    print(f"  抽样（5 条详细对比）:       {'✅ 通过' if sample_pass else '❌ 失败'} "
          f"({sample_pass_count}/{len(sample_rows)})")
    print(f"  验证总耗时:                {t_total:.2f}s")
    print("=" * 70)

    all_pass = level1_pass and level2_pass and level3_pass and sample_pass
    if all_pass:
        print("  🎉 数据完整性验证全部通过：数据 100% 无损")
    else:
        print("  ⚠️  存在验证失败项，请检查上方详细信息")

    return all_pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"[ERROR] 输入文件不存在: {input_path}")
        return

    print("=" * 70)
    print("Spark 发布日期预处理 —— 可行性验证（无损版）")
    print("=" * 70)
    print(f"输入: {input_path}")
    print(f"输出: {output_path}")
    print()

    # ============================================================
    # 阶段 0：初始化 Spark
    # ============================================================
    print("[阶段 0] 初始化 Spark...")
    t0 = time.time()
    spark = SparkSession.builder \
        .appName("release-date-preprocess-fs-test") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    t_spark_init = time.time() - t0
    print(f"        Spark 初始化完成: {t_spark_init:.2f}s\n")

    # ============================================================
    # 阶段 1：以纯文本读取 JSONL
    # ============================================================
    print("[阶段 1] 读取 JSONL（保留原始文本）...")
    t0 = time.time()
    df_raw = spark.read.text(str(input_path))
    count = df_raw.count()
    t_read = time.time() - t0
    print(f"        读取完成: {count} 行, 耗时 {t_read:.2f}s\n")

    # ============================================================
    # 阶段 2：提取日期
    # ============================================================
    print("[阶段 2] 提取发布日期...")
    t0 = time.time()

    df_parsed = df_raw.select(
        col("value").alias("raw_json"),
        from_json(col("value"), RECORD_SCHEMA).alias("data")
    )

    parse_udf = udf(parse_first_published, StringType())
    df_with_date = df_parsed.withColumn(
        "publish_date",
        parse_udf(col("data.metadata.versions"))
    ).drop("data")

    total = df_with_date.count()
    valid = df_with_date.filter(col("publish_date").isNotNull()).count()
    invalid = total - valid

    t_extract = time.time() - t0
    print(f"        解析完成: 有效 {valid}/{total}, 失败 {invalid}")
    print(f"        耗时: {t_extract:.2f}s\n")

    if invalid > 0:
        print(f"        [WARN] 有 {invalid} 条记录无法解析发布日期")

    # ============================================================
    # 阶段 3：分析时间分布
    # ============================================================
    print("[阶段 3] 分析时间分布...")
    t0 = time.time()

    year_stats = df_with_date \
        .filter(col("publish_date").isNotNull()) \
        .withColumn("year", col("publish_date").substr(1, 4)) \
        .groupBy("year") \
        .count() \
        .orderBy("year") \
        .collect()

    t_analyze = time.time() - t0
    print(f"        耗时: {t_analyze:.2f}s")

    years = [(row["year"], row["count"]) for row in year_stats]
    print(f"        年份数量: {len(years)} 年")
    print(f"        年份分布:")

    if len(years) <= 20:
        for y, c in years:
            print(f"          {y}: {c}")
    else:
        for y, c in years[:10]:
            print(f"          {y}: {c}")
        print(f"          ... (省略 {len(years) - 20} 年)")
        for y, c in years[-10:]:
            print(f"          {y}: {c}")

    if valid > 0:
        min_date = df_with_date.filter(col("publish_date").isNotNull()) \
            .agg({"publish_date": "min"}).collect()[0][0]
        max_date = df_with_date.filter(col("publish_date").isNotNull()) \
            .agg({"publish_date": "max"}).collect()[0][0]
        print(f"        时间跨度: {min_date} ~ {max_date}")
    else:
        min_date = max_date = "N/A"

    print()

    # ============================================================
    # 阶段 4：按日期分区写入 Parquet
    # ============================================================
    print("[阶段 4] 按日期分区写入 Parquet...")
    t0 = time.time()

    df_with_date \
        .filter(col("publish_date").isNotNull()) \
        .select("raw_json", "publish_date") \
        .write \
        .partitionBy("publish_date") \
        .mode("overwrite") \
        .parquet(str(output_path))

    t_write = time.time() - t0
    print(f"        写入完成: {t_write:.2f}s")

    partitions = list(output_path.glob("publish_date=*"))
    total_size = sum(
        p.stat().st_size
        for p in output_path.rglob("*.parquet")
    )

    print(f"        分区数: {len(partitions)} 天")
    print(f"        总大小: {total_size / 1024 / 1024:.2f} MB\n")

    # ============================================================
    # 阶段 5：验证读取 + 数据完整性（简要）
    # ============================================================
    print("[阶段 5] 验证读取 + 数据完整性...")
    t0 = time.time()

    if partitions:
        sample_dates = sorted([p.name.replace("publish_date=", "")
                              for p in partitions])[:3]

        for date_str in sample_dates:
            part_path = output_path / f"publish_date={date_str}"
            part_df = spark.read.parquet(str(part_path))
            part_count = part_df.count()

            first_row = part_df.first()
            raw_json = first_row["raw_json"]
            print(f"        {date_str}: {part_count} 条, raw_json 长度 {len(raw_json)} 字符")

            try:
                parsed = json.loads(raw_json)
                keys = list(parsed.keys())
                print(f"          字段: {keys}")
            except Exception as e:
                print(f"          [ERROR] JSON 解析失败: {e}")
    else:
        print("        [WARN] 无分区可验证")

    t_verify = time.time() - t0
    print(f"        耗时: {t_verify:.2f}s\n")

    # ============================================================
    # 阶段 6：数据完整性验证（新增）
    # ============================================================
    integrity_pass = verify_integrity(
        spark, input_path, output_path, count, valid
    )

    # ============================================================
    # 汇总
    # ============================================================
    print()
    print("=" * 70)
    print("总体验证结果汇总")
    print("=" * 70)
    print(f"  总记录数:      {count}")
    print(f"  有效日期:      {valid}")
    print(f"  解析失败:      {invalid}")
    print(f"  时间跨度:      {min_date} ~ {max_date}")
    print(f"  分区数:        {len(partitions)} 天")
    print(f"  输出大小:      {total_size / 1024 / 1024:.2f} MB")
    print(f"  数据完整性:    {'✅ 无损' if integrity_pass else '❌ 有问题'}")
    print()
    print("  各阶段耗时:")
    print(f"    Spark 初始化:  {t_spark_init:.2f}s")
    print(f"    读取 JSONL:   {t_read:.2f}s")
    print(f"    提取日期:      {t_extract:.2f}s")
    print(f"    分析分布:      {t_analyze:.2f}s")
    print(f"    分区写入:      {t_write:.2f}s")
    print(f"    验证读取:      {t_verify:.2f}s")
    total_time = t_spark_init + t_read + t_extract + t_analyze + t_write + t_verify
    print(f"    ----------------------")
    print(f"    预处理耗时:    {total_time:.2f}s")
    print()
    print(f"  单条处理:      {total_time * 1000 / count:.2f} ms")
    print(f"  推算 228 万条: {total_time * 2_280_000 / count / 60:.1f} 分钟")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()