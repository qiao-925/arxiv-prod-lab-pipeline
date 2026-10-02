#!/usr/bin/env python3
"""
download_test_2000.py - 从 HuggingFace 下载 2000 条测试论文（JSONL）

用法（在项目根目录运行）：
    python tests/download_test_2000.py
    python tests/download_test_2000.py --count 5000
"""

import os

os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")

import argparse
import json
import time
from pathlib import Path

from datasets import load_dataset


DATASET_NAME = "ines-besrour/unarxive_2024"
DEFAULT_COUNT = 2000
SCRIPT_DIR = Path(__file__).parent
DEFAULT_OUTPUT = SCRIPT_DIR / "data" / "test_2000.jsonl"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT)
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"数据集: {DATASET_NAME}")
    print(f"下载条数: {args.count}")
    print(f"输出文件: {output_path}")
    print("=" * 60)

    print("[1/3] 加载数据集（streaming）...")
    t0 = time.time()
    dataset = load_dataset(DATASET_NAME, split="train", streaming=True)
    print(f"      加载耗时: {time.time() - t0:.2f}s\n")

    print(f"[2/3] 下载并写入 {output_path} ...")
    t1 = time.time()
    count = 0
    failed = 0
    total_bytes = 0

    # 注意：streaming 的 example 对应底层文件块，`jsonl` 字段可能含多行
    # （多个 paper 拼接）或空行，因此按「行」计数，一行一篇论文。
    with open(output_path, "w", encoding="utf-8") as f:
        done = False
        for example in dataset:
            raw = example.get("jsonl")
            if raw is None:
                failed += 1
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")

            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    json.loads(line)  # 校验合法 JSON，坏行丢弃
                except json.JSONDecodeError:
                    failed += 1
                    continue
                f.write(line + "\n")
                total_bytes += len(line.encode("utf-8"))
                count += 1
                if count % 200 == 0:
                    elapsed = time.time() - t1
                    rate = count / elapsed if elapsed > 0 else 0
                    print(f"      [{count:>5}/{args.count}] "
                          f"耗时 {elapsed:6.1f}s | "
                          f"速率 {rate:5.1f} 条/秒 | "
                          f"已下载 {total_bytes/1024/1024:.1f} MB")
                if count >= args.count:
                    done = True
                    break
            if done:
                break

    download_time = time.time() - t1

    print(f"\n[3/3] 完成")
    print("=" * 60)
    print(f"  下载条数:   {count}")
    print(f"  解析失败:   {failed}")
    print(f"  总大小:     {total_bytes / 1024 / 1024:.2f} MB")
    if count > 0:
        print(f"  平均条大小: {total_bytes / count / 1024:.2f} KB")
    print(f"  下载耗时:   {download_time:.1f}s")
    if download_time > 0:
        print(f"  平均速率:   {count / download_time:.1f} 条/秒")
    print(f"  输出文件:   {output_path}")
    print("=" * 60)

    print("\n[预览] 前 3 条数据的字段结构：")
    with open(output_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= 3:
                break
            paper = json.loads(line)
            print(f"\n  --- 第 {i+1} 条 ---")
            print(f"  paper_id:    {paper.get('paper_id')}")
            meta = paper.get("metadata", {})
            print(f"  title:       {str(meta.get('title', ''))[:60]}...")
            print(f"  categories:  {meta.get('categories')}")
            print(f"  update_date: {meta.get('update_date')}")
            print(f"  sections:    {len(paper.get('sections', {}))} 个章节")
            print(f"  bib_entries: {len(paper.get('bib_entries', {}))} 条引用")


if __name__ == "__main__":
    main()
