#!/usr/bin/env python3
"""
test_hf_read.py - 测试从 HuggingFace 流式读取 unarxive_2024
"""

import os
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

import json
import sys
from datasets import load_dataset

DATASET_NAME = "ines-besrour/unarxive_2024"
SAMPLE_COUNT = 10
BATCH_SIZE = 5

from datasets import get_dataset_config_names
print(get_dataset_config_names("ines-besrour/unarxive_2024"))

def parse_example(example):
    """将原始 jsonl 字节流解析为真正的论文对象"""
    raw = example.get("jsonl")
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return json.loads(raw)
    except Exception as e:
        print(f"[WARN] JSON 解析失败: {e}")
        return None


def main():
    print(f"[INFO] 使用镜像: {os.environ['HF_ENDPOINT']}")
    print(f"[INFO] 开始加载数据集: {DATASET_NAME}\n")

    try:
        dataset = load_dataset(DATASET_NAME, split="train", streaming=True)
        print("[OK] 数据集加载成功\n")
    except Exception as e:
        print(f"[ERROR] 数据集加载失败: {e}")
        sys.exit(1)

    # ============================================================
    # 逐条读取
    # ============================================================
    count = 0
    for example in dataset:
        paper = parse_example(example)
        if paper is None:
            continue

        count += 1
        print(f"{'='*60}")
        print(f"[样本 {count}]")
        print(f"{'='*60}")

        print(f"顶层字段: {list(paper.keys())}")
        print(f"paper_id: {paper.get('paper_id')}")

        meta = paper.get("metadata", {})
        print(f"  title:       {str(meta.get('title', ''))[:80]}...")
        print(f"  categories:  {meta.get('categories')}")
        print(f"  update_date: {meta.get('update_date')}")
        print(f"  authors_parsed 数量: {len(meta.get('authors_parsed', []))}")
        print(f"  versions 数量: {len(meta.get('versions', []))}")

        abstract = paper.get("abstract", {})
        abstract_text = abstract.get("text", "") if isinstance(abstract, dict) else ""
        print(f"  abstract 长度: {len(abstract_text)} 字符")

        sections = paper.get("sections", {})
        print(f"  sections 章节数: {len(sections)}")
        if sections:
            print(f"  sections 章节名: {list(sections.keys())[:5]}")

        bib = paper.get("bib_entries", {})
        print(f"  bib_entries 数量: {len(bib)}")

        refs = paper.get("ref_entries", {})
        print(f"  ref_entries 数量: {len(refs)}")

        size_kb = len(json.dumps(paper, ensure_ascii=False).encode('utf-8')) / 1024
        print(f"  单条 JSON 大小: {size_kb:.2f} KB\n")

        if count >= SAMPLE_COUNT:
            break

    print(f"{'='*60}")
    print(f"[DONE] 共读取 {count} 条，数据读取正常")
    print(f"{'='*60}")

    # ============================================================
    # 批量读取测试
    # ============================================================
    print(f"\n[INFO] 测试批量读取模式 (batch_size={BATCH_SIZE})...")
    dataset = load_dataset(DATASET_NAME, split="train", streaming=True)

    batch_count = 0
    total_in_batch = 0
    for batch in dataset.batch(batch_size=BATCH_SIZE):
        batch_count += 1
        num = len(batch["jsonl"])
        total_in_batch += num
        print(f"[批次 {batch_count}] 本批 {num} 条")
        if batch_count >= 3:
            break

    print(f"\n[DONE] 批量读取正常，共 {batch_count} 批，{total_in_batch} 条")


if __name__ == "__main__":
    main()