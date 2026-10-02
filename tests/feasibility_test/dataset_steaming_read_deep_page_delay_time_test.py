#!/usr/bin/env python3
"""
benchmark_skip.py - 测试 HuggingFace 流式数据集的 skip 成本

直接测试：跳过 N 条需要多久
用法：
    python benchmark_skip.py
"""

import os
os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")

import time
from datasets import load_dataset


DATASET = "ines-besrour/unarxive_2024"

# 测试点：跳过这些数量后，读 500 条的实际耗时
SKIP_TARGETS = [0, 10_000, 100_000, 500_000, 1_000_000, 2_000_000]
READ_COUNT = 500


def benchmark_skip(skip_n: int):
    """跳过 skip_n 条，然后读 READ_COUNT 条，测量耗时"""
    ds = load_dataset(DATASET, split="train", streaming=True)

    # 阶段 1：跳过
    t0 = time.time()
    count = 0
    for _ in ds:
        count += 1
        if count >= skip_n:
            break
    skip_time = time.time() - t0

    # 阶段 2：读 500 条
    t1 = time.time()
    read = 0
    for example in ds:
        read += 1
        if read >= READ_COUNT:
            break
    read_time = time.time() - t1

    total_time = time.time() - t0
    return skip_time, read_time, total_time


def main():
    print(f"数据集: {DATASET}")
    print(f"每次读 {READ_COUNT} 条，测试不同 skip 位置的耗时\n")

    print(f"{'跳过条数':>12} | {'skip 耗时':>10} | {'读 500 条':>10} | {'总耗时':>10}")
    print("-" * 55)

    for skip_n in SKIP_TARGETS:
        print(f"测试跳过 {skip_n:,} 条...", end="", flush=True)
        try:
            skip_time, read_time, total_time = benchmark_skip(skip_n)
            print(f"\r{skip_n:>12,} | {skip_time:>9.1f}s | {read_time:>9.1f}s | {total_time:>9.1f}s")
        except Exception as e:
            print(f"\r{skip_n:>12,} | 失败: {e}")


if __name__ == "__main__":
    main()