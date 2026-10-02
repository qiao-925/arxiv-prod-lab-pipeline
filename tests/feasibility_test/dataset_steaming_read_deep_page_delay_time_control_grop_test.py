#!/usr/bin/env python3
"""
benchmark_state_dict.py - 测试 state_dict/load_state_dict 的恢复成本
对照组：与 benchmark_skip.py 的 skip 成本对比

模拟场景：周期性批次任务
    运行1：处理 N 条 → 保存 state_dict
    运行2：load_state_dict → 继续处理 → 测量恢复耗时
"""

import os
os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")

import argparse
import time
from datasets import load_dataset


DATASET = "ines-besrour/unarxive_2024"


def measure_restore(state_dict):
    """测量从 state_dict 恢复所需的耗时"""
    ds = load_dataset(DATASET, split="train", streaming=True)

    t0 = time.time()
    ds.load_state_dict(state_dict)
    restore_time = time.time() - t0

    # 读取下一条，验证恢复位置正确
    first_record = None
    for example in ds:
        first_record = example
        break

    return restore_time, first_record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoints", type=int, nargs="+",
                        default=[10_000, 100_000, 500_000],
                        help="保存 checkpoint 的位置（处理完 N 条后保存）")
    args = parser.parse_args()

    print(f"数据集: {DATASET}")
    print(f"测试 state_dict/load_state_dict 恢复成本\n")
    print(f"{'checkpoint位置':>15} | {'保存耗时':>10} | {'恢复耗时':>10} | {'读500条':>10} | {'总耗时':>10}")
    print("-" * 75)

    for checkpoint_at in args.checkpoints:
        # ============================================================
        # 阶段 1：处理到 checkpoint 位置，保存 state_dict
        # ============================================================
        print(f"处理到 {checkpoint_at:,} 条并保存 state_dict...", end="", flush=True)

        ds = load_dataset(DATASET, split="train", streaming=True)
        count = 0
        t0 = time.time()

        for example in ds:
            count += 1
            if count >= checkpoint_at:
                break

        state_dict = ds.state_dict()
        save_time = time.time() - t0

        # ============================================================
        # 阶段 2：load_state_dict 恢复，测量恢复耗时
        # ============================================================
        restore_time, _ = measure_restore(state_dict)

        # ============================================================
        # 阶段 3：从恢复位置读取 500 条，测量读取耗时
        # ============================================================
        ds2 = load_dataset(DATASET, split="train", streaming=True)
        ds2.load_state_dict(state_dict)

        t2 = time.time()
        read = 0
        for example in ds2:
            read += 1
            if read >= 500:
                break
        read_time = time.time() - t2

        total_restore = restore_time + read_time

        print(
            f"\r{checkpoint_at:>15,} | "
            f"{save_time:>9.1f}s | "
            f"{restore_time:>9.4f}s | "
            f"{read_time:>9.1f}s | "
            f"{total_restore:>9.1f}s"
        )

    # ============================================================
    # 对比总结
    # ============================================================
    print(f"\n{'='*75}")
    print(f"对比总结")
    print(f"{'='*75}")
    print(f"  方案 A（纯 skip）:  跳过 N 条 = O(N)，耗时与 N 成正比")
    print(f"  方案 B（state_dict）: 恢复 = 跳过已读 shard + 当前 shard 内逐条跳过")
    print(f"  优势: 跨 shard 跳过是常数时间，只有当前 shard 内是线性的")
    print(f"  限制: 如果数据集只有 1 个 shard，恢复仍需线性跳过")
    print(f"{'='*75}")


if __name__ == "__main__":
    main()