#!/usr/bin/env python3
"""download.py - 从 HuggingFace 下载 arXiv 数据为本地 JSONL

「数据准备」第一步：把 unarxive_2024 流式下载为本地 JSONL，
供 tools/preprocess.py 按发布日期分区。

用法：
    python tools/download.py --output data/raw --limit 2000   # 测试量
    python tools/download.py --output data/raw                # 全量

缓存：完成会写 _download_completed 标记，重复运行自动跳过（--force 强制重下）。
注意：streaming example 的 `jsonl` 字段是底层文件块，可能含多行（多个 paper
拼接，约 37%）或空行，故按「行」计数、逐行 JSON 校验后写入。
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import HF_DATASET_NAME, HF_ENDPOINT, get_data_raw, init_env

# 必须在导入 datasets 之前设置镜像端点
os.environ.setdefault("HF_ENDPOINT", HF_ENDPOINT)

from datasets import load_dataset


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

    return True


def _save_progress(output_dir: Path, total_count: int, completed: bool):
    progress = {
        "total_count": total_count,
        "completed": completed,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open(output_dir / "_progress.json", "w") as f:
        json.dump(progress, f, indent=2)

    if completed:
        (output_dir / "_download_completed").touch()


def download(output_dir: str, limit: int = 0, force: bool = False):
    """从 HuggingFace 下载 arXiv 论文到本地 JSONL。

    Args:
        output_dir: 输出目录
        limit: 下载论文条数（按行计），0 表示全量
        force: 为 True 时忽略缓存标记，强制重新下载
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    log = lambda msg: print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    print("=" * 70)
    print("HuggingFace 数据下载")
    print("=" * 70)
    print(f"数据集:   {HF_DATASET_NAME}")
    print(f"镜像:     {os.environ['HF_ENDPOINT']}")
    print(f"输出:     {output_path}")
    print(f"条数限制: {limit if limit > 0 else '全量'}")
    print()

    if not force and _is_download_completed(output_path, limit):
        log("✅ 下载已完成，跳过")
        return

    if limit > 0:
        output_file = output_path / f"test_{limit}.jsonl"
    else:
        output_file = output_path / "full.jsonl"

    log(f"输出文件: {output_file}")
    log("开始下载...")

    t0 = time.time()
    dataset = load_dataset(HF_DATASET_NAME, split="train", streaming=True)

    count = 0
    failed = 0
    total_bytes = 0
    done = False

    with open(output_file, "w", encoding="utf-8") as f:
        for example in dataset:
            raw = example.get("jsonl")
            if raw is None:
                failed += 1
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")

            # 一个 example 可能含多行（多个 paper 拼接）或空行，逐行处理
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

                if count % 500 == 0:
                    elapsed = time.time() - t0
                    rate = count / elapsed if elapsed > 0 else 0
                    log(f"  进度: {count} 条, "
                        f"{total_bytes/1024/1024:.1f} MB, {rate:.0f} 条/秒")

                if limit > 0 and count >= limit:
                    done = True
                    break
            if done:
                break

    elapsed = time.time() - t0
    _save_progress(output_path, count, completed=True)

    log("")
    log(f"✅ 下载完成: {count} 条, {total_bytes/1024/1024:.2f} MB, "
        f"失败 {failed} 条, 耗时 {elapsed:.0f}s")
    log(f"   输出文件: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="从 HuggingFace 下载 arXiv 数据为 JSONL")
    parser.add_argument("--env", choices=["test", "prod"], default=None,
                        help="环境，决定 --output 默认目录（test→data/raw_test, prod→data/raw）")
    parser.add_argument("--output", type=str, default=None,
                        help="输出目录（默认按 --env 派生）")
    parser.add_argument("--limit", type=int, default=0,
                        help="下载条数，0 表示全量（默认 0）")
    parser.add_argument("--force", action="store_true",
                        help="忽略缓存标记，强制重新下载")
    args = parser.parse_args()

    if args.env:
        init_env(args.env)
    output = args.output or str(get_data_raw())  # get_data_raw 要求已声明环境

    download(output_dir=output, limit=args.limit, force=args.force)


if __name__ == "__main__":
    main()
