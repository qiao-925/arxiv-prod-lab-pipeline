# tests/smoke_config.py
"""冒烟测试：直接导入真实 config，检查字段是否加载成功。

用法：
    python tests/smoke_config.py              # 默认：值全部脱敏
    python tests/smoke_config.py --verbose    # 显式要求才显示真实值（慎用）
    python tests/smoke_config.py --keys-only  # 只列字段名
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config as cfg


def _preview(value, *, verbose: bool) -> str:
    """非 verbose 模式：只显示类型 + 长度，绝不显示值。"""
    if verbose:
        return repr(value)
    t = type(value).__name__
    if isinstance(value, str):
        return f"<str len={len(value)}>"
    return f"<{t}>"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true",
                        help="打印真实值（仅限本地、勿用于 CI）")
    parser.add_argument("--keys-only", action="store_true",
                        help="只列出字段名")
    args = parser.parse_args()

    if args.verbose and sys.stdout.isatty() is False:
        # 非交互（CI / 重定向）时拒绝 verbose，防意外落盘
        print("拒绝：--verbose 仅限交互终端使用", file=sys.stderr)
        return 2

    # 1) 导入成功
    print("[1/3] 模块导入成功")

    # 2) 字段齐全
    missing = [n for n in cfg._FIELDS if not hasattr(cfg, n)]
    if missing:
        print(f"[2/3] 失败：未注入字段 {missing}")
        return 1
    print(f"[2/3] {len(cfg._FIELDS)} 个字段全部注入")

    # 3) 类型正确
    type_errors = [
        f"{n}: 期望 {t.__name__}, 实际 {type(getattr(cfg, n)).__name__}"
        for n, t in cfg._FIELDS.items()
        if not isinstance(getattr(cfg, n), t)
    ]
    if type_errors:
        print("[3/3] 失败：类型不符")
        for e in type_errors:
            print(f"      - {e}")
        return 1
    print("[3/3] 类型全部正确")

    # 快照
    if not args.keys_only:
        print("-" * 60)
        for name in sorted(cfg._FIELDS):
            value = getattr(cfg, name)
            print(f"  {name:24} = {_preview(value, verbose=args.verbose)}")
        print("-" * 60)

    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())