"""scheduler 集成测试的 dummy 模块。

支持：
    python -m tests.integration.dummy_job               → 打印 ok，退出 0
    python -m tests.integration.dummy_job --fail        → 退出 1
    python -m tests.integration.dummy_job --sleep N     → 睡 N 秒，退出 0
"""
import sys
import time


def main():
    args = sys.argv[1:]
    if "--fail" in args:
        sys.exit(1)
    if "--sleep" in args:
        idx = args.index("--sleep")
        secs = int(args[idx + 1]) if idx + 1 < len(args) else 5
        time.sleep(secs)
    print("ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
