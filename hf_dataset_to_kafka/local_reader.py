"""本地 JSONL 测试数据读取"""
import json
from pathlib import Path

from .config import Config


class LocalReader:
    def __init__(self, logger):
        self.log = logger
        self.path = Path(Config.LOCAL_DATA_PATH)

    def preflight(self) -> str:
        self.log.info(f"使用本地测试数据: {self.path}")
        if not self.path.exists():
            raise FileNotFoundError(
                f"测试数据不存在: {self.path}\n"
                f"请先运行: python tests/download_test_2000.py"
            )
        return "local"

    def stream(self):
        """逐条读取本地 JSONL（一行一篇论文）"""
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line

    @staticmethod
    def parse(raw):
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
