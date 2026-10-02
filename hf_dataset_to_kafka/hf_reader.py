"""HuggingFace 数据集读取与解析"""
import json
import os

from datasets import load_dataset, load_dataset_builder

from .config import Config


class HFReader:
    def __init__(self, logger):
        self.log = logger
        os.environ["HF_ENDPOINT"] = Config.HF_ENDPOINT
        self.dataset_name = Config.HF_DATASET_NAME

    def preflight(self) -> str:
        self.log.info(f"预检数据集: {self.dataset_name}")
        try:
            builder = load_dataset_builder(self.dataset_name)
            features = builder.info.features
        except Exception as e:
            self.log.warning(f"元数据获取失败（忽略，流式仍可用）: {e}")
            features = None
        if features:
            self.log.info(f"特征字段: {list(features.keys())}")
        else:
            self.log.info("特征字段: 无（viewer 禁用 / tar.gz 打包），按 raw jsonl 逐行解析")
        return "raw"

    def stream(self):
        """流式读取，逐条产出论文 JSON 行。

        unarXive 2024 是 tar.gz 打包，streaming example 的 `jsonl` 字段
        可能包含多行（多个 paper 拼接）或空行，需按行拆开逐条 yield。
        """
        dataset = load_dataset(self.dataset_name, split="train", streaming=True)
        for example in dataset:
            raw = example.get("jsonl")
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            for line in raw.splitlines():
                line = line.strip()
                if line:
                    yield line

    @staticmethod
    def parse(raw):
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
