# common/paths.py
"""项目路径常量。全项目唯一计算 ROOT 的地方。"""
import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


# 派生路径
DATA_RAW = ROOT / os.getenv("DATA_RAW",str(ROOT / "data/raw"))
DATA_SORTED = ROOT / os.getenv("DATA_SORTED",str(ROOT / "/data/sorted"))


