# common/minio_client.py
"""MinIO 对象存储封装（无业务语义，各业务包共享）

职责：
    - 连接管理：构造官方 minio.Minio 客户端、bucket 检查
    - 对象读写：put_jsonl / get_jsonl / upsert_jsonl / list_objects

注意：
    - 使用 split("\\n") 而非 splitlines()，避免在 Unicode 行边界符
      （\\u2028、\\u2029、\\x85 等）上错误切分 JSON 行
    - 连接参数统一取自 common.config
"""
import io
import json
from typing import Any, Dict, List

from minio import Minio

from common import config as cfg


class MinIOClient:
    def __init__(self, logger):
        self.log = logger
        self.log.info(f"连接 MinIO: {cfg.MINIO_ENDPOINT}")
        self.bucket = cfg.MINIO_BUCKET
        self.client = Minio(
            cfg.MINIO_ENDPOINT,
            access_key=cfg.MINIO_ACCESS_KEY,
            secret_key=cfg.MINIO_SECRET_KEY,
            secure=False,
        )
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            self.log.info(f"创建 bucket: {self.bucket}")
        self.log.info("MinIO 客户端就绪")

    # ---- 对象读写 ----
    def put_jsonl(self, object_name: str, records: List[Dict[str, Any]]) -> int:
        """写 JSONL 到 MinIO（覆盖），返回记录数。"""
        body = "\n".join(
            json.dumps(r, ensure_ascii=False, default=str) for r in records
        )
        data = body.encode("utf-8")
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/x-ndjson",
        )
        self.log.info(f"写入 MinIO: {self.bucket}/{object_name} ({len(records)} 条)")
        return len(records)

    def get_jsonl(self, object_name: str) -> List[Dict[str, Any]]:
        """从 MinIO 读 JSONL；对象不存在则返回空列表。

        注意：用 split("\\n") 而不是 splitlines()，
        避免在 \\u2028、\\u2029、\\x85 等 Unicode 行边界符上错误切分。
        """
        try:
            resp = self.client.get_object(self.bucket, object_name)
        except Exception:
            return []
        try:
            data = resp.read().decode("utf-8")
        finally:
            resp.close()
            resp.release_conn()

        result = []
        for line in data.split("\n"):
            line = line.strip()
            if not line:
                continue
            result.append(json.loads(line))
        return result

    def upsert_jsonl(
        self,
        object_name: str,
        records: List[Dict[str, Any]],
        key: str = "arxiv_id",
    ) -> int:
        """读已有 + 按 key 合并 + 覆盖写。"""
        existing = self.get_jsonl(object_name)

        merged: Dict[str, Dict[str, Any]] = {
            r[key]: r for r in existing if r.get(key)
        }
        for r in records:
            if r.get(key):
                merged[r[key]] = r

        return self.put_jsonl(object_name, list(merged.values()))

    def list_objects(self, prefix: str) -> List[str]:
        """列出指定前缀下的对象名。"""
        return [
            obj.object_name
            for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
        ]


# ---- 路径工具 ----
def papers_arxiv_oai_state_path(published_year: str, month: str) -> str:
    """按 published_date 年月分区的状态文件路径（每篇论文只存一份最新）。"""
    return f"papers/arxiv_oai/published_year={published_year}/month={month}/state.jsonl"
