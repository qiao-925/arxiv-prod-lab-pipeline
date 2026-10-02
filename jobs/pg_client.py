"""PostgreSQL 操作：批次记录、断点续传、健康检查"""
import hashlib
import json
from typing import List

import psycopg2

from .config import Config


class PGClient:
    def __init__(self, logger):
        self.log = logger
        self.conn = None
        self._connect()
        self._init_table()

    def _connect(self):
        self.conn = psycopg2.connect(
            host=Config.PG_HOST,
            port=Config.PG_PORT,
            dbname=Config.PG_DB,
            user=Config.PG_USER,
            password=Config.PG_PASSWORD,
        )

    def reconnect(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        self._connect()
        self.log.info("PostgreSQL 重连成功")

    def _init_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_batches (
                    batch_id        UUID PRIMARY KEY,
                    job_name        VARCHAR(50) NOT NULL,
                    source_offset   BIGINT NOT NULL,
                    source_end      BIGINT,
                    record_count    INT DEFAULT 0,
                    status          VARCHAR(20) DEFAULT 'PENDING',
                    checksum        VARCHAR(64),
                    error_message   TEXT,
                    created_at      TIMESTAMP DEFAULT NOW(),
                    committed_at    TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_ingestion_batches_status
                ON ingestion_batches (job_name, status);
            """)
        self.conn.commit()
        self.log.info("ingestion_batches 表就绪")

    def health_check(self) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    def ensure_alive(self):
        if not self.health_check():
            self.log.warning("PostgreSQL 连接断开，正在重连...")
            self.reconnect()

    def get_last_committed_offset(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT source_end FROM ingestion_batches
                WHERE job_name = %s AND status = 'COMMITTED'
                ORDER BY source_end DESC LIMIT 1
            """, (Config.JOB_NAME,))
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else 0

    def create_batch(self, batch_id: str, source_offset: int):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ingestion_batches (batch_id, job_name, source_offset, status)
                VALUES (%s, %s, %s, 'PENDING')
                ON CONFLICT (batch_id) DO NOTHING
            """, (batch_id, Config.JOB_NAME, source_offset))
        self.conn.commit()

    def commit_batch(self, batch_id: str, source_end: int,
                     record_count: int, checksum: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE ingestion_batches
                SET status = 'COMMITTED', source_end = %s,
                    record_count = %s, checksum = %s, committed_at = NOW()
                WHERE batch_id = %s
            """, (source_end, record_count, checksum, batch_id))
        self.conn.commit()

    def fail_batch(self, batch_id: str, error_msg: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE ingestion_batches
                SET status = 'FAILED', error_message = %s
                WHERE batch_id = %s
            """, (error_msg[:500], batch_id))
        self.conn.commit()

    @staticmethod
    def compute_checksum(records: List[dict]) -> str:
        h = hashlib.sha256()
        for rec in records:
            h.update(json.dumps(rec, sort_keys=True, ensure_ascii=False).encode("utf-8"))
        return h.hexdigest()

    def close(self):
        if self.conn:
            self.conn.close()
