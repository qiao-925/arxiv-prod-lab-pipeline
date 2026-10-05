# common/pg_client.py
"""PostgreSQL 连接封装（无业务语义，各业务包共享）

职责：
    - 连接管理：connect / reconnect / close / health_check
    - 查询执行：execute / fetch_one / fetch_all
    - 事务：cursor() 上下文管理器

不含任何业务表逻辑——业务 SQL 由调用方（job）自己写。
连接参数取自 common.config；库名按 ENV 派生（test → arxiv_test）。
"""
import os
from contextlib import contextmanager
from typing import Iterable, Optional

import psycopg2

from common import config as cfg


class PGClient:
    def __init__(self, logger):
        self.log = logger
        self.conn = None
        self._connect()

    # ---- 连接管理 ----
    def _connect(self):
        self.log.info(f"连接 PostgreSQL: {cfg.PG_HOST}:{cfg.PG_PORT}/{cfg.PG_DB}")
        self.conn = psycopg2.connect(
            host=cfg.PG_HOST,
            port=cfg.PG_PORT,
            user=cfg.PG_USER,
            password=cfg.PG_PASSWORD,
            dbname=cfg.PG_DB,
        )
        self.log.info("PostgreSQL 连接就绪")

    def health_check(self) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    def ensure_alive(self):
        if not self.health_check():
            self.log.warning("PostgreSQL 连接断开，重连...")
            self.reconnect()

    def reconnect(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        self._connect()

    def close(self):
        if self.conn:
            self.conn.close()
            self.log.info("PostgreSQL 连接已关闭")

    # ---- 查询执行 ----
    @contextmanager
    def cursor(self, commit: bool = True):
        """上下文管理器：正常则 commit，异常则 rollback。"""
        self.ensure_alive()
        cur = self.conn.cursor()
        try:
            yield cur
            if commit:
                self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def execute(self, sql: str, params: Iterable = None) -> int:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def fetch_one(self, sql: str, params: Iterable = None) -> Optional[tuple]:
        with self.cursor(commit=False) as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def fetch_all(self, sql: str, params: Iterable = None) -> list:
        with self.cursor(commit=False) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    # ---- with 语法支持 ----
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()