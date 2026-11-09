#!/usr/bin/env python3
"""OpenAlex 引用榜手动任务 CLI。

职责：
    - 维护 openalex_rank_task，兼作数据集更新日期登记与执行账本
    - 自动读取 MAX(as_of_date)，执行 OpenAlex 排名 SQL
    - 每个 window_name 只保留 Top 500
    - 导出两个 Parquet 并同步到 Hugging Face Bucket
    - 提供 init / run / verify 三个纯 CLI 命令

触发方式：
    ENV=test python -m jobs.job_openalex_rank init
    ENV=test python -m jobs.job_openalex_rank run
    ENV=test python -m jobs.job_openalex_rank verify

人工流程：
    1. init 幂等创建任务表
    2. OpenAlex 数据更新后，向 openalex_rank_task 插入 as_of_date
    3. run 自动选择最新日期；completed 跳过，pending/failed/running 执行或重试

设计约束：
    - 不注册到 scheduler，不暴露 HTTP API
    - 同一 as_of_date 重跑只替换该版本，其他版本保留
    - HF 上传失败会标记 failed；重跑会重新生成并尝试发布
"""
import json
import os
import sys
from pathlib import Path
from typing import Dict

import pandas as pd

from common import config as cfg
from common.logger import setup_logger
from common.pg_client import PGClient
from common.rank_paths import DEFAULT_RANK_OUTPUT_DIR, GITHUB_RANK_FILES
from jobs.job_daily_github_rank import (
    _split_sql_statements,
    clean_output_dir,
    upload_hf,
)

SQL_FILE = Path(__file__).parent / "rank_sql" / "02_openalex_arxiv.sql"
LOCK_NAME = "openalex_rank_cli"

TASK_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS openalex_rank_task (
    as_of_date    DATE PRIMARY KEY,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    error_message TEXT,
    row_counts    JSONB
)
"""


def ensure_task_table(pg: PGClient) -> None:
    """幂等创建唯一控制表。"""
    pg.execute(TASK_TABLE_SQL)
    pg.execute("""
        CREATE INDEX IF NOT EXISTS idx_openalex_rank_task_status
        ON openalex_rank_task (status, as_of_date DESC)
    """)


def latest_task(pg: PGClient):
    """返回最新日期任务，允许调用方统一处理空表。"""
    return pg.fetch_one("""
        SELECT as_of_date, status
        FROM openalex_rank_task
        ORDER BY as_of_date DESC
        LIMIT 1
    """)


def current_pg_date(pg: PGClient):
    row = pg.fetch_one("SELECT CURRENT_DATE")
    if not row or row[0] is None:
        raise RuntimeError("无法读取 PG CURRENT_DATE")
    return row[0]


def mark_running(pg: PGClient, as_of_date) -> None:
    pg.execute("""
        UPDATE openalex_rank_task
        SET status = 'running',
            started_at = NOW(),
            completed_at = NULL,
            error_message = NULL,
            row_counts = NULL
        WHERE as_of_date = %s
    """, (as_of_date,))


def mark_completed(
    pg: PGClient, as_of_date, counts: Dict[str, int]
) -> None:
    pg.execute("""
        UPDATE openalex_rank_task
        SET status = 'completed',
            completed_at = NOW(),
            error_message = NULL,
            row_counts = %s::jsonb
        WHERE as_of_date = %s
    """, (json.dumps(counts, ensure_ascii=False), as_of_date))


def mark_failed(pg: PGClient, as_of_date, error: str) -> None:
    pg.execute("""
        UPDATE openalex_rank_task
        SET status = 'failed',
            completed_at = NULL,
            error_message = %s
        WHERE as_of_date = %s
    """, (error[:2000], as_of_date))


def acquire_lock(pg: PGClient, env: str, log) -> bool:
    """获取会话级 advisory lock，连接关闭时由 PG 自动释放。"""
    row = pg.fetch_one(
        "SELECT pg_try_advisory_lock(hashtext(%s)::bigint)",
        (f"{LOCK_NAME}-{env}",),
    )
    acquired = bool(row and row[0])
    if acquired:
        log.info("[openalex] 已获取执行锁")
    return acquired


def release_lock(pg: PGClient, env: str) -> None:
    pg.execute(
        "SELECT pg_advisory_unlock(hashtext(%s)::bigint)",
        (f"{LOCK_NAME}-{env}",),
    )


def execute_rank_sql(pg: PGClient, log) -> Dict[str, int]:
    """逐语句执行 SQL，并读取末尾两表行数结果集。"""
    statements = _split_sql_statements(SQL_FILE.read_text(encoding="utf-8"))
    if not pg.conn.autocommit:
        pg.conn.commit()

    old_autocommit = pg.conn.autocommit
    try:
        pg.conn.autocommit = True
        counts: Dict[str, int] = {}
        with pg.conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
                if cur.description is not None:
                    for row in cur.fetchall():
                        if len(row) >= 2:
                            counts[str(row[0])] = int(row[1] or 0)
    except Exception:
        pg.conn.rollback()
        raise
    finally:
        pg.conn.autocommit = old_autocommit

    log.info(f"[openalex] 两表行数: {counts}")
    return counts


def export_outputs(pg: PGClient, as_of_date, out: Path, log) -> None:
    """导出最新 OpenAlex 版本的两个固定文件名 Parquet。"""
    ranking = pd.read_sql("""
        SELECT as_of_date, window_name, rank, arxiv_id, title, cited_by_count
        FROM openalex_ranking_top500
        WHERE as_of_date = %s
        ORDER BY window_name, rank
    """, pg.conn, params=(as_of_date,))
    ranking.to_parquet(out / "openalex_ranking_top500.parquet", index=False)

    details = pd.read_sql("""
        SELECT *
        FROM paper_openalex_details
        WHERE as_of_date = %s
        ORDER BY cited_by_count DESC, arxiv_id
    """, pg.conn, params=(as_of_date,))
    details.to_parquet(out / "paper_openalex_details.parquet", index=False)

    if ranking.empty or details.empty:
        raise RuntimeError(
            f"OpenAlex 导出为空: ranking={len(ranking)}, details={len(details)}"
        )
    log.info(
        f"[openalex] 导出 ranking={len(ranking)}，details={len(details)}"
    )


def publish_outputs(out: Path, log) -> None:
    """发布两个 Parquet；只有显式跳过时才允许不配置 HF Bucket。"""
    if os.getenv("OPENALEX_RANK_SKIP_UPLOAD") == "1":
        log.warning("[openalex] OPENALEX_RANK_SKIP_UPLOAD=1，跳过 HF 上传")
        return
    if not (cfg.HF_BUCKET_PATH or "").strip():
        raise RuntimeError("HF_BUCKET_PATH 未配置，拒绝将任务标记为完成")
    upload_hf(out, log)


def init_task(log) -> int:
    pg = PGClient(log)
    try:
        ensure_task_table(pg)
        log.info("[openalex] openalex_rank_task 已就绪")
        log.info(
            "[openalex] 数据更新后执行: INSERT INTO openalex_rank_task "
            "(as_of_date) VALUES (DATE 'YYYY-MM-DD');"
        )
        return 0
    finally:
        pg.close()


def run(env: str, log) -> int:
    pg = PGClient(log)
    as_of_date = None
    locked = False
    try:
        ensure_task_table(pg)
        locked = acquire_lock(pg, env, log)
        if not locked:
            raise RuntimeError("已有 OpenAlex 排名任务正在执行")

        task = latest_task(pg)
        if task is None:
            raise RuntimeError(
                "openalex_rank_task 为空；请先插入最新 as_of_date"
            )
        as_of_date, status = task
        if status == "completed":
            log.info(f"[openalex] 最新日期 {as_of_date} 已完成，跳过")
            return 0
        if as_of_date > current_pg_date(pg):
            raise RuntimeError(
                f"as_of_date={as_of_date} 晚于 PG CURRENT_DATE"
            )

        mark_running(pg, as_of_date)
        out = Path(
            os.getenv("RANK_OUTPUT_DIR")
            or os.getenv("OPENALEX_RANK_OUTPUT_DIR")
            or DEFAULT_RANK_OUTPUT_DIR
        ).resolve()
        clean_output_dir(out, log, keep_files=GITHUB_RANK_FILES)
        out.mkdir(parents=True, exist_ok=True)

        log.info(f"[openalex] 开始构建 {as_of_date}")
        counts = execute_rank_sql(pg, log)
        export_outputs(pg, as_of_date, out, log)
        publish_outputs(out, log)
        mark_completed(pg, as_of_date, counts)
        log.info(f"[openalex] {as_of_date} 完成")
        return 0
    except Exception as exc:
        log.error(
            f"[openalex] 执行失败: {type(exc).__name__}: {exc}"
        )
        if as_of_date is not None:
            try:
                mark_failed(pg, as_of_date, str(exc))
            except Exception:
                log.exception("[openalex] 写入失败状态时再次出错")
        raise
    finally:
        if locked:
            try:
                release_lock(pg, env)
            except Exception:
                log.warning("[openalex] 释放执行锁失败，连接关闭后自动释放")
        pg.close()


def verify(log) -> int:
    pg = PGClient(log)
    try:
        ensure_task_table(pg)
        task = latest_task(pg)
        if task is None:
            raise RuntimeError("openalex_rank_task 为空")
        as_of_date, status = task

        ranking_count, window_count, max_rank = pg.fetch_one("""
            SELECT COUNT(*), COUNT(DISTINCT window_name), COALESCE(MAX(rank), 0)
            FROM openalex_ranking_top500
            WHERE as_of_date = %s
        """, (as_of_date,))
        details_count = pg.fetch_one("""
            SELECT COUNT(*) FROM paper_openalex_details
            WHERE as_of_date = %s
        """, (as_of_date,))[0]
        bad_windows = pg.fetch_all("""
            SELECT window_name, COUNT(*), MAX(rank)
            FROM openalex_ranking_top500
            WHERE as_of_date = %s
            GROUP BY window_name
            HAVING COUNT(*) > 500 OR COUNT(*) <> MAX(rank)
            ORDER BY window_name
        """, (as_of_date,))
        missing_details = pg.fetch_one("""
            SELECT COUNT(DISTINCT r.arxiv_id)
            FROM openalex_ranking_top500 r
            LEFT JOIN paper_openalex_details d
              ON d.as_of_date = r.as_of_date AND d.arxiv_id = r.arxiv_id
            WHERE r.as_of_date = %s AND d.arxiv_id IS NULL
        """, (as_of_date,))[0]

        log.info(
            f"[openalex verify] date={as_of_date} status={status} "
            f"ranking={ranking_count} windows={window_count} "
            f"max_rank={max_rank} details={details_count}"
        )
        errors = []
        if ranking_count == 0:
            errors.append("ranking 为空")
        if max_rank > 500:
            errors.append("存在 rank > 500")
        if bad_windows:
            errors.append(f"异常窗口: {bad_windows}")
        if missing_details:
            errors.append(f"缺失详情: {missing_details}")
        if status != "completed":
            errors.append(f"任务状态为 {status}")
        if errors:
            for error in errors:
                log.error(f"[openalex verify] {error}")
            return 1
        log.info("[openalex verify] 通过")
        return 0
    finally:
        pg.close()


def main(argv=None) -> int:
    env = os.environ.get("ENV")
    if env not in ("test", "prod"):
        raise RuntimeError(f"ENV 必须是 test/prod，当前: {env!r}")

    log = setup_logger("openalex_rank")
    args = list(sys.argv[1:] if argv is None else argv)
    cmd = args[0] if args else "run"
    if len(args) > 1:
        log.error("命令不接受额外参数；执行日期请写入 openalex_rank_task")
        return 2
    if cmd == "init":
        return init_task(log)
    if cmd == "run":
        return run(env, log)
    if cmd == "verify":
        return verify(log)
    log.error(f"未知命令: {cmd}；可用命令: init / run / verify")
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(1)
