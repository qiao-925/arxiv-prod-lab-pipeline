#!/usr/bin/env python3
"""GitHub 排名每日快照 Job

职责：
    - 每日一次执行 jobs/rank_sql/01_github_arxiv.sql，覆盖生成：
      ranking_top500 / top500_arxiv_repo / paper_details / repo_details
    - 用 github_rank_run_log 记账：running / completed / failed，
      当天 completed 后整点轮巡直接跳过，失败日期下轮重试
    - 从四张 PG 结果表按原结构各导出一个 parquet，与数据库完全镜像：
      ranking_top500 / top500_arxiv_repo / paper_details / repo_details
    - 上传 Hugging Face Bucket（HF_BUCKET_PATH），使用独立 hf CLI
      （uv tool install hf；hf sync <dir> <bucket>）供 HF Space 前端读取

用法：
    ENV=test python -m jobs.job_daily_github_rank run
    ENV=test python -m jobs.job_daily_github_rank verify

快照语义：
    snapshot_date 取 PG CURRENT_DATE，与 SQL 内部一致；PG 四表保留当天快照，
    历史留存由 github_rank_run_log 账本记录。
"""
import json
import os
import re
import select
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict

import pandas as pd

from common import config as cfg
from common.logger import setup_logger
from common.pg_client import PGClient
from common.rank_paths import DEFAULT_RANK_OUTPUT_DIR, OPENALEX_RANK_FILES

SQL_FILE = Path(__file__).parent / "rank_sql" / "01_github_arxiv.sql"
METRICS = ("star", "fork", "issue", "pr")
DEFAULT_WINDOW = "d14"
DEFAULT_METRIC = "star"
UPLOAD_TIMEOUT_SECONDS = 1800
_DOLLAR_QUOTE_RE = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")
def job_name(env: str) -> str:
    return f"github-rank-{env}"


def current_pg_date(pg: PGClient):
    row = pg.fetch_one("SELECT CURRENT_DATE")
    if not row or row[0] is None:
        raise RuntimeError("无法读取 PG CURRENT_DATE")
    return row[0]


# ═══════════════════════════════════════════════════════════════════════
# 账本表：github_rank_run_log
# ═══════════════════════════════════════════════════════════════════════

def ensure_table(pg: PGClient) -> None:
    """幂等创建排名账本表。"""
    pg.execute("""
        CREATE TABLE IF NOT EXISTS github_rank_run_log (
            job_name      VARCHAR(64) NOT NULL,
            snapshot_date DATE NOT NULL,
            status        VARCHAR(20) NOT NULL,
            started_at    TIMESTAMP DEFAULT NOW(),
            committed_at  TIMESTAMP,
            error_message TEXT,
            row_counts    JSONB,
            PRIMARY KEY (job_name, snapshot_date)
        )
    """)
    pg.execute("""
        CREATE INDEX IF NOT EXISTS idx_github_rank_run_log_status
        ON github_rank_run_log (job_name, status)
    """)


def next_snapshot_date(pg: PGClient, job_name_key: str, target, log=None):
    """水位选择：running > failed > 当天 completed 则 None。"""
    row = pg.fetch_one("""
        SELECT snapshot_date FROM github_rank_run_log
        WHERE job_name = %s AND status = 'running'
        ORDER BY snapshot_date DESC LIMIT 1
    """, (job_name_key,))
    if row:
        if log:
            log.info(f"[rank] 恢复未完成快照 {row[0]}")
        return row[0]

    row = pg.fetch_one("""
        SELECT snapshot_date FROM github_rank_run_log
        WHERE job_name = %s AND status = 'failed'
        ORDER BY snapshot_date DESC, started_at DESC LIMIT 1
    """, (job_name_key,))
    if row:
        if log:
            log.warning(f"[rank] 重试失败快照 {row[0]}")
        return row[0]

    row = pg.fetch_one("""
        SELECT MAX(snapshot_date) FROM github_rank_run_log
        WHERE job_name = %s AND status = 'completed'
    """, (job_name_key,))
    if row and row[0] and row[0] >= target:
        if log:
            log.info(f"[rank] 当天 {target} 已完成，跳过")
        return None
    return target


def mark_running(pg: PGClient, job_name_key: str, snapshot_date) -> None:
    pg.execute("""
        INSERT INTO github_rank_run_log (job_name, snapshot_date, status, started_at)
        VALUES (%s, %s, 'running', NOW())
        ON CONFLICT (job_name, snapshot_date) DO UPDATE
        SET status = 'running', started_at = NOW(), error_message = NULL
    """, (job_name_key, snapshot_date))


def mark_completed(pg: PGClient, job_name_key: str, snapshot_date, counts: Dict[str, int]) -> None:
    pg.execute("""
        INSERT INTO github_rank_run_log (job_name, snapshot_date, status, row_counts, committed_at)
        VALUES (%s, %s, 'completed', %s::jsonb, NOW())
        ON CONFLICT (job_name, snapshot_date) DO UPDATE
        SET status = 'completed',
            row_counts = EXCLUDED.row_counts,
            committed_at = NOW(),
            error_message = NULL
    """, (job_name_key, snapshot_date, json.dumps(counts, ensure_ascii=False)))


def mark_failed(pg: PGClient, job_name_key: str, snapshot_date, error: str) -> None:
    pg.execute("""
        INSERT INTO github_rank_run_log (job_name, snapshot_date, status, error_message)
        VALUES (%s, %s, 'failed', %s)
        ON CONFLICT (job_name, snapshot_date) DO UPDATE
        SET status = 'failed', error_message = EXCLUDED.error_message
    """, (job_name_key, snapshot_date, error[:500]))


# ═══════════════════════════════════════════════════════════════════════
# SQL 执行
# ═══════════════════════════════════════════════════════════════════════

def _split_sql_statements(script: str) -> list[str]:
    """按顶层分号拆分 SQL，跳过 -- 注释和 $tag$ 引用块。"""
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(script)
    dollar_tag = None
    in_single = in_double = in_line_comment = False

    while i < n:
        c = script[i]

        if in_line_comment:
            if c == "\n":
                in_line_comment = False
                buf.append("\n")
            i += 1
            continue

        if dollar_tag:
            end = script.find(dollar_tag, i)
            if end == -1:
                buf.append(script[i:])
                i = n
                continue
            buf.append(script[i:end + len(dollar_tag)])
            i = end + len(dollar_tag)
            dollar_tag = None
            continue

        if in_single:
            buf.append(c)
            if c == "'":
                if i + 1 < n and script[i + 1] == "'":
                    buf.append("'")
                    i += 2
                    continue
                in_single = False
            i += 1
            continue

        if in_double:
            buf.append(c)
            if c == '"':
                if i + 1 < n and script[i + 1] == '"':
                    buf.append('"')
                    i += 2
                    continue
                in_double = False
            i += 1
            continue

        if c == "-" and i + 1 < n and script[i + 1] == "-":
            in_line_comment = True
            i += 2
            continue

        if c == "'":
            in_single = True
            buf.append(c)
            i += 1
            continue

        if c == '"':
            in_double = True
            buf.append(c)
            i += 1
            continue

        if c == "$":
            m = _DOLLAR_QUOTE_RE.match(script, i)
            if m:
                dollar_tag = m.group(0)
                buf.append(dollar_tag)
                i += len(dollar_tag)
                continue
            buf.append(c)
            i += 1
            continue

        if c == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
            i += 1
            continue

        buf.append(c)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def execute_rank_sql(pg: PGClient, log) -> Dict[str, int]:
    """逐语句执行 01_github_arxiv.sql，读取唯一行数摘要结果集。"""
    script = SQL_FILE.read_text(encoding="utf-8")
    statements = _split_sql_statements(script)
    # PGClient 的 fetch_one 用 commit=False，切换 autocommit 前必须先结束事务
    if not pg.conn.autocommit:
        pg.conn.commit()
    old_autocommit = pg.conn.autocommit
    try:
        pg.conn.autocommit = True
        with pg.conn.cursor() as cur:
            counts: Dict[str, int] = {}
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

    log.info(f"[sql] 四表行数: {counts}")
    return counts


# ═══════════════════════════════════════════════════════════════════════
# Parquet 导出
# ═══════════════════════════════════════════════════════════════════════

def export_rankings(pg: PGClient, snapshot_date, out: Path, log) -> None:
    """按表导出：ranking_top500 + top500_arxiv_repo 两个镜像 parquet。"""
    ranking_df = pd.read_sql("""
        SELECT * FROM ranking_top500
        WHERE snapshot_date = %s
        ORDER BY window_name, metric, rank
    """, pg.conn, params=(snapshot_date,))
    ranking_df.to_parquet(out / "ranking_top500.parquet", index=False)

    bridge_df = pd.read_sql("""
        SELECT * FROM top500_arxiv_repo
        WHERE snapshot_date = %s
    """, pg.conn, params=(snapshot_date,))
    bridge_df.to_parquet(out / "top500_arxiv_repo.parquet", index=False)

    log.info(
        f"[export] ranking_top500 {len(ranking_df)} 行，"
        f"top500_arxiv_repo {len(bridge_df)} 行"
    )


def export_details(pg: PGClient, snapshot_date, out: Path, log) -> None:
    """导出论文详情与仓库详情，文件名为 Gradio 固定读取名。"""
    paper_df = pd.read_sql(
        "SELECT * FROM paper_details WHERE snapshot_date = %s",
        pg.conn, params=(snapshot_date,),
    )
    paper_df.to_parquet(out / "paper_details_github.parquet", index=False)

    repo_df = pd.read_sql(
        "SELECT * FROM repo_details WHERE snapshot_date = %s",
        pg.conn, params=(snapshot_date,),
    )
    repo_df.to_parquet(out / "repo_details.parquet", index=False)
    log.info(f"[export] paper_details {len(paper_df)} 行，repo_details {len(repo_df)} 行")


def _stream_hf_sync(cmd, env, log, timeout: int) -> int:
    """运行 hf sync，并把 -v 输出逐行转发到 job 日志。"""
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, env=env,
    )
    started = time.monotonic()
    while True:
        rc = proc.poll()
        if rc is not None:
            for line in proc.stdout:
                line = line.rstrip()
                if line:
                    log.info(f"[hf] {line}")
            return rc
        remaining = timeout - (time.monotonic() - started)
        if remaining <= 0:
            proc.kill()
            raise RuntimeError(f"hf sync 超时 {timeout}s")
        ready, _, _ = select.select([proc.stdout], [], [], min(remaining, 1.0))
        if proc.stdout in ready:
            line = proc.stdout.readline()
            if line:
                line = line.rstrip()
                if line:
                    log.info(f"[hf] {line}")


def upload_hf(out: Path, log) -> None:
    """同步本地导出的 parquet 到 HF Bucket。"""
    bucket = (cfg.HF_BUCKET_PATH or "").strip()
    if not bucket:
        log.warning("[upload] HF_BUCKET_PATH 未设置，跳过上传")
        return

    token = (cfg.HF_TOKEN or "").strip()
    if not token:
        log.error("[upload] HF_BUCKET_PATH 已配置但缺少 HF_TOKEN")
        raise RuntimeError("HF_BUCKET_PATH 已配置但缺少 HF_TOKEN")

    endpoint = (cfg.HF_ENDPOINT or "https://hf-mirror.com").strip()
    files = sorted(
        p.relative_to(out).as_posix()
        for p in out.rglob("*.parquet")
    )
    if not files:
        log.error("[upload] 输出目录没有 parquet，拒绝上传（防止清空 HF bucket）")
        raise RuntimeError(f"输出目录无 parquet: {out}")

    upload_script = Path(__file__).parent / "upload_hf.sh"
    cmd = [
        "bash", str(upload_script), str(out), bucket,
        "--exclude", ".hf_cache/**",
        "--ignore-sizes",
    ]
    env = os.environ.copy()
    env["HF_TOKEN"] = token
    env["HF_ENDPOINT"] = endpoint
    env["HF_DEBUG"] = os.environ.get("HF_DEBUG") or "1"
    interpreter_bin = str(Path(sys.executable).parent)
    env["PATH"] = env.get("PATH", "") + os.pathsep + interpreter_bin
    if env.get("HF_HOME") is None:
        env["HF_HOME"] = str(out / ".hf_cache")

    cmd_line = " ".join(cmd)
    log.info(f"[upload] 待上传 {len(files)} 个 parquet：{out}")
    log.info(
        f"[upload] 目标 bucket={bucket} | endpoint={endpoint} "
        f"| HF_TOKEN 已配置=true | verbose=-v | HF_DEBUG=1"
    )
    log.info(f"[upload] cmd={cmd_line}")
    log.info(
        f"[upload] 实际上传命令：hf sync {out} {bucket} "
        f"--exclude .hf_cache/** --ignore-times --ignore-sizes -v "
        f"(由 {upload_script.name} 包装执行)"
    )

    started = time.monotonic()
    rc = _stream_hf_sync(cmd, env, log, UPLOAD_TIMEOUT_SECONDS)
    elapsed = time.monotonic() - started
    if rc != 0:
        log.error(
            f"[upload] 失败 | cmd={cmd_line} | rc={rc} "
            f"| elapsed={elapsed:.1f}s"
        )
        raise RuntimeError(f"HF 上传失败, rc={rc}")
    log.info(
        f"[upload] 完成 | cmd={cmd_line} | rc=0 | elapsed={elapsed:.1f}s "
        f"| 上传 {len(files)} 个文件"
    )


# ═══════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════

def clean_output_dir(out: Path, log, keep_files=()) -> None:
    """清理旧产物，同时保留另一个排名 Job 的共享文件。"""
    target = out.resolve()
    keep = set(keep_files)
    if target in (Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()):
        raise RuntimeError(f"拒绝清理危险目录: {target}")
    if target.exists():
        for item in target.iterdir():
            if item.is_file() and item.name in keep:
                log.info(f"[clean] 保留其他任务产物 {item.name}")
                continue
            if item.is_dir() and not item.is_symlink():
                shutil.rmtree(item)
            else:
                item.unlink()
        log.info(f"[clean] 已清空输出目录 {target}")
    log.info(f"[clean] 输出目录准备就绪 {target}")


def run(env: str, log) -> None:
    job_name_key = job_name(env)
    pg = PGClient(log)
    try:
        ensure_table(pg)
        target = current_pg_date(pg)
        snapshot_date = next_snapshot_date(pg, job_name_key, target, log)
        if snapshot_date is None:
            return

        out = Path(
            os.getenv("RANK_OUTPUT_DIR", DEFAULT_RANK_OUTPUT_DIR)
        ).resolve()
        clean_output_dir(out, log, keep_files=OPENALEX_RANK_FILES)
        out.mkdir(parents=True, exist_ok=True)

        mark_running(pg, job_name_key, snapshot_date)
        log.info(f"[rank] 开始生成快照 {snapshot_date}")
        try:
            counts = execute_rank_sql(pg, log)
            export_rankings(pg, snapshot_date, out, log)
            export_details(pg, snapshot_date, out, log)
            upload_hf(out, log)
            mark_completed(pg, job_name_key, snapshot_date, counts)
            log.info(f"[rank] 快照 {snapshot_date} 完成")
        except Exception as e:
            log.error(f"[rank] 快照 {snapshot_date} 失败: {type(e).__name__}: {e}")
            mark_failed(pg, job_name_key, snapshot_date, str(e))
    finally:
        pg.close()


def verify(env: str, log) -> int:
    job_name_key = job_name(env)
    pg = PGClient(log)
    try:
        ensure_table(pg)
        log.info(f"=== github_rank_run_log ({job_name_key}) ===")
        for status, count in pg.fetch_all("""
            SELECT status, COUNT(*) FROM github_rank_run_log
            WHERE job_name = %s GROUP BY status ORDER BY status
        """, (job_name_key,)):
            log.info(f"  {status}: {count}")

        log.info("=== 排名表 ===")
        for table_name, count in pg.fetch_all("""
            SELECT 'ranking_top500' AS table_name, COUNT(*) FROM ranking_top500
            UNION ALL SELECT 'top500_arxiv_repo', COUNT(*) FROM top500_arxiv_repo
            UNION ALL SELECT 'paper_details', COUNT(*) FROM paper_details
            UNION ALL SELECT 'repo_details', COUNT(*) FROM repo_details
        """):
            log.info(f"  {table_name}: {count}")
        return 0
    finally:
        pg.close()


def main(argv=None) -> int:
    env = os.environ.get("ENV")
    if env not in ("test", "prod"):
        raise RuntimeError(f"ENV 必须是 test/prod，当前: {env!r}")
    log = setup_logger("github_rank")
    args = list(sys.argv[1:] if argv is None else argv)
    cmd = args[0] if args else "run"
    if cmd == "run":
        run(env, log)
        return 0
    if cmd == "verify":
        return verify(env, log)
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(1)
