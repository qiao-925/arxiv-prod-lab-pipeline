#!/usr/bin/env python3
"""GitHub 活跃度增量同步 Job

职责：
    - P0（14天窗口）：走 job_hourly_github_sync_p0_task 按天推进
    - P2（全量扫描）：走 job_hourly_github_sync_p2_task 按轮推进，snapshot_at 锁定
    - 明细落库：job_hourly_github_sync_repo_task (lane='p0' / 'p2')
    - star 走 REST；fork/issue/pr 走 GraphQL
    - 限流分别捕获 RestPause / GraphQLPause

用法：
    ENV=test python -m jobs.job_hourly_github_sync run
    ENV=test python -m jobs.job_hourly_github_sync verify
"""
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests
from psycopg2.extras import execute_values

from common import config as cfg
from common.logger import setup_logger
from common.pg_client import PGClient

BASELINE_DATE = "2026-06-16"
P0_WINDOW_DAYS = 14
BATCH_SIZE = 50_000
GRAPHQL_PAGE = 100
STAR_WEEKS = 30
MAX_PAGES = 100
MAX_TRIES = 3
REST_THROTTLE = 0.1
INACTIVE_RETRY_DAYS = 30
TARGET_LAG = 1

GITHUB_API = "https://api.github.com"
GRAPHQL_URL = "https://api.github.com/graphql"
METRICS = ("star", "fork", "issue", "pr")
EVENT_TABLES = {"star": "events_stars", "fork": "events_forks", "issue": "events_issues", "pr": "events_prs"}

GITHUB_TOKEN = None
PG_CLIENT_CLASS = PGClient


def _token():
    return GITHUB_TOKEN or cfg.GITHUB_TOKEN


def utc_yesterday():
    return datetime.now(timezone.utc).date() - timedelta(days=TARGET_LAG)


class RestPause(Exception): pass
class GraphQLPause(Exception): pass
class AuthError(Exception): pass
class NotFoundError(Exception): pass
class SubTaskDone(Exception): pass


# ═══════════════════════════════════════════════════════════════════════
# 表 DDL
# ═══════════════════════════════════════════════════════════════════════
def ensure_table(pg):
    pg.execute("""
        CREATE TABLE IF NOT EXISTS job_hourly_github_sync_p0_task (
            task_date DATE PRIMARY KEY,
            status VARCHAR(20) NOT NULL DEFAULT 'running',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    pg.execute("""
        CREATE TABLE IF NOT EXISTS job_hourly_github_sync_p2_task (
            round_number INT PRIMARY KEY,
            snapshot_at TIMESTAMP NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'running',
            started_at TIMESTAMP DEFAULT NOW(),
            completed_at TIMESTAMP
        )
    """)
    pg.execute("""
        CREATE TABLE IF NOT EXISTS job_hourly_github_sync_repo_task (
            id BIGSERIAL PRIMARY KEY,
            lane VARCHAR(10) NOT NULL,
            task_date DATE NOT NULL,
            round_number INT NOT NULL DEFAULT 0,
            repo_id BIGINT NOT NULL,
            full_name VARCHAR(200) NOT NULL,
            scan_start DATE NOT NULL,
            scan_to DATE NOT NULL,
            progress JSONB NOT NULL DEFAULT '{}'::jsonb,
            status VARCHAR(20) NOT NULL DEFAULT 'running',
            try_number INT NOT NULL DEFAULT 0,
            error_msg TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (lane, task_date, round_number, repo_id)
        )
    """)
    pg.execute("""
        CREATE INDEX IF NOT EXISTS idx_repo_task_pending
        ON job_hourly_github_sync_repo_task(lane, task_date, round_number, status, id)
        WHERE status IN ('running', 'failed')
    """)
    pg.execute("""
        CREATE INDEX IF NOT EXISTS idx_repo_task_done
        ON job_hourly_github_sync_repo_task(repo_id, scan_to DESC)
        WHERE status = 'completed'
    """)


def ensure_baseline(pg):
    pg.execute("""
        INSERT INTO job_hourly_github_sync_p0_task (task_date, status)
        VALUES (%s, 'completed')
        ON CONFLICT DO NOTHING
    """, (BASELINE_DATE,))



# ═══════════════════════════════════════════════════════════════════════
# P0 锚点操作
# ═══════════════════════════════════════════════════════════════════════
def current_p0_date(pg, target):
    row = pg.fetch_one("""
        SELECT task_date FROM job_hourly_github_sync_p0_task
        WHERE status = 'running' ORDER BY task_date LIMIT 1
    """)
    if row:
        return row[0]
    row = pg.fetch_one("""
        SELECT MAX(task_date) FROM job_hourly_github_sync_p0_task WHERE status = 'completed'
    """)
    if row and row[0]:
        nxt = row[0] + timedelta(days=1)
        return nxt if nxt <= target else None
    return None


def mark_p0(pg, task_date, status):
    pg.execute("""
        INSERT INTO job_hourly_github_sync_p0_task (task_date, status)
        VALUES (%s, %s)
        ON CONFLICT (task_date) DO UPDATE
        SET status = EXCLUDED.status, updated_at = NOW()
    """, (task_date, status))


# ═══════════════════════════════════════════════════════════════════════
# P2 锚点操作
# ═══════════════════════════════════════════════════════════════════════
def current_p2_round(pg):
    row = pg.fetch_one("""
        SELECT round_number, snapshot_at FROM job_hourly_github_sync_p2_task
        WHERE status = 'running' ORDER BY round_number LIMIT 1
    """)
    if row:
        return row[0], row[1]
    row = pg.fetch_one("SELECT COALESCE(MAX(round_number), 0) FROM job_hourly_github_sync_p2_task")
    new_num = (row[0] if row and row[0] else 0) + 1
    snapshot = datetime.now(timezone.utc)
    pg.execute("""
        INSERT INTO job_hourly_github_sync_p2_task (round_number, snapshot_at, status)
        VALUES (%s, %s, 'running')
    """, (new_num, snapshot))
    return new_num, snapshot


def mark_p2_completed(pg, round_number):
    pg.execute("""
        UPDATE job_hourly_github_sync_p2_task
        SET status = 'completed', completed_at = NOW()
        WHERE round_number = %s
    """, (round_number,))


# ═══════════════════════════════════════════════════════════════════════
# repo_task 明细操作
# ═══════════════════════════════════════════════════════════════════════
def pick_one(pg, lane, task_date, round_number):
    """running + failed 都取，按 try_number 升序（先处理新失败的）。"""
    return pg.fetch_one("""
        SELECT id, repo_id, full_name, scan_start, scan_to, progress, status
        FROM job_hourly_github_sync_repo_task
        WHERE lane = %s AND task_date = %s AND round_number = %s
          AND status IN ('running', 'failed')
        ORDER BY try_number, id LIMIT 1
    """, (lane, task_date, round_number))




def save_progress(pg, sub_id, progress):
    pg.execute(
        "UPDATE job_hourly_github_sync_repo_task SET progress=%s::jsonb, updated_at=NOW() WHERE id=%s",
        (json.dumps(progress, ensure_ascii=False), sub_id),
    )


def mark_sub_completed(pg, sub_id):
    pg.execute("UPDATE job_hourly_github_sync_repo_task SET status='completed', updated_at=NOW() WHERE id=%s", (sub_id,))




def mark_sub_failed(pg, sub_id, reason):
    """确定性失败（404 等），下次 pick 仍会重试。"""
    pg.execute("""
        UPDATE job_hourly_github_sync_repo_task
        SET status = 'failed',
            try_number = try_number + 1,
            error_msg = %s,
            updated_at = NOW()
        WHERE id = %s
    """, (reason[:500], sub_id))


def fail_sub(pg, sub_id, error):
    """瞬时错误：< 3 保持 running，>= 3 标 failed（下次仍重试）。"""
    pg.execute("""
        UPDATE job_hourly_github_sync_repo_task
        SET try_number = try_number + 1,
            status = CASE WHEN try_number + 1 >= %s THEN 'failed' ELSE 'running' END,
            error_msg = %s, updated_at = NOW()
        WHERE id = %s
    """, (MAX_TRIES, (error or "")[:500], sub_id))


# ═══════════════════════════════════════════════════════════════════════
# 纯函数
# ═══════════════════════════════════════════════════════════════════════
def _pget(progress, metric):
    return (progress or {}).get(metric) or {}


def parse_star_weeks(payload, start_date, end_date):
    items = payload if isinstance(payload, list) else (payload or {}).get("weeks", [])
    counts = {}
    for item in items or []:
        raw_week, days = item.get("week"), item.get("days") or []
        if raw_week is None or not isinstance(days, list):
            continue
        try:
            ws = datetime.fromtimestamp(int(raw_week), tz=timezone.utc).date()
        except (TypeError, ValueError):
            continue
        for i, v in enumerate(days[:7]):
            try:
                value = int(v)
            except (TypeError, ValueError):
                continue
            if value <= 0:
                continue
            d = ws + timedelta(days=i)
            if start_date <= d <= end_date:
                counts[d] = counts.get(d, 0) + value
    return counts


def star_has_more(payload, start_date):
    items = payload if isinstance(payload, list) else (payload or {}).get("weeks", [])
    if len(items or []) < STAR_WEEKS:
        return False
    starts = [
        datetime.fromtimestamp(int(item["week"]), tz=timezone.utc).date()
        for item in items if item.get("week")
    ]
    return bool(starts) and min(starts) > start_date


def parse_connection(conn, start_date, end_date):
    nodes = (conn or {}).get("nodes") or []
    counts, dates = {}, []
    for node in nodes:
        raw = (node or {}).get("createdAt")
        if not raw:
            continue
        try:
            d = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
        except ValueError:
            continue
        dates.append(d)
        if start_date <= d <= end_date:
            counts[d] = counts.get(d, 0) + 1
    pi = (conn or {}).get("pageInfo") or {}
    return {
        "counts": counts,
        "oldest": min(dates) if dates else None,
        "has_next": bool(pi.get("hasNextPage")),
        "end_cursor": pi.get("endCursor"),
    }


def _connection(metric, cursor=None):
    if metric == "fork":
        base = f"forks(first: {GRAPHQL_PAGE}, orderBy: {{field: CREATED_AT, direction: DESC}}"
    elif metric == "issue":
        base = f"issues(first: {GRAPHQL_PAGE}, states: [OPEN, CLOSED], orderBy: {{field: CREATED_AT, direction: DESC}}"
    else:
        base = f"pullRequests(first: {GRAPHQL_PAGE}, states: [OPEN, CLOSED, MERGED], orderBy: {{field: CREATED_AT, direction: DESC}}"
    if cursor:
        base += f", after: {json.dumps(cursor)}"
    return f"{base}) {{ pageInfo {{ hasNextPage endCursor }} nodes {{ createdAt }} }}"


def build_graphql_query(full_name, metric, cursor=None):
    owner, _, name = (full_name or "").partition("/")
    return (
        f"query {{ rateLimit {{ remaining resetAt }} "
        f"repository(owner: {json.dumps(owner)}, name: {json.dumps(name)}) "
        f"{{ {metric}: {_connection(metric, cursor)} }} }}"
    )


# ═══════════════════════════════════════════════════════════════════════
# 客户端
# ═══════════════════════════════════════════════════════════════════════
class StarClient:
    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        self.remaining = None
        self._last = 0.0

    def fetch_page(self, full_name, page):
        if self.remaining is not None and self.remaining <= 0:
            raise RestPause()
        gap = time.time() - self._last
        if gap < REST_THROTTLE:
            time.sleep(REST_THROTTLE - gap)
        self._last = time.time()
        resp = self.session.get(
            f"{GITHUB_API}/repos/{full_name}/stargazers/history",
            params={"page": page}, timeout=30,
        )
        if "X-RateLimit-Remaining" in resp.headers:
            self.remaining = int(resp.headers["X-RateLimit-Remaining"])
        if resp.status_code == 401:
            raise AuthError("REST 401")
        if resp.status_code == 404:
            raise NotFoundError(full_name)
        if resp.status_code in (403, 429):
            raise RestPause()
        resp.raise_for_status()
        return resp.json()


class GraphQLClient:
    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        self.remaining = None

    def execute(self, query):
        if self.remaining is not None and self.remaining <= 0:
            raise GraphQLPause()
        resp = self.session.post(GRAPHQL_URL, json={"query": query}, timeout=60)
        if resp.status_code == 401:
            raise AuthError("GraphQL 401")
        if resp.status_code in (403, 429):
            raise GraphQLPause()
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") or {}
        rate = data.get("rateLimit") or {}
        if rate.get("remaining") is not None:
            self.remaining = int(rate["remaining"])
        errors = payload.get("errors") or []
        if not data and errors:
            raise RuntimeError(f"GraphQL error: {errors[:1]}")
        return data, errors


def upsert_events(cur, metric, repo_id, counts):
    rows = [(repo_id, d, int(c)) for d, c in counts.items() if c and int(c) > 0]
    if rows:
        execute_values(
            cur,
            f"INSERT INTO {EVENT_TABLES[metric]} (repo_id, date, count) VALUES %s "
            f"ON CONFLICT (repo_id, date) DO UPDATE SET count = EXCLUDED.count",
            rows, page_size=1000,
        )


# ═══════════════════════════════════════════════════════════════════════
# 单 metric 处理
# ═══════════════════════════════════════════════════════════════════════
def _process_star(pg, cli, sub_id, repo_id, full_name, scan_start, scan_to, progress):
    p = progress.setdefault("star", {})
    page = p.get("page", 1)
    while True:
        try:
            payload = cli.fetch_page(full_name, page)
        except NotFoundError:
            mark_sub_failed(pg, sub_id, f"404: star {full_name}")
            raise SubTaskDone
        more = star_has_more(payload, scan_start)
        if more and page >= MAX_PAGES:
            fail_sub(pg, sub_id, f"star page limit")
            raise SubTaskDone
        with pg.cursor() as cur:
            upsert_events(cur, "star", repo_id, parse_star_weeks(payload, scan_start, scan_to))
        if not more:
            p["done"], p["page"] = True, None
            save_progress(pg, sub_id, progress)
            return
        page += 1
        p["page"] = page
        save_progress(pg, sub_id, progress)


def _process_graphql(pg, cli, sub_id, repo_id, full_name, scan_start, scan_to, metric, progress):
    p = progress.setdefault(metric, {})
    cursor, page = p.get("cursor"), p.get("page", 1)
    while True:
        data, _ = cli.execute(build_graphql_query(full_name, metric, cursor))
        repo_data = data.get("repository")
        if repo_data is None:
            mark_sub_failed(pg, sub_id, f"404: {metric} repo missing")
            raise SubTaskDone
        conn = repo_data.get(metric)
        if conn is None:
            fail_sub(pg, sub_id, f"{metric} null")
            raise SubTaskDone
        parsed = parse_connection(conn, scan_start, scan_to)
        more = bool(parsed["has_next"] and parsed["oldest"] and parsed["oldest"] > scan_start)
        if more and not parsed["end_cursor"]:
            fail_sub(pg, sub_id, f"{metric}: no endCursor")
            raise SubTaskDone
        if more and page >= MAX_PAGES:
            fail_sub(pg, sub_id, f"{metric} page limit")
            raise SubTaskDone
        with pg.cursor() as cur:
            upsert_events(cur, metric, repo_id, parsed["counts"])
        if not more:
            p["done"], p["cursor"], p["page"] = True, None, None
            save_progress(pg, sub_id, progress)
            return
        cursor, page = parsed["end_cursor"], page + 1
        p["cursor"], p["page"] = cursor, page
        save_progress(pg, sub_id, progress)


def process_repo(pg, star_cli, gql_cli, sub):
    sub_id, repo_id, full_name, scan_start, scan_to, progress, _ = sub
    try:
        for metric in METRICS:
            if _pget(progress, metric).get("done"):
                continue
            if metric == "star":
                _process_star(pg, star_cli, sub_id, repo_id, full_name, scan_start, scan_to, progress)
            else:
                _process_graphql(pg, gql_cli, sub_id, repo_id, full_name, scan_start, scan_to, metric, progress)
    except SubTaskDone:
        return
    except (RestPause, GraphQLPause):
        # 限流保持 running，不增 try_number
        raise
    except AuthError:
        raise
    except (requests.RequestException, RuntimeError) as e:
        fail_sub(pg, sub_id, f"{type(e).__name__}: {e}")
        return
    mark_sub_completed(pg, sub_id)


# ═══════════════════════════════════════════════════════════════════════
# 任务生成
# ═══════════════════════════════════════════════════════════════════════
def ensure_p0_subtasks(pg, task_date, limit):
    sql = """
        INSERT INTO job_hourly_github_sync_repo_task (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to)
        SELECT 'p0', %(task_date)s, 0, f.repo_id, f.full_name,
               COALESCE(
                   (SELECT MAX(s.scan_to) + 1 FROM job_hourly_github_sync_repo_task s
                    WHERE s.repo_id = f.repo_id AND s.lane = 'p0' AND s.status = 'completed'),
                   LEAST(f.created, %(task_date)s),
                   %(task_date)s
               ),
               %(task_date)s
        FROM features_repo f
        WHERE EXISTS (
            SELECT 1 FROM edges_repo_arxiv e
            JOIN arxiv_papers a ON e.arxiv_id = a.arxiv_id
            WHERE e.repo_id = f.repo_id AND e.added = true
              AND a.published_date::date >= %(task_date)s - %(window)s
              AND a.published_date::date <= %(task_date)s
        )
          AND f.created <= %(task_date)s
          AND NOT EXISTS (
            SELECT 1 FROM job_hourly_github_sync_repo_task s
            WHERE s.lane = 'p0' AND s.task_date = %(task_date)s AND s.round_number = 0
              AND s.repo_id = f.repo_id
          )
          AND NOT EXISTS (
            SELECT 1 FROM job_hourly_github_sync_repo_task s
            WHERE s.repo_id = f.repo_id AND s.status = 'failed'
              AND s.task_date >= %(task_date)s - %(inactive_retry)s
          )
        ORDER BY f.created DESC NULLS LAST, f.repo_id
        LIMIT %(limit)s
    """
    return pg.execute(sql, {
        "task_date": task_date, "window": P0_WINDOW_DAYS,
        "inactive_retry": INACTIVE_RETRY_DAYS, "limit": limit,
    })


def ensure_p2_subtasks(pg, round_number, snapshot_at, limit):
    snapshot_date = snapshot_at.date() if hasattr(snapshot_at, "date") else snapshot_at
    sql = """
        INSERT INTO job_hourly_github_sync_repo_task (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to)
        SELECT 'p2', %(snapshot_date)s, %(round_number)s, f.repo_id, f.full_name,
               COALESCE(f.last_scan_to + 1, f.created, %(snapshot_date)s),
               %(snapshot_date)s
        FROM (
            SELECT f.repo_id, f.full_name, f.created,
                   (SELECT MAX(s.scan_to) FROM job_hourly_github_sync_repo_task s
                    WHERE s.repo_id = f.repo_id AND s.lane = 'p2' AND s.status = 'completed') AS last_scan_to
            FROM features_repo f
            WHERE f.created IS NULL OR f.created <= %(snapshot_date)s
        ) f
        WHERE COALESCE(f.last_scan_to, f.created - 1, %(snapshot_date)s - 1) < %(snapshot_date)s
          AND NOT EXISTS (
            SELECT 1 FROM job_hourly_github_sync_repo_task s
            WHERE s.lane = 'p2' AND s.round_number = %(round_number)s
              AND s.repo_id = f.repo_id
          )
          AND NOT EXISTS (
            SELECT 1 FROM job_hourly_github_sync_repo_task s
            WHERE s.repo_id = f.repo_id AND s.status = 'failed'
              AND s.task_date >= %(snapshot_date)s - %(inactive_retry)s
          )
        ORDER BY COALESCE(f.last_scan_to, '1970-01-01'::date) ASC, f.created DESC NULLS LAST
        LIMIT %(limit)s
    """
    return pg.execute(sql, {
        "snapshot_date": snapshot_date, "round_number": round_number,
        "inactive_retry": INACTIVE_RETRY_DAYS, "limit": limit,
    })


# ═══════════════════════════════════════════════════════════════════════
# 主循环
# ═══════════════════════════════════════════════════════════════════════
def run_p0(pg, star_cli, gql_cli, target, log):
    while True:
        task_date = current_p0_date(pg, target)
        if task_date is None:
            return True
        mark_p0(pg, task_date, "running")
        while True:
            sub = pick_one(pg, "p0", task_date, 0)
            if sub is None:
                if ensure_p0_subtasks(pg, task_date, BATCH_SIZE) == 0:
                    break
                sub = pick_one(pg, "p0", task_date, 0)
                if sub is None:
                    break
            try:
                process_repo(pg, star_cli, gql_cli, sub)
            except (RestPause, GraphQLPause):
                log.warning(f"P0 {task_date} 限流，保持 running")
                return False
        mark_p0(pg, task_date, "completed")
        log.info(f"✅ P0 {task_date} completed")


def run_p2(pg, star_cli, gql_cli, log):
    if pg.fetch_one("SELECT 1 FROM features_repo LIMIT 1") is None:
        log.info("features_repo 为空，P2 跳过")
        return
    round_num, snapshot_at = current_p2_round(pg)
    log.info(f"P2 round={round_num} snapshot={snapshot_at}")
    while True:
        sub = pick_one(pg, "p2", snapshot_at.date(), round_num)
        if sub is None:
            if ensure_p2_subtasks(pg, round_num, snapshot_at, BATCH_SIZE) == 0:
                mark_p2_completed(pg, round_num)
                log.info(f"✅ P2 round {round_num} completed")
                return
            sub = pick_one(pg, "p2", snapshot_at.date(), round_num)
            if sub is None:
                return
        try:
            process_repo(pg, star_cli, gql_cli, sub)
        except (RestPause, GraphQLPause):
            log.warning("P2 paused")
            return


# ═══════════════════════════════════════════════════════════════════════
# 入口
# ═══════════════════════════════════════════════════════════════════════
def run(env, log):
    log.info(f"GitHub Sync | env={env}")
    pg = PG_CLIENT_CLASS(log)
    try:
        ensure_table(pg)
        ensure_baseline(pg)
        target = utc_yesterday()
        star_cli = StarClient(_token())
        gql_cli = GraphQLClient(_token())
        if not run_p0(pg, star_cli, gql_cli, target, log):
            log.info("P0 未追平，跳过 P2")
            return
        run_p2(pg, star_cli, gql_cli, log)
    finally:
        pg.close()


def verify(log):
    pg = PG_CLIENT_CLASS(log)
    try:
        ensure_table(pg)
        log.info("=== p0_task ===")
        for s, c in pg.fetch_all("SELECT status, COUNT(*) FROM job_hourly_github_sync_p0_task GROUP BY 1"):
            log.info(f"  {s}: {c}")
        log.info("=== p2_task ===")
        for r, s, snap in pg.fetch_all("SELECT round_number, status, snapshot_at FROM job_hourly_github_sync_p2_task ORDER BY round_number DESC LIMIT 5"):
            log.info(f"  round {r} / {s} / snapshot={snap}")
        log.info("=== repo_task ===")
        for lane, s, c in pg.fetch_all("SELECT lane, status, COUNT(*) FROM job_hourly_github_sync_repo_task GROUP BY 1, 2 ORDER BY 1, 2"):
            log.info(f"  {lane} / {s}: {c}")
        for metric, table in EVENT_TABLES.items():
            r = pg.fetch_one(f"SELECT COUNT(*), MIN(date), MAX(date) FROM {table}")
            log.info(f"events {metric}: rows={r[0]} min={r[1]} max={r[2]}")
        return 0
    finally:
        pg.close()


def main(argv=None):
    env = os.environ.get("ENV")
    if env not in ("test", "prod"):
        raise RuntimeError(f"ENV 必须是 test/prod，当前: {env!r}")
    log = setup_logger("github_sync")
    args = sys.argv[1:] if argv is None else list(argv)
    cmd = args[0] if args else "run"
    if cmd == "run":
        run(env, log)
        return 0
    if cmd == "verify":
        return verify(log)
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(1)
