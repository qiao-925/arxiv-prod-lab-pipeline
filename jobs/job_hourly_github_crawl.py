#!/usr/bin/env python3
"""GitHub Crawl Job（发现 + README 提取合并版）

职责：
    - 按小时窗口用 Search API 发现 README 含 arxiv 的仓库
    - 动态 cursor 规避 Search API 1000 条硬上限
    - 立即拉 README 提取 arXiv ID
    - 单事务写 features_repo + edges_repo_arxiv
    - Search/REST 限流独立捕获，分别 pause

调度：
    每小时整点；断点续传靠 job_hourly_github_crawl_task.status

用法：
    ENV=test python -m jobs.job_hourly_github_crawl init
    ENV=test python -m jobs.job_hourly_github_crawl run
    ENV=test python -m jobs.job_hourly_github_crawl verify
"""
import os
import re
import sys
import time
from datetime import date, datetime, time as dtime, timedelta, timezone

import requests

from common import config as cfg
from common.logger import setup_logger
from common.pg_client import PGClient

# ═══════════════════════════════════════════════════════════════════════
# 常量
# ═══════════════════════════════════════════════════════════════════════
BASELINE_DATE = "2026-06-16"
SEARCH_URL = "https://api.github.com/search/repositories"
GITHUB_API = "https://api.github.com"
SEARCH_THROTTLE = 2.1
REST_THROTTLE = 0.8
PER_PAGE = 100
MAX_README_RETRIES = 3

PATTERNS = [
    (re.compile(r'arxiv\.org/(?:abs|pdf)/(\d{4}\.\d{4,5}|[a-z][a-z.-]*/\d{7})(?:v\d+)?', re.IGNORECASE), "url"),
    (re.compile(r'arXiv[:\s]+(\d{4}\.\d{4,5}|[a-z][a-z.-]*/\d{7})(?:v\d+)?', re.IGNORECASE), "inline"),
    (re.compile(r'eprint\s*=\s*[{\"\'](\d{4}\.\d{4,5}|[a-z][a-z.-]*/\d{7})', re.IGNORECASE), "bibtex"),
]


class SearchPause(Exception): pass
class RestPause(Exception): pass
class AuthError(Exception): pass


def extract_arxiv_ids(text: str) -> list:
    if not text:
        return []
    found = {}
    for pat, fmt in PATTERNS:
        for m in pat.finditer(text):
            aid = m.group(1).strip()
            if aid and aid not in found:
                found[aid] = fmt
    return list(found.items())


def utc_yesterday() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


# ═══════════════════════════════════════════════════════════════════════
# 表操作
# ═══════════════════════════════════════════════════════════════════════
def ensure_table(pg):
    pg.execute("""
        CREATE TABLE IF NOT EXISTS job_hourly_github_crawl_task (
            task_date DATE NOT NULL,
            task_hour INT NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            cursor TIMESTAMP,
            found_count INT DEFAULT 0,
            error_msg TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (task_date, task_hour)
        )
    """)
    pg.execute("""
        CREATE INDEX IF NOT EXISTS idx_crawl_pending
        ON job_hourly_github_crawl_task(status, task_date, task_hour)
        WHERE status IN ('pending', 'paused')
    """)


def ensure_baseline(pg):
    row = pg.fetch_one("SELECT COUNT(*) FROM job_hourly_github_crawl_task")
    if row and row[0] > 0:
        return
    pg.execute("""
        INSERT INTO job_hourly_github_crawl_task (task_date, task_hour, status)
        VALUES (%s, 0, 'pending')
        ON CONFLICT DO NOTHING
    """, (BASELINE_DATE,))


def next_crawl_task(pg, target_date: date):
    row = pg.fetch_one("""
        SELECT task_date, task_hour FROM job_hourly_github_crawl_task
        WHERE status IN ('pending', 'paused')
        ORDER BY task_date, task_hour LIMIT 1
    """)
    if row:
        return row[0], row[1]

    row = pg.fetch_one("""
        SELECT task_date, task_hour FROM job_hourly_github_crawl_task
        WHERE status = 'completed'
        ORDER BY task_date DESC, task_hour DESC LIMIT 1
    """)
    if not row:
        return None
    d, h = row[0], row[1]
    if h < 23:
        return d, h + 1
    next_d = d + timedelta(days=1)
    if next_d > target_date:
        return None
    return next_d, 0


def mark_status(pg, task_date, task_hour, status, error=None):
    pg.execute("""
        INSERT INTO job_hourly_github_crawl_task (task_date, task_hour, status, error_msg)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (task_date, task_hour) DO UPDATE
        SET status = EXCLUDED.status,
            error_msg = EXCLUDED.error_msg,
            updated_at = NOW()
    """, (task_date, task_hour, status, error[:500] if error else None))


def update_cursor(pg, task_date, task_hour, cursor: datetime):
    pg.execute("""
        UPDATE job_hourly_github_crawl_task
        SET cursor = %s, updated_at = NOW()
        WHERE task_date = %s AND task_hour = %s
    """, (cursor, task_date, task_hour))


def get_cursor(pg, task_date, task_hour):
    row = pg.fetch_one("""
        SELECT cursor FROM job_hourly_github_crawl_task WHERE task_date = %s AND task_hour = %s
    """, (task_date, task_hour))
    return row[0] if row and row[0] else None


# ═══════════════════════════════════════════════════════════════════════
# Search 客户端（30/min，1000 上限）
# ═══════════════════════════════════════════════════════════════════════
class SearchClient:
    def __init__(self, token: str, log):
        self.log = log
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        })
        self.remaining = None
        self.reset_at = None
        self._last = 0.0

    def _throttle(self):
        gap = time.time() - self._last
        if gap < SEARCH_THROTTLE:
            time.sleep(SEARCH_THROTTLE - gap)
        self._last = time.time()

    def search(self, cursor: datetime, hour_end: datetime, page: int = 1):
        if self.remaining is not None and self.remaining <= 0:
            raise SearchPause()
        self._throttle()
        q = (
            f"arxiv in:readme fork:false "
            f"created:{cursor.strftime('%Y-%m-%dT%H:%M:%S')}"
            f"..{hour_end.strftime('%Y-%m-%dT%H:%M:%S')}"
        )
        resp = self.session.get(
            SEARCH_URL,
            params={
                "q": q,
                "sort": "created",
                "order": "asc",
                "per_page": PER_PAGE,
                "page": page,
            },
            timeout=30,
        )
        if resp.status_code == 401:
            raise AuthError("Search 401")
        if resp.status_code in (403, 429):
            ra = resp.headers.get("Retry-After")
            rm = resp.headers.get("X-RateLimit-Remaining")
            if ra or rm == "0":
                raise SearchPause()
            raise RuntimeError(f"Search 403: {resp.text[:200]}")
        resp.raise_for_status()
        if "X-RateLimit-Remaining" in resp.headers:
            self.remaining = int(resp.headers["X-RateLimit-Remaining"])
        return resp.json().get("items", [])


# ═══════════════════════════════════════════════════════════════════════
# REST 客户端（5000/h，README）
# ═══════════════════════════════════════════════════════════════════════
class RestClient:
    def __init__(self, token: str, log):
        self.log = log
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.raw+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        self.remaining = None
        self.reset_at = None
        self._last = 0.0

    def _throttle(self):
        gap = time.time() - self._last
        if gap < REST_THROTTLE:
            time.sleep(REST_THROTTLE - gap)
        self._last = time.time()

    def fetch_readme(self, full_name: str):
        self._throttle()
        for attempt in range(MAX_README_RETRIES):
            try:
                resp = self.session.get(
                    f"{GITHUB_API}/repos/{full_name}/readme", timeout=30,
                )
                if resp.status_code == 401:
                    raise AuthError("REST 401")
                if resp.status_code == 404:
                    return None
                if resp.status_code in (403, 429):
                    ra = resp.headers.get("Retry-After")
                    rm = resp.headers.get("X-RateLimit-Remaining")
                    if ra or rm == "0":
                        raise RestPause()
                    self.log.warning(f"{full_name} REST 403: {resp.text[:100]}")
                    return None
                resp.raise_for_status()
                if "X-RateLimit-Remaining" in resp.headers:
                    self.remaining = int(resp.headers["X-RateLimit-Remaining"])
                return resp.text
            except requests.RequestException as e:
                if attempt == MAX_README_RETRIES - 1:
                    self.log.warning(f"{full_name} README 拉取失败: {e}")
                    return None
                time.sleep(2 ** attempt)
        return None


# ═══════════════════════════════════════════════════════════════════════
# 写入：features_repo + edges_repo_arxiv（单事务）
# ═══════════════════════════════════════════════════════════════════════
def persist_repo(pg, item: dict, links: list, log):
    repo_id = item.get("id")
    full_name = (item.get("full_name") or "").strip()
    if not repo_id or "/" not in full_name:
        return
    owner, _, name = full_name.partition("/")
    created = None
    raw = item.get("created_at")
    if raw:
        try:
            created = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
        except ValueError:
            pass

    has_papers = pg.fetch_one("SELECT to_regclass('arxiv_papers')")[0] is not None

    with pg.cursor() as cur:
        cur.execute("""
            INSERT INTO features_repo (repo_id, full_name, owner, name, created)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (repo_id) DO UPDATE
            SET full_name = EXCLUDED.full_name,
                owner = EXCLUDED.owner,
                name = EXCLUDED.name,
                created = COALESCE(features_repo.created, EXCLUDED.created)
        """, (repo_id, full_name, owner, name, created))

        if not links:
            return
        arxiv_ids = [aid for aid, _ in links]
        extracted_on = datetime.now(timezone.utc).date()

        cur.execute(
            "UPDATE edges_repo_arxiv SET added = TRUE "
            "WHERE repo_id = %s AND arxiv_id = ANY(%s::text[])",
            (repo_id, arxiv_ids),
        )

        if has_papers:
            cur.execute("""
                INSERT INTO edges_repo_arxiv (repo_id, arxiv_id, added, date)
                SELECT %s, t.arxiv_id, TRUE, COALESCE(a.published_date::date, %s::date)
                FROM unnest(%s::text[]) AS t(arxiv_id)
                LEFT JOIN arxiv_papers a ON a.arxiv_id = t.arxiv_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM edges_repo_arxiv e
                    WHERE e.repo_id = %s AND e.arxiv_id = t.arxiv_id
                )
            """, (repo_id, extracted_on, arxiv_ids, repo_id))
        else:
            cur.execute("""
                INSERT INTO edges_repo_arxiv (repo_id, arxiv_id, added, date)
                SELECT %s, t.arxiv_id, TRUE, %s::date
                FROM unnest(%s::text[]) AS t(arxiv_id)
                WHERE NOT EXISTS (
                    SELECT 1 FROM edges_repo_arxiv e
                    WHERE e.repo_id = %s AND e.arxiv_id = t.arxiv_id
                )
            """, (repo_id, extracted_on, arxiv_ids, repo_id))


# ═══════════════════════════════════════════════════════════════════════
# 单小时处理
# ═══════════════════════════════════════════════════════════════════════
def process_hour(pg, search_cli, rest_cli, task_date, task_hour, log):
    hour_start = datetime.combine(task_date, dtime(task_hour, 0, 0), tzinfo=timezone.utc)
    hour_end = hour_start + timedelta(hours=1)

    cursor = get_cursor(pg, task_date, task_hour) or hour_start
    if cursor.tzinfo is None:
        cursor = cursor.replace(tzinfo=timezone.utc)

    found_total = 0

    while cursor < hour_end:
        items = search_cli.search(cursor, hour_end)
        if not items:
            return

        for item in items:
            readme = rest_cli.fetch_readme(item["full_name"])
            links = extract_arxiv_ids(readme) if readme else []
            persist_repo(pg, item, links, log)
            found_total += 1

        if len(items) < PER_PAGE:
            return

        last_raw = items[-1].get("created_at")
        if not last_raw:
            return
        try:
            last_dt = datetime.fromisoformat(last_raw.replace("Z", "+00:00"))
        except ValueError:
            return
        cursor = last_dt + timedelta(seconds=1)
        update_cursor(pg, task_date, task_hour, cursor)

    log.info(f"{task_date} hour={task_hour} 处理 {found_total} 个仓库")


# ═══════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════
def run(env, log):
    log.info("=" * 70)
    log.info(f"GitHub Crawl | env={env}")
    log.info("=" * 70)

    pg = PG_CLIENT_CLASS(log)
    try:
        ensure_table(pg)
        ensure_baseline(pg)

        target = utc_yesterday()
        search_cli = SearchClient(_token(), log)
        rest_cli = RestClient(_token(), log)

        while True:
            task = next_crawl_task(pg, target)
            if task is None:
                log.info("✅ 所有小时窗口已处理")
                break
            task_date, task_hour = task
            mark_status(pg, task_date, task_hour, "running")
            log.info(f"--- 处理 {task_date} hour={task_hour} ---")

            try:
                process_hour(pg, search_cli, rest_cli, task_date, task_hour, log)
                mark_status(pg, task_date, task_hour, "completed")
            except SearchPause:
                mark_status(pg, task_date, task_hour, "paused", "Search 限流")
                log.warning("Search 限流，暂停")
                return
            except RestPause:
                mark_status(pg, task_date, task_hour, "paused", "REST 限流")
                log.warning("REST 限流，暂停")
                return
    finally:
        pg.close()


def init(log):
    pg = PG_CLIENT_CLASS(log)
    try:
        ensure_table(pg)
        ensure_baseline(pg)
        log.info(f"Crawl 已初始化 baseline={BASELINE_DATE}")
        return 0
    finally:
        pg.close()


def verify(log):
    pg = PG_CLIENT_CLASS(log)
    try:
        ensure_table(pg)
        log.info("=== crawl_task ===")
        for status, n in pg.fetch_all("""
            SELECT status, COUNT(*) FROM job_hourly_github_crawl_task GROUP BY status
        """):
            log.info(f"  {status}: {n}")
        return 0
    finally:
        pg.close()


GITHUB_TOKEN = None
PG_CLIENT_CLASS = PGClient


def _token():
    return GITHUB_TOKEN or cfg.GITHUB_TOKEN


def main(argv=None):
    env = os.environ.get("ENV")
    if env not in ("test", "prod"):
        raise RuntimeError(f"ENV 必须是 test/prod，当前: {env!r}")
    log = setup_logger("github_crawl")
    args = sys.argv[1:] if argv is None else list(argv)
    cmd = args[0] if args else "run"
    if cmd == "init":
        return init(log)
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
