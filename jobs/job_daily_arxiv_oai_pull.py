# jobs/job_daily_arxiv_oai_pull.py
"""arXiv OAI-PMH 采集 Job

职责：
    - 从 OAI-PMH 拉取 arXivRaw 记录
    - 提取 published_date（v1 发布时间），按 published_year/month 分区
    - 按 arxiv_id upsert 写入 MinIO
    - 用 PG arxiv_oai_pull_log 记账，按天推进，断点续传

调度方式：
    每晚定时执行，脚本自动从上次 COMMITTED 日期推进到"昨天"（UTC）。
    幂等：重复执行不会重复拉同一天。

用法：
    ENV=test uv run python -m jobs.job_daily_arxiv_oai_pull
"""
import os
import sys
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import requests

from common.logger import setup_logger
from common.minio_client import MinIOClient, papers_arxiv_oai_state_path
from common.pg_client import PGClient

OAI_URL = "https://oaipmh.arxiv.org/oai"
NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "arxiv": "http://arxiv.org/OAI/arXivRaw/",
}
RATE_LIMIT_SECONDS = 3.0
DEFAULT_INIT_DATE = "2026-08-30"


# ═══════════════════════════════════════════════════════════════════════
# 解析工具（纯函数）
# ═══════════════════════════════════════════════════════════════════════

def parse_arxiv_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%a, %d %b %Y %H:%M:%S GMT")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _version_num(v: Dict[str, Any]) -> int:
    ver = (v.get("version") or "").strip()
    if ver.startswith("v"):
        try:
            return int(ver[1:])
        except ValueError:
            return 0
    return 0


def extract_published_date(versions: List[Dict[str, Any]]) -> Optional[str]:
    if not versions:
        return None
    return sorted(versions, key=_version_num)[0].get("date")


def parse_record(record_elem) -> Optional[Dict[str, Any]]:
    header = record_elem.find("oai:header", NS)
    if header is None or header.attrib.get("status") == "deleted":
        return None

    datestamp = header.findtext("oai:datestamp", default=None, namespaces=NS)
    identifier = header.findtext("oai:identifier", default=None, namespaces=NS)
    set_specs = [s.text for s in header.findall("oai:setSpec", NS) if s.text]

    metadata = record_elem.find("oai:metadata", NS)
    if metadata is None:
        return None
    arxiv_raw = metadata.find("arxiv:arXivRaw", NS)
    if arxiv_raw is None:
        return None

    def txt(tag: str) -> Optional[str]:
        el = arxiv_raw.find(f"arxiv:{tag}", NS)
        return el.text.strip() if el is not None and el.text else None

    versions: List[Dict[str, Any]] = []
    for v in arxiv_raw.findall("arxiv:version", NS):
        versions.append({
            "version": v.attrib.get("version"),
            "date": v.findtext("arxiv:date", default=None, namespaces=NS),
            "size": v.findtext("arxiv:size", default=None, namespaces=NS),
            "source_type": v.findtext("arxiv:source_type", default=None, namespaces=NS),
        })

    return {
        "arxiv_id": txt("id"),
        "title": txt("title"),
        "abstract": txt("abstract"),
        "authors": txt("authors"),
        "categories": txt("categories"),
        "set_specs": set_specs,
        "submitter": txt("submitter"),
        "comments": txt("comments"),
        "journal_ref": txt("journal-ref"),
        "doi": txt("doi"),
        "report_no": txt("report-no"),
        "license": txt("license"),
        "versions": versions,
        "published_date": extract_published_date(versions),
        "datestamp": datestamp,
        "oai_identifier": identifier,
    }


# ═══════════════════════════════════════════════════════════════════════
# arxiv_oai_pull_log 表操作
# ═══════════════════════════════════════════════════════════════════════

def ensure_table(pg: PGClient) -> None:
    pg.execute("""
        CREATE TABLE IF NOT EXISTS arxiv_oai_pull_log (
            job_name       VARCHAR(64) NOT NULL,
            pull_date      DATE        NOT NULL,
            status         VARCHAR(20) NOT NULL,
            record_count   INT         DEFAULT 0,
            error_message  TEXT,
            started_at     TIMESTAMP   DEFAULT NOW(),
            committed_at   TIMESTAMP,
            PRIMARY KEY (job_name, pull_date)
        )
    """)
    pg.execute("""
        CREATE INDEX IF NOT EXISTS idx_arxiv_oai_pull_log_status
        ON arxiv_oai_pull_log (job_name, status)
    """)


def ensure_initial_record(pg: PGClient, job_name: str, init_date: str) -> None:
    """首次运行时插入基线记录：(job_name, init_date, COMMITTED, 0)。"""
    row = pg.fetch_one(
        "SELECT COUNT(*) FROM arxiv_oai_pull_log WHERE job_name = %s",
        (job_name,),
    )
    if row is not None and row[0] > 0:
        return
    pg.execute("""
        INSERT INTO arxiv_oai_pull_log
            (job_name, pull_date, status, record_count, started_at, committed_at)
        VALUES (%s, %s, 'COMMITTED', 0, NOW(), NOW())
        ON CONFLICT (job_name, pull_date) DO NOTHING
    """, (job_name, init_date))


def query_last_committed(pg: PGClient, job_name: str) -> str:
    row = pg.fetch_one("""
        SELECT MAX(pull_date) FROM arxiv_oai_pull_log
        WHERE job_name = %s AND status = 'COMMITTED'
    """, (job_name,))
    if row is None or row[0] is None:
        raise RuntimeError(f"arxiv_oai_pull_log 中无 {job_name} 的 COMMITTED 记录")
    return str(row[0])


def mark_committed(pg: PGClient, job_name: str, pull_date: str, n: int) -> None:
    pg.execute("""
        INSERT INTO arxiv_oai_pull_log
            (job_name, pull_date, status, record_count, committed_at)
        VALUES (%s, %s, 'COMMITTED', %s, NOW())
        ON CONFLICT (job_name, pull_date) DO UPDATE
        SET status = 'COMMITTED',
            record_count = EXCLUDED.record_count,
            committed_at = NOW(),
            error_message = NULL
    """, (job_name, pull_date, n))


def mark_failed(pg: PGClient, job_name: str, pull_date: str, error: str) -> None:
    pg.execute("""
        INSERT INTO arxiv_oai_pull_log
            (job_name, pull_date, status, error_message, started_at)
        VALUES (%s, %s, 'FAILED', %s, NOW())
        ON CONFLICT (job_name, pull_date) DO UPDATE
        SET status = 'FAILED', error_message = EXCLUDED.error_message
    """, (job_name, pull_date, error[:500]))


# ═══════════════════════════════════════════════════════════════════════
# OAI-PMH 采集
# ═══════════════════════════════════════════════════════════════════════

OnPage = Callable[[List[Dict[str, Any]]], int]


def harvest(from_date: str, until_date: str, *, on_page: OnPage, log) -> Dict[str, int]:
    """采集指定日期区间，每页立即交给 on_page 处理。"""
    session = requests.Session()
    params = {
        "verb": "ListRecords",
        "metadataPrefix": "arXivRaw",
        "from": from_date,
        "until": until_date,
    }

    fetched = written = deleted = parse_fail = date_fail = 0
    page = 0

    while True:
        if page > 0:
            time.sleep(RATE_LIMIT_SECONDS)
        page += 1

        log.info(f"第 {page} 页请求中 ...")
        resp = session.get(OAI_URL, params=params, timeout=60)
        resp.raise_for_status()

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as e:
            raise RuntimeError(f"XML 解析失败: {e}") from e

        error_elem = root.find("oai:error", NS)
        if error_elem is not None:
            code = error_elem.attrib.get("code", "unknown")
            if code == "noRecordsMatch":
                log.info("此窗口无记录。")
                break
            raise RuntimeError(f"OAI-PMH 错误 [{code}]: {error_elem.text}")

        records = root.findall(".//oai:record", NS)
        fetched += len(records)
        log.info(f"第 {page} 页返回 {len(records)} 条记录")

        parsed_records: List[Dict[str, Any]] = []
        for rec in records:
            header = rec.find("oai:header", NS)
            if header is not None and header.attrib.get("status") == "deleted":
                deleted += 1
                continue
            parsed = parse_record(rec)
            if parsed is None:
                parse_fail += 1
                continue
            pub_dt = parse_arxiv_date(parsed.get("published_date"))
            if pub_dt is None:
                date_fail += 1
                continue
            parsed["published_date_iso"] = pub_dt.isoformat()
            parsed_records.append(parsed)

        if parsed_records:
            page_written = on_page(parsed_records)
            written += page_written
            log.info(f"第 {page} 页写入 {page_written} 条")

        token_elem = root.find(".//oai:resumptionToken", NS)
        token = token_elem.text.strip() if token_elem is not None and token_elem.text else None
        if not token:
            break
        params = {"verb": "ListRecords", "resumptionToken": token}

    stats = {"fetched": fetched, "written": written, "pages": page,
             "deleted": deleted, "parse_fail": parse_fail, "date_fail": date_fail}
    log.info(f"采集完成：{stats}")
    return stats


# ═══════════════════════════════════════════════════════════════════════
# MinIO on_page
# ═══════════════════════════════════════════════════════════════════════

def make_minio_on_page(client: MinIOClient) -> OnPage:
    def on_page(records: List[Dict[str, Any]]) -> int:
        groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
        for r in records:
            pub_iso = r.get("published_date_iso", "")
            if len(pub_iso) >= 7:
                groups[(pub_iso[:4], pub_iso[5:7])].append(r)

        total = 0
        for (year, month), rows in groups.items():
            object_name = papers_arxiv_oai_state_path(year, month)
            total += client.upsert_jsonl(object_name, rows)
        return total
    return on_page


# ═══════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════

def run(env: str, log) -> None:
    job_name = f"arxiv-oai-pull-{env}"
    target_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

    log.info("=" * 70)
    log.info(f"arXiv OAI-PMH Job | job={job_name} | target={target_date}")
    log.info("=" * 70)

    pg = PGClient(log)
    try:
        ensure_table(pg)
        ensure_initial_record(pg, job_name, DEFAULT_INIT_DATE)

        last = query_last_committed(pg, job_name)
        log.info(f"上次已提交: {last}")

        if last >= target_date:
            log.info("✅ 已追上 target，退出")
            return

        client = MinIOClient(log)
        on_page = make_minio_on_page(client)

        current = date.fromisoformat(last)
        target = date.fromisoformat(target_date)

        while current < target:
            next_day = (current + timedelta(days=1)).isoformat()
            log.info(f"===== 拉取 {next_day} =====")

            t0 = time.time()
            try:
                stats = harvest(next_day, next_day, on_page=on_page, log=log)
                mark_committed(pg, job_name, next_day, stats["written"])
                log.info(f"✅ {next_day} 完成：{stats}")
            except Exception as e:
                log.error(f"❌ {next_day} 失败：{e}")
                mark_failed(pg, job_name, next_day, str(e))
                raise

            elapsed = time.time() - t0
            wait = max(0.0, RATE_LIMIT_SECONDS - elapsed)
            if wait > 0:
                time.sleep(wait)

            current = date.fromisoformat(next_day)

        log.info("✅ 所有日期已推进完成")
    finally:
        pg.close()


def main() -> int:
    env = os.environ.get("ENV")
    if env not in ("test", "prod"):
        raise RuntimeError(f"ENV 必须是 test/prod，当前: {env!r}")
    run(env, setup_logger())
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(1)
