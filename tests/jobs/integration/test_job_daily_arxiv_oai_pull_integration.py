"""job_daily_arxiv_oai_pull 集成测试（完全自包含，无 conftest）"""
import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from common.pg_client import PGClient
from common.logger import setup_logger
from jobs.job_daily_arxiv_oai_pull import (
    ensure_tables, ensure_initial_record,
    mark_completed, mark_failed, next_pull_date, harvest,
    run as oai_run,
)

# ─── 常量 ───
USE_REAL_OAI = os.getenv("OAI_TEST_REAL_API", "0") == "1"
TEST_JOB = "integration-oai-test"


@pytest.fixture
def pg():
    client = PGClient(setup_logger("integration_oai"))
    ensure_tables(client)
    client.execute("DELETE FROM arxiv_oai_pull_log WHERE job_name = %s", (TEST_JOB,))
    yield client
    client.execute("DELETE FROM arxiv_oai_pull_log WHERE job_name = %s", (TEST_JOB,))
    client.close()


@pytest.mark.skipif(not USE_REAL_OAI, reason="OAI_TEST_REAL_API=0")
def test_harvest_real_oai_one_day():
    target = (date.today() - timedelta(days=5)).isoformat()
    kept = []

    def on_page(records):
        kept.extend(records)
        return len(records)

    stats = harvest(target, target, on_page=on_page, log=MagicMock())
    assert stats["pages"] >= 1
    for r in kept[:3]:
        assert "arxiv_id" in r
        assert "title" in r
        assert "published_date_iso" in r


def test_ensure_tables_idempotent(pg):
    ensure_tables(pg)
    ensure_tables(pg)
    row = pg.fetch_one(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name IN ('arxiv_oai_pull_log', 'arxiv_papers')"
    )
    assert row[0] == 2


def test_ensure_initial_record_flow(pg):
    ensure_initial_record(pg, TEST_JOB, "2026-10-01")
    assert pg.fetch_one(
        "SELECT COUNT(*) FROM arxiv_oai_pull_log WHERE job_name = %s", (TEST_JOB,)
    )[0] == 1

    ensure_initial_record(pg, TEST_JOB, "2026-10-01")
    assert pg.fetch_one(
        "SELECT COUNT(*) FROM arxiv_oai_pull_log WHERE job_name = %s", (TEST_JOB,)
    )[0] == 1


def test_mark_completed(pg):
    ensure_initial_record(pg, TEST_JOB, "2026-10-01")
    mark_completed(pg, TEST_JOB, "2026-10-02", 100)

    row = pg.fetch_one(
        "SELECT status, record_count FROM arxiv_oai_pull_log "
        "WHERE job_name = %s AND pull_date = %s",
        (TEST_JOB, "2026-10-02"),
    )
    assert row == ("completed", 100)

    # 取代 query_last_committed：直接查 completed 最大日期
    row = pg.fetch_one(
        "SELECT MAX(pull_date) FROM arxiv_oai_pull_log "
        "WHERE job_name = %s AND status = 'completed'",
        (TEST_JOB,),
    )
    assert str(row[0]) == "2026-10-02"


def test_mark_failed(pg):
    ensure_initial_record(pg, TEST_JOB, "2026-10-01")
    mark_failed(pg, TEST_JOB, "2026-10-02", "boom")

    row = pg.fetch_one(
        "SELECT status, error_message FROM arxiv_oai_pull_log "
        "WHERE job_name = %s AND pull_date = %s",
        (TEST_JOB, "2026-10-02"),
    )
    assert row == ("failed", "boom")


def test_next_pull_date_priority(pg):
    """三态优先级：running > failed > completed+1"""
    ensure_initial_record(pg, TEST_JOB, "2026-10-01")  # completed
    pg.execute(
        "INSERT INTO arxiv_oai_pull_log (job_name, pull_date, status) "
        "VALUES (%s, %s, 'failed')",
        (TEST_JOB, "2026-10-05"),
    )
    pg.execute(
        "INSERT INTO arxiv_oai_pull_log (job_name, pull_date, status) "
        "VALUES (%s, %s, 'running')",
        (TEST_JOB, "2026-10-03"),
    )

    # running 优先
    assert next_pull_date(pg, TEST_JOB, "2026-10-10") == "2026-10-03"

    # 清掉 running 后 failed 优先
    pg.execute(
        "DELETE FROM arxiv_oai_pull_log WHERE job_name = %s AND pull_date = %s",
        (TEST_JOB, "2026-10-03"),
    )
    assert next_pull_date(pg, TEST_JOB, "2026-10-10") == "2026-10-05"

    # 清掉 failed 后用 completed+1
    pg.execute(
        "DELETE FROM arxiv_oai_pull_log WHERE job_name = %s AND pull_date = %s",
        (TEST_JOB, "2026-10-05"),
    )
    assert next_pull_date(pg, TEST_JOB, "2026-10-10") == "2026-10-02"


def test_next_pull_date_caught_up(pg):
    """completed+1 > target → 返回 None"""
    ensure_initial_record(pg, TEST_JOB, "2026-10-10")
    assert next_pull_date(pg, TEST_JOB, "2026-10-10") is None


# ═══════════════════════════════════════════════════════════════════════
# run() 端到端：harvest 失败 → mark_failed → 不 raise → 下次可重试
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_daily_arxiv_oai_pull.MinIOClient")
@patch("jobs.job_daily_arxiv_oai_pull.harvest")
@patch("jobs.job_daily_arxiv_oai_pull.PGClient")
def test_run_failure_recoverable(mock_pg_cls, mock_harvest, mock_minio_cls, pg):
    """端到端：completed(10-01) 起点，harvest 抛 RuntimeError →
       10-02 落库为 failed，run() 不 raise，next_pull_date 能捡到 10-02。"""
    job_name = "arxiv-oai-pull-test"

    original_close = pg.close
    pg.close = lambda: None
    mock_pg_cls.return_value = pg

    pg.execute("DELETE FROM arxiv_oai_pull_log WHERE job_name = %s", (job_name,))
    ensure_initial_record(pg, job_name, "2026-10-01")

    mock_harvest.side_effect = RuntimeError("boom")

    try:
        # 不 raise
        oai_run("test", MagicMock())

        # 失败日期落库为 failed
        row = pg.fetch_one(
            "SELECT status FROM arxiv_oai_pull_log WHERE job_name = %s AND pull_date = %s",
            (job_name, "2026-10-02"),
        )
        assert row is not None
        assert row[0] == "failed"

        # 下次运行能捡到 failed 日期（证明不是"永久跳过"）
        assert next_pull_date(pg, job_name, "2026-10-10") == "2026-10-02"
    finally:
        pg.execute("DELETE FROM arxiv_oai_pull_log WHERE job_name = %s", (job_name,))
        pg.close = original_close
