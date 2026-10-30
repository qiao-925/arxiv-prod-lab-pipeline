"""job_daily_arxiv_oai_pull 集成测试（完全自包含，无 conftest）"""
import os
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from common.pg_client import PGClient
from common.logger import setup_logger
from jobs.job_daily_arxiv_oai_pull import (
    ensure_tables, ensure_initial_record, query_last_committed,
    mark_committed, harvest,
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


def test_mark_committed_and_query(pg):
    ensure_initial_record(pg, TEST_JOB, "2026-10-01")
    mark_committed(pg, TEST_JOB, "2026-10-02", 100)
    assert query_last_committed(pg, TEST_JOB) == "2026-10-02"

    row = pg.fetch_one(
        "SELECT status, record_count FROM arxiv_oai_pull_log "
        "WHERE job_name = %s AND pull_date = %s",
        (TEST_JOB, "2026-10-02"),
    )
    assert row == ("COMMITTED", 100)
