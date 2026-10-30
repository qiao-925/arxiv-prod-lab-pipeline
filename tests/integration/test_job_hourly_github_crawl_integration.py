"""job_hourly_github_crawl 集成测试（完全自包含，无 conftest）"""
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from common import config as cfg
from common.pg_client import PGClient
from common.logger import setup_logger
from jobs.job_hourly_github_crawl import (
    ensure_table, process_hour, next_crawl_task,
    SearchClient, RestClient, SearchPause, RestPause,
    run as crawl_run,
)

# ─── 常量 ───
USE_REAL_API = os.getenv("GITHUB_TEST_REAL_API", "1") == "1"
TEST_REPOS = ["octocat/Hello-World", "github/gitignore", "cli/cli"]
TEST_DATE = date(2026, 10, 30)
TEST_HOUR = 10
TEST_REPO_ID = 999999999
TEST_FULL_NAME = "test-owner/test-repo"


def get_token():
    return cfg.GITHUB_TOKEN


@pytest.fixture
def pg():
    client = PGClient(setup_logger("integration_crawl"))
    ensure_table(client)
    _cleanup(client)
    real_close = client.close
    client.close = lambda: None
    yield client
    _cleanup(client)
    real_close()


def _cleanup(client):
    client.execute("DELETE FROM job_hourly_github_crawl_task WHERE task_date = %s", (TEST_DATE,))
    client.execute(
        "DELETE FROM features_repo WHERE repo_id IN (%s, %s)",
        (TEST_REPO_ID, TEST_REPO_ID + 1),
    )
    client.execute(
        "DELETE FROM edges_repo_arxiv WHERE repo_id IN (%s, %s)",
        (TEST_REPO_ID, TEST_REPO_ID + 1),
    )


# ═══════════════════════════════════════════════════════════════════════
# 真实 API
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not USE_REAL_API, reason="GITHUB_TEST_REAL_API=0")
def test_real_search_api():
    cli = SearchClient(get_token(), MagicMock())
    hour_start = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
    hour_end = hour_start + timedelta(hours=1)
    items = cli.search(hour_start, hour_end)
    assert isinstance(items, list)
    for item in items[:3]:
        assert "id" in item
        assert "full_name" in item


@pytest.mark.skipif(not USE_REAL_API, reason="GITHUB_TEST_REAL_API=0")
def test_real_readme_api():
    cli = RestClient(get_token(), MagicMock())
    text = cli.fetch_readme(TEST_REPOS[0])
    if text is not None:
        assert isinstance(text, str)


# ═══════════════════════════════════════════════════════════════════════
# 真实 PG：process_hour
# ═══════════════════════════════════════════════════════════════════════

@patch.object(RestClient, "fetch_readme", return_value="arxiv:2310.00826")
@patch.object(SearchClient, "search")
def test_process_hour_writes_to_db(mock_search, mock_readme, pg):
    mock_search.return_value = [
        {"id": TEST_REPO_ID, "full_name": TEST_FULL_NAME, "created_at": "2026-10-01T00:00:00Z"},
    ]
    process_hour(
        pg, SearchClient("fake", MagicMock()), RestClient("fake", MagicMock()),
        TEST_DATE, TEST_HOUR, MagicMock(),
    )

    repo = pg.fetch_one(
        "SELECT full_name, owner, name FROM features_repo WHERE repo_id = %s",
        (TEST_REPO_ID,),
    )
    assert repo == (TEST_FULL_NAME, "test-owner", "test-repo")

    edges = pg.fetch_all(
        "SELECT arxiv_id, added FROM edges_repo_arxiv WHERE repo_id = %s",
        (TEST_REPO_ID,),
    )
    assert len(edges) == 1
    assert edges[0] == ("2310.00826", True)


@patch.object(RestClient, "fetch_readme", return_value=None)
@patch.object(SearchClient, "search")
def test_process_hour_no_readme(mock_search, mock_readme, pg):
    mock_search.return_value = [
        {"id": TEST_REPO_ID, "full_name": TEST_FULL_NAME, "created_at": "2026-10-01T00:00:00Z"},
    ]
    process_hour(
        pg, SearchClient("fake", MagicMock()), RestClient("fake", MagicMock()),
        TEST_DATE, TEST_HOUR, MagicMock(),
    )

    assert pg.fetch_one(
        "SELECT COUNT(*) FROM features_repo WHERE repo_id = %s", (TEST_REPO_ID,)
    )[0] == 1
    assert pg.fetch_one(
        "SELECT COUNT(*) FROM edges_repo_arxiv WHERE repo_id = %s", (TEST_REPO_ID,)
    )[0] == 0


# ═══════════════════════════════════════════════════════════════════════
# run 主循环
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_crawl.process_hour")
@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_marks_paused_on_search_pause(mock_pg_cls, mock_next, mock_process, pg):
    mock_pg_cls.return_value = pg
    pg.execute(
        "INSERT INTO job_hourly_github_crawl_task (task_date, task_hour, status) "
        "VALUES (%s, %s, 'pending')",
        (TEST_DATE, TEST_HOUR),
    )
    mock_next.return_value = (TEST_DATE, TEST_HOUR)
    mock_process.side_effect = SearchPause()

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"):
        crawl_run("test", MagicMock())

    row = pg.fetch_one(
        "SELECT status, error_msg FROM job_hourly_github_crawl_task "
        "WHERE task_date = %s AND task_hour = %s",
        (TEST_DATE, TEST_HOUR),
    )
    assert row[0] == "paused"
    assert "Search" in row[1]


@patch("jobs.job_hourly_github_crawl.process_hour")
@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_marks_paused_on_rest_pause(mock_pg_cls, mock_next, mock_process, pg):
    mock_pg_cls.return_value = pg
    pg.execute(
        "INSERT INTO job_hourly_github_crawl_task (task_date, task_hour, status) "
        "VALUES (%s, %s, 'pending')",
        (TEST_DATE, TEST_HOUR),
    )
    mock_next.return_value = (TEST_DATE, TEST_HOUR)
    mock_process.side_effect = RestPause()

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"):
        crawl_run("test", MagicMock())

    row = pg.fetch_one(
        "SELECT status FROM job_hourly_github_crawl_task "
        "WHERE task_date = %s AND task_hour = %s",
        (TEST_DATE, TEST_HOUR),
    )
    assert row[0] == "paused"


@patch("jobs.job_hourly_github_crawl.process_hour")
@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_marks_completed(mock_pg_cls, mock_next, mock_process, pg):
    mock_pg_cls.return_value = pg
    pg.execute(
        "INSERT INTO job_hourly_github_crawl_task (task_date, task_hour, status) "
        "VALUES (%s, %s, 'pending')",
        (TEST_DATE, TEST_HOUR),
    )
    mock_next.side_effect = [(TEST_DATE, TEST_HOUR), None]
    mock_process.return_value = None

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"):
        crawl_run("test", MagicMock())

    row = pg.fetch_one(
        "SELECT status FROM job_hourly_github_crawl_task "
        "WHERE task_date = %s AND task_hour = %s",
        (TEST_DATE, TEST_HOUR),
    )
    assert row[0] == "completed"


# ═══════════════════════════════════════════════════════════════════════
# DDL / next_crawl_task
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_table_idempotent(pg):
    ensure_table(pg)
    ensure_table(pg)


def test_next_crawl_task_advances(pg):
    pg.execute("DELETE FROM job_hourly_github_crawl_task WHERE status IN ('pending', 'paused')")
    pg.execute(
        "INSERT INTO job_hourly_github_crawl_task (task_date, task_hour, status) "
        "VALUES (%s, 23, 'completed')",
        (TEST_DATE,),
    )
    result = next_crawl_task(pg, TEST_DATE + timedelta(days=1))
    assert result == (TEST_DATE + timedelta(days=1), 0)
