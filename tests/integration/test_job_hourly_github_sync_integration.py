"""job_hourly_github_sync 集成测试（完全自包含，无 conftest）"""
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from common import config as cfg
from common.pg_client import PGClient
from common.logger import setup_logger
from jobs.job_hourly_github_sync import (
    ensure_table, reset_stale,
    process_repo, run_p0,
    ensure_p2_subtasks,
    StarClient, GraphQLClient, RestPause,
    parse_star_weeks,
    fail_sub, MAX_TRIES,
)

# ─── 常量 ───
USE_REAL_API = os.getenv("GITHUB_TEST_REAL_API", "1") == "1"
TEST_REPOS = ["octocat/Hello-World", "github/gitignore", "cli/cli"]
TEST_DATE = date(2026, 10, 30)
TEST_REPO_ID = 999999998
TEST_FULL_NAME = "test-sync/repo"


def get_token():
    return cfg.GITHUB_TOKEN


@pytest.fixture
def pg():
    client = PGClient(setup_logger("integration_sync"))
    ensure_table(client)
    _cleanup(client)
    real_close = client.close
    client.close = lambda: None
    yield client
    _cleanup(client)
    real_close()


def _cleanup(client):
    client.execute("DELETE FROM job_hourly_github_sync_p0_task WHERE task_date >= %s", (TEST_DATE,))
    client.execute("DELETE FROM job_hourly_github_sync_p2_task")
    client.execute(
        "DELETE FROM job_hourly_github_sync_repo_task WHERE repo_id IN (%s, %s)",
        (TEST_REPO_ID, TEST_REPO_ID + 1),
    )
    for tbl in ("events_stars", "events_forks", "events_issues", "events_prs"):
        client.execute(f"DELETE FROM {tbl} WHERE repo_id = %s", (TEST_REPO_ID,))


# ═══════════════════════════════════════════════════════════════════════
# 真实 API
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not USE_REAL_API, reason="GITHUB_TEST_REAL_API=0")
def test_real_star_api():
    cli = StarClient(get_token())
    payload = cli.fetch_page(TEST_REPOS[0], page=1)
    counts = parse_star_weeks(payload, date(2026, 1, 1), date(2026, 12, 31))
    assert isinstance(counts, dict)


@pytest.mark.skipif(not USE_REAL_API, reason="GITHUB_TEST_REAL_API=0")
def test_real_graphql_api():
    cli = GraphQLClient(get_token())
    query = (
        'query { repository(owner: "octocat", name: "Hello-World") '
        '{ fork: forks(first: 1) { pageInfo { hasNextPage endCursor } '
        'nodes { createdAt } } } }'
    )
    data, errors = cli.execute(query)
    assert errors == []
    assert data.get("repository") is not None


# ═══════════════════════════════════════════════════════════════════════
# 真实 PG：process_repo
# ═══════════════════════════════════════════════════════════════════════

@patch.object(GraphQLClient, "execute")
@patch.object(StarClient, "fetch_page")
def test_process_repo_writes_events(mock_star, mock_gql, pg):
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    mock_star.return_value = [{"week": ts, "days": [0, 5, 10, 0, 0, 0, 0]}]
    mock_gql.return_value = (
        {"repository": {
            "fork": {"pageInfo": {"hasNextPage": False}, "nodes": [{"createdAt": "2026-10-20T10:00:00Z"}]},
            "issue": {"pageInfo": {"hasNextPage": False}, "nodes": []},
            "pr": {"pageInfo": {"hasNextPage": False}, "nodes": []},
        }}, []
    )

    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task 
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to)
        VALUES ('p0', %s, 0, %s, %s, %s, %s) RETURNING id
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))
    sub_id = pg.fetch_one(
        "SELECT id FROM job_hourly_github_sync_repo_task WHERE repo_id = %s",
        (TEST_REPO_ID,),
    )[0]

    sub = (sub_id, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE, {}, "pending")
    process_repo(pg, StarClient("fake"), GraphQLClient("fake"), sub)

    row = pg.fetch_one(
        "SELECT status, progress FROM job_hourly_github_sync_repo_task WHERE id = %s",
        (sub_id,),
    )
    assert row[0] == "completed"
    assert row[1]["star"]["done"] is True
    assert row[1]["fork"]["done"] is True

    stars = pg.fetch_all(
        "SELECT date, count FROM events_stars WHERE repo_id = %s ORDER BY date",
        (TEST_REPO_ID,),
    )
    assert len(stars) == 2
    assert stars[0] == (date(2026, 10, 19), 5)


# ═══════════════════════════════════════════════════════════════════════
# UNIQUE / fail_sub / reset_stale
# ═══════════════════════════════════════════════════════════════════════

def test_repo_task_unique_constraint(pg):
    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task 
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to)
        VALUES ('p0', %s, 0, %s, %s, %s, %s)
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))

    with pytest.raises(Exception) as exc:
        pg.execute("""
            INSERT INTO job_hourly_github_sync_repo_task 
            (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to)
            VALUES ('p0', %s, 0, %s, %s, %s, %s)
        """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))
    assert "unique" in str(exc.value).lower() or "duplicate" in str(exc.value).lower()


def test_fail_sub_reaches_max_tries(pg):
    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task 
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to)
        VALUES ('p0', %s, 0, %s, %s, %s, %s)
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))
    sub_id = pg.fetch_one(
        "SELECT id FROM job_hourly_github_sync_repo_task WHERE repo_id = %s",
        (TEST_REPO_ID,),
    )[0]

    for _ in range(MAX_TRIES):
        fail_sub(pg, sub_id, "boom")

    row = pg.fetch_one(
        "SELECT status, try_number FROM job_hourly_github_sync_repo_task WHERE id = %s",
        (sub_id,),
    )
    assert row == ("failed", MAX_TRIES)


def test_reset_stale(pg):
    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task 
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to,
         status, updated_at)
        VALUES ('p0', %s, 0, %s, %s, %s, %s, 'running', NOW() - INTERVAL '3 hours')
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))

    reset_stale(pg)

    row = pg.fetch_one(
        "SELECT status FROM job_hourly_github_sync_repo_task WHERE repo_id = %s",
        (TEST_REPO_ID,),
    )
    assert row[0] == "pending"


def test_reset_stale_keeps_recent_running(pg):
    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task 
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to,
         status, updated_at)
        VALUES ('p0', %s, 0, %s, %s, %s, %s, 'running', NOW() - INTERVAL '30 minutes')
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))

    reset_stale(pg)

    row = pg.fetch_one(
        "SELECT status FROM job_hourly_github_sync_repo_task WHERE repo_id = %s",
        (TEST_REPO_ID,),
    )
    assert row[0] == "running"


# ═══════════════════════════════════════════════════════════════════════
# P0 状态机
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.BATCH_SIZE", 1)
@patch.object(GraphQLClient, "execute")
@patch.object(StarClient, "fetch_page")
def test_run_p0_pending_to_completed(mock_star, mock_gql, pg):
    mock_star.return_value = []
    mock_gql.return_value = (
        {"repository": {
            "fork": {"pageInfo": {"hasNextPage": False}, "nodes": []},
            "issue": {"pageInfo": {"hasNextPage": False}, "nodes": []},
            "pr": {"pageInfo": {"hasNextPage": False}, "nodes": []},
        }}, []
    )

    pg.execute("""
        INSERT INTO job_hourly_github_sync_p0_task (task_date, status)
        VALUES (%s, 'completed') ON CONFLICT (task_date) DO UPDATE SET status = 'completed'
    """, (TEST_DATE - timedelta(days=1),))

    result = run_p0(pg, StarClient("fake"), GraphQLClient("fake"), TEST_DATE, MagicMock())
    assert result is True

    row = pg.fetch_one(
        "SELECT status FROM job_hourly_github_sync_p0_task WHERE task_date = %s",
        (TEST_DATE,),
    )
    assert row[0] == "completed"


@patch("jobs.job_hourly_github_sync.BATCH_SIZE", 1)
@patch.object(GraphQLClient, "execute")
@patch.object(StarClient, "fetch_page")
def test_run_p0_paused_on_limit(mock_star, mock_gql, pg):
    mock_star.side_effect = RestPause()

    pg.execute("""
        INSERT INTO job_hourly_github_sync_p0_task (task_date, status)
        VALUES (%s, 'completed') ON CONFLICT (task_date) DO UPDATE SET status = 'completed'
    """, (TEST_DATE - timedelta(days=1),))

    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task 
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to, status)
        VALUES ('p0', %s, 0, %s, %s, %s, %s, 'pending')
        ON CONFLICT DO NOTHING
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))

    result = run_p0(pg, StarClient("fake"), GraphQLClient("fake"), TEST_DATE, MagicMock())
    assert result is False

    row = pg.fetch_one(
        "SELECT status FROM job_hourly_github_sync_p0_task WHERE task_date = %s",
        (TEST_DATE,),
    )
    assert row[0] == "paused"


# ═══════════════════════════════════════════════════════════════════════
# P2：SQL smoke + snapshot
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_p2_subtasks_sql_executes(pg):
    snapshot = datetime(2026, 10, 30, tzinfo=timezone.utc)
    try:
        n = ensure_p2_subtasks(pg, round_number=999, snapshot_at=snapshot, limit=10)
        assert n >= 0
    finally:
        pg.execute("DELETE FROM job_hourly_github_sync_repo_task WHERE lane='p2' AND round_number=999")


def test_ensure_p2_subtasks_snapshot_respected(pg):
    pg.execute(
        "DELETE FROM features_repo WHERE repo_id IN (%s, %s)",
        (TEST_REPO_ID, TEST_REPO_ID + 1),
    )
    pg.execute(
        "INSERT INTO features_repo (repo_id, full_name, owner, name, created) "
        "VALUES (%s, %s, 'a', 'b', %s)",
        (TEST_REPO_ID, TEST_FULL_NAME, date(2026, 1, 1)),
    )
    pg.execute(
        "INSERT INTO features_repo (repo_id, full_name, owner, name, created) "
        "VALUES (%s, %s, 'a', 'b', %s)",
        (TEST_REPO_ID + 1, "test-sync/repo2", date(2027, 1, 1)),
    )

    # snapshot 设为 2000-01-01：测试仓库（2026/2027 创建）都应被排除
    # 避免扫描 44 万真实仓库，测试控制在毫秒级
    snapshot = datetime(2000, 1, 1, tzinfo=timezone.utc)
    try:
        ensure_p2_subtasks(pg, round_number=999, snapshot_at=snapshot, limit=10)

        assert pg.fetch_one(
            "SELECT COUNT(*) FROM job_hourly_github_sync_repo_task "
            "WHERE lane='p2' AND round_number=999 AND repo_id = %s",
            (TEST_REPO_ID,),
        )[0] == 0

        assert pg.fetch_one(
            "SELECT COUNT(*) FROM job_hourly_github_sync_repo_task "
            "WHERE lane='p2' AND round_number=999 AND repo_id = %s",
            (TEST_REPO_ID + 1,),
        )[0] == 0
    finally:
        pg.execute("DELETE FROM job_hourly_github_sync_repo_task WHERE lane='p2' AND round_number=999")


def test_ensure_table_idempotent(pg):
    ensure_table(pg)
    ensure_table(pg)
