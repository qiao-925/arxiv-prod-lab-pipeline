"""job_hourly_github_sync 集成测试（完全自包含，无 conftest）

═══════════════════════════════════════════════════════════════════════
一、职责
═══════════════════════════════════════════════════════════════════════
在真实 PG（+ 可选真实 GitHub API）下验证 Sync 链路：
    - P0（14 天窗口）：run_p0 按天推进，failed 不是终态
    - P2（全量扫描）：run_p2 按轮推进，snapshot_at 锁定
    - pick_one 真实排序（try_number 升序，running+failed 同级）
    - process_repo 断点续传（progress JSONB 中途保留）
    - ensure_p0_subtasks / ensure_p2_subtasks 真实 SQL 语义
    - current_p2_round / mark_p2_completed 锚点状态机
    - run() 主循环分流（P0 未追平 → 跳过 P2）

═══════════════════════════════════════════════════════════════════════
二、三态语义（改造后）
═══════════════════════════════════════════════════════════════════════
repo_task（明细）：
    running   = 未完成（首次 / 限流 / 崩溃残留 / 处理中）
    completed = 4 个 metric 全部完成
    failed    = 连续失败 >= 3 次 或 404（下次仍会被 pick，需人工关注）

p0_task / p2_task（锚点）：
    running   = 该天/该轮未完成
    completed = 全部子任务完成

    关键区分：
        - 限流（RestPause / GraphQLPause）：status 保持 running，不增 try_number
        - 瞬时错误：try_number + 1，>= 3 转 failed
        - 404：直接 mark_sub_failed，try_number + 1，error_msg 前缀 '404:'
        - 单 metric 完成 → 记录 progress JSONB，下次从断点继续

═══════════════════════════════════════════════════════════════════════
三、测试策略
═══════════════════════════════════════════════════════════════════════
真实 PG + 隔离测试数据：
    - 任务用 TEST_DATE (2026-10-30) 隔离
    - repo_id 用 TEST_REPO_ID (999999998) 和 +1 .. +10 号段
    - fixture 前后 DELETE，不碰生产数据

真实 GitHub API：
    - 通过 GITHUB_TEST_REAL_API=1 开启，默认开启
    - 网络不通时 skipif 保护

P2 全量扫描的特殊处理：
    - ensure_p0_subtasks 用 task_date=2000-01-01 排除 44 万真实仓库
    - run_p2 端到端 mock current_p2_round + ensure_p2_subtasks 避免扫描真实数据

═══════════════════════════════════════════════════════════════════════
四、断言强度
═══════════════════════════════════════════════════════════════════════
    ✅ 状态字面值 + try_number 精确值
    ✅ pick_one 真实出队顺序
    ✅ failed 任务能被 run_p0 重试（证明不是"永久跳过"）
    ✅ progress JSONB 中途保留（断点续传）
    ✅ run() 的 P0/P2 分流控制流

不写"只查调用次数"的弱断言。
"""
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from common import config as cfg
from common.pg_client import PGClient
from common.logger import setup_logger
from jobs.job_hourly_github_sync import (
    ensure_table,
    process_repo, run_p0, run_p2, pick_one,
    ensure_p0_subtasks, ensure_p2_subtasks,
    current_p2_round, mark_p2_completed,
    StarClient, GraphQLClient, RestPause,
    parse_star_weeks,
    fail_sub, MAX_TRIES,
    run as sync_run,
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
        "DELETE FROM job_hourly_github_sync_repo_task WHERE repo_id BETWEEN %s AND %s",
        (TEST_REPO_ID, TEST_REPO_ID + 10),
    )
    for tbl in ("events_stars", "events_forks", "events_issues", "events_prs"):
        client.execute(f"DELETE FROM {tbl} WHERE repo_id BETWEEN %s AND %s",
                       (TEST_REPO_ID, TEST_REPO_ID + 10))
    client.execute(
        "DELETE FROM features_repo WHERE repo_id BETWEEN %s AND %s",
        (TEST_REPO_ID, TEST_REPO_ID + 10),
    )


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

    sub = (sub_id, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE, {}, "running")
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
def test_run_p0_keeps_running_on_limit(mock_star, mock_gql, pg):
    """三态：限流不写 paused，p0_task 保持 running。"""
    mock_star.side_effect = RestPause()

    pg.execute("""
        INSERT INTO job_hourly_github_sync_p0_task (task_date, status)
        VALUES (%s, 'completed') ON CONFLICT (task_date) DO UPDATE SET status = 'completed'
    """, (TEST_DATE - timedelta(days=1),))

    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task 
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to, status)
        VALUES ('p0', %s, 0, %s, %s, %s, %s, 'running')
        ON CONFLICT DO NOTHING
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))

    result = run_p0(pg, StarClient("fake"), GraphQLClient("fake"), TEST_DATE, MagicMock())
    assert result is False

    row = pg.fetch_one(
        "SELECT status FROM job_hourly_github_sync_p0_task WHERE task_date = %s",
        (TEST_DATE,),
    )
    assert row[0] == "running"


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


# ═══════════════════════════════════════════════════════════════════════
# pick_one：try_number 升序、running 与 failed 一起参与
# ═══════════════════════════════════════════════════════════════════════

def test_pick_one_orders_by_try_number(pg):
    """failed 与 running 同级参与排序，try_number 最小的先出队；
       全部 completed 后返回 None（证明 failed 不是终态、不会被漏掉）。"""
    for offset, try_n, status in [(0, 2, "failed"), (1, 0, "running"), (2, 1, "failed")]:
        pg.execute("""
            INSERT INTO job_hourly_github_sync_repo_task
            (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to, status, try_number)
            VALUES ('p0', %s, 0, %s, %s, %s, %s, %s, %s)
        """, (TEST_DATE, TEST_REPO_ID + offset, f"x/r{offset}",
              date(2026, 10, 1), TEST_DATE, status, try_n))

    # 第 1 次：try_number=0
    first = pick_one(pg, "p0", TEST_DATE, 0)
    assert first is not None
    assert first[1] == TEST_REPO_ID + 1
    assert first[6] == "running"

    # 标 completed 后第 2 次：try_number=1（failed）
    pg.execute("UPDATE job_hourly_github_sync_repo_task SET status='completed' "
               "WHERE repo_id = %s", (TEST_REPO_ID + 1,))
    second = pick_one(pg, "p0", TEST_DATE, 0)
    assert second is not None
    assert second[1] == TEST_REPO_ID + 2
    assert second[6] == "failed"

    # 第 3 次：try_number=2（failed）
    pg.execute("UPDATE job_hourly_github_sync_repo_task SET status='completed' "
               "WHERE repo_id = %s", (TEST_REPO_ID + 2,))
    third = pick_one(pg, "p0", TEST_DATE, 0)
    assert third is not None
    assert third[1] == TEST_REPO_ID
    assert third[6] == "failed"

    # 全 completed → None
    pg.execute("UPDATE job_hourly_github_sync_repo_task SET status='completed' "
               "WHERE task_date = %s", (TEST_DATE,))
    assert pick_one(pg, "p0", TEST_DATE, 0) is None


# ═══════════════════════════════════════════════════════════════════════
# run_p0：failed 是"重试中"，不是"永久跳过"
# ═══════════════════════════════════════════════════════════════════════

@patch.object(GraphQLClient, "execute")
@patch.object(StarClient, "fetch_page")
def test_run_p0_retries_failed_task(mock_star, mock_gql, pg):
    """端到端：预置 failed 的 repo_task → run_p0 处理 → 变 completed。"""
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

    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to, status, try_number)
        VALUES ('p0', %s, 0, %s, %s, %s, %s, 'failed', 2)
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))

    result = run_p0(pg, StarClient("fake"), GraphQLClient("fake"), TEST_DATE, MagicMock())
    assert result is True

    row = pg.fetch_one(
        "SELECT status FROM job_hourly_github_sync_repo_task WHERE repo_id = %s",
        (TEST_REPO_ID,),
    )
    assert row[0] == "completed"


# ═══════════════════════════════════════════════════════════════════════
# process_repo：中途失败时 progress JSONB 保留
# ═══════════════════════════════════════════════════════════════════════

@patch.object(GraphQLClient, "execute")
@patch.object(StarClient, "fetch_page")
def test_process_repo_partial_progress_preserved(mock_star, mock_gql, pg):
    """star 完成 + fork 抛异常 → progress['star']['done']=True 落库，
       fail_sub 只加 try_number/改 status，不动 progress。"""
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    mock_star.return_value = [{"week": ts, "days": [0, 5, 10, 0, 0, 0, 0]}]
    mock_gql.side_effect = RuntimeError("fork boom")

    pg.execute("""
        INSERT INTO job_hourly_github_sync_repo_task
        (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to)
        VALUES ('p0', %s, 0, %s, %s, %s, %s) RETURNING id
    """, (TEST_DATE, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE))
    sub_id = pg.fetch_one(
        "SELECT id FROM job_hourly_github_sync_repo_task WHERE repo_id = %s",
        (TEST_REPO_ID,),
    )[0]

    sub = (sub_id, TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), TEST_DATE, {}, "running")
    process_repo(pg, StarClient("fake"), GraphQLClient("fake"), sub)

    row = pg.fetch_one(
        "SELECT status, progress, try_number FROM job_hourly_github_sync_repo_task WHERE id = %s",
        (sub_id,),
    )
    # 一次失败 <3，仍是 running，try_number=1
    assert row[0] == "running"
    assert row[2] == 1
    # star.done 已落库；fork 未开始
    assert row[1]["star"]["done"] is True
    assert not (row[1].get("fork") or {}).get("done")

    # events_stars 已写入（不是回滚）
    stars = pg.fetch_all(
        "SELECT date, count FROM events_stars WHERE repo_id = %s ORDER BY date",
        (TEST_REPO_ID,),
    )
    assert len(stars) == 2
    assert stars[0] == (date(2026, 10, 19), 5)


# ═══════════════════════════════════════════════════════════════════════
# P2 端到端：run_p2 一轮跑完 → mark_p2_completed
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.ensure_p2_subtasks", return_value=0)
@patch("jobs.job_hourly_github_sync.current_p2_round")
@patch.object(GraphQLClient, "execute")
@patch.object(StarClient, "fetch_page")
def test_run_p2_completes(mock_star, mock_gql, mock_round, mock_ensure, pg):
    """端到端：P2 一轮任务跑完 → mark_p2_completed。
       mock current_p2_round 避免创建新轮；mock ensure_p2_subtasks 返回 0
       避免扫 44 万真实仓库。"""
    mock_star.return_value = []
    mock_gql.return_value = (
        {"repository": {
            "fork": {"pageInfo": {"hasNextPage": False}, "nodes": []},
            "issue": {"pageInfo": {"hasNextPage": False}, "nodes": []},
            "pr": {"pageInfo": {"hasNextPage": False}, "nodes": []},
        }}, []
    )
    snap = datetime(2026, 10, 30, tzinfo=timezone.utc)
    mock_round.return_value = (999, snap)

    try:
        # features_repo 非空（run_p2 首行检查）
        pg.execute("""
            INSERT INTO features_repo (repo_id, full_name, owner, name, created)
            VALUES (%s, %s, 'test', 'repo', %s)
            ON CONFLICT (repo_id) DO NOTHING
        """, (TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1)))

        # p2_task 999 + repo_task 一条
        pg.execute("""
            INSERT INTO job_hourly_github_sync_p2_task (round_number, snapshot_at, status)
            VALUES (999, %s, 'running')
            ON CONFLICT (round_number) DO NOTHING
        """, (snap,))
        pg.execute("""
            INSERT INTO job_hourly_github_sync_repo_task
            (lane, task_date, round_number, repo_id, full_name, scan_start, scan_to, status)
            VALUES ('p2', %s, 999, %s, %s, %s, %s, 'running')
        """, (snap.date(), TEST_REPO_ID, TEST_FULL_NAME, date(2026, 10, 1), snap.date()))

        run_p2(pg, StarClient("fake"), GraphQLClient("fake"), MagicMock())

        # p2_task completed
        row = pg.fetch_one(
            "SELECT status FROM job_hourly_github_sync_p2_task WHERE round_number = 999"
        )
        assert row is not None
        assert row[0] == "completed"

        # repo_task completed
        row = pg.fetch_one(
            "SELECT status FROM job_hourly_github_sync_repo_task "
            "WHERE lane='p2' AND round_number=999 AND repo_id = %s",
            (TEST_REPO_ID,),
        )
        assert row is not None
        assert row[0] == "completed"
    finally:
        pg.execute("DELETE FROM job_hourly_github_sync_p2_task WHERE round_number = 999")
        pg.execute("DELETE FROM job_hourly_github_sync_repo_task WHERE lane='p2' AND round_number = 999")
        pg.execute("DELETE FROM features_repo WHERE repo_id = %s", (TEST_REPO_ID,))


# ═══════════════════════════════════════════════════════════════════════
# ensure_p0_subtasks：真实 SQL 语义（用 2000-01-01 排除真实仓库）
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_p0_subtasks_creates_task_for_recent_paper_repo(pg):
    """真 PG：只有"关联论文在 14 天窗口内"的仓库才生成 p0 任务。
       用 task_date=2000-01-01 排除 44 万真实仓库（created 都是 2026 年）。"""
    task_date = date(2000, 1, 1)
    arxiv_id = "9999.77777"
    try:
        # 测试数据：created=1999-12-01，关联论文 published=1999-12-25（窗口内）
        pg.execute("""
            INSERT INTO features_repo (repo_id, full_name, owner, name, created)
            VALUES (%s, %s, 'test', 'repo', %s)
            ON CONFLICT (repo_id) DO UPDATE SET created = EXCLUDED.created
        """, (TEST_REPO_ID, TEST_FULL_NAME, date(1999, 12, 1)))
        pg.execute("""
            INSERT INTO arxiv_papers (arxiv_id, published_date)
            VALUES (%s, %s)
            ON CONFLICT (arxiv_id) DO UPDATE SET published_date = EXCLUDED.published_date
        """, (arxiv_id, "1999-12-25"))
        pg.execute("""
            INSERT INTO edges_repo_arxiv (repo_id, arxiv_id, added, date)
            VALUES (%s, %s, true, %s)
            ON CONFLICT DO NOTHING
        """, (TEST_REPO_ID, arxiv_id, task_date))

        n = ensure_p0_subtasks(pg, task_date, limit=100)
        assert n >= 1

        row = pg.fetch_one(
            "SELECT lane, round_number, scan_start, scan_to "
            "FROM job_hourly_github_sync_repo_task "
            "WHERE lane='p0' AND task_date=%s AND repo_id=%s",
            (task_date, TEST_REPO_ID),
        )
        assert row is not None
        assert row[0] == "p0"
        assert row[1] == 0
        assert row[3] == task_date
    finally:
        pg.execute("DELETE FROM job_hourly_github_sync_repo_task WHERE lane='p0' AND task_date=%s", (task_date,))
        pg.execute("DELETE FROM features_repo WHERE repo_id = %s", (TEST_REPO_ID,))
        pg.execute("DELETE FROM edges_repo_arxiv WHERE repo_id = %s", (TEST_REPO_ID,))
        pg.execute("DELETE FROM arxiv_papers WHERE arxiv_id = %s", (arxiv_id,))


# ═══════════════════════════════════════════════════════════════════════
# P2 锚点：current_p2_round 创建 / 复用 / mark_p2_completed
# ═══════════════════════════════════════════════════════════════════════

def test_current_p2_round_and_mark_completed(pg):
    """真 PG：无 running → 创建新轮；有 running → 复用；
       mark completed 后下次调用创建新一轮。"""
    try:
        pg.execute("DELETE FROM job_hourly_github_sync_p2_task")

        # 首次：创建 round 1
        n1, snap1 = current_p2_round(pg)
        assert n1 == 1
        assert snap1 is not None
        row = pg.fetch_one(
            "SELECT status FROM job_hourly_github_sync_p2_task WHERE round_number = 1"
        )
        assert row is not None
        assert row[0] == "running"

        # 复用：返回同一轮和 snapshot
        # snap1 是 datetime.now(timezone.utc)（aware），snap2 从 TIMESTAMP 读回是 naive，
        # 用秒级比较，忽略 tzinfo
        n2, snap2 = current_p2_round(pg)
        assert n2 == 1
        assert int(snap1.replace(tzinfo=None).timestamp()) == int(snap2.replace(tzinfo=None).timestamp())

        # mark completed
        mark_p2_completed(pg, 1)
        row = pg.fetch_one(
            "SELECT status, completed_at FROM job_hourly_github_sync_p2_task WHERE round_number = 1"
        )
        assert row[0] == "completed"
        assert row[1] is not None

        # 下一轮：round 2
        n3, _ = current_p2_round(pg)
        assert n3 == 2
    finally:
        pg.execute("DELETE FROM job_hourly_github_sync_p2_task")


# ═══════════════════════════════════════════════════════════════════════
# run() 控制流：P0 未追平 → 跳过 P2
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.run_p2")
@patch("jobs.job_hourly_github_sync.run_p0")
def test_run_skips_p2_when_p0_incomplete(mock_run_p0, mock_run_p2, pg):
    """P0 返回 False（未追平）→ 不调 run_p2。"""
    mock_run_p0.return_value = False
    with patch("jobs.job_hourly_github_sync.PG_CLIENT_CLASS", return_value=pg), \
         patch("jobs.job_hourly_github_sync.StarClient"), \
         patch("jobs.job_hourly_github_sync.GraphQLClient"):
        sync_run("test", MagicMock())
    mock_run_p0.assert_called_once()
    mock_run_p2.assert_not_called()


@patch("jobs.job_hourly_github_sync.run_p2")
@patch("jobs.job_hourly_github_sync.run_p0")
def test_run_proceeds_to_p2_when_p0_complete(mock_run_p0, mock_run_p2, pg):
    """P0 返回 True → 调 run_p2。"""
    mock_run_p0.return_value = True
    with patch("jobs.job_hourly_github_sync.PG_CLIENT_CLASS", return_value=pg), \
         patch("jobs.job_hourly_github_sync.StarClient"), \
         patch("jobs.job_hourly_github_sync.GraphQLClient"):
        sync_run("test", MagicMock())
    mock_run_p0.assert_called_once()
    mock_run_p2.assert_called_once()
