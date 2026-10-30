"""job_hourly_github_sync 测试套件（补测版）

覆盖：
    - 纯函数解析
    - 客户端限流（Star / GraphQL）
    - 锚点推进（P0 / P2）
    - process_repo 主流程
    - _process_star / _process_graphql 翻页 + 进度
    - run_p0 / run_p2 主循环状态机
    - upsert_events / fail_sub 边界
"""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from jobs.job_hourly_github_sync import (
    AuthError,
    GraphQLPause,
    NotFoundError,
    RestPause,
    StarClient,
    GraphQLClient,
    build_graphql_query,
    current_p0_date,
    current_p2_round,
    ensure_p0_subtasks,
    ensure_p2_subtasks,
    parse_connection,
    parse_star_weeks,
    process_repo,
    star_has_more,
    upsert_events,
    fail_sub,
    mark_p0,
    mark_p2_completed,
    _process_star,
    _process_graphql,
    run_p0,
    run_p2,
)


# ═══════════════════════════════════════════════════════════════════════
# 1. 纯函数：star / GraphQL 解析
# ═══════════════════════════════════════════════════════════════════════

def test_parse_star_weeks():
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    payload = [{"week": ts, "days": [0, 12, 7, 0, 0, 0, 0]}]
    counts = parse_star_weeks(payload, date(2026, 10, 19), date(2026, 10, 20))
    assert counts == {date(2026, 10, 19): 12, date(2026, 10, 20): 7}


def test_star_has_more():
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    assert star_has_more([{"week": ts}] * 29, date(2026, 1, 1)) is False
    assert star_has_more([{"week": ts}] * 30, date(2026, 10, 17)) is True
    assert star_has_more([{"week": ts}] * 30, date(2026, 10, 18)) is False


def test_parse_connection():
    conn = {
        "pageInfo": {"hasNextPage": True, "endCursor": "CUR"},
        "nodes": [
            {"createdAt": "2026-10-23T10:00:00Z"},
            {"createdAt": "2026-06-01T10:00:00Z"},
        ],
    }
    parsed = parse_connection(conn, date(2026, 6, 17), date(2026, 10, 23))
    assert parsed["counts"] == {date(2026, 10, 23): 1}
    assert parsed["oldest"] == date(2026, 6, 1)
    assert parsed["has_next"] is True
    assert parsed["end_cursor"] == "CUR"


def test_build_graphql_query():
    q = build_graphql_query("owner/repo", "fork", cursor="CUR1")
    assert 'repository(owner: "owner", name: "repo")' in q
    assert 'forks(first: 100, orderBy: {field: CREATED_AT, direction: DESC}, after: "CUR1")' in q


# ═══════════════════════════════════════════════════════════════════════
# 2. 客户端限流
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.time.sleep", lambda _: None)
def test_star_client_rate_limit():
    cli = StarClient("fake")
    cli.remaining = 0
    with pytest.raises(RestPause):
        cli.fetch_page("o/r", 1)


@patch("jobs.job_hourly_github_sync.requests.Session.post")
def test_graphql_client_rate_limit(mock_post):
    cli = GraphQLClient("fake")
    mock_resp = MagicMock()
    mock_resp.status_code = 403
    mock_post.return_value = mock_resp
    with pytest.raises(GraphQLPause):
        cli.execute("query {}")


# ═══════════════════════════════════════════════════════════════════════
# 3. 锚点推进
# ═══════════════════════════════════════════════════════════════════════

def test_current_p0_date_paused():
    pg = MagicMock()
    pg.fetch_one.return_value = (date(2026, 10, 1),)
    assert current_p0_date(pg, date(2026, 10, 5)) == date(2026, 10, 1)


def test_current_p0_date_next():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, (date(2026, 10, 3),)]
    assert current_p0_date(pg, date(2026, 10, 5)) == date(2026, 10, 4)


def test_current_p0_date_caught_up():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, (date(2026, 10, 5),)]
    assert current_p0_date(pg, date(2026, 10, 5)) is None


def test_current_p2_round_create_new():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, (0,)]
    num, snap = current_p2_round(pg)
    assert num == 1
    assert isinstance(snap, datetime)


def test_current_p2_round_reuses_running():
    pg = MagicMock()
    snap = datetime(2026, 10, 1, tzinfo=timezone.utc)
    pg.fetch_one.return_value = (3, snap)
    num, got = current_p2_round(pg)
    assert num == 3
    assert got == snap


# ═══════════════════════════════════════════════════════════════════════
# 4. 任务生成 SQL
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_p0_subtasks():
    pg = MagicMock()
    ensure_p0_subtasks(pg, date(2026, 10, 1), limit=100)
    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "INSERT INTO job_hourly_github_sync_repo_task" in sql
    assert "lane = 'p0'" in sql
    assert params["limit"] == 100


def test_ensure_p2_subtasks():
    pg = MagicMock()
    snap_at = datetime(2026, 10, 1, tzinfo=timezone.utc)
    ensure_p2_subtasks(pg, 1, snap_at, limit=100)
    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "INSERT INTO job_hourly_github_sync_repo_task" in sql
    assert "lane = 'p2'" in sql
    assert params["round_number"] == 1
    assert params["snapshot_date"] == date(2026, 10, 1)


# ═══════════════════════════════════════════════════════════════════════
# 5. process_repo 主流程
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_repo_404(mock_upsert):
    pg = MagicMock()
    star_cli = MagicMock()
    gql_cli = MagicMock()
    star_cli.fetch_page.side_effect = NotFoundError("404")
    sub = (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "running")

    process_repo(pg, star_cli, gql_cli, sub)

    pg.execute.assert_any_call(
        "UPDATE job_hourly_github_sync_repo_task SET status='inactive', error_msg=%s, updated_at=NOW() WHERE id=%s",
        ("star 404: o/r", 1),
    )


@patch("jobs.job_hourly_github_sync.upsert_events")
@patch("jobs.job_hourly_github_sync.save_progress")
def test_process_repo_rest_pause(mock_save, mock_upsert):
    pg = MagicMock()
    star_cli = MagicMock()
    gql_cli = MagicMock()
    star_cli.fetch_page.side_effect = RestPause()
    sub = (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "running")

    with pytest.raises(RestPause):
        process_repo(pg, star_cli, gql_cli, sub)

    pg.execute.assert_any_call(
        "UPDATE job_hourly_github_sync_repo_task SET status='paused', updated_at=NOW() WHERE id=%s",
        (1,),
    )


@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_repo_generic_exception_fails(mock_upsert):
    pg = MagicMock()
    star_cli = MagicMock()
    gql_cli = MagicMock()
    star_cli.fetch_page.side_effect = RuntimeError("boom")
    sub = (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "running")

    process_repo(pg, star_cli, gql_cli, sub)

    # fail_sub 被调用（status 是 pending 或 failed 由 try_number 决定）
    found = False
    for call in pg.execute.call_args_list:
        if "try_number = try_number + 1" in call[0][0]:
            found = True
            break
    assert found


# ═══════════════════════════════════════════════════════════════════════
# 6. _process_star 翻页 + 进度
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.save_progress")
@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_star_single_page(mock_upsert, mock_save):
    pg = MagicMock()
    cli = MagicMock()
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    # 少于 30 周 → 不翻页
    cli.fetch_page.return_value = [{"week": ts, "days": [1, 0, 0, 0, 0, 0, 0]}]
    progress = {}

    _process_star(pg, cli, 1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), progress)

    assert progress["star"]["done"] is True
    assert mock_save.call_count >= 1


@patch("jobs.job_hourly_github_sync.save_progress")
@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_star_multi_page(mock_upsert, mock_save):
    pg = MagicMock()
    cli = MagicMock()
    ts_old = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    ts_new = int(datetime(2026, 10, 25, tzinfo=timezone.utc).timestamp())
    # 第一次满 30 周 → 翻页；第二次少量 → 结束
    cli.fetch_page.side_effect = [
        [{"week": ts_new, "days": [0] * 7}] * 30,
        [{"week": ts_old, "days": [0] * 7}],
    ]
    progress = {}

    _process_star(pg, cli, 1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), progress)

    assert progress["star"]["done"] is True
    assert cli.fetch_page.call_count == 2


@patch("jobs.job_hourly_github_sync.fail_sub")
@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_star_page_limit(mock_upsert, mock_fail):
    pg = MagicMock()
    cli = MagicMock()
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    # 一直返回满页 → 超过 MAX_PAGES
    cli.fetch_page.return_value = [{"week": ts, "days": [0] * 7}] * 30
    progress = {"star": {"page": 100}}

    from jobs.job_hourly_github_sync import SubTaskDone
    with pytest.raises(SubTaskDone):
        _process_star(pg, cli, 1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), progress)

    mock_fail.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════
# 7. _process_graphql 翻页 + 进度
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.save_progress")
@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_graphql_single_page(mock_upsert, mock_save):
    pg = MagicMock()
    cli = MagicMock()
    cli.execute.return_value = (
        {"repository": {"fork": {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [{"createdAt": "2026-10-20T10:00:00Z"}],
        }}},
        [],
    )
    progress = {}

    _process_graphql(pg, cli, 1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), "fork", progress)

    assert progress["fork"]["done"] is True


@patch("jobs.job_hourly_github_sync.save_progress")
@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_graphql_repo_missing(mock_upsert, mock_save):
    pg = MagicMock()
    cli = MagicMock()
    cli.execute.return_value = ({"repository": None}, [])
    progress = {}

    from jobs.job_hourly_github_sync import SubTaskDone
    with pytest.raises(SubTaskDone):
        _process_graphql(pg, cli, 1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), "fork", progress)

    # 标记了 inactive
    found = any("inactive" in str(c[0][0]) for c in pg.execute.call_args_list)
    assert found


# ═══════════════════════════════════════════════════════════════════════
# 8. upsert_events / fail_sub 边界
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.execute_values")
def test_upsert_events_empty(mock_execute):
    cur = MagicMock()
    upsert_events(cur, "star", 123, {})
    mock_execute.assert_not_called()


@patch("jobs.job_hourly_github_sync.execute_values")
def test_upsert_events_with_data(mock_execute):
    cur = MagicMock()
    counts = {date(2026, 10, 1): 5, date(2026, 10, 2): 0}
    upsert_events(cur, "star", 123, counts)
    mock_execute.assert_called_once()
    rows = mock_execute.call_args[0][2]
    assert len(rows) == 1  # 0 被过滤


def test_fail_sub_reaches_max_tries():
    pg = MagicMock()
    # 模拟 try_number = MAX_TRIES - 1 时的失败
    pg.execute.side_effect = None
    fail_sub(pg, 1, "boom")
    sql = pg.execute.call_args[0][0]
    assert "try_number + 1 >= %s" in sql
    params = pg.execute.call_args[0][1]
    assert params[0] == 3  # MAX_TRIES


def test_mark_p0_upsert():
    pg = MagicMock()
    mark_p0(pg, date(2026, 10, 1), "running")
    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "ON CONFLICT (task_date) DO UPDATE" in sql
    assert params == (date(2026, 10, 1), "running")


def test_mark_p2_completed():
    pg = MagicMock()
    mark_p2_completed(pg, 5)
    sql = pg.execute.call_args[0][0]
    assert "status = 'completed'" in sql
    assert pg.execute.call_args[0][1] == (5,)


# ═══════════════════════════════════════════════════════════════════════
# 9. run_p0 / run_p2 主循环状态机
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.mark_p0")
@patch("jobs.job_hourly_github_sync.process_repo")
@patch("jobs.job_hourly_github_sync.ensure_p0_subtasks")
@patch("jobs.job_hourly_github_sync.pick_one")
@patch("jobs.job_hourly_github_sync.current_p0_date")
def test_run_p0_completes(mock_current, mock_pick, mock_ensure, mock_process, mock_mark):
    """P0 一天跑完后标记 completed，推进到下一天，直到 None。"""
    pg = MagicMock()
    log = MagicMock()
    star_cli = MagicMock()
    gql_cli = MagicMock()

    mock_current.side_effect = [date(2026, 10, 1), None]
    mock_pick.side_effect = [(1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "pending"), None]
    mock_ensure.return_value = 0
    mock_process.return_value = None

    result = run_p0(pg, star_cli, gql_cli, date(2026, 10, 5), log)

    assert result is True
    mock_mark.assert_any_call(pg, date(2026, 10, 1), "running")
    mock_mark.assert_any_call(pg, date(2026, 10, 1), "completed")


@patch("jobs.job_hourly_github_sync.mark_p0")
@patch("jobs.job_hourly_github_sync.process_repo")
@patch("jobs.job_hourly_github_sync.ensure_p0_subtasks")
@patch("jobs.job_hourly_github_sync.pick_one")
@patch("jobs.job_hourly_github_sync.current_p0_date")
def test_run_p0_paused(mock_current, mock_pick, mock_ensure, mock_process, mock_mark):
    """process_repo 抛 RestPause 时，run_p0 返回 False 并标记 paused。"""
    pg = MagicMock()
    log = MagicMock()
    star_cli = MagicMock()
    gql_cli = MagicMock()

    mock_current.return_value = date(2026, 10, 1)
    mock_pick.return_value = (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "pending")
    mock_process.side_effect = RestPause()

    result = run_p0(pg, star_cli, gql_cli, date(2026, 10, 5), log)

    assert result is False
    mock_mark.assert_any_call(pg, date(2026, 10, 1), "paused")


@patch("jobs.job_hourly_github_sync.log")
def test_run_p2_empty_repo(mock_log):
    """features_repo 为空 → P2 直接跳过。"""
    pg = MagicMock()
    pg.fetch_one.return_value = None

    run_p2(pg, MagicMock(), MagicMock(), MagicMock())

    mock_log.info.assert_called_with("features_repo 为空，P2 跳过")


@patch("jobs.job_hourly_github_sync.mark_p2_completed")
@patch("jobs.job_hourly_github_sync.ensure_p2_subtasks")
@patch("jobs.job_hourly_github_sync.pick_one")
@patch("jobs.job_hourly_github_sync.current_p2_round")
def test_run_p2_completes(mock_round, mock_pick, mock_ensure, mock_mark):
    """当前轮无 pending 且 ensure 返回 0 → mark completed。"""
    pg = MagicMock()
    # 第一次 fetch_one 检查 features_repo 非空
    pg.fetch_one.return_value = (1,)
    snap = datetime(2026, 10, 1, tzinfo=timezone.utc)
    mock_round.return_value = (1, snap)
    mock_pick.return_value = None
    mock_ensure.return_value = 0

    run_p2(pg, MagicMock(), MagicMock(), MagicMock())

    mock_mark.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════
# 10. 冒烟
# ═══════════════════════════════════════════════════════════════════════

def test_smoke_imports():
    import jobs.job_hourly_github_sync as mod
    for name in ("run", "run_p0", "run_p2", "process_repo", "verify", "main"):
        assert hasattr(mod, name)
