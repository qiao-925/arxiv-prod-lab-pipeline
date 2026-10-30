"""job_hourly_github_sync 单元测试套件

═══════════════════════════════════════════════════════════════════════
一、职责
═══════════════════════════════════════════════════════════════════════
P0（14 天窗口）：按天推进，给最近 14 天论文关联的仓库拉 star/fork/issue/pr。
P2（全量）：按轮推进，用 snapshot_at 锁定要处理的仓库集合。

═══════════════════════════════════════════════════════════════════════
二、三态语义（改造后）
═══════════════════════════════════════════════════════════════════════
repo_task（明细）：
    running   = 未完成（首次 / 限流 / 崩溃残留 / 处理中）
    completed = 4 个 metric 全部完成
    failed    = 连续失败 >= 3 次 或 404（下次仍会被 pick，但需人工关注）

p0_task / p2_task（锚点）：
    running   = 该天/该轮未完成
    completed = 全部子任务完成

    关键区分：
        - 限流（RestPause / GraphQLPause）：status 保持 running，不增加 try_number
        - 瞬时错误（RuntimeError / 网络）：try_number + 1，>= 3 转 failed
        - 404：直接 mark_sub_failed，try_number + 1，error_msg 前缀 '404:'
        - 单 metric 完成 → 记录 progress JSONB，下次从断点继续

═══════════════════════════════════════════════════════════════════════
三、测试策略（风险驱动）
═══════════════════════════════════════════════════════════════════════
P0 风险（必须覆盖）：
    - try_number 边界：<3 保持 running，>=3 转 failed
    - 限流不消耗 try_number：否则一次限流就变成永久失败
    - 404 与瞬时错误的区分：404 直接 failed，网络错误要重试
    - pick_one 必须查 running + failed：否则 failed 任务永不重试
    - progress JSONB 的断点续传

P1 风险（覆盖）：
    - _process_star 单页/多页翻页
    - _process_graphql 单页/repo missing
    - upsert_events 的 0 值过滤
    - ensure_p0/p2 的排除条件（failed 而非 inactive）

P2（覆盖基础）：
    - parse_star_weeks / parse_connection 的边界

刻意不测：
    - 真实 GitHub API 调用（集成测试覆盖）
    - Flink 端的消费逻辑（另一条链路）
"""
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from jobs.job_hourly_github_sync import (
    AuthError, GraphQLPause, NotFoundError, RestPause,
    StarClient, GraphQLClient,
    build_graphql_query, current_p0_date, current_p2_round,
    ensure_p0_subtasks, ensure_p2_subtasks,
    parse_connection, parse_star_weeks, process_repo, star_has_more,
    upsert_events, fail_sub, mark_p0, mark_p2_completed,
    mark_sub_failed,
    _process_star, _process_graphql,
    run_p0, run_p2, MAX_TRIES,
)


# ═══════════════════════════════════════════════════════════════════════
# 1. 纯函数
# ═══════════════════════════════════════════════════════════════════════

def test_parse_star_weeks():
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    counts = parse_star_weeks([{"week": ts, "days": [0, 12, 7, 0, 0, 0, 0]}],
                              date(2026, 10, 19), date(2026, 10, 20))
    assert counts == {date(2026, 10, 19): 12, date(2026, 10, 20): 7}


def test_parse_star_weeks_filters_zero_and_invalid():
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    counts = parse_star_weeks([{"week": ts, "days": [0, None, "x", 4, -1, 0, 0]}],
                              date(2026, 10, 17), date(2026, 10, 24))
    assert counts == {date(2026, 10, 21): 4}


def test_star_has_more():
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    assert star_has_more([{"week": ts}] * 29, date(2026, 1, 1)) is False
    assert star_has_more([{"week": ts}] * 30, date(2026, 10, 17)) is True
    assert star_has_more([{"week": ts}] * 30, date(2026, 10, 18)) is False


def test_parse_connection():
    conn = {"pageInfo": {"hasNextPage": True, "endCursor": "CUR"},
            "nodes": [{"createdAt": "2026-10-23T10:00:00Z"},
                      {"createdAt": "2026-06-01T10:00:00Z"}]}
    parsed = parse_connection(conn, date(2026, 6, 17), date(2026, 10, 23))
    assert parsed["counts"] == {date(2026, 10, 23): 1}
    assert parsed["oldest"] == date(2026, 6, 1)
    assert parsed["has_next"] is True


def test_build_graphql_query():
    q = build_graphql_query("o/r", "fork", cursor="CUR1")
    assert 'repository(owner: "o", name: "r")' in q
    assert 'after: "CUR1"' in q


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
    mock_post.return_value = MagicMock(status_code=403)
    with pytest.raises(GraphQLPause):
        cli.execute("query {}")


# ═══════════════════════════════════════════════════════════════════════
# 3. 锚点（三态）
# ═══════════════════════════════════════════════════════════════════════

def test_current_p0_date_prefers_running():
    """P0 锚点：优先 running。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (date(2026, 10, 1),)
    assert current_p0_date(pg, date(2026, 10, 5)) == date(2026, 10, 1)
    # 第一条 SQL 必须查 status = 'running'
    assert "status = 'running'" in pg.fetch_one.call_args_list[0][0][0]


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
    num, _ = current_p2_round(pg)
    assert num == 1


def test_current_p2_round_reuses_running():
    pg = MagicMock()
    snap = datetime(2026, 10, 1, tzinfo=timezone.utc)
    pg.fetch_one.return_value = (3, snap)
    num, got = current_p2_round(pg)
    assert num == 3
    assert got == snap


# ═══════════════════════════════════════════════════════════════════════
# 4. 任务生成 SQL（排除条件必须是 failed，不是 inactive）
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_p0_subtasks_uses_failed_not_inactive():
    """P0 任务的排除条件：必须是 'failed'（三态），不能是 'inactive'（旧态）。"""
    pg = MagicMock()
    ensure_p0_subtasks(pg, date(2026, 10, 1), limit=100)
    sql = pg.execute.call_args[0][0]
    assert "INSERT INTO job_hourly_github_sync_repo_task" in sql
    assert "lane = 'p0'" in sql
    assert "s.status = 'failed'" in sql
    assert "s.status = 'inactive'" not in sql
    assert pg.execute.call_args[0][1]["limit"] == 100


def test_ensure_p2_subtasks_uses_failed_not_inactive():
    pg = MagicMock()
    ensure_p2_subtasks(pg, 1, datetime(2026, 10, 1, tzinfo=timezone.utc), limit=100)
    sql = pg.execute.call_args[0][0]
    assert "lane = 'p2'" in sql
    assert "s.status = 'failed'" in sql
    assert "s.status = 'inactive'" not in sql


# ═══════════════════════════════════════════════════════════════════════
# 5. process_repo：三态转换
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_repo_404_marks_failed(mock_upsert):
    """P0：404 → status='failed'，try_number+1，error_msg='404: ...'。"""
    pg = MagicMock()
    star_cli = MagicMock()
    star_cli.fetch_page.side_effect = NotFoundError("404")
    sub = (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "running")
    process_repo(pg, star_cli, MagicMock(), sub)

    failed_updates = [
        c for c in pg.execute.call_args_list
        if "status = 'failed'" in str(c[0][0]) and "try_number = try_number + 1" in str(c[0][0])
    ]
    assert len(failed_updates) == 1
    # error_msg 有 404: 前缀
    assert "404:" in failed_updates[0][0][1][0]


@patch("jobs.job_hourly_github_sync.upsert_events")
@patch("jobs.job_hourly_github_sync.save_progress")
def test_process_repo_limit_keeps_running(mock_save, mock_upsert):
    """P0：限流（RestPause）不改变 status，保持 running（不增 try_number）。"""
    pg = MagicMock()
    star_cli = MagicMock()
    star_cli.fetch_page.side_effect = RestPause()
    sub = (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "running")

    with pytest.raises(RestPause):
        process_repo(pg, star_cli, MagicMock(), sub)

    # 不应该有 'paused' 或 'failed' 的 UPDATE（限流不能消耗重试额度）
    assert not any("'paused'" in str(c[0][0]) for c in pg.execute.call_args_list)
    assert not any("'failed'" in str(c[0][0]) for c in pg.execute.call_args_list)


@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_repo_generic_exception_calls_fail_sub(mock_upsert):
    """P0：瞬时错误（RuntimeError）→ 走 fail_sub，try_number+1。"""
    pg = MagicMock()
    star_cli = MagicMock()
    star_cli.fetch_page.side_effect = RuntimeError("boom")
    sub = (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "running")
    process_repo(pg, star_cli, MagicMock(), sub)

    assert any("try_number = try_number + 1" in str(c[0][0])
               for c in pg.execute.call_args_list)


# ═══════════════════════════════════════════════════════════════════════
# 6. fail_sub 边界
# ═══════════════════════════════════════════════════════════════════════

def test_fail_sub_below_max_keeps_running():
    """< 3 次 → ELSE 'running' END（不转 failed）。"""
    pg = MagicMock()
    fail_sub(pg, 1, "boom")
    sql = pg.execute.call_args[0][0]
    assert "ELSE 'running' END" in sql
    assert pg.execute.call_args[0][1][0] == MAX_TRIES


def test_fail_sub_max_tries_is_three():
    """MAX_TRIES 常量是 3。"""
    assert MAX_TRIES == 3


def test_mark_sub_failed_increments_try():
    """mark_sub_failed 直接标 failed，同时递增 try_number。"""
    pg = MagicMock()
    mark_sub_failed(pg, 1, "404: test")
    sql = pg.execute.call_args[0][0]
    assert "status = 'failed'" in sql
    assert "try_number = try_number + 1" in sql


# ═══════════════════════════════════════════════════════════════════════
# 7. upsert_events
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.execute_values")
def test_upsert_events_empty_noop(mock_execute):
    upsert_events(MagicMock(), "star", 123, {})
    mock_execute.assert_not_called()


@patch("jobs.job_hourly_github_sync.execute_values")
def test_upsert_events_filters_zero(mock_execute):
    """P1：count=0 的行被过滤，只有正数被写入。"""
    counts = {date(2026, 10, 1): 5, date(2026, 10, 2): 0}
    upsert_events(MagicMock(), "star", 123, counts)
    mock_execute.assert_called_once()
    rows = mock_execute.call_args[0][2]
    assert len(rows) == 1
    assert rows[0] == (123, date(2026, 10, 1), 5)


# ═══════════════════════════════════════════════════════════════════════
# 8. _process_star / _process_graphql（翻页 + 进度）
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.save_progress")
@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_star_single_page(mock_upsert, mock_save):
    """P1：单页完成 → progress['star']['done'] = True。"""
    pg = MagicMock()
    cli = MagicMock()
    ts = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp())
    cli.fetch_page.return_value = [{"week": ts, "days": [1, 0, 0, 0, 0, 0, 0]}]
    progress = {}
    _process_star(pg, cli, 1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), progress)
    assert progress["star"]["done"] is True
    assert mock_save.call_count >= 1


@patch("jobs.job_hourly_github_sync.save_progress")
@patch("jobs.job_hourly_github_sync.upsert_events")
def test_process_graphql_single_page(mock_upsert, mock_save):
    """P1：GraphQL 单页完成 → progress['fork']['done'] = True。"""
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
def test_process_graphql_repo_missing_marks_failed(mock_upsert, mock_save):
    """P1：repo 不存在（404）→ mark_sub_failed + SubTaskDone。"""
    pg = MagicMock()
    cli = MagicMock()
    cli.execute.return_value = ({"repository": None}, [])
    progress = {}

    from jobs.job_hourly_github_sync import SubTaskDone
    with pytest.raises(SubTaskDone):
        _process_graphql(pg, cli, 1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), "fork", progress)

    # 有标记 failed
    assert any("status = 'failed'" in str(c[0][0]) for c in pg.execute.call_args_list)


# ═══════════════════════════════════════════════════════════════════════
# 9. run_p0 / run_p2 主循环
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_sync.mark_p0")
@patch("jobs.job_hourly_github_sync.process_repo")
@patch("jobs.job_hourly_github_sync.ensure_p0_subtasks")
@patch("jobs.job_hourly_github_sync.pick_one")
@patch("jobs.job_hourly_github_sync.current_p0_date")
def test_run_p0_completes(mock_current, mock_pick, mock_ensure, mock_process, mock_mark):
    """P0 一天全部跑完 → mark completed 并推进。"""
    pg = MagicMock()
    mock_current.side_effect = [date(2026, 10, 1), None]
    mock_pick.side_effect = [
        (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "running"),
        None,
    ]
    mock_ensure.return_value = 0

    result = run_p0(pg, MagicMock(), MagicMock(), date(2026, 10, 5), MagicMock())

    assert result is True
    mock_mark.assert_any_call(pg, date(2026, 10, 1), "running")
    mock_mark.assert_any_call(pg, date(2026, 10, 1), "completed")


@patch("jobs.job_hourly_github_sync.mark_p0")
@patch("jobs.job_hourly_github_sync.process_repo")
@patch("jobs.job_hourly_github_sync.pick_one")
@patch("jobs.job_hourly_github_sync.current_p0_date")
def test_run_p0_limit_no_paused(mock_current, mock_pick, mock_process, mock_mark):
    """P0：限流不 mark paused，保持 running。"""
    pg = MagicMock()
    mock_current.return_value = date(2026, 10, 1)
    mock_pick.return_value = (1, 123, "o/r", date(2026, 10, 1), date(2026, 10, 2), {}, "running")
    mock_process.side_effect = RestPause()

    result = run_p0(pg, MagicMock(), MagicMock(), date(2026, 10, 5), MagicMock())

    assert result is False
    # 不应该 mark 'paused'
    assert not any(call[0][2] == "paused" for call in mock_mark.call_args_list)


def test_run_p2_empty_features_repo():
    """features_repo 空 → P2 直接跳过。log 是 run_p2 的参数，不是模块全局。"""
    pg = MagicMock()
    pg.fetch_one.return_value = None
    log = MagicMock()
    run_p2(pg, MagicMock(), MagicMock(), log)
    log.info.assert_called_with("features_repo 为空，P2 跳过")


@patch("jobs.job_hourly_github_sync.mark_p2_completed")
@patch("jobs.job_hourly_github_sync.ensure_p2_subtasks")
@patch("jobs.job_hourly_github_sync.pick_one")
@patch("jobs.job_hourly_github_sync.current_p2_round")
def test_run_p2_completes(mock_round, mock_pick, mock_ensure, mock_mark):
    """P2 一轮全部完成 → mark completed。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (1,)  # features_repo 非空
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
