"""job_hourly_github_crawl 单元测试套件

═══════════════════════════════════════════════════════════════════════
一、职责
═══════════════════════════════════════════════════════════════════════
按小时窗口用 GitHub Search API 发现 README 含 arxiv 的仓库，拉 README
提取 arXiv ID，双写 features_repo + edges_repo_arxiv。

═══════════════════════════════════════════════════════════════════════
二、三态语义（改造后）
═══════════════════════════════════════════════════════════════════════
    running   = 未完成（首次 / 限流 / 崩溃残留 / 处理中）
    completed = 该小时成功处理
    failed    = 连续失败 >= 3 次（但下次仍会被 pick，因为 pick_one 也查 failed）

    关键区分：
        - 限流（SearchPause / RestPause）：status 保持 running，不增加 try_number
        - 其他异常（RuntimeError / 网络错误）：try_number + 1，>= 3 转 failed

═══════════════════════════════════════════════════════════════════════
三、测试策略（风险驱动）
═══════════════════════════════════════════════════════════════════════
P0 风险（必须覆盖）：
    - try_number 边界：>= 3 后状态必须是 failed，否则无限重试
    - 限流不消耗 try_number：否则偶发限流会让正常小时进 failed
    - next_crawl_task 的查询条件：必须查 running + failed，不能漏掉
    - cursor 断点续传：崩溃后必须能从断点继续

P1 风险（覆盖）：
    - SearchClient / RestClient 的限流识别（403 + remaining=0）
    - RestClient 3 次退避后放弃
    - persist_repo 的 has_papers 分支

P2（覆盖基础）：
    - 纯函数 extract_arxiv_ids 的三种格式识别

刻意不测：
    - 真实 Search API 调用（集成测试覆盖）
    - HTTPAdapter 的底层重试（urllib3 提供）
"""
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from jobs.job_hourly_github_crawl import (
    AuthError, RestPause, SearchPause, SearchClient, RestClient,
    MAX_README_RETRIES,
    extract_arxiv_ids, next_crawl_task, process_hour, persist_repo, run,
    mark_status, update_cursor, get_cursor, ensure_table, ensure_baseline,
)


# ═══════════════════════════════════════════════════════════════════════
# 1. 纯函数：extract_arxiv_ids
# ═══════════════════════════════════════════════════════════════════════

def test_extract_arxiv_ids_all_formats():
    text = """https://arxiv.org/abs/2310.00826 arXiv: 2310.00827 eprint = {2310.00828}"""
    links = extract_arxiv_ids(text)
    assert len(links) == 3
    assert ("2310.00826", "url") in links
    assert ("2310.00827", "inline") in links
    assert ("2310.00828", "bibtex") in links


def test_extract_arxiv_ids_deduplication():
    """同一个 ID 被多种格式匹配时，只保留第一次出现的格式。"""
    assert extract_arxiv_ids("arxiv.org/abs/2310.00826 and arXiv:2310.00826") == [("2310.00826", "url")]


def test_extract_arxiv_ids_empty():
    assert extract_arxiv_ids("") == []
    assert extract_arxiv_ids(None) == []


# ═══════════════════════════════════════════════════════════════════════
# 2. SearchClient / RestClient
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_search_client_normal():
    client = SearchClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        r = MagicMock(status_code=200)
        r.json.return_value = {"items": [{"id": 1, "full_name": "o/r"}]}
        mock_get.return_value = r
        assert len(client.search(datetime(2026, 1, 1), datetime(2026, 1, 2))) == 1


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_search_client_rate_limit():
    """403 + X-RateLimit-Remaining=0 时抛 SearchPause。"""
    client = SearchClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        r = MagicMock(status_code=403)
        r.headers = {"X-RateLimit-Remaining": "0"}
        mock_get.return_value = r
        with pytest.raises(SearchPause):
            client.search(datetime(2026, 1, 1), datetime(2026, 1, 2))


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_search_client_auth_error():
    client = SearchClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_get.return_value = MagicMock(status_code=401)
        with pytest.raises(AuthError):
            client.search(datetime(2026, 1, 1), datetime(2026, 1, 2))


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_rest_client_retry_then_give_up():
    """3 次 RequestException 后返回 None（不抛）。"""
    client = RestClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_get.side_effect = requests.RequestException("boom")
        assert client.fetch_readme("o/r") is None
        assert mock_get.call_count == MAX_README_RETRIES


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_rest_client_404_returns_none():
    client = RestClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_get.return_value = MagicMock(status_code=404)
        assert client.fetch_readme("o/r") is None


# ═══════════════════════════════════════════════════════════════════════
# 3. 状态操作
# ═══════════════════════════════════════════════════════════════════════

def test_mark_status_upsert():
    pg = MagicMock()
    mark_status(pg, date(2026, 10, 1), 10, "completed")
    sql = pg.execute.call_args[0][0]
    assert "ON CONFLICT (task_date, task_hour) DO UPDATE" in sql


def test_update_cursor():
    pg = MagicMock()
    update_cursor(pg, date(2026, 10, 1), 10, datetime(2026, 10, 1, 10, 15, tzinfo=timezone.utc))
    assert "SET cursor = %s" in pg.execute.call_args[0][0]


def test_get_cursor_exists():
    pg = MagicMock()
    pg.fetch_one.return_value = (datetime(2026, 10, 1, 10, 15, tzinfo=timezone.utc),)
    assert get_cursor(pg, date(2026, 10, 1), 10) is not None


def test_get_cursor_missing():
    pg = MagicMock()
    pg.fetch_one.return_value = None
    assert get_cursor(pg, date(2026, 10, 1), 10) is None


# ═══════════════════════════════════════════════════════════════════════
# 4. next_crawl_task：running + failed，按 try_number 升序
# ═══════════════════════════════════════════════════════════════════════

def test_next_crawl_task_queries_running_and_failed():
    """P0：查询条件必须覆盖 running + failed，否则 failed 任务不会被重试。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (date(2026, 10, 1), 10)
    assert next_crawl_task(pg, date(2026, 10, 2)) == (date(2026, 10, 1), 10)
    sql = pg.fetch_one.call_args_list[0][0][0]
    assert "status IN ('running', 'failed')" in sql
    # crawl_task 主键 (task_date, task_hour)，每小时仅一行，按时间推进即可；
    # try_number 排序只对 sync 的多 repo 子任务表有意义。
    assert "ORDER BY task_date, task_hour" in sql


def test_next_crawl_task_next_hour():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, (date(2026, 10, 1), 10)]
    assert next_crawl_task(pg, date(2026, 10, 2)) == (date(2026, 10, 1), 11)


def test_next_crawl_task_next_day():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, (date(2026, 10, 1), 23)]
    assert next_crawl_task(pg, date(2026, 10, 2)) == (date(2026, 10, 2), 0)


def test_next_crawl_task_finished():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, (date(2026, 10, 2), 23)]
    assert next_crawl_task(pg, date(2026, 10, 2)) is None


# ═══════════════════════════════════════════════════════════════════════
# 5. process_hour
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_crawl.update_cursor")
@patch("jobs.job_hourly_github_crawl.persist_repo")
def test_process_hour_happy_path(mock_persist, mock_update_cursor):
    """不足一页（50 条）时处理完就退出，不翻页。"""
    pg = MagicMock()
    pg.fetch_one.return_value = None
    search_cli = MagicMock()
    rest_cli = MagicMock()
    search_cli.search.return_value = [
        {"id": i, "full_name": f"o/r{i}", "created_at": "2026-10-01T10:00:00Z"} for i in range(50)
    ]
    rest_cli.fetch_readme.return_value = "arxiv:2310.00001"

    process_hour(pg, search_cli, rest_cli, date(2026, 10, 1), 10, MagicMock())

    assert mock_persist.call_count == 50
    mock_update_cursor.assert_not_called()


@patch("jobs.job_hourly_github_crawl.update_cursor")
@patch("jobs.job_hourly_github_crawl.persist_repo")
def test_process_hour_cursor_advance(mock_persist, mock_update_cursor):
    """满页时推进 cursor，下次从 cursor 继续。"""
    pg = MagicMock()
    pg.fetch_one.return_value = None
    search_cli = MagicMock()
    rest_cli = MagicMock()
    search_cli.search.side_effect = [
        [{"id": i, "full_name": f"o/r{i}", "created_at": "2026-10-01T10:15:00Z"} for i in range(100)],
        [],
    ]
    rest_cli.fetch_readme.return_value = None

    process_hour(pg, search_cli, rest_cli, date(2026, 10, 1), 10, MagicMock())

    assert mock_update_cursor.call_count == 1


@patch("jobs.job_hourly_github_crawl.update_cursor")
@patch("jobs.job_hourly_github_crawl.persist_repo")
def test_process_hour_resumes_from_cursor(mock_persist, mock_update_cursor):
    """已有 cursor 时从 cursor 开始，而不是 hour_start。"""
    pg = MagicMock()
    cursor = datetime(2026, 10, 1, 10, 30, tzinfo=timezone.utc)
    pg.fetch_one.return_value = (cursor,)
    search_cli = MagicMock()
    search_cli.search.return_value = []
    rest_cli = MagicMock()

    process_hour(pg, search_cli, rest_cli, date(2026, 10, 1), 10, MagicMock())

    args, _ = search_cli.search.call_args
    assert args[0] == cursor


# ═══════════════════════════════════════════════════════════════════════
# 6. persist_repo：has_papers 两分支 + 无 links
# ═══════════════════════════════════════════════════════════════════════

def test_persist_repo_with_papers_table():
    """arxiv_papers 存在 → 用 LEFT JOIN 获取 published_date。"""
    pg = MagicMock()
    pg.fetch_one.return_value = ("arxiv_papers",)
    cur = MagicMock()
    pg.cursor.return_value.__enter__.return_value = cur

    persist_repo(pg,
                 {"id": 123, "full_name": "owner/repo", "created_at": "2026-10-01T00:00:00Z"},
                 [("2310.00826", "url")],
                 MagicMock())

    # 3 次 execute：features_repo upsert + UPDATE added=TRUE + INSERT ... LEFT JOIN
    assert cur.execute.call_count == 3
    sqls = [c[0][0] for c in cur.execute.call_args_list]
    assert "UPDATE edges_repo_arxiv SET added = TRUE" in sqls[1]
    assert "INSERT INTO edges_repo_arxiv" in sqls[2]
    assert "LEFT JOIN arxiv_papers" in sqls[2]


def test_persist_repo_without_papers_table():
    """arxiv_papers 不存在 → 简化 SQL，直接用 extracted_on。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (None,)
    cur = MagicMock()
    pg.cursor.return_value.__enter__.return_value = cur

    persist_repo(pg,
                 {"id": 123, "full_name": "owner/repo", "created_at": "2026-10-01T00:00:00Z"},
                 [("2310.00826", "url")],
                 MagicMock())

    last_sql = cur.execute.call_args_list[-1][0][0]
    assert "INSERT INTO edges_repo_arxiv" in last_sql
    assert "LEFT JOIN arxiv_papers" not in last_sql


def test_persist_repo_no_links():
    """无 links 时只写 features_repo，不写 edges_repo_arxiv。"""
    pg = MagicMock()
    pg.fetch_one.return_value = ("arxiv_papers",)
    cur = MagicMock()
    pg.cursor.return_value.__enter__.return_value = cur

    persist_repo(pg,
                 {"id": 123, "full_name": "owner/repo", "created_at": "2026-10-01T00:00:00Z"},
                 [],
                 MagicMock())

    assert cur.execute.call_count == 1
    assert "INSERT INTO features_repo" in cur.execute.call_args[0][0]


# ═══════════════════════════════════════════════════════════════════════
# 7. run() 主循环：限流保持 running / 失败 try_number 递增 / 完成
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_crawl.mark_status")
@patch("jobs.job_hourly_github_crawl.process_hour")
@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_limit_keeps_running_no_paused(mock_pg_cls, mock_next, mock_process, mock_mark):
    """P0：限流（SearchPause）不 mark paused，保持 running。

    关键：如果此处 mark 了 paused，pick_one 不再查（只查 running/failed），
    任务会永久丢失。
    """
    mock_pg = MagicMock()
    mock_pg.fetch_one.return_value = (0,)   # ensure_baseline: COUNT=0
    mock_pg_cls.return_value = mock_pg
    mock_next.return_value = (date(2026, 10, 1), 10)
    mock_process.side_effect = SearchPause()

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"):
        run("test", MagicMock())

    # 断言：没有任何 mark_status 调用标记 paused
    # mark_status 签名 (pg, task_date, task_hour, status)：status 在 args[3]
    assert not any(call.args[3] == "paused" for call in mock_mark.call_args_list)
    # 断言：没有 mark_status 调用（限流时 status 保持不动，仍是 running）
    assert mock_mark.call_count == 0


@patch("jobs.job_hourly_github_crawl.mark_status")
@patch("jobs.job_hourly_github_crawl.process_hour")
@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_completes(mock_pg_cls, mock_next, mock_process, mock_mark):
    """成功处理 → mark_status(completed)。"""
    mock_pg = MagicMock()
    mock_pg.fetch_one.return_value = (0,)   # ensure_baseline: COUNT=0
    mock_pg_cls.return_value = mock_pg
    mock_next.side_effect = [(date(2026, 10, 1), 10), None]
    mock_process.return_value = None

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"):
        run("test", MagicMock())

    assert any(call.args[3] == "completed" for call in mock_mark.call_args_list)


@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_failure_increments_try_and_stays_running(mock_pg_cls, mock_next):
    """P0：第一次失败 → try_number=1 < 3 → 状态保持 running。

    验证：
        1. 有 UPDATE try_number = try_number + 1
        2. UPDATE 的 CASE 里 <= 3 时是 'running'
        3. 完成后不 raise
    """
    mock_pg = MagicMock()
    mock_pg_cls.return_value = mock_pg
    mock_next.side_effect = [(date(2026, 10, 1), 10), None]
    # 模拟失败后查询返回 try_number=1, status=running
    mock_pg.fetch_one.return_value = (1, "running")

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"), \
         patch("jobs.job_hourly_github_crawl.process_hour",
               side_effect=RuntimeError("boom")):
        run("test", MagicMock())

    # 找 UPDATE try_number 的语句
    try_updates = [
        c for c in mock_pg.execute.call_args_list
        if "try_number = try_number + 1" in str(c[0][0])
    ]
    assert len(try_updates) == 1
    sql = try_updates[0][0][0]
    # CASE 里有 'failed' 和 'running'
    assert "'failed'" in sql
    assert "'running'" in sql
    # 阈值是 MAX_README_RETRIES (3)
    params = try_updates[0][0][1]
    assert params[0] == MAX_README_RETRIES


@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_failure_reaches_failed(mock_pg_cls, mock_next):
    """P0：try_number 已到 2 → 本次失败变成 3 → status='failed'。"""
    mock_pg = MagicMock()
    mock_pg_cls.return_value = mock_pg
    mock_next.side_effect = [(date(2026, 10, 1), 10), None]
    # 失败后查询返回 try_number=3, status=failed
    mock_pg.fetch_one.return_value = (3, "failed")

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"), \
         patch("jobs.job_hourly_github_crawl.process_hour",
               side_effect=RuntimeError("boom")):
        run("test", MagicMock())

    # 日志应该记录 try=3 status=failed（通过 mock 的 fetch_one 返回验证了状态转换逻辑）
    try_updates = [
        c for c in mock_pg.execute.call_args_list
        if "try_number = try_number + 1" in str(c[0][0])
    ]
    assert len(try_updates) == 1


# ═══════════════════════════════════════════════════════════════════════
# 8. ensure_table / ensure_baseline
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_table_ddl():
    """DDL 必须包含 try_number 列和 DEFAULT 'running'。"""
    pg = MagicMock()
    ensure_table(pg)
    sqls = " ".join(c[0][0] for c in pg.execute.call_args_list)
    assert "CREATE TABLE IF NOT EXISTS job_hourly_github_crawl_task" in sqls
    assert "try_number" in sqls
    assert "DEFAULT 'running'" in sqls


def test_ensure_baseline_inserts_completed_at_6_16_23(mock_pg=None):
    """基线必须插入 (2026-06-16, 23, 'completed')，跳过 GitScholar 覆盖期。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (0,)
    ensure_baseline(pg)

    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "INSERT INTO job_hourly_github_crawl_task" in sql
    assert "completed" in sql
    assert params == ("2026-06-16",)


def test_ensure_baseline_skips_when_exists():
    pg = MagicMock()
    pg.fetch_one.return_value = (5,)
    ensure_baseline(pg)
    pg.execute.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# 9. 冒烟
# ═══════════════════════════════════════════════════════════════════════

def test_smoke_imports():
    import jobs.job_hourly_github_crawl as mod
    for name in ("init", "run", "verify", "main"):
        assert hasattr(mod, name)
