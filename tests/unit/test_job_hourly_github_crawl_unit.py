"""job_hourly_github_crawl 测试套件（补测版）

覆盖：
    - 纯函数解析
    - SearchClient / RestClient 限流
    - RestClient 3 次退避重试
    - next_crawl_task 状态推进
    - process_hour 主流程
    - persist_repo（has_papers 两分支）
    - run 主循环：SearchPause / RestPause
"""
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from jobs.job_hourly_github_crawl import (
    AuthError,
    RestPause,
    SearchPause,
    SearchClient,
    RestClient,
    extract_arxiv_ids,
    next_crawl_task,
    process_hour,
    persist_repo,
    run,
    mark_status,
    update_cursor,
    get_cursor,
    ensure_table,
    ensure_baseline,
)


# ═══════════════════════════════════════════════════════════════════════
# 1. 纯函数：extract_arxiv_ids
# ═══════════════════════════════════════════════════════════════════════

def test_extract_arxiv_ids_all_formats():
    text = """
    Check https://arxiv.org/abs/2310.00826
    and arXiv: 2310.00827
    and bibtex eprint = {2310.00828}
    """
    links = extract_arxiv_ids(text)
    assert len(links) == 3
    assert ("2310.00826", "url") in links
    assert ("2310.00827", "inline") in links
    assert ("2310.00828", "bibtex") in links


def test_extract_arxiv_ids_deduplication():
    text = "arxiv.org/abs/2310.00826 and arXiv:2310.00826"
    links = extract_arxiv_ids(text)
    assert links == [("2310.00826", "url")]


def test_extract_arxiv_ids_empty():
    assert extract_arxiv_ids("") == []
    assert extract_arxiv_ids(None) == []


# ═══════════════════════════════════════════════════════════════════════
# 2. SearchClient / RestClient 正常与限流
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_search_client_normal():
    client = SearchClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"items": [{"id": 1, "full_name": "o/r"}]}
        mock_get.return_value = mock_resp

        items = client.search(datetime(2026, 1, 1), datetime(2026, 1, 2))
        assert len(items) == 1


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_search_client_rate_limit():
    client = SearchClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.headers = {"X-RateLimit-Remaining": "0"}
        mock_get.return_value = mock_resp

        with pytest.raises(SearchPause):
            client.search(datetime(2026, 1, 1), datetime(2026, 1, 2))


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_search_client_auth_error():
    client = SearchClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_get.return_value = mock_resp

        with pytest.raises(AuthError):
            client.search(datetime(2026, 1, 1), datetime(2026, 1, 2))


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_rest_client_404():
    client = RestClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        assert client.fetch_readme("fake/non-exist") is None


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_rest_client_rate_limit():
    client = RestClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.headers = {"X-RateLimit-Remaining": "0"}
        mock_get.return_value = mock_resp

        with pytest.raises(RestPause):
            client.fetch_readme("o/r")


@patch("jobs.job_hourly_github_crawl.time.sleep", lambda _: None)
def test_rest_client_retry_then_give_up():
    """3 次网络异常后退避放弃，返回 None。"""
    client = RestClient("fake", MagicMock())
    with patch.object(client.session, "get") as mock_get:
        mock_get.side_effect = requests.RequestException("boom")

        result = client.fetch_readme("o/r")

        assert result is None
        assert mock_get.call_count == 3


# ═══════════════════════════════════════════════════════════════════════
# 3. 状态操作
# ═══════════════════════════════════════════════════════════════════════

def test_mark_status_upsert():
    pg = MagicMock()
    mark_status(pg, date(2026, 10, 1), 10, "paused", "限流")
    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "ON CONFLICT (task_date, task_hour) DO UPDATE" in sql
    assert params == (date(2026, 10, 1), 10, "paused", "限流")


def test_mark_status_truncates_error():
    pg = MagicMock()
    mark_status(pg, date(2026, 10, 1), 10, "failed", "x" * 1000)
    params = pg.execute.call_args[0][1]
    assert len(params[3]) == 500


def test_update_cursor():
    pg = MagicMock()
    update_cursor(pg, date(2026, 10, 1), 10, datetime(2026, 10, 1, 10, 15, tzinfo=timezone.utc))
    sql = pg.execute.call_args[0][0]
    assert "SET cursor = %s" in sql


def test_get_cursor_exists():
    pg = MagicMock()
    pg.fetch_one.return_value = (datetime(2026, 10, 1, 10, 15, tzinfo=timezone.utc),)
    result = get_cursor(pg, date(2026, 10, 1), 10)
    assert result is not None


def test_get_cursor_missing():
    pg = MagicMock()
    pg.fetch_one.return_value = None
    assert get_cursor(pg, date(2026, 10, 1), 10) is None


# ═══════════════════════════════════════════════════════════════════════
# 4. next_crawl_task 状态推进
# ═══════════════════════════════════════════════════════════════════════

def test_next_crawl_task_paused_priority():
    pg = MagicMock()
    pg.fetch_one.return_value = (date(2026, 10, 1), 10)
    assert next_crawl_task(pg, date(2026, 10, 2)) == (date(2026, 10, 1), 10)


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
# 5. process_hour 主流程
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_crawl.update_cursor")
@patch("jobs.job_hourly_github_crawl.persist_repo")
def test_process_hour_happy_path(mock_persist, mock_update_cursor):
    pg = MagicMock()
    pg.fetch_one.return_value = None

    search_cli = MagicMock()
    rest_cli = MagicMock()

    search_cli.search.return_value = [
        {"id": i, "full_name": f"o/r{i}", "created_at": "2026-10-01T10:00:00Z"}
        for i in range(50)
    ]
    rest_cli.fetch_readme.return_value = "arxiv:2310.00001"

    process_hour(pg, search_cli, rest_cli, date(2026, 10, 1), 10, MagicMock())

    assert mock_persist.call_count == 50
    mock_update_cursor.assert_not_called()


@patch("jobs.job_hourly_github_crawl.update_cursor")
@patch("jobs.job_hourly_github_crawl.persist_repo")
def test_process_hour_cursor_advance(mock_persist, mock_update_cursor):
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
    args, _ = mock_update_cursor.call_args
    assert args[3] == datetime(2026, 10, 1, 10, 15, 1, tzinfo=timezone.utc)


@patch("jobs.job_hourly_github_crawl.update_cursor")
@patch("jobs.job_hourly_github_crawl.persist_repo")
def test_process_hour_empty_search(mock_persist, mock_update_cursor):
    """search 返回空时，直接 return 不调用 persist。"""
    pg = MagicMock()
    pg.fetch_one.return_value = None
    search_cli = MagicMock()
    search_cli.search.return_value = []
    rest_cli = MagicMock()

    process_hour(pg, search_cli, rest_cli, date(2026, 10, 1), 10, MagicMock())

    mock_persist.assert_not_called()
    mock_update_cursor.assert_not_called()


@patch("jobs.job_hourly_github_crawl.persist_repo")
def test_process_hour_resumes_from_cursor(mock_persist):
    """已有 cursor 时，从 cursor 开始搜索。"""
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
# 6. persist_repo（has_papers 两分支）
# ═══════════════════════════════════════════════════════════════════════

def test_persist_repo_with_papers_table():
    pg = MagicMock()
    pg.fetch_one.return_value = ("arxiv_papers",)
    mock_cur = MagicMock()
    pg.cursor.return_value.__enter__.return_value = mock_cur

    item = {"id": 123, "full_name": "owner/repo", "created_at": "2026-10-01T00:00:00Z"}
    links = [("2310.00826", "url")]
    persist_repo(pg, item, links, MagicMock())

    # 最后一次 SQL 是 edges_repo_arxiv + LEFT JOIN arxiv_papers
    last_sql = mock_cur.execute.call_args_list[-1][0][0]
    assert "INSERT INTO edges_repo_arxiv" in last_sql
    assert "LEFT JOIN arxiv_papers" in last_sql


def test_persist_repo_without_papers_table():
    """arxiv_papers 不存在时，走无 JOIN 的简化 SQL。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (None,)
    mock_cur = MagicMock()
    pg.cursor.return_value.__enter__.return_value = mock_cur

    item = {"id": 123, "full_name": "owner/repo", "created_at": "2026-10-01T00:00:00Z"}
    links = [("2310.00826", "url")]
    persist_repo(pg, item, links, MagicMock())

    last_sql = mock_cur.execute.call_args_list[-1][0][0]
    assert "INSERT INTO edges_repo_arxiv" in last_sql
    assert "LEFT JOIN arxiv_papers" not in last_sql


def test_persist_repo_no_links():
    """无 links 时只写 features_repo，不写 edges_repo_arxiv。"""
    pg = MagicMock()
    pg.fetch_one.return_value = ("arxiv_papers",)
    mock_cur = MagicMock()
    pg.cursor.return_value.__enter__.return_value = mock_cur

    item = {"id": 123, "full_name": "owner/repo", "created_at": "2026-10-01T00:00:00Z"}
    persist_repo(pg, item, [], MagicMock())

    assert mock_cur.execute.call_count == 1
    assert "INSERT INTO features_repo" in mock_cur.execute.call_args[0][0]


# ═══════════════════════════════════════════════════════════════════════
# 7. run 主循环：SearchPause / RestPause
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_hourly_github_crawl.mark_status")
@patch("jobs.job_hourly_github_crawl.process_hour")
@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_search_pause(mock_pg_cls, mock_next, mock_process, mock_mark):
    """SearchPause → 标记 paused 并退出。"""
    mock_pg = MagicMock()
    mock_pg_cls.return_value = mock_pg
    mock_next.return_value = (date(2026, 10, 1), 10)
    mock_process.side_effect = SearchPause()

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"):
        run("test", MagicMock())

    # 至少有一次标记 paused
    found = any(call[0][2] == "paused" for call in mock_mark.call_args_list)
    assert found


@patch("jobs.job_hourly_github_crawl.mark_status")
@patch("jobs.job_hourly_github_crawl.process_hour")
@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_rest_pause(mock_pg_cls, mock_next, mock_process, mock_mark):
    """RestPause → 标记 paused 并退出。"""
    mock_pg = MagicMock()
    mock_pg_cls.return_value = mock_pg
    mock_next.return_value = (date(2026, 10, 1), 10)
    mock_process.side_effect = RestPause()

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"):
        run("test", MagicMock())

    found = any(call[0][2] == "paused" for call in mock_mark.call_args_list)
    assert found


@patch("jobs.job_hourly_github_crawl.mark_status")
@patch("jobs.job_hourly_github_crawl.process_hour")
@patch("jobs.job_hourly_github_crawl.next_crawl_task")
@patch("jobs.job_hourly_github_crawl.PG_CLIENT_CLASS")
def test_run_completes_all(mock_pg_cls, mock_next, mock_process, mock_mark):
    """next_crawl_task 返回 None → 所有窗口完成，正常退出。"""
    mock_pg = MagicMock()
    mock_pg_cls.return_value = mock_pg
    mock_next.return_value = None

    with patch("jobs.job_hourly_github_crawl.SearchClient"), \
         patch("jobs.job_hourly_github_crawl.RestClient"):
        run("test", MagicMock())

    mock_process.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# 8. ensure_table / ensure_baseline
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_table_creates():
    pg = MagicMock()
    ensure_table(pg)
    sqls = [c[0][0] for c in pg.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS job_hourly_github_crawl_task" in s for s in sqls)
    assert any("idx_crawl_pending" in s for s in sqls)


def test_ensure_baseline_skips_when_exists():
    pg = MagicMock()
    pg.fetch_one.return_value = (5,)
    ensure_baseline(pg)
    pg.execute.assert_not_called()


def test_ensure_baseline_inserts_when_empty():
    pg = MagicMock()
    pg.fetch_one.return_value = (0,)
    ensure_baseline(pg)
    pg.execute.assert_called_once()
    sql = pg.execute.call_args[0][0]
    assert "INSERT INTO job_hourly_github_crawl_task" in sql


# ═══════════════════════════════════════════════════════════════════════
# 9. 冒烟
# ═══════════════════════════════════════════════════════════════════════

def test_smoke_imports():
    import jobs.job_hourly_github_crawl as mod
    for name in ("init", "run", "verify", "main"):
        assert hasattr(mod, name)
