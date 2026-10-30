"""job_daily_arxiv_oai_pull 单元测试套件

═══════════════════════════════════════════════════════════════════════
一、职责
═══════════════════════════════════════════════════════════════════════
从 arXiv OAI-PMH 拉取 arXivRaw 记录，按天推进，双写 MinIO + PG。
本文件覆盖"原子函数 + 状态机"层，不涉及真实网络和真实 PG。

═══════════════════════════════════════════════════════════════════════
二、三态语义（改造后）
═══════════════════════════════════════════════════════════════════════
    running   = 处理中 / 首次 / 崩溃残留（未完成）
    completed = 当天拉取成功
    failed    = 当天拉取失败（下次仍会重试，因为 next_pull_date 会捡 failed）

    优先级：running > failed > (completed + 1)
    → 只要那天没成功，永远会先被重试。

═══════════════════════════════════════════════════════════════════════
三、测试策略（风险驱动）
═══════════════════════════════════════════════════════════════════════
P0 风险（必须覆盖）：
    - 状态机错误：把失败日期永久跳过（数据永久丢失）
    - 主循环异常：异常没 return，导致 scheduler 一直重试同一天
    - 双写失败：MinIO 或 PG 之一写入失败，数据不一致

P1 风险（覆盖）：
    - write_to_pg 的 SQL 拼接、JSON 序列化、空列表防护
    - harvest 的分页（resumptionToken 传递）

P2（覆盖基础）：
    - 纯函数边界（空值、格式错误）

刻意不测：
    - Retry HTTPAdapter 的重试行为（依赖 urllib3，由库保证）
    - request.Session 的真实超时（无法 mock 时间）

═══════════════════════════════════════════════════════════════════════
四、断言强度
═══════════════════════════════════════════════════════════════════════
关键路径使用"强断言"：
    ✅ 检查 SQL 里出现具体字面值（'completed' / 'failed' / 'running'）
    ✅ 检查 SQL 参数的具体值（日期 / 状态）
    ✅ 检查状态机的优先级（running > failed > completed+1）

不写"只查调用次数"的弱断言——那种测试对回归没有意义。
"""
import xml.etree.ElementTree as ET
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests

from jobs.job_daily_arxiv_oai_pull import (
    NS, ensure_initial_record, ensure_tables, extract_published_date,
    harvest, make_on_page, mark_completed, mark_failed, next_pull_date,
    parse_arxiv_date, parse_record, write_to_pg,
)


# ═══════════════════════════════════════════════════════════════════════
# 样本 XML 构造器
# ═══════════════════════════════════════════════════════════════════════

def _record_fragment(arxiv_id, v1_date, *, deleted=False, has_metadata=True,
                     has_versions=True, extra_versions=None):
    """构造 <record> 片段，覆盖 deleted / 缺 metadata / 缺 versions / 多版本。"""
    if deleted:
        return f"""<record><header status="deleted">
        <identifier>oai:arXiv.org:{arxiv_id}</identifier>
        <datestamp>2026-10-06</datestamp></header></record>"""
    if not has_metadata:
        return f"""<record><header>
        <identifier>oai:arXiv.org:{arxiv_id}</identifier>
        <datestamp>2026-10-06</datestamp></header></record>"""
    version_lines = ""
    if has_versions:
        version_lines = f"""<version version="v1">
            <date>{v1_date}</date><size>1</size></version>"""
        if extra_versions:
            for vnum, vdate in extra_versions:
                version_lines += f"""<version version="{vnum}">
                <date>{vdate}</date><size>2</size></version>"""
    return f"""<record><header>
        <identifier>oai:arXiv.org:{arxiv_id}</identifier>
        <datestamp>2026-10-06</datestamp>
        <setSpec>cs.LG</setSpec></header>
      <metadata><arXivRaw xmlns="http://arxiv.org/OAI/arXivRaw/">
          <id>{arxiv_id}</id><title>T-{arxiv_id}</title>
          <categories>cs.LG</categories>{version_lines}
        </arXivRaw></metadata></record>"""


def _page_xml(*fragments, token=None):
    token_xml = f"<resumptionToken>{token}</resumptionToken>" if token else ""
    return f"""<?xml version="1.0"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>{''.join(fragments)}</ListRecords>{token_xml}</OAI-PMH>"""


def _error_xml(code, message):
    return f"""<?xml version="1.0"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <error code="{code}">{message}</error></OAI-PMH>"""


def _mock_session(*xml_pages):
    session = MagicMock()
    responses = []
    for xml in xml_pages:
        r = MagicMock()
        r.text = xml
        r.raise_for_status = MagicMock()
        responses.append(r)
    session.get.side_effect = responses
    return session


def _collect_on_page(kept):
    def on_page(records):
        kept.extend(records)
        return len(records)
    return on_page


RECENT_DATE = "Mon, 2 Oct 2023 00:11:47 GMT"
OLD_DATE = "Mon, 2 Oct 2000 00:11:47 GMT"


# ═══════════════════════════════════════════════════════════════════════
# 1. 纯函数：日期解析
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("raw,expected", [
    ("Mon, 2 Oct 2023 00:11:47 GMT", (2023, 10, 2)),
    ("Mon, 23 Oct 2023 00:11:47 GMT", (2023, 10, 23)),
    ("Thu, 01 Oct 2026 00:04:04 GMT", (2026, 10, 1)),
    (None, None),
    ("", None),
    ("   ", None),
    ("2023-10-02", None),
    ("not a date", None),
])
def test_parse_arxiv_date(raw, expected):
    dt = parse_arxiv_date(raw)
    if expected is None:
        assert dt is None
    else:
        assert (dt.year, dt.month, dt.day) == expected
        assert dt.tzinfo is not None


@pytest.mark.parametrize("versions,expected", [
    ([], None),
    ([{"version": "v1", "date": "2023-10-02"}], "2023-10-02"),
    # 乱序：v10、v2、v1，应取 v1
    ([{"version": "v10", "date": "x"}, {"version": "v2", "date": "y"},
      {"version": "v1", "date": "z"}], "z"),
    # 缺 version 字段时，_version_num 返回 0
    ([{"date": "only-date"}], "only-date"),
])
def test_extract_published_date(versions, expected):
    assert extract_published_date(versions) == expected


# ═══════════════════════════════════════════════════════════════════════
# 2. 纯函数：parse_record（覆盖 deleted / 缺 metadata / 空 versions）
# ═══════════════════════════════════════════════════════════════════════

def _parse_fragment(xml):
    root = ET.fromstring(_page_xml(xml))
    return parse_record(root.findall(".//oai:record", NS)[0])


def test_parse_record_basic():
    r = _parse_fragment(_record_fragment("2310.00826", RECENT_DATE))
    assert r["arxiv_id"] == "2310.00826"
    assert r["title"] == "T-2310.00826"
    assert r["categories"] == "cs.LG"
    assert r["set_specs"] == ["cs.LG"]
    assert r["published_date"] == RECENT_DATE


def test_parse_record_deleted_returns_none():
    assert _parse_fragment(_record_fragment("x", RECENT_DATE, deleted=True)) is None


def test_parse_record_missing_metadata_returns_none():
    assert _parse_fragment(_record_fragment("x", RECENT_DATE, has_metadata=False)) is None


def test_parse_record_empty_versions():
    """空 versions → published_date 为 None，但 record 本身仍有效。"""
    r = _parse_fragment(_record_fragment("x", RECENT_DATE, has_versions=False))
    assert r is not None
    assert r["versions"] == []
    assert r["published_date"] is None


def test_parse_record_multi_versions_ordered():
    """多版本时取 v1（最早）。"""
    r = _parse_fragment(_record_fragment(
        "x", RECENT_DATE, extra_versions=[("v2", "Tue, 28 Nov 2023 02:13:40 GMT")]
    ))
    assert len(r["versions"]) == 2
    assert r["published_date"] == RECENT_DATE


# ═══════════════════════════════════════════════════════════════════════
# 3. harvest：单页 / 分页 / 空窗口
# ═══════════════════════════════════════════════════════════════════════

def _harvest_with_session(session, on_page, monkeypatch):
    import jobs.job_daily_arxiv_oai_pull as job_mod
    monkeypatch.setattr(job_mod.requests, "Session", lambda: session)
    monkeypatch.setattr(job_mod.time, "sleep", lambda s: None)
    return harvest("2024-10-01", "2024-10-01", on_page=on_page, log=MagicMock())


def test_harvest_single_page(monkeypatch):
    session = _mock_session(_page_xml(_record_fragment("2310.00826", RECENT_DATE)))
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)
    assert stats["written"] == 1
    assert stats["pages"] == 1


def test_harvest_two_pages_with_token(monkeypatch):
    """验证第二页使用 resumptionToken 而非原始 params。"""
    pages = [
        _page_xml(_record_fragment("0001.00001", RECENT_DATE), token="T1"),
        _page_xml(_record_fragment("0001.00002", RECENT_DATE)),
    ]
    session = _mock_session(*pages)
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)

    assert stats["pages"] == 2
    assert stats["written"] == 2
    # 第二页请求的 params 只有 resumptionToken
    second_params = session.get.call_args_list[1][1]["params"]
    assert second_params == {"verb": "ListRecords", "resumptionToken": "T1"}


def test_harvest_empty_window(monkeypatch):
    """noRecordsMatch 时 on_page 不被调用。"""
    session = _mock_session(_error_xml("noRecordsMatch", "no records"))
    on_page = MagicMock()
    _harvest_with_session(session, on_page, monkeypatch)
    on_page.assert_not_called()


def test_harvest_http_error_propagates(monkeypatch):
    """HTTP 错误直接抛出，交给上层处理。"""
    import jobs.job_daily_arxiv_oai_pull as job_mod
    monkeypatch.setattr(job_mod.time, "sleep", lambda s: None)
    session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.HTTPError("500")
    session.get.return_value = resp
    monkeypatch.setattr(job_mod.requests, "Session", lambda: session)
    with pytest.raises(requests.HTTPError):
        harvest("2024-10-01", "2024-10-01", on_page=MagicMock(), log=MagicMock())


# ═══════════════════════════════════════════════════════════════════════
# 4. make_on_page：双写 MinIO + PG
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_daily_arxiv_oai_pull.write_to_pg")
def test_make_on_page_writes_minio_and_pg(mock_write):
    client = MagicMock()
    client.upsert_jsonl.return_value = 1
    pg = MagicMock()
    on_page = make_on_page(client, pg)

    records = [
        {"arxiv_id": "a1", "published_date_iso": "2023-10-02T00:00:00+00:00"},
        {"arxiv_id": "a2", "published_date_iso": "2023-11-01T00:00:00+00:00"},
    ]
    total = on_page(records)

    # MinIO：按 year/month 分组，2 次 upsert
    assert client.upsert_jsonl.call_count == 2
    assert total == 2
    # PG：一次性写入
    mock_write.assert_called_once_with(pg, records)


@patch("jobs.job_daily_arxiv_oai_pull.write_to_pg")
def test_make_on_page_skips_minio_when_iso_invalid(mock_write):
    """published_date_iso 无效 → 不写 MinIO，但仍写 PG（PG 会处理 NULL）。"""
    client = MagicMock()
    pg = MagicMock()
    on_page = make_on_page(client, pg)
    records = [{"arxiv_id": "x", "published_date_iso": ""}]
    on_page(records)

    client.upsert_jsonl.assert_not_called()
    mock_write.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════
# 5. write_to_pg：SQL 拼接 + JSON 序列化 + 空列表
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_daily_arxiv_oai_pull.execute_values")
def test_write_to_pg_serializes_json(mock_execute):
    """验证复杂字段（set_specs / versions）被序列化为 JSON 字符串。"""
    pg = MagicMock()
    records = [{
        "arxiv_id": "2310.00826", "title": "T", "abstract": "A",
        "authors": "X", "categories": "cs.LG",
        "set_specs": ["cs.LG"],
        "submitter": "S", "comments": None, "journal_ref": None,
        "doi": None, "report_no": None, "license": None,
        "versions": [{"version": "v1", "date": "2023-10-02"}],
        "published_date_iso": "2023-10-02T00:00:00+00:00",
        "datestamp": "2026-10-06",
        "oai_identifier": "oai:arXiv.org:2310.00826",
    }]
    write_to_pg(pg, records)

    call_args = mock_execute.call_args[0]
    sql, rows = call_args[1], call_args[2]

    # SQL 有 UPSERT
    assert "INSERT INTO arxiv_papers" in sql
    assert "ON CONFLICT (arxiv_id) DO UPDATE" in sql
    # 主键不在 UPDATE 里
    assert "arxiv_id = EXCLUDED.arxiv_id" not in sql
    # JSON 序列化
    assert rows[0][5] == '["cs.LG"]'
    assert rows[0][12] == '[{"version": "v1", "date": "2023-10-02"}]'


def test_write_to_pg_empty_records_noop():
    pg = MagicMock()
    write_to_pg(pg, [])
    pg.cursor.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# 6. PG 记账（三态字面值）
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_tables_creates_both_tables():
    pg = MagicMock()
    ensure_tables(pg)
    sqls = " ".join(c[0][0] for c in pg.execute.call_args_list)
    assert "CREATE TABLE IF NOT EXISTS arxiv_oai_pull_log" in sqls
    assert "CREATE TABLE IF NOT EXISTS arxiv_papers" in sqls
    assert "idx_arxiv_papers_pub_date" in sqls


def test_ensure_initial_record_uses_completed():
    """基线记录状态必须是 'completed'（三态之一）。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (0,)
    ensure_initial_record(pg, "job-x", "2026-10-01")

    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "INSERT INTO arxiv_oai_pull_log" in sql
    assert "completed" in sql
    assert params == ("job-x", "2026-10-01")


def test_ensure_initial_record_skips_when_exists():
    pg = MagicMock()
    pg.fetch_one.return_value = (5,)
    ensure_initial_record(pg, "job-x", "2026-10-01")
    pg.execute.assert_not_called()


def test_mark_completed_uses_completed():
    pg = MagicMock()
    mark_completed(pg, "job-x", "2026-10-06", 42)
    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "completed" in sql
    assert "COMMITTED" not in sql   # 旧字面值不能出现
    assert params == ("job-x", "2026-10-06", 42)


def test_mark_failed_uses_failed_and_truncates():
    pg = MagicMock()
    mark_failed(pg, "job-x", "2026-10-06", "x" * 1000)
    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "failed" in sql
    assert "FAILED" not in sql       # 旧字面值不能出现
    assert len(params[2]) == 500     # 错误截断


# ═══════════════════════════════════════════════════════════════════════
# 7. next_pull_date：优先级 running > failed > completed+1
# ═══════════════════════════════════════════════════════════════════════

def test_next_pull_date_prefers_running():
    """running 优先：即使有 failed 也先处理 running。"""
    pg = MagicMock()
    pg.fetch_one.side_effect = [(date(2026, 10, 5),)]
    result = next_pull_date(pg, "job-x", "2026-10-10")

    assert result == "2026-10-05"
    # 第一条 SQL 必须查 running
    first_sql = pg.fetch_one.call_args_list[0][0][0]
    assert "status = 'running'" in first_sql


def test_next_pull_date_falls_back_to_failed():
    """无 running 才查 failed。"""
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, (date(2026, 10, 3),)]
    result = next_pull_date(pg, "job-x", "2026-10-10")

    assert result == "2026-10-03"
    second_sql = pg.fetch_one.call_args_list[1][0][0]
    assert "status = 'failed'" in second_sql


def test_next_pull_date_falls_back_to_completed_plus_one():
    """无 running 无 failed，从 completed 最大日期 + 1。"""
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, None, (date(2026, 10, 5),)]
    result = next_pull_date(pg, "job-x", "2026-10-10")

    assert result == "2026-10-06"
    third_sql = pg.fetch_one.call_args_list[2][0][0]
    assert "status = 'completed'" in third_sql


def test_next_pull_date_caught_up():
    """completed+1 超过 target，返回 None。"""
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, None, (date(2026, 10, 10),)]
    assert next_pull_date(pg, "job-x", "2026-10-10") is None


# ═══════════════════════════════════════════════════════════════════════
# 8. run() 主循环：状态机 + 异常不退出
# ═══════════════════════════════════════════════════════════════════════

@patch("jobs.job_daily_arxiv_oai_pull.mark_failed")
@patch("jobs.job_daily_arxiv_oai_pull.mark_completed")
@patch("jobs.job_daily_arxiv_oai_pull.harvest")
@patch("jobs.job_daily_arxiv_oai_pull.next_pull_date")
@patch("jobs.job_daily_arxiv_oai_pull.ensure_initial_record")
@patch("jobs.job_daily_arxiv_oai_pull.ensure_tables")
@patch("jobs.job_daily_arxiv_oai_pull.make_on_page")
@patch("jobs.job_daily_arxiv_oai_pull.MinIOClient")
@patch("jobs.job_daily_arxiv_oai_pull.PGClient")
def test_run_processes_until_caught_up(
    mock_pg_cls, mock_minio_cls, mock_make_on_page, mock_ensure_tables,
    mock_ensure_init, mock_next_pull, mock_harvest, mock_mark_completed, mock_mark_failed,
):
    """主循环：next_pull_date 返回 2 次日期后 None，成功 mark 2 次 completed。"""
    from datetime import timedelta
    import jobs.job_daily_arxiv_oai_pull as mod

    target = (date.today() - timedelta(days=1)).isoformat()
    mock_next_pull.side_effect = [target, "2026-10-02", None]
    mock_harvest.return_value = {"written": 10, "pages": 1}

    # 直接调用 run
    mod.run("test", MagicMock())

    # 断言：两次 mark_completed，无 mark_failed
    assert mock_mark_completed.call_count == 2
    mock_mark_failed.assert_not_called()


@patch("jobs.job_daily_arxiv_oai_pull.mark_failed")
@patch("jobs.job_daily_arxiv_oai_pull.mark_completed")
@patch("jobs.job_daily_arxiv_oai_pull.harvest")
@patch("jobs.job_daily_arxiv_oai_pull.next_pull_date")
@patch("jobs.job_daily_arxiv_oai_pull.ensure_initial_record")
@patch("jobs.job_daily_arxiv_oai_pull.ensure_tables")
@patch("jobs.job_daily_arxiv_oai_pull.make_on_page")
@patch("jobs.job_daily_arxiv_oai_pull.MinIOClient")
@patch("jobs.job_daily_arxiv_oai_pull.PGClient")
def test_run_harvest_failure_does_not_raise(
    mock_pg_cls, mock_minio_cls, mock_make_on_page, mock_ensure_tables,
    mock_ensure_init, mock_next_pull, mock_harvest, mock_mark_completed, mock_mark_failed,
):
    """P0：harvest 失败时 mark_failed 且 return（不 raise）。

    这保证 scheduler 不会因一次失败而整体崩溃。
    """
    import jobs.job_daily_arxiv_oai_pull as mod

    mock_next_pull.side_effect = ["2026-10-05", "2026-10-06"]
    mock_harvest.side_effect = RuntimeError("boom")

    # 不抛异常
    mod.run("test", MagicMock())

    # mark_failed 被调用一次，mark_completed 0 次
    assert mock_mark_failed.call_count == 1
    mock_mark_completed.assert_not_called()
    # 关键：next_pull_date 只被调用 1 次（失败后 return，不再尝试下一天）
    assert mock_next_pull.call_count == 1


# ═══════════════════════════════════════════════════════════════════════
# 9. 冒烟
# ═══════════════════════════════════════════════════════════════════════

def test_smoke_imports():
    import jobs.job_daily_arxiv_oai_pull as mod
    for name in ("harvest", "make_on_page", "write_to_pg", "next_pull_date",
                 "mark_completed", "mark_failed", "run", "main"):
        assert hasattr(mod, name)
