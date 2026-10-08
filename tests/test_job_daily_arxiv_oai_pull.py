# tests/test_job_daily_arxiv_oai_pull.py
"""job_daily_arxiv_oai_pull 测试套件

对齐新 Job API：
    harvest(from_date, until_date, *, on_page, log)
    make_minio_on_page(client)
    ensure_table / ensure_initial_record / query_last_committed / mark_*

运行：
    uv run pytest tests/test_job_daily_arxiv_oai_pull.py -v
"""
from unittest.mock import MagicMock

import pytest
import requests

from jobs.job_daily_arxiv_oai_pull import (
    NS,
    ensure_initial_record,
    ensure_table,
    extract_published_date,
    harvest,
    make_minio_on_page,
    mark_committed,
    mark_failed,
    parse_arxiv_date,
    parse_record,
    query_last_committed,
)


# ═══════════════════════════════════════════════════════════════════════
# 样本 XML 构造器
# ═══════════════════════════════════════════════════════════════════════

def _record_fragment(
    arxiv_id: str,
    v1_date: str,
    *,
    deleted: bool = False,
    has_metadata: bool = True,
    has_versions: bool = True,
    extra_versions: list | None = None,
) -> str:
    """构造 <record> 片段（不含 XML 声明和 OAI-PMH 根）。"""
    if deleted:
        return f"""<record>
          <header status="deleted">
            <identifier>oai:arXiv.org:{arxiv_id}</identifier>
            <datestamp>2026-10-06</datestamp>
          </header>
        </record>"""

    if not has_metadata:
        return f"""<record>
          <header>
            <identifier>oai:arXiv.org:{arxiv_id}</identifier>
            <datestamp>2026-10-06</datestamp>
          </header>
        </record>"""

    version_lines = ""
    if has_versions:
        version_lines = f"""<version version="v1">
                <date>{v1_date}</date>
                <size>1</size>
              </version>"""
        if extra_versions:
            for vnum, vdate in extra_versions:
                version_lines += f"""
              <version version="{vnum}">
                <date>{vdate}</date>
                <size>2</size>
              </version>"""

    return f"""<record>
      <header>
        <identifier>oai:arXiv.org:{arxiv_id}</identifier>
        <datestamp>2026-10-06</datestamp>
        <setSpec>cs.LG</setSpec>
      </header>
      <metadata>
        <arXivRaw xmlns="http://arxiv.org/OAI/arXivRaw/">
          <id>{arxiv_id}</id>
          <title>T-{arxiv_id}</title>
          <categories>cs.LG</categories>
          {version_lines}
        </arXivRaw>
      </metadata>
    </record>"""


def _page_xml(*fragments: str, token: str | None = None) -> str:
    token_xml = f"<resumptionToken>{token}</resumptionToken>" if token else ""
    body = "\n".join(fragments)
    return f"""<?xml version="1.0"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        {body}
      </ListRecords>
      {token_xml}
    </OAI-PMH>"""


def _error_xml(code: str, message: str) -> str:
    return f"""<?xml version="1.0"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <error code="{code}">{message}</error>
    </OAI-PMH>"""


def _mock_session(*xml_pages: str) -> MagicMock:
    session = MagicMock()
    responses = []
    for xml in xml_pages:
        r = MagicMock()
        r.text = xml
        r.raise_for_status = MagicMock()
        responses.append(r)
    session.get.side_effect = responses
    return session


def _collect_on_page(kept: list):
    def on_page(records):
        kept.extend(records)
        return len(records)
    return on_page


RECENT_DATE = "Mon, 2 Oct 2023 00:11:47 GMT"
OLD_DATE = "Mon, 2 Oct 2000 00:11:47 GMT"


# ═══════════════════════════════════════════════════════════════════════
# 1. 纯函数：parse_arxiv_date
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
        assert dt is not None
        assert (dt.year, dt.month, dt.day) == expected
        assert dt.tzinfo is not None


# ═══════════════════════════════════════════════════════════════════════
# 2. 纯函数：extract_published_date
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("versions,expected", [
    ([], None),
    ([{"version": "v1", "date": "2023-10-02"}], "2023-10-02"),
    # 乱序：v10、v2、v1 → 应取 v1
    ([{"version": "v10", "date": "x"},
      {"version": "v2", "date": "y"},
      {"version": "v1", "date": "z"}], "z"),
    # 缺 version 字段
    ([{"date": "only-date"}], "only-date"),
    # v2 在前，v1 在后
    ([{"version": "v2", "date": "newer"},
      {"version": "v1", "date": "older"}], "older"),
])
def test_extract_published_date(versions, expected):
    assert extract_published_date(versions) == expected


# ═══════════════════════════════════════════════════════════════════════
# 3. 纯函数：parse_record
# ═══════════════════════════════════════════════════════════════════════

import xml.etree.ElementTree as ET


def _parse_fragment(xml_fragment: str):
    """把 <record> 片段包成完整 OAI-PMH 再解析。"""
    page = _page_xml(xml_fragment)
    root = ET.fromstring(page)
    rec = root.findall(".//oai:record", NS)[0]
    return parse_record(rec)


def test_parse_record_basic():
    r = _parse_fragment(_record_fragment("2310.00826", RECENT_DATE))
    assert r is not None
    assert r["arxiv_id"] == "2310.00826"
    assert r["title"] == "T-2310.00826"
    assert r["categories"] == "cs.LG"
    assert r["set_specs"] == ["cs.LG"]
    assert len(r["versions"]) == 1
    assert r["published_date"] == RECENT_DATE
    assert r["datestamp"] == "2026-10-06"
    assert r["oai_identifier"] == "oai:arXiv.org:2310.00826"


def test_parse_record_deleted_returns_none():
    r = _parse_fragment(_record_fragment("9999.99999", RECENT_DATE, deleted=True))
    assert r is None


def test_parse_record_missing_metadata_returns_none():
    r = _parse_fragment(_record_fragment("9999.00001", RECENT_DATE, has_metadata=False))
    assert r is None


def test_parse_record_empty_versions():
    r = _parse_fragment(_record_fragment("9999.00002", RECENT_DATE, has_versions=False))
    assert r is not None
    assert r["versions"] == []
    assert r["published_date"] is None


def test_parse_record_multi_versions_ordered():
    r = _parse_fragment(_record_fragment(
        "2310.00826", RECENT_DATE,
        extra_versions=[("v2", "Tue, 28 Nov 2023 02:13:40 GMT")],
    ))
    assert r is not None
    assert len(r["versions"]) == 2
    assert r["published_date"] == RECENT_DATE


# ═══════════════════════════════════════════════════════════════════════
# 4. harvest：主流程
# ═══════════════════════════════════════════════════════════════════════

def _harvest_with_mock(xml_pages, on_page, monkeypatch):
    """注入 mock session，跳过 sleep。"""
    import jobs.job_daily_arxiv_oai_pull as job_mod
    monkeypatch.setattr(job_mod.time, "sleep", lambda s: None)

    session = _mock_session(*xml_pages)
    return harvest(
        "2024-10-01", "2024-10-01",
        on_page=on_page,
        log=MagicMock(),
    ) if False else _harvest_with_session(session, on_page, monkeypatch)


def _harvest_with_session(session, on_page, monkeypatch):
    """harvest 当前不接收 session 参数，改用 monkeypatch 替换 requests.Session。"""
    import jobs.job_daily_arxiv_oai_pull as job_mod
    monkeypatch.setattr(job_mod.requests, "Session", lambda: session)
    monkeypatch.setattr(job_mod.time, "sleep", lambda s: None)
    return harvest(
        "2024-10-01", "2024-10-01",
        on_page=on_page,
        log=MagicMock(),
    )


def test_harvest_keeps_all_records(monkeypatch):
    session = _mock_session(
        _page_xml(
            _record_fragment("2310.00826", RECENT_DATE),
            _record_fragment("2310.00827", OLD_DATE),
        )
    )
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)

    assert stats["written"] == 2
    assert {r["arxiv_id"] for r in kept} == {"2310.00826", "2310.00827"}


def test_harvest_skips_deleted(monkeypatch):
    session = _mock_session(
        _page_xml(_record_fragment("9999.99999", RECENT_DATE, deleted=True))
    )
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)

    assert stats["written"] == 0
    assert stats["deleted"] == 1


def test_harvest_skips_missing_metadata(monkeypatch):
    session = _mock_session(
        _page_xml(_record_fragment("9999.00001", RECENT_DATE, has_metadata=False))
    )
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)

    assert stats["written"] == 0
    assert stats["parse_fail"] == 1


def test_harvest_skips_empty_versions(monkeypatch):
    """空 versions → published_date 为 None → date_fail。"""
    session = _mock_session(
        _page_xml(_record_fragment("9999.00002", RECENT_DATE, has_versions=False))
    )
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)

    assert stats["written"] == 0
    assert stats["date_fail"] == 1


def test_harvest_empty_window(monkeypatch):
    session = _mock_session(_error_xml("noRecordsMatch", "no records"))
    on_page = MagicMock()
    stats = _harvest_with_session(session, on_page, monkeypatch)

    assert stats["written"] == 0
    assert stats["fetched"] == 0
    on_page.assert_not_called()


def test_harvest_on_page_not_called_when_empty(monkeypatch):
    """无记录时 on_page 不应被调用。"""
    session = _mock_session(_error_xml("noRecordsMatch", "no records"))
    on_page = MagicMock()
    _harvest_with_session(session, on_page, monkeypatch)
    on_page.assert_not_called()


def test_harvest_output_record_quality(monkeypatch):
    session = _mock_session(_page_xml(_record_fragment("2310.00826", RECENT_DATE)))
    kept = []
    _harvest_with_session(session, _collect_on_page(kept), monkeypatch)

    assert len(kept) == 1
    r = kept[0]
    assert r["arxiv_id"] == "2310.00826"
    assert r["published_date_iso"].startswith("2023-10-02")
    assert r["published_date_iso"].endswith("+00:00")
    assert r["categories"] == "cs.LG"


# ═══════════════════════════════════════════════════════════════════════
# 5. harvest：分页
# ═══════════════════════════════════════════════════════════════════════

def test_harvest_single_page(monkeypatch):
    session = _mock_session(_page_xml(_record_fragment("0001.00001", RECENT_DATE)))
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)
    assert stats["pages"] == 1
    assert stats["written"] == 1


def test_harvest_two_pages(monkeypatch):
    pages = [
        _page_xml(_record_fragment("0001.00001", RECENT_DATE), token="T1"),
        _page_xml(_record_fragment("0001.00002", RECENT_DATE)),
    ]
    session = _mock_session(*pages)
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)

    assert stats["pages"] == 2
    assert stats["written"] == 2
    second_params = session.get.call_args_list[1][1]["params"]
    assert second_params == {"verb": "ListRecords", "resumptionToken": "T1"}


def test_harvest_three_pages(monkeypatch):
    pages = [
        _page_xml(_record_fragment("0001.00001", RECENT_DATE), token="T1"),
        _page_xml(_record_fragment("0001.00002", RECENT_DATE), token="T2"),
        _page_xml(_record_fragment("0001.00003", RECENT_DATE)),
    ]
    session = _mock_session(*pages)
    kept = []
    stats = _harvest_with_session(session, _collect_on_page(kept), monkeypatch)

    assert stats["pages"] == 3
    assert stats["written"] == 3
    assert session.get.call_args_list[1][1]["params"]["resumptionToken"] == "T1"
    assert session.get.call_args_list[2][1]["params"]["resumptionToken"] == "T2"


def test_harvest_rate_limit_between_pages(monkeypatch):
    """分页之间应调用 sleep；第一页不 sleep。"""
    import jobs.job_daily_arxiv_oai_pull as job_mod

    sleep_calls = []
    monkeypatch.setattr(job_mod.time, "sleep", lambda s: sleep_calls.append(s))

    pages = [
        _page_xml(_record_fragment("0001.00001", RECENT_DATE), token="T1"),
        _page_xml(_record_fragment("0001.00002", RECENT_DATE), token="T2"),
        _page_xml(_record_fragment("0001.00003", RECENT_DATE)),
    ]
    session = _mock_session(*pages)
    monkeypatch.setattr(job_mod.requests, "Session", lambda: session)

    harvest("2024-10-01", "2024-10-01",
            on_page=MagicMock(), log=MagicMock())

    # 三页 → sleep 两次
    assert sleep_calls == [3.0, 3.0]


# ═══════════════════════════════════════════════════════════════════════
# 6. harvest：异常
# ═══════════════════════════════════════════════════════════════════════

def test_harvest_http_error(monkeypatch):
    import jobs.job_daily_arxiv_oai_pull as job_mod
    monkeypatch.setattr(job_mod.time, "sleep", lambda s: None)

    session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.HTTPError("500")
    session.get.return_value = resp
    monkeypatch.setattr(job_mod.requests, "Session", lambda: session)

    with pytest.raises(requests.HTTPError):
        harvest("2024-10-01", "2024-10-01",
                on_page=MagicMock(), log=MagicMock())


def test_harvest_xml_parse_error(monkeypatch):
    session = _mock_session("not valid xml <<<")
    with pytest.raises(RuntimeError, match="XML 解析失败"):
        _harvest_with_session(session, MagicMock(), monkeypatch)


def test_harvest_oai_error_code(monkeypatch):
    session = _mock_session(_error_xml("badArgument", "bad request"))
    with pytest.raises(RuntimeError, match="OAI-PMH 错误"):
        _harvest_with_session(session, MagicMock(), monkeypatch)


# ═══════════════════════════════════════════════════════════════════════
# 7. on_page：make_minio_on_page
# ═══════════════════════════════════════════════════════════════════════

def test_minio_on_page_groups_by_published_month():
    mock_client = MagicMock()
    mock_client.upsert_jsonl.return_value = 1

    on_page = make_minio_on_page(mock_client)
    records = [
        {"arxiv_id": "0001.00001", "published_date_iso": "2023-10-02T00:11:47+00:00"},
        {"arxiv_id": "0001.00002", "published_date_iso": "2023-10-15T00:11:47+00:00"},
        {"arxiv_id": "0001.00003", "published_date_iso": "2023-11-01T00:11:47+00:00"},
    ]
    on_page(records)

    assert mock_client.upsert_jsonl.call_count == 2
    object_names = [c[0][0] for c in mock_client.upsert_jsonl.call_args_list]
    assert "papers/arxiv_oai/published_year=2023/month=10/state.jsonl" in object_names
    assert "papers/arxiv_oai/published_year=2023/month=11/state.jsonl" in object_names


def test_minio_on_page_skips_invalid_iso():
    """published_date_iso 长度不足 7 → 跳过。"""
    mock_client = MagicMock()
    on_page = make_minio_on_page(mock_client)
    on_page([
        {"arxiv_id": "x", "published_date_iso": ""},
        {"arxiv_id": "y", "published_date_iso": "2023"},
    ])
    mock_client.upsert_jsonl.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# 8. PG 记账函数（mock pg）
# ═══════════════════════════════════════════════════════════════════════

def test_ensure_table_calls_ddl():
    pg = MagicMock()
    ensure_table(pg)
    assert pg.execute.call_count == 2
    first_sql = pg.execute.call_args_list[0][0][0]
    assert "CREATE TABLE IF NOT EXISTS arxiv_oai_pull_log" in first_sql


def test_ensure_initial_record_skips_when_exists():
    """已有记录 → 不插入。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (5,)
    ensure_initial_record(pg, "job-x", "2026-08-30")
    # 只调用 fetch_one，不调用 execute
    pg.execute.assert_not_called()


def test_ensure_initial_record_inserts_when_empty():
    """无记录 → 插入基线。"""
    pg = MagicMock()
    pg.fetch_one.return_value = (0,)
    ensure_initial_record(pg, "job-x", "2026-08-30")
    pg.execute.assert_called_once()
    sql = pg.execute.call_args[0][0]
    assert "INSERT INTO arxiv_oai_pull_log" in sql
    assert "COMMITTED" in sql
    # 参数检查
    params = pg.execute.call_args[0][1]
    assert params == ("job-x", "2026-08-30")


def test_query_last_committed():
    pg = MagicMock()
    from datetime import date
    pg.fetch_one.return_value = (date(2026, 10, 6),)
    result = query_last_committed(pg, "job-x")
    assert result == "2026-10-06"


def test_query_last_committed_raises_when_none():
    pg = MagicMock()
    pg.fetch_one.return_value = (None,)
    with pytest.raises(RuntimeError, match="无 .* 的 COMMITTED 记录"):
        query_last_committed(pg, "job-x")


def test_mark_committed():
    pg = MagicMock()
    mark_committed(pg, "job-x", "2026-10-06", 42)
    pg.execute.assert_called_once()
    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "COMMITTED" in sql
    assert params == ("job-x", "2026-10-06", 42)


def test_mark_failed_truncates_error():
    pg = MagicMock()
    long_error = "x" * 1000
    mark_failed(pg, "job-x", "2026-10-06", long_error)
    params = pg.execute.call_args[0][1]
    # 第 3 个参数是 error，被截断到 500
    assert len(params[2]) == 500


# ═══════════════════════════════════════════════════════════════════════
# 9. 冒烟
# ═══════════════════════════════════════════════════════════════════════

def test_smoke_imports():
    import jobs.job_daily_arxiv_oai_pull as job_mod
    assert hasattr(job_mod, "harvest")
    assert hasattr(job_mod, "main")
    assert hasattr(job_mod, "run")
