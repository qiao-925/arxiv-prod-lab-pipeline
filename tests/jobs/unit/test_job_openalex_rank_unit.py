"""job_openalex_rank 单元测试。

覆盖：
    - openalex_rank_task 建表与状态机
    - 最新日期选择、completed 跳过、失败重试
    - advisory lock
    - SQL 结果集读取和两个 Parquet 导出
    - run / verify / CLI 参数错误

全部使用 Mock，不连接 PG、不执行 SQL、不上传 HF。
"""
import json
import re
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import jobs.job_openalex_rank as rank
from common.rank_paths import GITHUB_RANK_FILES

AS_OF = date(2026, 10, 1)


def test_ensure_task_table_creates_control_table():
    pg = MagicMock()
    rank.ensure_task_table(pg)
    assert "CREATE TABLE IF NOT EXISTS openalex_rank_task" in pg.execute.call_args_list[0][0][0]
    assert "idx_openalex_rank_task_status" in pg.execute.call_args_list[1][0][0]


def test_latest_task_uses_date_desc():
    pg = MagicMock()
    pg.fetch_one.return_value = (AS_OF, "pending")
    assert rank.latest_task(pg) == (AS_OF, "pending")
    assert "ORDER BY as_of_date DESC" in pg.fetch_one.call_args[0][0]


def test_status_transitions_use_single_control_table():
    pg = MagicMock()
    rank.mark_running(pg, AS_OF)
    rank.mark_completed(pg, AS_OF, {"openalex_ranking_top500": 100})
    rank.mark_failed(pg, AS_OF, "boom")

    running_sql = pg.execute.call_args_list[0][0][0]
    completed_sql = pg.execute.call_args_list[1][0][0]
    failed_sql = pg.execute.call_args_list[2][0][0]
    assert "status = 'running'" in running_sql
    assert "status = 'completed'" in completed_sql
    assert "row_counts" in completed_sql
    assert json.loads(pg.execute.call_args_list[1][0][1][0]) == {
        "openalex_ranking_top500": 100
    }
    assert "status = 'failed'" in failed_sql


def test_acquire_and_release_lock():
    pg = MagicMock()
    pg.fetch_one.return_value = (True,)
    log = MagicMock()
    assert rank.acquire_lock(pg, "test", log) is True
    assert "pg_try_advisory_lock" in pg.fetch_one.call_args[0][0]
    rank.release_lock(pg, "test")
    assert "pg_advisory_unlock" in pg.execute.call_args[0][0]


def test_execute_rank_sql_reads_two_counts():
    pg = MagicMock()
    pg.conn.autocommit = False
    cur = MagicMock()
    cur.description = [("table_name",), ("rows",)]
    cur.fetchall.return_value = [
        ("openalex_ranking_top500", 120),
        ("paper_openalex_details", 80),
    ]
    pg.conn.cursor.return_value.__enter__.return_value = cur

    counts = rank.execute_rank_sql(pg, MagicMock())

    assert counts == {
        "openalex_ranking_top500": 120,
        "paper_openalex_details": 80,
    }


def test_export_writes_two_fixed_parquet_files(tmp_path):
    ranking = pd.DataFrame([{
        "as_of_date": AS_OF, "window_name": "recent_3m", "rank": 1,
        "arxiv_id": "1234.5678", "title": "T", "cited_by_count": 10,
    }])
    details = pd.DataFrame([{
        "as_of_date": AS_OF, "arxiv_id": "1234.5678", "title": "T",
        "cited_by_count": 10,
    }])
    pg = MagicMock()
    with patch(
        "jobs.job_openalex_rank.pd.read_sql",
        side_effect=[ranking, details],
    ):
        rank.export_outputs(pg, AS_OF, tmp_path, MagicMock())

    assert {p.name for p in tmp_path.glob("*.parquet")} == {
        "openalex_ranking_top500.parquet",
        "paper_openalex_details.parquet",
    }


def test_export_rejects_empty_results(tmp_path):
    pg = MagicMock()
    empty = pd.DataFrame()
    with patch(
        "jobs.job_openalex_rank.pd.read_sql",
        side_effect=[empty, empty],
    ):
        with pytest.raises(RuntimeError, match="导出为空"):
            rank.export_outputs(pg, AS_OF, tmp_path, MagicMock())


def test_publish_skips_only_with_explicit_flag(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENALEX_RANK_SKIP_UPLOAD", "1")
    with patch("jobs.job_openalex_rank.upload_hf") as upload:
        rank.publish_outputs(tmp_path, MagicMock())
    upload.assert_not_called()


def test_publish_requires_bucket(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENALEX_RANK_SKIP_UPLOAD", raising=False)
    monkeypatch.setattr(
        rank, "cfg", type("Cfg", (), {"HF_BUCKET_PATH": ""})()
    )
    with pytest.raises(RuntimeError, match="HF_BUCKET_PATH"):
        rank.publish_outputs(tmp_path, MagicMock())


@patch("jobs.job_openalex_rank.PGClient")
@patch("jobs.job_openalex_rank.release_lock")
@patch("jobs.job_openalex_rank.acquire_lock", return_value=True)
@patch("jobs.job_openalex_rank.latest_task", return_value=(AS_OF, "completed"))
@patch("jobs.job_openalex_rank.ensure_task_table")
def test_run_skips_completed(
    _ensure, _latest, _acquire, _release, pg_cls
):
    result = rank.run("test", MagicMock())
    assert result == 0
    _release.assert_called_once()


@patch("jobs.job_openalex_rank.PGClient")
@patch("jobs.job_openalex_rank.release_lock")
@patch("jobs.job_openalex_rank.mark_completed")
@patch("jobs.job_openalex_rank.publish_outputs")
@patch("jobs.job_openalex_rank.export_outputs")
@patch("jobs.job_openalex_rank.execute_rank_sql", return_value={"openalex_ranking_top500": 5})
@patch("jobs.job_openalex_rank.clean_output_dir")
@patch("jobs.job_openalex_rank.current_pg_date", return_value=AS_OF)
@patch("jobs.job_openalex_rank.mark_running")
@patch("jobs.job_openalex_rank.latest_task", return_value=(AS_OF, "pending"))
@patch("jobs.job_openalex_rank.acquire_lock", return_value=True)
@patch("jobs.job_openalex_rank.ensure_task_table")
def test_run_success_runs_full_chain(
    _ensure, _acquire, _latest, _running, _today, _clean,
    _execute, _export, _publish, _completed, _release, pg_cls
):
    result = rank.run("test", MagicMock())
    assert result == 0
    _execute.assert_called_once()
    _export.assert_called_once()
    _publish.assert_called_once()
    _completed.assert_called_once()
    assert _clean.call_args.kwargs["keep_files"] == GITHUB_RANK_FILES
    _release.assert_called_once()


@patch("jobs.job_openalex_rank.PGClient")
@patch("jobs.job_openalex_rank.release_lock")
@patch("jobs.job_openalex_rank.mark_failed")
@patch("jobs.job_openalex_rank.execute_rank_sql", side_effect=RuntimeError("boom"))
@patch("jobs.job_openalex_rank.clean_output_dir")
@patch("jobs.job_openalex_rank.current_pg_date", return_value=AS_OF)
@patch("jobs.job_openalex_rank.mark_running")
@patch("jobs.job_openalex_rank.latest_task", return_value=(AS_OF, "failed"))
@patch("jobs.job_openalex_rank.acquire_lock", return_value=True)
@patch("jobs.job_openalex_rank.ensure_task_table")
def test_run_failure_marks_failed_and_raises(
    _ensure, _acquire, _latest, _running, _today, _clean,
    _execute, _failed, _release, pg_cls
):
    with pytest.raises(RuntimeError, match="boom"):
        rank.run("test", MagicMock())
    _failed.assert_called_once()
    _release.assert_called_once()


def test_verify_reports_success():
    pg = MagicMock()
    pg.fetch_one.side_effect = [
        (AS_OF, "completed"),
        (100, 6, 500),
        (80,),
        (0,),
    ]
    pg.fetch_all.return_value = []
    with patch("jobs.job_openalex_rank.PGClient", return_value=pg):
        assert rank.verify(MagicMock()) == 0


def test_verify_detects_bad_window():
    pg = MagicMock()
    pg.fetch_one.side_effect = [
        (AS_OF, "completed"),
        (501, 2, 501),
        (400,),
        (0,),
    ]
    pg.fetch_all.return_value = [("recent_3m", 501, 501)]
    with patch("jobs.job_openalex_rank.PGClient", return_value=pg):
        assert rank.verify(MagicMock()) == 1


def test_main_rejects_date_arguments():
    with patch.dict("os.environ", {"ENV": "test"}):
        assert rank.main(["run", "2026-10-01"]) == 2


def test_sql_contract_has_500_limit_and_required_filters():
    sql = rank.SQL_FILE.read_text(encoding="utf-8")
    assert "LIMIT 500" in sql
    assert "cited_by_count > 0" in sql
    assert "is_retracted IS NOT TRUE" in sql
    assert re.search(r"is_paratext\s+IS NOT TRUE", sql)
    assert "MAX(as_of_date)" not in sql
    assert "ORDER BY as_of_date DESC" in sql
