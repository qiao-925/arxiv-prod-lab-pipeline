"""job_daily_github_rank 单元测试套件

职责：
    - 检查 github_rank_run_log 水位状态机：running > failed > completed
    - 校验 SQL 唯一结果集（四表行数）读取与 row_counts 落账
    - 校验 Gradio parquet 导出：四张 PG 表按原结构镜像导出
    - 覆盖 run 成功 / 失败可重试 / 当天已完成跳过

只用 Mock，不连真实 PG、不执行 SQL、不上传 HF。
"""
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import jobs.job_daily_github_rank as rank
from jobs.job_daily_github_rank import (
    _split_sql_statements, clean_output_dir, execute_rank_sql,
    export_rankings, mark_completed, next_snapshot_date,
    upload_hf,
)

TARGET = date(2026, 11, 5)


# ═══════════════════════════════════════════════════════════════════════
# 1. 水位状态机
# ═══════════════════════════════════════════════════════════════════════

def test_next_snapshot_prefers_running():
    pg = MagicMock()
    pg.fetch_one.return_value = (TARGET,)
    assert next_snapshot_date(pg, "github-rank-test", TARGET) == TARGET
    sql = pg.fetch_one.call_args_list[0][0][0]
    assert "status = 'running'" in sql


def test_next_snapshot_retries_failed():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, (date(2026, 11, 4),)]
    assert next_snapshot_date(pg, "github-rank-test", TARGET) == date(2026, 11, 4)
    sql = pg.fetch_one.call_args_list[1][0][0]
    assert "status = 'failed'" in sql


def test_next_snapshot_skips_when_completed_today():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, None, (TARGET,)]
    assert next_snapshot_date(pg, "github-rank-test", TARGET) is None


def test_next_snapshot_returns_target_when_no_record():
    pg = MagicMock()
    pg.fetch_one.side_effect = [None, None, None]
    assert next_snapshot_date(pg, "github-rank-test", TARGET) == TARGET


def test_mark_completed_writes_row_counts():
    pg = MagicMock()
    counts = {"ranking_top500": 100, "paper_details": 80}
    mark_completed(pg, "github-rank-test", TARGET, counts)
    sql = pg.execute.call_args[0][0]
    params = pg.execute.call_args[0][1]
    assert "status = 'completed'" in sql
    assert "row_counts" in sql
    assert '"ranking_top500"' in params[2]


# ═══════════════════════════════════════════════════════════════════════
# 2. SQL 结果集读取
# ═══════════════════════════════════════════════════════════════════════

def test_execute_rank_sql_reads_only_row_counts():
    pg = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("ranking_top500", 10),
        ("top500_arxiv_repo", 8),
        ("paper_details", 6),
        ("repo_details", 4),
    ]
    pg.conn.cursor.return_value.__enter__.return_value = cur

    counts = execute_rank_sql(pg, MagicMock())

    assert counts == {
        "ranking_top500": 10,
        "top500_arxiv_repo": 8,
        "paper_details": 6,
        "repo_details": 4,
    }
    script = rank.SQL_FILE.read_text(encoding="utf-8")
    assert cur.execute.call_count == len(_split_sql_statements(script))


def test_split_sql_preserves_do_blocks():
    script = """
        BEGIN;
        DO $do$
        BEGIN
            UPDATE t SET a = 1;
            SELECT 2;
        END
        $do$;
        SELECT 1 AS n;
        COMMIT;
    """
    parts = _split_sql_statements(script)
    assert parts[0] == "BEGIN"
    assert any(part.startswith("DO $do$") for part in parts)
    assert parts[-2] == "SELECT 1 AS n"
    assert parts[-1] == "COMMIT"


def test_split_sql_preserves_empty_dollar_blocks():
    script = """
        DO $$
        BEGIN
            UPDATE t SET a = 1;
            SELECT 2;
        END
        $$;
        SELECT 3;
    """
    parts = _split_sql_statements(script)
    assert len([part for part in parts if part.startswith("DO $$")]) == 1
    assert parts[-1] == "SELECT 3"


# ═══════════════════════════════════════════════════════════════════════
# 3. 窗口映射与 Gradio 导出
# ═══════════════════════════════════════════════════════════════════════

def test_export_rankings_mirrors_pg_tables(tmp_path):
    ranking_df = pd.DataFrame([{
        "snapshot_date": TARGET, "window_name": "d14", "metric": "star",
        "rank": 1, "arxiv_id": "a", "title": "t", "metric_value": 10,
    }])
    bridge_df = pd.DataFrame([{
        "snapshot_date": TARGET, "arxiv_id": "a", "repo_id": 1,
    }])

    pg = MagicMock()
    pg.conn = MagicMock()
    with patch("jobs.job_daily_github_rank.pd.read_sql",
               side_effect=[ranking_df, bridge_df]) as mock_read:
        export_rankings(pg, TARGET, tmp_path, MagicMock())

    assert (tmp_path / "ranking_top500.parquet").exists()
    assert (tmp_path / "top500_arxiv_repo.parquet").exists()
    assert not (tmp_path / "views.parquet").exists()

    ranking = pd.read_parquet(tmp_path / "ranking_top500.parquet")
    assert list(ranking.columns) == [
        "snapshot_date", "window_name", "metric", "rank",
        "arxiv_id", "title", "metric_value",
    ]
    bridge = pd.read_parquet(tmp_path / "top500_arxiv_repo.parquet")
    assert list(bridge.columns) == ["snapshot_date", "arxiv_id", "repo_id"]
    assert mock_read.call_count == 2


# ═══════════════════════════════════════════════════════════════════════
# 4. 上传
# ═══════════════════════════════════════════════════════════════════════

def _fake_cfg(bucket: str = "", token: str = ""):
    return SimpleNamespace(
        HF_BUCKET_PATH=bucket,
        HF_TOKEN=token,
        HF_ENDPOINT="https://huggingface.co",
    )


def test_upload_skips_without_bucket(monkeypatch):
    monkeypatch.setattr(rank, "cfg", _fake_cfg())
    with patch("jobs.job_daily_github_rank.subprocess.run") as mock_run:
        upload_hf(Path("/tmp/ranking"), MagicMock())
    mock_run.assert_not_called()


def test_upload_requires_token(monkeypatch):
    monkeypatch.setattr(
        rank, "cfg",
        _fake_cfg(bucket="hf://buckets/u/arxiv-rankings"),
    )
    with patch("jobs.job_daily_github_rank.subprocess.run"):
        try:
            upload_hf(Path("/tmp/ranking"), MagicMock())
        except RuntimeError as e:
            assert "HF_TOKEN" in str(e)
        else:
            raise AssertionError("缺少 HF_TOKEN 时应报错")


def test_upload_success_logs_command_and_files(tmp_path, monkeypatch):
    monkeypatch.setattr(
        rank, "cfg",
        _fake_cfg(
            bucket="hf://buckets/u/arxiv-rankings",
            token="hf-secret",
        ),
    )
    (tmp_path / "d14").mkdir(parents=True)
    pd.DataFrame(
        [{"rank": 1, "arxiv_id": "a", "title": "t", "metric_value": 1}]
    ).to_parquet(tmp_path / "d14" / "star.parquet", index=False)

    log = MagicMock()
    with patch("jobs.job_daily_github_rank._stream_hf_sync",
               return_value=0) as mock_stream:
        upload_hf(tmp_path, log)

    mock_stream.assert_called_once()
    cmd = mock_stream.call_args[0][0]
    env = mock_stream.call_args[0][1]
    assert str(cmd[1]).endswith("upload_hf.sh")
    assert env["HF_DEBUG"] == "1"
    info = " ".join(str(c) for c in log.info.call_args_list)
    assert "upload_hf.sh" in info
    assert "hf sync" in info
    assert "1 个 parquet" in info
    assert "verbose=-v" in info
    assert "[upload] 完成" in info


# ═══════════════════════════════════════════════════════════════════════
# 5. run 主流程
# ═══════════════════════════════════════════════════════════════════════

def test_clean_output_dir_removes_stale_files(tmp_path):
    (tmp_path / "old.parquet").write_text("stale")
    (tmp_path / "old_dir").mkdir()
    (tmp_path / "old_dir" / "x.parquet").write_text("stale")

    log = MagicMock()
    clean_output_dir(tmp_path, log)

    assert not (tmp_path / "old.parquet").exists()
    assert not (tmp_path / "old_dir").exists()
    info = " ".join(str(c) for c in log.info.call_args_list)
    assert "已清空输出目录" in info
    assert "输出目录准备就绪" in info


def test_clean_output_dir_rejects_dangerous_path():
    with pytest.raises(RuntimeError, match="拒绝清理危险目录"):
        clean_output_dir(Path("/"), MagicMock())

@patch("jobs.job_daily_github_rank.PGClient")
@patch("jobs.job_daily_github_rank.upload_hf")
@patch("jobs.job_daily_github_rank.export_details")
@patch("jobs.job_daily_github_rank.export_rankings")
@patch("jobs.job_daily_github_rank.execute_rank_sql")
def test_run_success_marks_completed(mock_execute, mock_rankings, mock_details,
                                     mock_upload, mock_pg_cls):
    pg = MagicMock()
    pg.fetch_one.side_effect = [(TARGET,), None, None, None]
    mock_pg_cls.return_value = pg
    mock_execute.return_value = {"ranking_top500": 10}

    from jobs.job_daily_github_rank import run
    run("test", MagicMock())

    completed = [
        c for c in pg.execute.call_args_list
        if "status = 'completed'" in str(c[0][0])
    ]
    assert len(completed) == 1
    assert mock_upload.called


@patch("jobs.job_daily_github_rank.PGClient")
@patch("jobs.job_daily_github_rank.upload_hf")
@patch("jobs.job_daily_github_rank.export_details")
@patch("jobs.job_daily_github_rank.export_rankings")
@patch("jobs.job_daily_github_rank.execute_rank_sql")
def test_run_failure_marks_failed(mock_execute, mock_rankings, mock_details,
                                  mock_upload, mock_pg_cls):
    pg = MagicMock()
    pg.fetch_one.side_effect = [(TARGET,), None, None, None]
    mock_pg_cls.return_value = pg
    mock_execute.side_effect = RuntimeError("boom")

    from jobs.job_daily_github_rank import run
    run("test", MagicMock())  # 不 raise，账本可重试

    failed = [
        c for c in pg.execute.call_args_list
        if "status = 'failed'" in str(c[0][0])
    ]
    assert len(failed) == 1


@patch("jobs.job_daily_github_rank.PGClient")
@patch("jobs.job_daily_github_rank.execute_rank_sql")
def test_run_skips_completed_today(mock_execute, mock_pg_cls):
    pg = MagicMock()
    pg.fetch_one.side_effect = [(TARGET,), None, None, (TARGET,)]
    mock_pg_cls.return_value = pg

    from jobs.job_daily_github_rank import run
    run("test", MagicMock())

    mock_execute.assert_not_called()
