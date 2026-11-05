"""job_daily_github_rank 集成测试（真实 PG 测试环境）

覆盖：
    - github_rank_run_log 幂等建表与状态转换
    - 完整 SQL 执行（RANK_TEST_FULL_SQL=1 时启用）：
      四表覆盖生成、唯一行数摘要、parquet 导出
"""
import os
from datetime import date

import pandas as pd
import pytest

from common.logger import setup_logger
from common.pg_client import PGClient
from jobs.job_daily_github_rank import (
    current_pg_date, ensure_table, execute_rank_sql, export_details,
    export_rankings, mark_completed, mark_failed, mark_running,
    next_snapshot_date,
)

TEST_JOB = "integration-github-rank-test"


@pytest.fixture
def pg():
    client = PGClient(setup_logger("integration_github_rank"))
    ensure_table(client)
    client.execute(
        "DELETE FROM github_rank_run_log WHERE job_name = %s",
        (TEST_JOB,),
    )
    yield client
    client.execute(
        "DELETE FROM github_rank_run_log WHERE job_name = %s",
        (TEST_JOB,),
    )
    client.close()


def test_ensure_table_idempotent(pg):
    ensure_table(pg)
    ensure_table(pg)
    row = pg.fetch_one(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'github_rank_run_log'"
    )
    assert row[0] == 1


def test_watermark_state_transitions(pg):
    today = current_pg_date(pg)
    mark_completed(pg, TEST_JOB, today, {"ranking_top500": 3})
    assert next_snapshot_date(pg, TEST_JOB, today) is None

    mark_failed(pg, TEST_JOB, today, "boom")
    assert next_snapshot_date(pg, TEST_JOB, today) == today

    mark_running(pg, TEST_JOB, today)
    assert next_snapshot_date(pg, TEST_JOB, today) == today

    row = pg.fetch_one(
        "SELECT status, row_counts FROM github_rank_run_log "
        "WHERE job_name = %s AND snapshot_date = %s ORDER BY status LIMIT 1",
        (TEST_JOB, today),
    )
    assert row is not None
    assert "ranking_top500" in str(row[1])


@pytest.mark.skipif(
    os.getenv("RANK_TEST_FULL_SQL", "0") != "1",
    reason="RANK_TEST_FULL_SQL=1 才执行完整排名 SQL",
)
def test_full_sql_and_export(pg, tmp_path):
    today = current_pg_date(pg)
    counts = execute_rank_sql(pg, setup_logger("integration_github_rank_full"))

    assert set(counts) == {
        "ranking_top500",
        "top500_arxiv_repo",
        "paper_details",
        "repo_details",
    }

    export_rankings(pg, today, tmp_path, setup_logger("integration_github_rank_export"))
    export_details(pg, today, tmp_path, setup_logger("integration_github_rank_export"))

    assert (tmp_path / "ranking_top500.parquet").exists()
    assert (tmp_path / "top500_arxiv_repo.parquet").exists()
    assert not (tmp_path / "views.parquet").exists()
    assert (tmp_path / "paper_details_github.parquet").exists()
    assert (tmp_path / "repo_details.parquet").exists()
    ranking = pd.read_parquet(tmp_path / "ranking_top500.parquet")
    assert set(ranking.columns) == {
        "snapshot_date", "window_name", "metric", "rank",
        "arxiv_id", "title", "metric_value",
    }
    assert ranking["window_name"].nunique() >= 1
    bridge = pd.read_parquet(tmp_path / "top500_arxiv_repo.parquet")
    assert set(bridge.columns) == {"snapshot_date", "arxiv_id", "repo_id"}
    row = pg.fetch_one("SELECT COUNT(*) FROM ranking_top500")
    assert row[0] > 0
