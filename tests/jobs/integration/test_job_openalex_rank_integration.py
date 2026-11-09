"""job_openalex_rank 集成测试（真实 PG 测试环境）。

覆盖：
    - openalex_rank_task 与控制状态
    - 完整 OpenAlex SQL 的窗口、Top 500 和详情关联
    - 同 as_of_date 重跑幂等
    - 两个 Parquet 导出

完整 SQL 测试默认跳过；设置 OPENALEX_RANK_FULL_SQL=1 启用。
测试使用 2099-01-01 哨兵日期，避免影响已有真实任务版本。
"""
import os
from datetime import date

import pandas as pd
import pytest

from common.logger import setup_logger
from common.pg_client import PGClient
from jobs.job_openalex_rank import (
    ensure_task_table,
    execute_rank_sql,
    export_outputs,
    latest_task,
)

TEST_AS_OF = date(2099, 1, 1)


@pytest.fixture
def pg():
    client = PGClient(setup_logger("integration_openalex_rank"))
    ensure_task_table(client)
    client.execute(
        "DELETE FROM openalex_rank_task WHERE as_of_date = %s",
        (TEST_AS_OF,),
    )
    client.execute(
        "DELETE FROM openalex_ranking_top500 WHERE as_of_date = %s",
        (TEST_AS_OF,),
    )
    client.execute(
        "DELETE FROM paper_openalex_details WHERE as_of_date = %s",
        (TEST_AS_OF,),
    )
    client.execute("""
        INSERT INTO openalex_rank_task (as_of_date, status)
        VALUES (%s, 'pending')
    """, (TEST_AS_OF,))
    yield client
    client.execute(
        "DELETE FROM openalex_rank_task WHERE as_of_date = %s",
        (TEST_AS_OF,),
    )
    client.execute(
        "DELETE FROM openalex_ranking_top500 WHERE as_of_date = %s",
        (TEST_AS_OF,),
    )
    client.execute(
        "DELETE FROM paper_openalex_details WHERE as_of_date = %s",
        (TEST_AS_OF,),
    )
    client.close()


def test_control_table_and_latest_date(pg):
    task = latest_task(pg)
    assert task is not None
    assert task[0] >= TEST_AS_OF
    assert pg.fetch_one(
        "SELECT COUNT(*) FROM openalex_rank_task WHERE as_of_date = %s",
        (TEST_AS_OF,),
    )[0] == 1


@pytest.mark.skipif(
    os.getenv("OPENALEX_RANK_FULL_SQL", "0") != "1",
    reason="OPENALEX_RANK_FULL_SQL=1 才执行完整 OpenAlex 排名 SQL",
)
def test_full_sql_top500_idempotent_and_export(pg, tmp_path):
    log = setup_logger("integration_openalex_rank_full")
    first_counts = execute_rank_sql(pg, log)
    second_counts = execute_rank_sql(pg, log)

    assert first_counts == second_counts
    assert first_counts["openalex_ranking_top500"] > 0
    assert first_counts["paper_openalex_details"] > 0

    bad_windows = pg.fetch_all("""
        SELECT window_name, COUNT(*), MAX(rank)
        FROM openalex_ranking_top500
        WHERE as_of_date = %s
        GROUP BY window_name
        HAVING COUNT(*) > 500 OR COUNT(*) <> MAX(rank)
    """, (TEST_AS_OF,))
    missing_details = pg.fetch_one("""
        SELECT COUNT(DISTINCT r.arxiv_id)
        FROM openalex_ranking_top500 r
        LEFT JOIN paper_openalex_details d
          ON d.as_of_date = r.as_of_date AND d.arxiv_id = r.arxiv_id
        WHERE r.as_of_date = %s AND d.arxiv_id IS NULL
    """, (TEST_AS_OF,))[0]
    ineligible_rows = pg.fetch_one("""
        SELECT COUNT(*)
        FROM openalex_ranking_top500 r
        JOIN openalex_works o ON o.arxiv_id = r.arxiv_id
        WHERE r.as_of_date = %s
          AND (
              o.cited_by_count <= 0
              OR o.is_retracted
              OR o.is_paratext
          )
    """, (TEST_AS_OF,))[0]
    assert bad_windows == []
    assert missing_details == 0
    assert ineligible_rows == 0

    export_outputs(pg, TEST_AS_OF, tmp_path, log)
    assert (tmp_path / "openalex_ranking_top500.parquet").exists()
    assert (tmp_path / "paper_openalex_details.parquet").exists()
    window_names = set(
        pd.read_parquet(tmp_path / "openalex_ranking_top500.parquet")
        ["window_name"].unique()
    )
    assert {"year_1991", "total"} <= window_names
