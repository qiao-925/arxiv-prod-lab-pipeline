#!/usr/bin/env python3
"""job_daily_push.py - 按日期分区逐日推送到 Kafka（时间模拟）

数据流：
    data/sorted{,_test}/<batch>/publish_date=YYYY-MM-DD/*.parquet
        ↓ 逐日期分区读取
    Kafka: arxiv-papers-{ENV}
        ↓ 每个日期分区一行
    PG: daily_push_log（按 (job_name, publish_date) 记账）

时间模拟：
    每处理一个日期分区，与下一次之间 sleep(SIM_DAY_SECONDS) 秒。
    自适应：若本日推送耗时 > SIM_DAY_SECONDS，则不 sleep。
    默认 5 秒 = 1 天，可通过环境变量 SIM_DAY_SECONDS 调整（0 = 全速）。

断点续传：
    查询 daily_push_log 中 status='COMMITTED' 的 publish_date，跳过。
    表主键 (job_name, publish_date) 天然幂等。

用法：
    ENV=test python jobs/job_daily_push.py
    ENV=test SIM_DAY_SECONDS=0 python jobs/job_daily_push.py     # 全速
    ENV=test SIM_DAY_SECONDS=1 python jobs/job_daily_push.py     # 快速演示

依赖环境变量：
    ENV                 test / prod（必须显式设置；未设即报错）
    SIM_DAY_SECONDS     可选，默认 5
    DATA_ROOT / DATA_SORTED   可选，默认按 ENV 派生
    KAFKA_BOOTSTRAP / PG_* / MINIO_*   由 common.config 从 Infisical 拉
"""
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from common.paths import DATA_SORTED,ROOT
from common.kafka_client import KafkaClient
from common.logger import setup_logger
from common.pg_client import PGClient


# ═══════════════════════════════════════════════════════════════════════
# 工具
# ═══════════════════════════════════════════════════════════════════════
def _compute_checksum(records: list) -> str:
    h = hashlib.sha256()
    for rec in records:
        h.update(json.dumps(rec, sort_keys=True, ensure_ascii=False).encode("utf-8"))
    return h.hexdigest()


# ═══════════════════════════════════════════════════════════════════════
# 扫描 Parquet 目录
# ═══════════════════════════════════════════════════════════════════════
def scan_dates(data_sorted: Path) -> dict:
    """扫描 data_sorted/，返回 {publish_date: [parquet 文件列表]}。

    目录结构：
        data_sorted/<batch>/publish_date=YYYY-MM-DD/*.parquet
    同一日期可能出现在多个 batch 目录下（不同批次预处理），合并。
    """
    from collections import defaultdict

    dates_to_files = defaultdict(list)
    if not data_sorted.exists():
        return {}

    for batch_dir in sorted(data_sorted.iterdir()):
        if not batch_dir.is_dir():
            continue
        for date_dir in sorted(batch_dir.glob("publish_date=*")):
            date_str = date_dir.name.split("=", 1)[1]
            parquets = sorted(date_dir.glob("*.parquet"))
            dates_to_files[date_str].extend(parquets)

    return dict(dates_to_files)


# ═══════════════════════════════════════════════════════════════════════
# daily_push_log 表操作（业务记账）
# ═══════════════════════════════════════════════════════════════════════
def ensure_table(pg: PGClient) -> None:
    pg.execute("""
        CREATE TABLE IF NOT EXISTS daily_push_log (
            job_name      VARCHAR(50) NOT NULL,
            publish_date  DATE        NOT NULL,
            status        VARCHAR(20) NOT NULL,
            record_count  INT         DEFAULT 0,
            checksum      VARCHAR(64),
            error_message TEXT,
            started_at    TIMESTAMP   DEFAULT NOW(),
            committed_at  TIMESTAMP,
            PRIMARY KEY (job_name, publish_date)
        )
    """)
    pg.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_push_log_status
        ON daily_push_log (job_name, status)
    """)


def query_committed_dates(pg: PGClient, job_name: str) -> set:
    rows = pg.fetch_all("""
        SELECT publish_date FROM daily_push_log
        WHERE job_name = %s AND status = 'COMMITTED'
    """, (job_name,))
    return {str(r[0]) for r in rows}


def mark_pending(pg: PGClient, job_name: str, publish_date: str) -> None:
    pg.execute("""
        INSERT INTO daily_push_log (job_name, publish_date, status, started_at)
        VALUES (%s, %s, 'PENDING', NOW())
        ON CONFLICT (job_name, publish_date) DO UPDATE
        SET status = 'PENDING',
            started_at = NOW(),
            error_message = NULL
    """, (job_name, publish_date))


def mark_committed(pg: PGClient, job_name: str, publish_date: str,
                   record_count: int, checksum: str) -> None:
    pg.execute("""
        UPDATE daily_push_log
        SET status = 'COMMITTED',
            record_count = %s,
            checksum = %s,
            committed_at = NOW()
        WHERE job_name = %s AND publish_date = %s
    """, (record_count, checksum, job_name, publish_date))


def mark_failed(pg: PGClient, job_name: str, publish_date: str, error: str) -> None:
    pg.execute("""
        UPDATE daily_push_log
        SET status = 'FAILED', error_message = %s
        WHERE job_name = %s AND publish_date = %s
    """, (error[:500], job_name, publish_date))


# ═══════════════════════════════════════════════════════════════════════
# 单日推送
# ═══════════════════════════════════════════════════════════════════════
def push_one_day(kafka: KafkaClient, pg: PGClient,
                 kafka_topic: str, job_name: str,
                 publish_date: str, parquet_files: list, log) -> int:
    """读一个日期分区的所有 Parquet，逐条 send 到 Kafka，返回条数。"""
    import pyarrow.parquet as pq

    mark_pending(pg, job_name, publish_date)

    count = 0
    records = []

    for f in parquet_files:
        table = pq.read_table(f)
        for row in table.to_pylist():
            raw_json = row.get("raw_json")
            if not raw_json:
                continue
            try:
                paper = json.loads(raw_json)
            except json.JSONDecodeError:
                continue

            paper["_publish_date"] = publish_date
            paper["_ingested_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

            kafka.send(kafka_topic, paper)
            records.append(paper)
            count += 1

    kafka.flush()
    checksum = _compute_checksum(records)
    mark_committed(pg, job_name, publish_date, count, checksum)

    return count


# ═══════════════════════════════════════════════════════════════════════
# 核心逻辑：纯函数，参数全显式
# ═══════════════════════════════════════════════════════════════════════
def run(
    kafka_topic: str,
    job_name: str,
    sim_day_seconds: float,
    log,
) -> None:
    """按日期逐日推送。所有依赖显式传入，不读全局、不 sys.exit。"""
    log.info("=" * 70)
    log.info(f"Daily Push Job | job_name={job_name} | sim_day_seconds={sim_day_seconds}")
    log.info(f"数据目录: {DATA_SORTED}")
    log.info(f"Kafka: {kafka_topic}")
    log.info("=" * 70)

    # 1. 扫描可用日期
    dates_to_files = scan_dates(DATA_SORTED)
    available = sorted(dates_to_files.keys())
    log.info(f"扫描到 {len(available)} 个日期分区")
    if not available:
        log.warning("没有可用数据，退出")
        return

    # 2. 连接 Kafka / PG
    kafka = KafkaClient(log)
    pg = PGClient(log)

    try:
        # 3. 建表 + 查询已提交日期
        ensure_table(pg)
        committed = query_committed_dates(pg, job_name)
        pending = [d for d in available if d not in committed]

        log.info(f"已提交: {len(committed)} 天，待推送: {len(pending)} 天")
        if not pending:
            log.info("✅ 所有日期已推送，退出")
            return

        # 4. 逐日推送
        total_records = 0
        for i, publish_date in enumerate(pending):
            log.info(f"[{i+1}/{len(pending)}] {publish_date} "
                     f"({len(dates_to_files[publish_date])} 个 Parquet 文件)")

            t0 = time.time()
            try:
                n = push_one_day(kafka, pg, kafka_topic, job_name, publish_date,
                                 dates_to_files[publish_date], log)
                total_records += n
                log.info(f"  ✅ 推送 {n} 条")
            except Exception as e:
                log.error(f"  ❌ 失败: {e}")
                mark_failed(pg, job_name, publish_date, str(e))
                raise

            # 自适应 sleep：本日耗时若超过 sim_day_seconds，不 sleep
            if sim_day_seconds > 0 and i < len(pending) - 1:
                elapsed = time.time() - t0
                wait = max(0.0, sim_day_seconds - elapsed)
                if wait > 0:
                    log.info(f"  ⏱  模拟时钟等待 {wait:.2f}s")
                    time.sleep(wait)

        log.info("=" * 70)
        log.info(f"完成: 推送 {len(pending)} 天, {total_records} 条")
        log.info("=" * 70)

    finally:
        kafka.close()
        pg.close()


# ═══════════════════════════════════════════════════════════════════════
# 入口：读 ENV、派生资源名、调用 run
# ═══════════════════════════════════════════════════════════════════════
def main() -> int:
    """配置层：读 ENV → 派生资源名 → 调用 run。返回退出码。"""
    env = os.environ.get("ENV")
    if env not in ("test", "prod"):
        raise RuntimeError(
            f"ENV 必须是 test/prod，当前: {env!r}\n"
            f"用法：ENV=test python jobs/job_daily_push.py"
        )

    sim_day_seconds = float(os.getenv("SIM_DAY_SECONDS", "5"))

    run(
        kafka_topic=f"arxiv-papers-{env}",
        job_name=f"daily-push-{env}",
        sim_day_seconds=sim_day_seconds,
        log=setup_logger(),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(1)