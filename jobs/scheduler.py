"""应用入口：常驻调度器（arXiv 采集 + GitHub 全链路）。

═══════════════════════════════════════════════════════════════════════
一、全局 Job 地图（2026-10-30 重构版）
═══════════════════════════════════════════════════════════════════════

数据流：

  GitHub 搜索发现 + README 提取（合并）
    Search API ──每小时──► job_hourly_github_crawl
                             ├─► features_repo（canonical）
                             └─► edges_repo_arxiv（canonical）

  arXiv OAI 增量拉取
    OAI API ──每小时──► job_daily_arxiv_oai_pull
                         ├─► MinIO（数据湖归档）
                         └─► arxiv_papers（canonical）

  GitHub 仓库活跃度增量同步
    REST / GraphQL ──每小时──► job_hourly_github_sync
                                ├─► P0（14天窗口）：扫描最近 14 天关联论文的仓库
                                └─► P2（全量扫描）：按轮扫描所有仓库
                                        │
                                        ▼
                              events_stars / events_forks
                              events_issues / events_prs

二、Job 清单

  | Job                         | 水位 / 状态表                                              | 写入                                  | 只读                                  |
  |-----------------------------|------------------------------------------------------------|---------------------------------------|---------------------------------------|
  | job_daily_arxiv_oai_pull    | arxiv_oai_pull_log                                         | MinIO OAI 分区、arxiv_papers          | —                                     |
  | job_hourly_github_crawl     | job_hourly_github_crawl_task                               | features_repo、edges_repo_arxiv       | —                                     |
  | job_hourly_github_sync      | job_hourly_github_sync_p0_task / p2_task / repo_task       | events_stars/forks/issues/prs         | features_repo、edges_repo_arxiv、     |
  |                             |                                                            |                                       | arxiv_papers                          |

三、调度规则

  1. 启动必跑：每个 job 在调度器启动后按固定间隔错开各跑一次。
  2. 整点轮巡 + 加盐：每小时 minute=0 触发，CronTrigger 加 0–JITTER 秒随机抖动。
  3. 水位自控：scheduler 不做业务判断，由各 job 入口检查自己的状态表：
       OAI     MAX(COMMITTED) >= 昨天
       crawl   task_date/task_hour 推进到昨天
       sync    P0/P2 工作状态自控（限额暂停后续跑）
  4. 同 job 不并发：进程内 threading.Lock + APScheduler max_instances=1。
     多副本部署时需把 Lock 换成 PG advisory lock（当前 replicas=1）。

四、执行方式

  scheduler 只做监督，不直接 import 业务模块；每个 job 以独立子进程运行：

      python -m jobs.<module> [argv]

  这样某个 job 崩溃、卡死或超时不会影响其他 job。

五、设计约束

  - 表所有权：
      * job_hourly_github_crawl 拥有 job_hourly_github_crawl_task，对 canonical 做 upsert
      * job_hourly_github_sync 拥有 p0_task/p2_task/repo_task，对 events_* 做 upsert
      * job_daily_arxiv_oai_pull 拥有 arxiv_oai_pull_log，对 arxiv_papers/MinIO 做 upsert
  - GitScholar 历史导入是一次性 seed，禁止 TRUNCATE / DROP / 全表删除。
  - 所有长任务用 PG 账本表 + 状态列续跑，重跑幂等。
"""
import os
import subprocess
import sys
import threading
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from common.logger import setup_logger
from common.paths import ROOT

logger = setup_logger("scheduler")

TZ = ZoneInfo("Asia/Shanghai")
JITTER_SECONDS = 120
STARTUP_STAGGER_SECONDS = 30
DEFAULT_TIMEOUT_SECONDS = 3300


@dataclass(frozen=True)
class JobSpec:
    id: str
    label: str
    module: str
    argv: tuple = ()
    timeout: int = DEFAULT_TIMEOUT_SECONDS
    enabled: bool = True


JOB_SPECS = [
    JobSpec("oai_pull", "OAI-PMH pull", "jobs.job_daily_arxiv_oai_pull"),
    JobSpec("github_crawl", "GitHub crawl", "jobs.job_hourly_github_crawl", argv=("run",)),
    JobSpec("github_sync", "GitHub sync", "jobs.job_hourly_github_sync", argv=("run",)),
]

# 进程内互斥：启动任务和整点任务使用不同 APScheduler job id，
# max_instances 管不到彼此，所以每个逻辑 job 单独一把锁。
_LOCKS = {spec.id: threading.Lock() for spec in JOB_SPECS if spec.enabled}

# 优雅退出信号：SIGTERM/SIGINT 触发后主循环退出，APScheduler 等 job 线程完成
_shutdown = threading.Event()


def _handle_signal(signum, frame):
    logger.info(f"收到信号 {signum}，准备退出")
    _shutdown.set()



def _run_job(spec: JobSpec, source: str) -> None:
    """以子进程运行一个 job；同 job 已在跑则直接跳过。"""
    lock = _LOCKS[spec.id]
    if not lock.acquire(blocking=False):
        logger.warning(f"[{source}] {spec.label} 上一轮仍在运行，跳过")
        return

    cmd = [sys.executable, "-m", spec.module, *spec.argv]
    started = time.monotonic()
    logger.info(
        f"[{source}] {spec.label} 开始 | timeout={spec.timeout}s | "
        f"cmd={' '.join(cmd)}"
    )
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=os.environ.copy(),
            timeout=spec.timeout,
            check=False,
        )
        elapsed = time.monotonic() - started
        if proc.returncode == 0:
            logger.info(f"[{source}] {spec.label} 完成 | {elapsed:.1f}s")
        else:
            logger.error(
                f"[{source}] {spec.label} 退出码 {proc.returncode} | {elapsed:.1f}s"
            )
    except subprocess.TimeoutExpired:
        logger.error(
            f"[{source}] {spec.label} 超时 {spec.timeout}s，已终止；"
            f"进度由 job 自己的状态表续跑"
        )
    except Exception:
        logger.exception(f"[{source}] {spec.label} 启动失败")
    finally:
        lock.release()


def _next_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)


def _register_jobs(scheduler: BackgroundScheduler, now: datetime):
    specs = [spec for spec in JOB_SPECS if spec.enabled]
    startup_window = max(0, len(specs) - 1) * STARTUP_STAGGER_SECONDS
    # 启动任务覆盖当前小时；第一次整点轮巡放到下一个整点之后，
    # 避免“启动任务还没跑完，整点任务又进来”。
    first_hour = _next_hour(now + timedelta(seconds=startup_window + 60))

    for i, spec in enumerate(specs):
        scheduler.add_job(
            _run_job,
            trigger="date",
            run_date=now + timedelta(seconds=i * STARTUP_STAGGER_SECONDS),
            args=[spec, "startup"],
            id=f"{spec.id}@startup",
            name=f"{spec.label} (startup)",
            misfire_grace_time=600,
            replace_existing=True,
        )
        scheduler.add_job(
            _run_job,
            trigger=CronTrigger(
                minute=0,
                jitter=JITTER_SECONDS,
                start_date=first_hour,
                timezone=TZ,
            ),
            args=[spec, "hourly"],
            id=f"{spec.id}@hourly",
            name=f"{spec.label} (hourly)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
            replace_existing=True,
        )
    return specs, first_hour


def main() -> int:
    env = os.environ.get("ENV")
    if env not in ("test", "prod"):
        raise RuntimeError(f"ENV 必须是 test/prod，当前: {env!r}")

    now = datetime.now(TZ)
    scheduler = BackgroundScheduler(
        timezone=TZ,
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 600,
        },
    )
    specs, first_hour = _register_jobs(scheduler, now)
    scheduler.start()

    logger.info("=" * 70)
    logger.info(f"调度器启动 | env={env} | {len(specs)} 个 job")
    logger.info(f"  启动：按 {STARTUP_STAGGER_SECONDS}s 间隔各跑一次")
    logger.info(f"  轮巡：每小时整点 + jitter 0–{JITTER_SECONDS}s")
    logger.info(f"  首轮整点：{first_hour}（+ jitter）")
    logger.info("  同 job 互斥：进程内锁 + max_instances=1")
    for spec in specs:
        logger.info(f"  - {spec.id}: {spec.label} (timeout={spec.timeout}s)")
    logger.info("=" * 70)

    # 注册信号：SIGTERM 是 K8s 发的，SIGINT 是本地 Ctrl+C
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        while not _shutdown.is_set():
            _shutdown.wait(timeout=60)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("等待活跃 job 完成（受 terminationGracePeriodSeconds 保护）...")
        # wait=True 让 job 线程有机会完成；K8s 在 grace 到期后 SIGKILL
        scheduler.shutdown(wait=True)
        logger.info("scheduler 已关闭")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(1)