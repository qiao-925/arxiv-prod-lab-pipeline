"""应用入口：启动立即执行一次，之后按 cron 定时兜底。"""
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from common.logger import setup_logger

logger = setup_logger("scheduler")
JOB_HOUR = 20
JOB_MINUTE = 0

def _run_oai_pull():
    try:
        logger.info("OAI-PMH 拉取任务开始")
        from jobs.job_daily_arxiv_oai_pull import main
        main()
        logger.info("OAI-PMH 拉取任务完成")
    except Exception as e:
        logger.exception(f"OAI-PMH 拉取失败: {e}")

def main():
    logger.info("调度器启动：启动立即跑一次 + 每天 20:00 兜底")
    logger.info("[启动执行] 首次拉取 ...")
    _run_oai_pull()
    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(_run_oai_pull, CronTrigger(hour=JOB_HOUR, minute=JOB_MINUTE),
                      id="oai_pull", max_instances=1, coalesce=True, misfire_grace_time=3600)
    scheduler.start()
    logger.info("调度器已启动，等待定时触发 ...")
    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    main()
