"""jobs - 业务 Job 包（数据接入的完整实现）

模块平铺在本目录：
    scheduler.py              调度器入口与子进程监督
    job_daily_arxiv_oai_pull  arXiv OAI-PMH 增量采集
    job_hourly_github_crawl   GitHub 仓库发现与论文关联
    job_hourly_github_sync    GitHub 活跃度增量同步
    job_daily_github_rank     热度排名与 Parquet 导出
    rank_sql/                 排名聚合 SQL
    upload_hf.sh              Hugging Face Bucket 上传包装
"""
