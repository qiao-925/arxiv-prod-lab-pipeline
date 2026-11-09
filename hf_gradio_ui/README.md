---
title: Arxiv & Github Ranking
emoji: 📊
colorFrom: blue
colorTo: purple
sdk: gradio
sdk_version: 6.31.0
app_file: app.py
pinned: false
---

# Arxiv, Github & OpenAlex Ranking

左侧展示 GitHub 工程活跃度排名（Star / Fork / Issue / PR），
右侧展示 OpenAlex 累计引用排名。

## 环境变量

优先使用平台挂载到 `/data` 的 Parquet，无需配置变量；
`HF_BUCKET_PATH` 仅作为未挂载 `/data` 时的回退项（本地联调或 hf:// bucket）。

## 数据源

GitHub 数据由 `jobs.job_daily_github_rank` 每日生成；OpenAlex 数据由
`jobs.job_openalex_rank` 在数据集更新后手动生成：

```bash
ENV=prod python -m jobs.job_openalex_rank init

# OpenAlex 数据集更新后，在 PostgreSQL 中登记本次版本日期
# INSERT INTO openalex_rank_task (as_of_date) VALUES (DATE 'YYYY-MM-DD');

ENV=prod python -m jobs.job_openalex_rank run
ENV=prod python -m jobs.job_openalex_rank verify
```

生成的 `openalex_ranking_top500.parquet` 和
`paper_openalex_details.parquet` 同步到挂载目录 `/data`（或 HF Bucket）。

## 本地 UI 调试

默认读取两个排名 Job 的共享目录 `/tmp/ranking`；文件缺失时对应榜单展示空态：

```bash
GRADIO_LOCAL_DEBUG=1 .venv/bin/python hf_gradio_ui/app.py
```

可通过 `GRADIO_LOCAL_DATA_DIR` 指定其他 Parquet 目录。

如需加载真实本地数据：

```bash
HF_BUCKET_PATH=/absolute/parquet-dir .venv/bin/python hf_gradio_ui/app.py
```
