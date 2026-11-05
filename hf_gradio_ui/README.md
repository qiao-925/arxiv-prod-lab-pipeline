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

# Arxiv & Github Ranking

GitHub 工程活跃度排名（Star / Fork / Issue / PR 四列同表展示，表头点击排序）。

## 环境变量

优先使用平台挂载到 `/data` 的 Parquet，无需配置变量；
`HF_BUCKET_PATH` 仅作为未挂载 `/data` 时的回退项（本地联调或 hf:// bucket）。

## 数据源

由 `arxiv-prod-lab-pipeline` 仓库的 `jobs.job_daily_github_rank` 每日生成快照，
并同步到挂载目录 `/data`（或 HF Bucket）。
