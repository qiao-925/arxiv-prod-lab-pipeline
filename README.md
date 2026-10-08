# arxiv-prod-lab-pipeline

从 HuggingFace 拉取 arXiv 论文，按发布日期分区为 Parquet，逐日"回放"推送到 Kafka，并用 PostgreSQL 记录推送进度。5 秒 = 1 天，模拟真实数据流。

## 快速开始

```bash
uv venv && uv pip install -r requirements.txt
cp .env.example .env          # 填 ENV + Infisical 凭据

python tests/smoke_test.py    # 验证 Kafka / PG / MinIO 可达
python tests/run_e2e_test.py  # 全链路：下载 → 预处理 → 推送 → 对账
```

## 运行

```bash
# 逐日推送（5 秒 = 1 天）
ENV=test python jobs/job_daily_push.py

# 全速（调试）
ENV=test SIM_DAY_SECONDS=0 python jobs/job_daily_push.py

# 数据准备
python tools/download.py --limit 2000      # → data/raw_test
python tools/preprocess.py                 # → data/sorted_test
```

## 结构

```
common/     共享组件（config / kafka_client / pg_client / paths / logger）
jobs/       业务 job（job_daily_push.py）
tools/      数据准备（download / preprocess）
tests/      冒烟 + E2E
sql/        表 DDL
data/       原始 JSONL + 分区 Parquet
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `ENV` | 必填 | `test` / `prod`，未设即报错 |
| `SIM_DAY_SECONDS` | `5` | 几秒 = 1 天，`0` = 全速 |
| `DATA_RAW` / `DATA_SORTED` | 按 ROOT 派生 | 覆盖数据目录 |
| `KAFKA_BOOTSTRAP` / `PG_*` / `MINIO_*` | Infisical 拉取 | 密钥 |

## 环境隔离

同一套基础设施，靠资源名后缀区分：

| 资源 | test | prod |
|---|---|---|
| Kafka Topic | `arxiv-papers-test` | `arxiv-papers-prod` |
| PG Database | `arxiv_test` | `arxiv_prod` |
| Job Name | `daily-push-test` | `daily-push-prod` |

## 设计

- **配置惰性加载** — `import common.config` 无副作用；首次访问 `config.PG_HOST` 才读 `ENV` 并拉 Infisical
- **Job 分层** — `main()` 读配置，`run(...)` 纯逻辑，参数全显式
- **断点续传** — `daily_push_log` 按 `(job_name, publish_date)` 记账，PENDING 是崩溃锚点，重跑自动跳过已 COMMITTED 日期
- **at-least-once** — 崩溃后重推整天，下游去重

## 依赖

`kafka-python` `psycopg2-binary` `pyarrow` `pyspark` `datasets` `python-dotenv` `infisical-sdk`

## huggingface 数据集下载

```angular2html
uvx hf download ines-besrour/unarxive_2024 --repo-type=dataset

uvx hf cache verify ines-besrour/unarxive_2024 --repo-type=dataset

```