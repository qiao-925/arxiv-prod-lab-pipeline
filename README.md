# arxiv-prod-lab-pipeline

arXiv 数据管道：**数据准备**（HuggingFace → 本地 JSONL → 按日期分区 Parquet）
与**数据接入**（读取 → Kafka → PG 批次记账）两个独立职责。

## 目录结构

按职责分目录，脚本均可单独执行（不搞包嵌套）：

- `common/` - 跨业务共享的基础设施：`config.py`（连接端点 + 数据路径 + 环境命名，含 `init_env()`）、`kafka.py`（Kafka Producer 封装）与 `logger.py`（统一日志）
- `run_test.py` / `run_prod.py` - **入口**，分别显式 `init_env("test")` / `init_env("prod")`
- `jobs/` - 业务 Job（由 DolphinScheduler 触发），数据接入实现平铺于此
  - `ingest_to_kafka.py` - 数据接入入口（`--env` 声明环境；读取 → Kafka → PG）
  - `config.py` / `ingestor.py` / `pg_client.py` / `hf_reader.py` / `local_reader.py` - 业务模块
- `tools/` - 辅助脚本
  - `download.py` - 从 HuggingFace 下载为本地 JSONL
  - `preprocess.py` - JSONL → 按日期分区 Parquet
- `tests/` - 测试入口（`run_test.py` 端到端）与可研脚本
- `env/` - 环境文件（`test.env` / `prod.env`，仅端点与凭据）
- `sql/` - SQL 分级：`test/`、`prod/safe/`（GitOps 自动）、`prod/manual/`（人工执行）
- `k8s/` - Kubernetes 声明文件，双份：`test/` 与 `prod/`
- `gitops/` - Argo CD `Application`（指向 `k8s/test` 与 `k8s/prod`）
- `scripts/` - 资源初始化脚本（`init-resources.sh` / `init-minio.sh` / `gen-sql-configmap.sh`）

`jobs/ingest_to_kafka.py` 是数据接入入口，业务模块平铺在 `jobs/`；
`tools/download.py` 产出的 JSONL 即 `tools/preprocess.py` 的输入。

## 快速开始

### 1. 安装依赖

```bash
uv venv
uv pip install -r requirements.txt
```

### 2. 准备测试数据（2000 条论文）

```bash
python tests/run_test.py
```

该命令依次执行：下载 2000 条 → `data/raw_test/`，再预处理 → `data/sorted_test/`。
也可单独运行 `python tools/download.py --env test --limit 2000`。

### 3. 初始化隔离资源（Kafka Topic / PG 库 / MinIO Bucket）

逻辑隔离：一套基础设施，test / prod 靠资源名后缀区分（见「环境隔离」一节）。

```bash
# 一键创建 test + prod 全部资源
# （会先清理旧的 dev/prod 资源；不想清理用 SKIP_CLEANUP=1 跳过）
bash scripts/init-resources.sh

# 或分别执行
kubectl apply -f k8s/test/kafka-topic.yaml -f k8s/prod/kafka-topic.yaml
bash scripts/gen-sql-configmap.sh          # 由 sql/ 生成 ConfigMap 清单
kubectl apply -f k8s/test/pg-init.yaml -f k8s/prod/pg-init.yaml
bash scripts/init-minio.sh                 # 共享 bucket：arxiv
```

### 4. 运行数据接入

**使用本地测试数据（推荐先跑通）：**

> 注意：Kafka broker 对外通告的是内网 DNS 地址，用 `port-forward` 连 9092 会
> 因 advertised.listeners 解析失败而超时。本地测试请改用 **external NodePort 监听器**。

```bash
# 终端 1：端口转发 PostgreSQL
kubectl port-forward svc/postgres -n database 5432:5432

# 终端 2：运行（KAFKA_BOOTSTRAP 用 external NodePort，见下）
KAFKA_BOOTSTRAP=192.168.1.7:30966 \
PG_HOST=localhost \
python jobs/ingest_to_kafka.py --env test
```

external NodePort 地址可这样取得：

```bash
NODE_IP=$(kubectl get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
KAFKA_NODEPORT=$(kubectl get svc arxiv-cluster-kafka-external-bootstrap -n kafka \
  -o jsonpath='{.spec.ports[0].nodePort}')
echo "KAFKA_BOOTSTRAP=${NODE_IP}:${KAFKA_NODEPORT}"
```

（生产环境建议直接以 K8s Job/Deployment 在集群内运行，此时使用默认的内网地址
`arxiv-cluster-kafka-bootstrap.kafka:9092` 与 `postgres.database:5432` 即可。）

**使用 HuggingFace 流式（模拟生产）：**

```bash
DATA_SOURCE=huggingface python jobs/ingest_to_kafka.py --env test --run-limit 1000
```

生产入口（`init_env("prod")`，无需再传 `--env`）：

```bash
python run_prod.py --run-limit 10000
```

DolphinScheduler Shell 任务（生产）：

```bash
cd /home/qiao/arxiv-prod-lab-pipeline
source .venv/bin/activate
python run_prod.py --run-limit 10000
```

## 配置项

所有配置通过环境变量注入。**环境本身不靠环境变量**：入口必须显式调用
`init_env("test" | "prod")`（或给工具传 `--env`）。资源名（Topic / 库名 / 前缀 /
Job）由 `ENV` 派生，同名环境变量可显式覆盖：

| 变量 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `ENV` | 无（必须显式声明） | `test` 或 `prod`，经 `init_env()` / `--env` 指定 |
| `DATA_SOURCE` | `local` | `local` 或 `huggingface` |
| `HF_DATASET_NAME` | `ines-besrour/unarxive_2024` | 数据源数据集（common.config 共享配置） |
| `HF_ENDPOINT` | `https://hf-mirror.com` | HuggingFace 镜像端点（common.config 共享配置） |
| `LOCAL_DATA_PATH` | `tests/data/test_2000.jsonl` | `local` 源读取的 JSONL 路径 |
| `KAFKA_BOOTSTRAP` | `arxiv-cluster-kafka-bootstrap.kafka:9092` | Kafka 地址 |
| `KAFKA_TOPIC` | 派生：`arxiv-papers-test` / `-prod` | Kafka Topic |
| `KAFKA_GROUP_ID` | 派生：`flink-test` / `flink-prod` | 消费者组（供下游 Flink 用） |
| `PG_HOST` | `postgres.database` | PostgreSQL 地址 |
| `PG_DB` | 派生：`arxiv_test` / `arxiv_prod` | PostgreSQL 库名 |
| `MINIO_ENDPOINT` | `http://minio.minio:9000` | MinIO 端点 |
| `MINIO_BUCKET` | `arxiv` | MinIO bucket（**全环境共用**） |
| `MINIO_PREFIX` | 派生：`test/` / `prod/` | MinIO 对象前缀（逻辑隔离靠它） |
| `JOB_NAME` | 派生：`hf-ingestor-test` / `-prod` | 批次记账 / 断点续传按 Job 分隔 |
| `CONFIRM_STARTUP` | `0` | 置 `1` 时启动前要求人工确认环境 |
| `RUN_LIMIT` | `0` | 本次处理条数，0 表示不限制 |

## 环境隔离

资源有限时不做物理隔离（双命名空间），而是**同一套基础设施 + 资源名后缀**区分
test / prod。规则统一：短横线后缀用于 Kafka / Job，下划线用于 PG 库名；
MinIO 共用 bucket `arxiv`，靠对象前缀 `test/` 与 `prod/` 区分。

| 资源 | `ENV=test` | `ENV=prod` |
| :--- | :--- | :--- |
| Kafka Topic | `arxiv-papers-test` | `arxiv-papers-prod` |
| Kafka Consumer Group | `flink-test` | `flink-prod` |
| PG Database | `arxiv_test` | `arxiv_prod` |
| MinIO | `arxiv/test/` | `arxiv/prod/` |
| Job Name | `hf-ingestor-test` | `hf-ingestor-prod` |
| 数据目录 | `data/raw_test`、`data/sorted_test` | `data/raw`、`data/sorted` |

### 环境显式声明（核心安全机制）

**资源名统一在 `common/config.py` 的 `get_*` 函数派生（单一事实来源），且在任何
`get_xxx()` 之前必须显式声明环境。** 未声明时调用会立即报错，绝不静默落到 test：

```python
# 入口：先声明，再导入业务模块
from common.config import init_env
init_env("prod")
from jobs.ingest_to_kafka import main
main()
```

```bash
$ python jobs/ingest_to_kafka.py
RuntimeError: 请先调用 init_env('test' 或 'prod')
```

- 测试 → `python tests/run_test.py`（内部 `init_env("test")`，子进程透传 `--env test`）
- 生产 → `python run_prod.py`（内部 `init_env("prod")`）
- 单个工具 → `python tools/download.py --env test ...`
- 直连入口 → `python jobs/ingest_to_kafka.py --env test`

环境文件只放端点与凭据（不再含 `ENV`）：

```bash
set -a; source env/test.env; set +a     # 或 env/prod.env
python run_prod.py --run-limit 10000
```

保险措施：Job 启动时会打印解析出的环境与资源名（设 `CONFIRM_STARTUP=1` 则额外
要求人工确认 `y`），防止 prod 误连 test。

## SQL 分级

| 级别 | 操作 | 处理方式 |
| :--- | :--- | :--- |
| **安全** | `CREATE TABLE IF NOT EXISTS`、`CREATE DATABASE`、新增可空列、小表加索引 | GitOps 自动执行（`k8s/<env>/pg-init.yaml`） |
| **谨慎** | 大表加索引、加非空列、修改列类型 | GitOps + 人工确认（Argo CD 手动 Sync） |
| **危险** | `DROP TABLE`、`DROP COLUMN`、`TRUNCATE`、大表结构重建 | 手动执行（`sql/prod/manual/` + 执行记录） |

```text
sql/
├── test/init.sql            # 测试初始化（全自动）
└── prod/
    ├── safe/init.sql        # 生产安全 SQL（GitOps 自动）
    └── manual/README.md     # 危险 SQL + 执行流程与记录
```

`sql/` 是单一事实来源：`scripts/gen-sql-configmap.sh` 把它包成
`k8s/<env>/pg-init-sql.yaml`（ConfigMap），由 Job 挂载执行。改 SQL 后重跑该脚本。

**切换时机（由数据量与事故决定，不提前设计）：** 表超百万行 → 大表 DDL 转手动；
出现第一次锁表事故 → 所有 DDL 转人工审批；有真实用户 → 危险操作全部手动。

## 数据集说明

数据源 `ines-besrour/unarxive_2024`（LREC 2026）为 tar.gz 打包、viewer 禁用，
`load_dataset(..., streaming=True)` 每个 example 的 `jsonl` 字段可能包含**多行**
（多个 paper 拼接）或空行。读取器已按行拆分逐条解析。

## 下载 + 预处理（生成按日期分区 Parquet）

预处理链路与 Kafka 接入相互独立：先从 HuggingFace 下载为本地 JSONL，再由
Spark 按论文发布日期（`metadata.versions[0].created`）分区写出 Parquet，
供后续 Spark 引用网络分析 / MinIO 落地使用。

**端到端测试（2000 条）：**

```bash
python tests/run_test.py
```

**生产：**

```bash
python tools/download.py --env prod                     # 全量下载 → data/raw
python tools/preprocess.py --env prod                   # data/raw → data/sorted
```

输出路径：

| 环境 | 原始 JSONL | 分区 Parquet |
| :--- | :--- | :--- |
| 测试 | `data/raw_test/` | `data/sorted_test/` |
| 生产 | `data/raw/` | `data/sorted/` |

**断点续传 / 缓存：** 下载完成会写 `_download_completed` 标记，预处理每个输入
文件对应一个批次并在 `data/sorted*/<文件名>/_completed` 打标；重跑时自动跳过
已完成部分。删除对应标记（或加 `--force`）即可强制重跑。

## 验证

```bash
# 查看批次记录（test 环境 → arxiv_test；prod 环境 → arxiv_prod）
psql -h localhost -U postgres -d arxiv_test -c "
    SELECT batch_id, source_offset, source_end, record_count, status
    FROM ingestion_batches ORDER BY created_at DESC LIMIT 10;
"

# 消费 Kafka 消息
kubectl exec -it arxiv-cluster-dual-role-0 -n kafka -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic arxiv-papers-test \
  --from-beginning \
  --max-messages 5
```
