# arxiv-prod-lab-pipeline

arXiv 数据管道：**数据准备**（HuggingFace → 本地 JSONL → 按日期分区 Parquet）
与**数据接入**（读取 → Kafka → PG 批次记账）两个独立职责。

## 目录结构

按职责分目录，脚本均可单独执行（不搞包嵌套）：

- `config.py` - 全局共享配置（数据源身份 + 数据路径 + 环境命名）
- `jobs/` - 业务 Job（由 DolphinScheduler 触发）
  - `ingest_to_kafka.py` - 数据接入 Job（读取 → Kafka → PG，薄入口）
- `tools/` - 辅助脚本
  - `download.py` - 从 HuggingFace 下载为本地 JSONL
  - `preprocess.py` - JSONL → 按日期分区 Parquet
- `hf_dataset_to_kafka/` - 数据接入**实现**包（Kafka/PG 客户端、reader、批次逻辑）
- `tests/` - 测试入口（`run_test.py` 端到端）与可研脚本
- `env/` - 环境文件（`test.env` / `prod.env`，端点与凭据）
- `scripts/` - 资源初始化脚本（`init-resources.sh` / `init-minio.sh`）
- `k8s/` - Kubernetes 声明文件

`jobs/ingest_to_kafka.py` 是薄入口，内部调用 `hf_dataset_to_kafka`；
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
也可单独运行 `python tools/download.py --output data/raw_test --limit 2000`。
（旧的 `tests/download_test_2000.py` 仅生成原始 JSONL，不产出分区 Parquet。）

### 3. 初始化隔离资源（Kafka Topic / PG 库 / MinIO Bucket）

逻辑隔离：一套基础设施，dev / prod 靠资源名后缀区分（见「环境隔离」一节）。

```bash
# 一键创建 dev + prod 全部资源
bash scripts/init-resources.sh

# 或分别执行
kubectl apply -f k8s/kafka-topic-dev.yaml -f k8s/kafka-topic-prod.yaml
kubectl apply -f k8s/pg-init.yaml
ENV=test bash scripts/init-minio.sh && ENV=prod bash scripts/init-minio.sh
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
python -m hf_dataset_to_kafka
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
DATA_SOURCE=huggingface python -m hf_dataset_to_kafka --run-limit 1000
```

等价入口（Job 封装，DolphinScheduler 触发用）：

```bash
python jobs/ingest_to_kafka.py --run-limit 10000
```

DolphinScheduler Shell 任务：

```bash
cd /home/qiao/arxiv-prod-lab-pipeline
source .venv/bin/activate
python jobs/ingest_to_kafka.py --run-limit 10000
```

## 配置项

所有配置通过环境变量注入。资源名（Topic / 库名 / Bucket / Job）默认由 `ENV`
派生，同名环境变量可显式覆盖：

| 变量 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `ENV` | `test` | `test`（dev）或 `prod`，决定资源名后缀 |
| `DATA_SOURCE` | `local` | `local` 或 `huggingface` |
| `HF_DATASET_NAME` | `ines-besrour/unarxive_2024` | 数据源数据集（顶层共享配置） |
| `HF_ENDPOINT` | `https://hf-mirror.com` | HuggingFace 镜像端点（顶层共享配置） |
| `LOCAL_DATA_PATH` | `tests/data/test_2000.jsonl` | `local` 源读取的 JSONL 路径 |
| `KAFKA_BOOTSTRAP` | `arxiv-cluster-kafka-bootstrap.kafka:9092` | Kafka 地址 |
| `KAFKA_TOPIC` | 派生：`arxiv-papers-dev` / `-prod` | Kafka Topic |
| `KAFKA_GROUP_ID` | 派生：`flink-dev` / `flink-prod` | 消费者组（供下游 Flink 用） |
| `PG_HOST` | `postgres.database` | PostgreSQL 地址 |
| `PG_DB` | 派生：`arxiv_dev` / `arxiv_prod` | PostgreSQL 库名 |
| `MINIO_ENDPOINT` | `http://minio.minio:9000` | MinIO 端点 |
| `MINIO_BUCKET` | 派生：`arxiv-dev` / `arxiv-prod` | MinIO Bucket（预留） |
| `JOB_NAME` | 派生：`hf-ingestor-dev` / `-prod` | 批次记账 / 断点续传按 Job 分隔 |
| `CONFIRM_STARTUP` | `0` | 置 `1` 时启动前要求人工确认环境 |
| `RUN_LIMIT` | `0` | 本次处理条数，0 表示不限制 |

## 环境隔离

资源有限时不做物理隔离（双命名空间），而是**同一套基础设施 + 资源名后缀**区分
dev / prod。规则统一：短横线后缀用于 Kafka / MinIO / Job，下划线用于 PG 库名。

| 资源 | `ENV=test`（dev） | `ENV=prod` |
| :--- | :--- | :--- |
| Kafka Topic | `arxiv-papers-dev` | `arxiv-papers-prod` |
| Kafka Consumer Group | `flink-dev` | `flink-prod` |
| PG Database | `arxiv_dev` | `arxiv_prod` |
| MinIO Bucket | `arxiv-dev` | `arxiv-prod` |
| Job Name | `hf-ingestor-dev` | `hf-ingestor-prod` |
| 数据目录 | `data/raw_test`、`data/sorted_test` | `data/raw`、`data/sorted` |

**切换环境只改 `ENV`，不改代码。** 资源名统一在顶层 `config.py` 的 `get_*`
函数派生（单一事实来源），Python 与 Java（Flink）都据此决定用哪套资源名。

环境文件（只放端点与凭据，资源名由 `ENV` 派生）：

```bash
set -a; source env/test.env; set +a     # 或 env/prod.env
python jobs/ingest_to_kafka.py --run-limit 10000
```

保险措施：Job 启动时会打印解析出的环境与资源名（设 `CONFIRM_STARTUP=1` 则额外
要求人工确认 `y`），防止 prod 误连 dev。

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
python tools/download.py --output data/raw                                   # 全量下载
python tools/preprocess.py --input data/raw --output data/sorted             # 预处理
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
# 查看批次记录（test 环境 → arxiv_dev；prod 环境 → arxiv_prod）
psql -h localhost -U postgres -d arxiv_dev -c "
    SELECT batch_id, source_offset, source_end, record_count, status
    FROM ingestion_batches ORDER BY created_at DESC LIMIT 10;
"

# 消费 Kafka 消息
kubectl exec -it arxiv-cluster-dual-role-0 -n kafka -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic arxiv-papers-dev \
  --from-beginning \
  --max-messages 5
```
