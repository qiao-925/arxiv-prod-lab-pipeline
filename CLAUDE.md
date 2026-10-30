# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# 代码构建规范

1. 最小改动原则，修改前后列出修改清单，方便用户核对

# 注释增强
1. 每个文件头都需要补充详细注释描述当前文件功能

# 测试构建规范 [Agent锚点，强制手写，可控微调]

## 测试构建设计哲学
1. 严格使用自适应策略：防止过度设计；具体问题具体分析，将当前面对的业务场景作为第一评估优先级
2. 最终目标锚定：经过具体的业务场景评估后，在有限成本和测试覆盖率的天平上取得平衡，保证系统可靠性
3. 优先级锚定：优先覆盖核心链路，高风险链路

## 测试构建实现过程与约束
1. 测试的形神合一：以上述设计哲学为神，灵活使用不同的测试方法与思想（形），完成测试目标
	TDD：不适合业务逻辑高频变化的场景，应根据变化强度决定是否采用。
2. 镜像生产环境构建：构建与生产运行环境的镜像基础设施，最大化模拟生产实际运行过程
	镜像环境已构建，使用env配置即可直连镜像测试环境
3. 

4. 单元测试100%覆盖：覆盖主链路运行过程以及异常，边界情况
5. 集成测试 / API 测试：覆盖除单元测试外的组件、模块之间的连接细节以及外部依赖的细节

---

<!-- 以下由 /init 生成，可随时改写；上方为用户手写规范，优先于本节 -->

# Agent 生成：项目速查

## 常用命令
```
uv sync                                                   # 安装依赖（uv 管理，无 requirements.txt）
ENV=test uv run python -m jobs.job_daily_arxiv_oai_pull
ENV=test uv run python -m jobs.job_hourly_github_crawl run   # run|verify|init
ENV=test uv run python -m jobs.job_hourly_github_sync run    # run|verify
ENV=prod uv run python -m jobs.scheduler                     # 常驻调度器
uv run pytest                                                # 全量
uv run pytest tests/job/test_xxx.py::test_name               # 单个用例
python tests/smoke_test/smoke_config.py --keys-only          # 配置脱敏检查
```

## 架构（读多个文件才能看懂的部分）
- `common/config.py`：配置**只来自 Infisical**，按 `ENV` 选环境；首次访问 `cfg.X` 才加载，因此 `ENV` 必须在访问前设好。
- `jobs/`：每个 Job `main()` 读配置 → `run(...)` 纯逻辑；PG 账本表记录水位实现断点续传，写操作走 `ON CONFLICT` 幂等。
- `jobs/scheduler.py`：APScheduler 按 Job 起独立子进程，启动错峰 + 整点 cron；新增 Job 要同时登记文件头 Job 地图和 `JOB_SPECS`。
- 数据流：OAI → `arxiv_papers` → sync；crawl 发现仓库 + 提取 README → `features_repo` / `edges_repo_arxiv` → sync 只读。
- 发布：改 `common/**` 或 `jobs/**` 推 main → Actions 构建镜像 → 自动改 `k8s/deployment.yaml` tag。

## 约定
- 文件头首行 `# 相对路径`，紧跟中文 docstring（职责/用法/注意）。
- 测试注入缝：模块级 `GITHUB_TOKEN`、`PG_CLIENT_CLASS` 用 `patch` 替换。
- 用 `uv`，不引入 requirements.txt。
