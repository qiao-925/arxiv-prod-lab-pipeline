"""排名任务共享目录与产物文件定义。

职责：
    - 为 GitHub 与 OpenAlex 排名 Job 提供统一输出目录
    - 明确各 Job 拥有的 Parquet 文件，避免清理时误删另一方产物
    - 供 Gradio 本地调试定位完整排名数据

两个 Job 共用同一目录，但清理阶段只删除自己的文件。
"""

DEFAULT_RANK_OUTPUT_DIR = "/tmp/ranking"

GITHUB_RANK_FILES = frozenset({
    "ranking_top500.parquet",
    "top500_arxiv_repo.parquet",
    "paper_details_github.parquet",
    "repo_details.parquet",
})

OPENALEX_RANK_FILES = frozenset({
    "openalex_ranking_top500.parquet",
    "paper_openalex_details.parquet",
})
