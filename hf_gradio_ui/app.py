"""Gradio: Arxiv & Github Ranking（读挂载 Parquet / HF Bucket）。

布局：
    - 左侧 50%：时间窗口筛选 + Top 500 表格（Star/Fork/Issue/PR 四列）
    - 右侧 50%：OpenAlex 引用榜（独立时间窗口 + 累计引用列）
    - 点击任一榜单行：打开详情弹窗（HTML 覆盖层）
    - 右侧 50%：OpenAlex 预留面板（当前 openalex_data 为空时展示空态）
    - 点击榜单行：打开非全屏大弹窗（HTML 覆盖层），展示论文详情和关联仓库
    - 榜单表头可点击排序

数据源：
    - 优先 /data 挂载目录，回退到 HF_BUCKET_PATH 指定的 hf:// bucket
      或本地目录
    - GitHub 数据由 job_daily_github_rank 生成
    - OpenAlex 数据由 job_openalex_rank 手动生成

容错：
    OpenAlex 两个 Parquet 尚未发布时，右侧展示空态，不影响 GitHub 榜加载。

本地调试：
    默认读取共享目录 /tmp/ranking，可用 GRADIO_LOCAL_DATA_DIR 覆盖。

    本地重启：
    pkill -f 'python hf_gradio_ui/app.py' || true
    GRADIO_LOCAL_DEBUG=1 .venv/bin/python hf_gradio_ui/app.py

数据源：优先 /data 挂载目录，回退到 HF_BUCKET_PATH 指定的 hf:// bucket 或本地目录。
"""
import functools
import html
import json
import os
import threading

import gradio as gr
import pandas as pd
import spaces


@spaces.GPU
def gpu_runtime_check(value: int = 0) -> str:
    """ZeroGPU 启动检测与运行时可用性检查，返回可序列化字符串。"""
    return f"GPU runtime ready: {value}"

# 优先使用已挂载的 Storage Bucket；LOCAL_DEBUG 只用于无数据预览 UI。
LOCAL_DEBUG = os.getenv("GRADIO_LOCAL_DEBUG") == "1"
if os.path.isdir("/data") and os.access("/data", os.W_OK):
    ROOT = "/data"
    print("✅ 使用挂载的 Storage Bucket: /data")
elif LOCAL_DEBUG:
    ROOT = os.getenv("GRADIO_LOCAL_DATA_DIR", "/tmp/ranking")
    print(f"⚠️ GRADIO_LOCAL_DEBUG=1：读取本地目录 {ROOT}")
else:
    ROOT = os.environ.get("HF_BUCKET_PATH", "")
    if ROOT.startswith("hf://"):
        pass
    elif ROOT and os.path.isdir(ROOT):
        ROOT = os.path.abspath(ROOT)
    else:
        raise RuntimeError(
            f"HF_BUCKET_PATH 未设置或路径无效，且 /data 未挂载。当前: {ROOT or 'None'}"
        )

ROOT = ROOT.rstrip("/")

METRICS = ("star", "fork", "issue", "pr")
GITHUB_RANK_COLUMNS = (
    "snapshot_date", "window_name", "metric", "rank",
    "arxiv_id", "title", "metric_value",
)


def _empty_frame(columns) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _read_optional_parquet(path: str) -> pd.DataFrame:
    """读取可选数据源；文件尚未发布时返回空表。"""
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        print(f"⚠️ 可选数据读取失败，按空态处理: {path} ({type(exc).__name__})")
        return pd.DataFrame()


_RANKING = (
    _read_optional_parquet(f"{ROOT}/ranking_top500.parquet")
    if LOCAL_DEBUG
    else pd.read_parquet(f"{ROOT}/ranking_top500.parquet")
)
if _RANKING.empty:
    _RANKING = _empty_frame(GITHUB_RANK_COLUMNS)
_SNAPSHOT = _RANKING["snapshot_date"].max()
_RANKING = _RANKING[_RANKING["snapshot_date"] == _SNAPSHOT]

_OPENALEX_RANKING = (
    _read_optional_parquet(f"{ROOT}/openalex_ranking_top500.parquet")
)
if _OPENALEX_RANKING.empty:
    _OPENALEX_SNAPSHOT = None
else:
    _OPENALEX_SNAPSHOT = _OPENALEX_RANKING["as_of_date"].max()
    _OPENALEX_RANKING = _OPENALEX_RANKING[
        _OPENALEX_RANKING["as_of_date"] == _OPENALEX_SNAPSHOT
    ]


def _window_label(window_name: str, snapshot=None) -> str:
    snapshot = snapshot if snapshot is not None else _SNAPSHOT
    if window_name == "d14":
        return "发表后14天"
    if window_name.startswith("rolling_"):
        lm = {"7d": "最近7天", "30d": "最近30天",
              "3m": "最近3个月", "6m": "最近6个月"}
        pv = window_name.replace("rolling_", "")
        return lm.get(pv, f"最近{pv}")
    if window_name.startswith("recent_"):
        lm = {"3m": "近3个月", "6m": "近6个月", "12m": "近12个月"}
        return lm.get(window_name.replace("recent_", ""), window_name)
    if window_name == "year_ytd":
        return f"{snapshot.year}年YTD"
    if window_name.startswith("year_"):
        return f"{window_name[5:]}年"
    if window_name == "total":
        return "总榜"
    return window_name


def _window_sort_key(window_name: str):
    fixed = {
        "d14": (0, ""),
        "rolling_7d": (1, "1"), "rolling_30d": (1, "2"),
        "rolling_3m": (1, "3"), "rolling_6m": (1, "4"),
        "recent_3m": (1, "3"), "recent_6m": (1, "4"),
        "recent_12m": (1, "5"),
        "total": (99, ""),
    }
    if window_name in fixed:
        return fixed[window_name]
    if window_name == "year_ytd":
        return (2, "")
    if window_name.startswith("year_"):
        return (3, "".join(chr(0x9fff - ord(c)) for c in window_name[5:]))
    return (50, window_name)


WINDOW_NAMES = sorted(_RANKING["window_name"].unique(), key=_window_sort_key)
TIME_OPTIONS = [(_window_label(wn), wn) for wn in WINDOW_NAMES]
DEFAULT_WINDOW = (
    "d14" if "d14" in WINDOW_NAMES
    else (WINDOW_NAMES[0] if WINDOW_NAMES else None)
)

OPENALEX_WINDOW_NAMES = (
    sorted(_OPENALEX_RANKING["window_name"].unique(), key=_window_sort_key)
    if not _OPENALEX_RANKING.empty else []
)
OPENALEX_TIME_OPTIONS = [
    (_window_label(wn, _OPENALEX_SNAPSHOT), wn)
    for wn in OPENALEX_WINDOW_NAMES
]
OPENALEX_DEFAULT_WINDOW = (
    "recent_3m" if "recent_3m" in OPENALEX_WINDOW_NAMES
    else (OPENALEX_WINDOW_NAMES[-1] if OPENALEX_WINDOW_NAMES else None)
)

_paper_details = None
_repo_details = None
_openalex_details = None
_details_loaded = False
_lock = threading.Lock()


def ensure_details_loaded():
    global _paper_details, _repo_details, _openalex_details, _details_loaded
    with _lock:
        if _details_loaded:
            return
        if LOCAL_DEBUG:
            github_papers = _read_optional_parquet(
                f"{ROOT}/paper_details_github.parquet"
            )
            _paper_details = (
                github_papers if not github_papers.empty else _empty_frame([
                "arxiv_id", "title", "abstract", "authors", "categories",
                "published_date", "linked_repos",
                ])
            ).set_index("arxiv_id")
            repo_details = _read_optional_parquet(
                f"{ROOT}/repo_details.parquet"
            )
            _repo_details = (
                repo_details if not repo_details.empty else _empty_frame([
                "repo_id", "full_name",
                ])
            ).set_index("repo_id")
        else:
            _paper_details = pd.read_parquet(
                f"{ROOT}/paper_details_github.parquet"
            ).set_index("arxiv_id")
            _repo_details = pd.read_parquet(
                f"{ROOT}/repo_details.parquet"
            ).set_index("repo_id")
        openalex = _read_optional_parquet(
            f"{ROOT}/paper_openalex_details.parquet"
        )
        _openalex_details = (
            openalex.set_index("arxiv_id") if not openalex.empty else None
        )
        _details_loaded = True


@functools.lru_cache(maxsize=32)
def load_ranking_wide(window_name: str | None) -> pd.DataFrame:
    """从 ranking_top500.parquet 过滤窗口并透视成四指标宽表。"""
    if not window_name:
        return pd.DataFrame()
    df = _RANKING[_RANKING["window_name"] == window_name]
    if df.empty:
        return pd.DataFrame(columns=["rank", "arxiv_id", "title",
                                     "star", "fork", "issue", "pr"])
    wide = df.pivot_table(
        index=["arxiv_id", "title"],
        columns="metric",
        values="metric_value",
        aggfunc="max",
    ).reset_index()
    for metric in METRICS:
        if metric not in wide.columns:
            wide[metric] = None
    wide = wide[["arxiv_id", "title", *METRICS]]
    primary = next(
        (m for m in METRICS if wide[m].notna().any()),
        METRICS[0],
    )
    wide = wide.sort_values(
        [primary, "arxiv_id"],
        ascending=[False, True],
        na_position="last",
    ).reset_index(drop=True)
    wide.insert(0, "rank", range(1, len(wide) + 1))
    return wide


@functools.lru_cache(maxsize=32)
def load_openalex_ranking(window_name: str | None) -> pd.DataFrame:
    """过滤最新 OpenAlex 快照并返回单窗口 Top 500。"""
    columns = ["rank", "arxiv_id", "title", "cited_by_count"]
    if not window_name or _OPENALEX_RANKING.empty:
        return pd.DataFrame(columns=columns)
    df = _OPENALEX_RANKING[
        _OPENALEX_RANKING["window_name"] == window_name
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)
    return (
        df[columns]
        .sort_values(["rank", "arxiv_id"], ascending=[True, True])
        .reset_index(drop=True)
    )


def _esc(value) -> str:
    return html.escape(str(value), quote=True)


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if hasattr(value, "ndim") and value.ndim > 0:
        return len(value) == 0
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def render_paper_detail_html(arxiv_id: str) -> str:
    """生成论文详情的弹窗内容（纯 HTML，可直接嵌入覆盖层）。"""
    ensure_details_loaded()
    if _paper_details is None or arxiv_id not in _paper_details.index:
        return f"<h2>未找到论文</h2><p>arXiv ID: {_esc(arxiv_id)}</p>"
    row = _paper_details.loc[arxiv_id]
    title = "无标题" if _is_missing(row.get("title")) else row["title"]
    lines = [f"<h2>{_esc(title)}</h2>",
             f"<p><strong>arXiv ID</strong>: {_esc(arxiv_id)}</p>"]
    for key, label in [("published_date", "发布时间"), ("authors", "作者"),
                       ("categories", "分类")]:
        if not _is_missing(row.get(key)):
            lines.append(f"<p><strong>{label}</strong>: {_esc(row[key])}</p>")
    if not _is_missing(row.get("abstract")):
        lines.append(f"<h3>摘要</h3><p>{_esc(row['abstract'])}</p>")
    linked = row.get("linked_repos")
    if not _is_missing(linked):
        if isinstance(linked, str):
            try:
                repos = json.loads(linked)
            except Exception:
                repos = []
        elif hasattr(linked, "tolist"):
            repos = linked.tolist()
        else:
            repos = list(linked) if isinstance(linked, (list, tuple)) else []
        if isinstance(repos, dict):
            repos = list(repos.values())
        lines.append(f"<h3>关联仓库（{len(repos)} 个）</h3><ul>")
        for r in repos[:20]:
            if not isinstance(r, dict):
                continue
            fn = r.get("full_name", "")
            lines.append(
                f"<li><a href=\"https://github.com/{_esc(fn)}\" "
                f"target=\"_blank\" rel=\"noopener\">{_esc(fn)}</a></li>"
            )
        if len(repos) > 20:
            lines.append(f"<li>还有 {len(repos) - 20} 个</li>")
        lines.append("</ul>")
    return "\n".join(lines)


def render_openalex_paper_detail_html(arxiv_id: str) -> str:
    """渲染 OpenAlex 榜单论文详情，数据来自独立详情 Parquet。"""
    ensure_details_loaded()
    if _openalex_details is None or arxiv_id not in _openalex_details.index:
        return (
            f"<h2>未找到 OpenAlex 详情</h2>"
            f"<p>arXiv ID: {_esc(arxiv_id)}</p>"
        )

    row = _openalex_details.loc[arxiv_id]
    title = "无标题" if _is_missing(row.get("title")) else row["title"]
    lines = [
        f"<h2>{_esc(title)}</h2>",
        f"<p><strong>arXiv ID</strong>: "
        f"<a href=\"https://arxiv.org/abs/{_esc(arxiv_id)}\" "
        f"target=\"_blank\" rel=\"noopener\">{_esc(arxiv_id)}</a></p>",
    ]
    for key, label in [
        ("cited_by_count", "累计引用"),
        ("published_date", "arXiv 发布时间"),
        ("openalex_publication_date", "OpenAlex 发布日期"),
        ("authors", "作者"),
        ("categories", "分类"),
        ("type", "类型"),
        ("oa_status", "OA 状态"),
        ("primary_topic_name", "主要主题"),
        ("primary_topic_field", "主题领域"),
        ("primary_topic_domain", "主题域"),
    ]:
        value = row.get(key)
        if not _is_missing(value):
            lines.append(
                f"<p><strong>{label}</strong>: {_esc(value)}</p>"
            )
    if not _is_missing(row.get("doi")):
        doi = str(row["doi"])
        lines.append(
            f"<p><strong>DOI</strong>: "
            f"<a href=\"https://doi.org/{_esc(doi)}\" "
            f"target=\"_blank\" rel=\"noopener\">{_esc(doi)}</a></p>"
        )
    if not _is_missing(row.get("abstract")):
        lines.append(f"<h3>摘要</h3><p>{_esc(row['abstract'])}</p>")
    return "\n".join(lines)


_PAPER_DETAIL_THEME_JS = """
() => {
  const colorScheme = window.matchMedia('(prefers-color-scheme: dark)');
  const applyPaperDetailTheme = () => {
    document.querySelectorAll('#paper-detail-panel').forEach((panel) => {
      panel.dataset.theme = colorScheme.matches ? 'dark' : 'light';
    });
  };

  if (!window.__paperDetailThemeListenerReady) {
    window.__paperDetailThemeListenerReady = true;
    colorScheme.addEventListener('change', applyPaperDetailTheme);
    new MutationObserver(applyPaperDetailTheme).observe(document.body, {
      childList: true,
      subtree: true,
    });
  }

  applyPaperDetailTheme();
}
"""


def _detail_modal_html(arxiv_id: str, source: str = "github") -> str:
    body = (
        render_openalex_paper_detail_html(arxiv_id)
        if source == "openalex"
        else render_paper_detail_html(arxiv_id)
    )
    return f"""
<style>
#paper-detail-panel {{
  background-color: #fff;
  color: #111;
}}
#paper-detail-panel[data-theme="dark"] {{
  background-color: #0f0f11;
  color: #f5f5f5;
  color-scheme: dark;
}}
#paper-detail-panel[data-theme="dark"] a {{
  color: #8ab4f8;
}}
#paper-detail-panel[data-theme="dark"] button {{
  background-color: #222;
  border-color: #666;
  color: #fff;
}}
</style>
<div id="paper-detail-modal"
     style="position:fixed;inset:0;z-index:1000;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.45);">
  <div id="paper-detail-panel"
       style="width:min(920px,92vw);max-height:84vh;overflow:auto;border-radius:8px;padding:20px;box-shadow:0 10px 40px rgba(0,0,0,0.35);">
    <div style="display:flex;justify-content:flex-end;margin-bottom:8px;">
      <button onclick="this.closest('#paper-detail-modal').remove()">关闭</button>
    </div>
    {body}
  </div>
</div>
"""


def open_paper_detail(evt: gr.SelectData, df: pd.DataFrame):
    """点击 GitHub 榜单行，打开论文与关联仓库详情。"""
    if df is None or df.empty:
        return gr.update(visible=False)
    try:
        idx = evt.index[0] if isinstance(evt.index, (list, tuple)) else evt.index
        arxiv_id = df.iloc[idx]["arxiv_id"]
    except Exception:
        return gr.update(visible=False)
    if arxiv_id in ("-", None) or pd.isna(arxiv_id):
        return gr.update(visible=False)
    arxiv_id = str(arxiv_id)
    return gr.update(
        value=_detail_modal_html(arxiv_id, source="github"),
        visible=True,
    )


def open_openalex_paper_detail(evt: gr.SelectData, df: pd.DataFrame):
    """点击 OpenAlex 榜单行，打开引用与论文详情。"""
    if df is None or df.empty:
        return gr.update(visible=False)
    try:
        idx = evt.index[0] if isinstance(evt.index, (list, tuple)) else evt.index
        arxiv_id = df.iloc[idx]["arxiv_id"]
    except Exception:
        return gr.update(visible=False)
    if arxiv_id in ("-", None) or pd.isna(arxiv_id):
        return gr.update(visible=False)
    return gr.update(
        value=_detail_modal_html(str(arxiv_id), source="openalex"),
        visible=True,
    )


def warmup():
    threading.Thread(target=ensure_details_loaded, daemon=True).start()


with gr.Blocks(title="Arxiv, Github & OpenAlex Ranking") as demo:
    with gr.Row(equal_height=True):
        with gr.Column(min_width=420):
            gr.Markdown("# Arxiv & Github Ranking")
            window_sel = gr.Dropdown(
                choices=TIME_OPTIONS,
                value=DEFAULT_WINDOW,
                label="时间窗口",
            )
            ranking_table = gr.Dataframe(
                value=load_ranking_wide(DEFAULT_WINDOW),
                interactive=False,
                label="GitHub Top 500",
                max_height=900
            )
        with gr.Column(min_width=420):
            gr.Markdown("# Arxiv & OpenAlex Ranking")
            openalex_window_sel = gr.Dropdown(
                choices=OPENALEX_TIME_OPTIONS,
                value=OPENALEX_DEFAULT_WINDOW,
                label="OpenAlex 时间窗口",
            )
            openalex_ranking_table = gr.Dataframe(
                value=load_openalex_ranking(OPENALEX_DEFAULT_WINDOW),
                interactive=False,
                label="OpenAlex Top 500",
                max_height=900
            )



    detail_modal = gr.HTML(visible=False)

    window_sel.change(load_ranking_wide, inputs=window_sel, outputs=ranking_table)
    openalex_window_sel.change(
        load_openalex_ranking,
        inputs=openalex_window_sel,
        outputs=openalex_ranking_table,
    )
    ranking_table.select(
        open_paper_detail,
        inputs=ranking_table,
        outputs=detail_modal,
    )
    openalex_ranking_table.select(
        open_openalex_paper_detail,
        inputs=openalex_ranking_table,
        outputs=detail_modal,
    )
    demo.load(warmup, js=_PAPER_DETAIL_THEME_JS)


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
