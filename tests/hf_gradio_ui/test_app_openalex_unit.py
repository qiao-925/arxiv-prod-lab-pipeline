"""Gradio OpenAlex 榜单单元测试。

职责：
    - 使用临时 Parquet 加载 hf_gradio_ui/app.py
    - 验证右侧 OpenAlex 窗口过滤、详情 HTML 和标签
    - 验证 OpenAlex 文件缺失时返回空态且不抛异常

测试不连接 HF Hub，不启动 Gradio 服务，不访问真实 /data。
"""
import importlib.util
import os
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

APP_PATH = (
    Path(__file__).resolve().parents[2] / "hf_gradio_ui" / "app.py"
)


def _write_required_github_files(root: Path) -> None:
    pd.DataFrame([{
        "snapshot_date": "2026-11-06",
        "window_name": "d14",
        "metric": "star",
        "rank": 1,
        "arxiv_id": "1234.5678",
        "title": "GitHub Paper",
        "metric_value": 10,
    }]).to_parquet(root / "ranking_top500.parquet", index=False)
    pd.DataFrame([{
        "arxiv_id": "1234.5678",
        "title": "GitHub Paper",
        "abstract": "Abstract",
        "authors": "Author",
        "categories": "cs.LG",
        "published_date": "2026-10-01",
        "linked_repos": "[]",
    }]).to_parquet(root / "paper_details_github.parquet", index=False)
    pd.DataFrame([{
        "repo_id": 1,
        "full_name": "owner/repo",
    }]).to_parquet(root / "repo_details.parquet", index=False)


def _write_openalex_files(root: Path) -> None:
    pd.DataFrame([{
        "as_of_date": "2026-10-01",
        "window_name": "recent_3m",
        "rank": 1,
        "arxiv_id": "1234.5678",
        "title": "OpenAlex Paper",
        "cited_by_count": 321,
    }]).to_parquet(
        root / "openalex_ranking_top500.parquet", index=False
    )
    pd.DataFrame([{
        "as_of_date": "2026-10-01",
        "arxiv_id": "1234.5678",
        "title": "OpenAlex Paper",
        "abstract": "OpenAlex abstract",
        "authors": "Author",
        "categories": "cs.LG",
        "published_date": "2026-09-01T00:00:00+00:00",
        "openalex_publication_date": "2026-09-01",
        "cited_by_count": 321,
        "doi": "10.1000/example",
        "type": "article",
        "oa_status": "green",
        "primary_topic_name": "Machine Learning",
        "primary_topic_field": "Computer Science",
        "primary_topic_domain": "Physical Sciences",
    }]).to_parquet(
        root / "paper_openalex_details.parquet", index=False
    )


def _load_app(monkeypatch, root: Path):
    monkeypatch.setenv("HF_BUCKET_PATH", str(root))
    real_isdir = os.path.isdir
    monkeypatch.setattr(
        "os.path.isdir",
        lambda path: False if str(path) == "/data" else real_isdir(path),
    )

    class Component:
        def change(self, *args, **kwargs):
            return None

        def select(self, *args, **kwargs):
            return None

        def load(self, *args, **kwargs):
            return None

    class Container(Component):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    fake_gradio = types.ModuleType("gradio")
    fake_gradio.Blocks = lambda **kwargs: Container()
    fake_gradio.Row = lambda **kwargs: Container()
    fake_gradio.Column = lambda **kwargs: Container()
    fake_gradio.Markdown = lambda *args, **kwargs: Component()
    fake_gradio.Dropdown = lambda *args, **kwargs: Component()
    fake_gradio.Dataframe = lambda *args, **kwargs: Component()
    fake_gradio.HTML = lambda *args, **kwargs: Component()
    fake_gradio.update = lambda **kwargs: kwargs
    fake_gradio.SelectData = object
    fake_spaces = types.ModuleType("spaces")
    fake_spaces.GPU = lambda fn: fn
    monkeypatch.setitem(sys.modules, "gradio", fake_gradio)
    monkeypatch.setitem(sys.modules, "spaces", fake_spaces)

    module_name = f"openalex_gradio_test_{id(root)}"
    spec = importlib.util.spec_from_file_location(module_name, APP_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_openalex_table_filter_and_detail_render(tmp_path, monkeypatch):
    _write_required_github_files(tmp_path)
    _write_openalex_files(tmp_path)
    app = _load_app(monkeypatch, tmp_path)

    assert app.OPENALEX_DEFAULT_WINDOW == "recent_3m"
    table = app.load_openalex_ranking("recent_3m")
    assert list(table.columns) == [
        "rank", "arxiv_id", "title", "cited_by_count"
    ]
    assert table.iloc[0]["cited_by_count"] == 321
    assert app._window_label("recent_3m") == "近3个月"

    detail = app.render_openalex_paper_detail_html("1234.5678")
    assert "OpenAlex Paper" in detail
    assert "321" in detail
    assert "Machine Learning" in detail


def test_missing_openalex_files_render_empty_state(tmp_path, monkeypatch):
    _write_required_github_files(tmp_path)
    app = _load_app(monkeypatch, tmp_path)

    assert app.OPENALEX_WINDOW_NAMES == []
    assert app.load_openalex_ranking(None).empty
    assert "未找到 OpenAlex 详情" in (
        app.render_openalex_paper_detail_html("missing")
    )


def test_local_debug_ui_starts_without_parquet(tmp_path, monkeypatch):
    monkeypatch.setenv("GRADIO_LOCAL_DEBUG", "1")
    monkeypatch.setenv("GRADIO_LOCAL_DATA_DIR", str(tmp_path))
    app = _load_app(monkeypatch, tmp_path)

    assert app.ROOT == str(tmp_path)
    assert app._RANKING.empty
    assert app._OPENALEX_RANKING.empty
    app.ensure_details_loaded()
    assert app._paper_details.empty
    assert app._openalex_details is None
