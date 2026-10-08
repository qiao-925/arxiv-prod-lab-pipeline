"""
获取 arXiv 官方 API 字段结构
- Atom API: 实时查询接口
- OAI-PMH arXivRaw: 批量收割接口（含版本历史）
"""

import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

import requests

# ---------- 配置 ----------
TEST_ARXIV_ID = "1706.03762"  # Attention Is All You Need
ATOM_API = "http://export.arxiv.org/api/query"
OAI_PMH = "https://oaipmh.arxiv.org/oai"

ATOM_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
}


def print_section(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


# ---------- 1. Atom API ----------
def fetch_atom_api(arxiv_id: str) -> Dict[str, Any]:
    print_section(f"Atom API: {arxiv_id}")

    params = {"id_list": arxiv_id, "max_results": 1}
    resp = requests.get(ATOM_API, params=params, timeout=30)
    resp.raise_for_status()

    # 打印原始 XML 前 2000 字符
    print("\n--- 原始 XML（前 2000 字符）---")
    print(resp.text[:2000])
    print("...")

    root = ET.fromstring(resp.text)
    entry = root.find("atom:entry", ATOM_NS)
    if entry is None:
        print("未找到 entry")
        return {}

    # 解析字段
    raw_id = entry.find("atom:id", ATOM_NS).text
    arxiv_id_clean = raw_id.rstrip("/").split("/")[-1]
    if "v" in arxiv_id_clean:
        arxiv_id_clean = arxiv_id_clean.split("v")[0]

    title = " ".join((entry.find("atom:title", ATOM_NS).text or "").split())
    summary = " ".join((entry.find("atom:summary", ATOM_NS).text or "").split())

    authors = [
        a.find("atom:name", ATOM_NS).text.strip()
        for a in entry.findall("atom:author", ATOM_NS)
        if a.find("atom:name", ATOM_NS) is not None
    ]

    published = entry.find("atom:published", ATOM_NS).text
    updated = entry.find("atom:updated", ATOM_NS).text

    categories = [
        c.attrib.get("term", "")
        for c in entry.findall("atom:category", ATOM_NS)
    ]

    primary_cat_el = entry.find("arxiv:primary_category", ATOM_NS)
    primary_category = (
        primary_cat_el.attrib.get("term", "")
        if primary_cat_el is not None
        else None
    )

    doi_el = entry.find("arxiv:doi", ATOM_NS)
    doi = doi_el.text if doi_el is not None else None

    journal_el = entry.find("arxiv:journal_ref", ATOM_NS)
    journal_ref = journal_el.text if journal_el is not None else None

    comment_el = entry.find("arxiv:comment", ATOM_NS)
    comment = comment_el.text if comment_el is not None else None

    # 链接
    abstract_url = None
    pdf_url = None
    for link in entry.findall("atom:link", ATOM_NS):
        rel = link.attrib.get("rel")
        link_type = link.attrib.get("type")
        href = link.attrib.get("href")
        if rel == "alternate" and link_type == "text/html":
            abstract_url = href
        elif rel == "related" and link_type == "application/pdf":
            pdf_url = href

    fields = {
        "arxiv_id": arxiv_id_clean,
        "title": title,
        "abstract": summary[:120] + "..." if len(summary) > 120 else summary,
        "authors": authors,
        "n_authors": len(authors),
        "published": published,
        "updated": updated,
        "categories": categories,
        "primary_category": primary_category,
        "doi": doi,
        "journal_ref": journal_ref,
        "comment": comment,
        "abstract_url": abstract_url,
        "pdf_url": pdf_url,
    }

    print("\n--- 解析后字段 ---")
    for k, v in fields.items():
        print(f"  {k:20s}: {v}")

    return fields


# ---------- 2. OAI-PMH arXivRaw ----------
def fetch_oai_pmh_arxiv_raw(arxiv_id: str) -> Dict[str, Any]:
    print_section(f"OAI-PMH arXivRaw: {arxiv_id}")

    identifier = f"oai:arXiv.org:{arxiv_id}"
    params = {
        "verb": "GetRecord",
        "identifier": identifier,
        "metadataPrefix": "arXivRaw",
    }
    resp = requests.get(OAI_PMH, params=params, timeout=30)
    resp.raise_for_status()

    print("\n--- 原始 XML（前 3000 字符）---")
    print(resp.text[:3000])
    print("...")

    root = ET.fromstring(resp.text)

    # OAI-PMH 命名空间
    ns = {
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "arxiv": "http://arxiv.org/OAI/arXivRaw/",
    }

    record = root.find(".//oai:record", ns)
    if record is None:
        print("未找到 record")
        return {}

    # arXivRaw 的字段都在 arxiv 命名空间下
    def get_text(tag: str):
        el = record.find(f".//arxiv:{tag}", ns)
        return el.text.strip() if el is not None and el.text else None

    # 版本历史
    versions = []
    for v in record.findall(".//arxiv:version", ns):
        versions.append({
            "version": v.attrib.get("version"),
            "created": v.find("arxiv:date", ns).text if v.find("arxiv:date", ns) is not None else None,
            "size": v.find("arxiv:size", ns).text if v.find("arxiv:size", ns) is not None else None,
        })

    fields = {
        "id": get_text("id"),
        "submitter": get_text("submitter"),
        "authors": get_text("authors"),
        "title": get_text("title"),
        "comments": get_text("comments"),
        "journal_ref": get_text("journal-ref"),
        "doi": get_text("doi"),
        "report_no": get_text("report-no"),
        "categories": get_text("categories"),
        "license": get_text("license"),
        "abstract": (get_text("abstract") or "")[:120] + "...",
        "versions": versions,
        "n_versions": len(versions),
        "update_date": get_text("update_date"),
    }

    print("\n--- 解析后字段 ---")
    for k, v in fields.items():
        print(f"  {k:20s}: {v}")

    return fields


# ---------- 3. 主流程 ----------
def main():
    # Atom API
    atom_fields = fetch_atom_api(TEST_ARXIV_ID)
    time.sleep(3)  # 遵守速率限制

    # OAI-PMH
    oai_fields = fetch_oai_pmh_arxiv_raw(TEST_ARXIV_ID)

    # 字段对比汇总
    print_section("字段对比汇总")

    atom_keys = set(atom_fields.keys())
    oai_keys = set(oai_fields.keys())

    print("\nAtom API 独有字段:")
    for k in sorted(atom_keys - oai_keys):
        print(f"  - {k}")

    print("\nOAI-PMH arXivRaw 独有字段:")
    for k in sorted(oai_keys - atom_keys):
        print(f"  - {k}")

    print("\n两接口共有字段:")
    for k in sorted(atom_keys & oai_keys):
        print(f"  - {k}")


if __name__ == "__main__":
    main()