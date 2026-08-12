"""
build_papers_table.py
---------------------
Build a standalone reference table for all found papers:

    data/papers_table.csv
    bibcode | doi | title | year | status | pdf_file | source_table

Status values
-------------
  downloaded    — PDF is on disk
  available     — open-access (arXiv / EPRINT_PDF) but not yet downloaded
  bot_protected — IOPScience/Radware blocks automated access
  paywall       — behind a subscription paywall (MNRAS, NewA, etc.)
  failed        — download was attempted but returned a non-PDF / network error
  no_source     — no PDF source found (no arXiv ID, no open-access link)

Sources
-------
  Bibcodes are collected from:
    1. search_download_table.csv
    2. similarity_download_table.csv
    3. catalogue.csv  (papers that were extracted into the object catalogue)

Usage
-----
    source .venv/bin/activate
    python scripts/build_papers_table.py
"""

import os
import sys
import time
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

with open(PROJECT_ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)


def cfg_path(key: str) -> Path:
    return PROJECT_ROOT / CFG["paths"][key]


# ── ADS helpers (reuse existing module) ──────────────────────────────────────
from src.ads_parser import (
    _make_ads_request_with_retry,
    get_ads_headers,
    download_pdfs,
    ADS_API_BASE_URL,
)


# ── Step 1: collect all unique bibcodes ──────────────────────────────────────

def collect_bibcodes() -> dict[str, set[str]]:
    """
    Returns {bibcode: set_of_source_table_names}.
    """
    sources: dict[str, set[str]] = {}

    for table_key, label in [
        ("search_download_table",     "search"),
        ("similarity_download_table", "similarity"),
        ("catalogue_csv",             "catalogue"),
    ]:
        path = cfg_path(table_key)
        if not path.exists():
            print(f"  ⚠️  {path.name} not found — skipping")
            continue
        df = pd.read_csv(path)
        col = "Bibcode" if "Bibcode" in df.columns else "bibcode"
        if col not in df.columns:
            print(f"  ⚠️  No bibcode column in {path.name} — skipping")
            continue
        bibs = df[col].dropna().unique().tolist()
        for b in bibs:
            b = str(b).strip()
            if b:
                sources.setdefault(b, set()).add(label)
        print(f"  📄 {path.name}: {len(bibs)} bibcodes")

    return sources


# ── Step 2: fetch metadata from ADS ──────────────────────────────────────────

def fetch_ads_metadata(bibcodes: list[str]) -> pd.DataFrame:
    """
    Returns DataFrame with columns: bibcode, doi, title, year, esources.
    """
    headers = get_ads_headers()
    batch_size = 100
    rows = []

    total = len(bibcodes)
    print(f"\n🔍 Fetching ADS metadata for {total} bibcodes …")

    for start in range(0, total, batch_size):
        batch = bibcodes[start : start + batch_size]
        query  = " OR ".join(f"bibcode:{b}" for b in batch)
        params = {
            "q":    query,
            "fl":   "bibcode,doi,title,year,esources",
            "rows": len(batch),
        }
        response = _make_ads_request_with_retry(
            f"{ADS_API_BASE_URL}/search/query", headers, params
        )
        if response is None or response.status_code != 200:
            print(f"  ⚠️  Batch {start // batch_size + 1} failed")
            continue

        docs = response.json().get("response", {}).get("docs", [])
        for doc in docs:
            doi_list  = doc.get("doi") or []
            title_list = doc.get("title") or []
            rows.append({
                "bibcode":  doc.get("bibcode", ""),
                "doi":      doi_list[0]  if doi_list  else "",
                "title":    title_list[0] if title_list else "",
                "year":     doc.get("year", ""),
                "esources": "|".join(doc.get("esources") or []),
            })
        time.sleep(0.5)

    meta = pd.DataFrame(rows)
    # Align with input list (some bibcodes may not exist in ADS)
    meta = meta.set_index("bibcode").reindex(bibcodes).reset_index()
    print(f"  ✅ Metadata fetched for {meta['title'].notna().sum()}/{total} bibcodes")
    return meta


# ── Step 3: determine download status ────────────────────────────────────────

def determine_statuses(
    bibcodes: list[str],
    esources_map: dict[str, str],
    pdf_dir: Path,
) -> tuple[dict[str, str], dict[str, str]]:
    """
    Returns:
        status_map  — {bibcode: status_string}
        pdf_map     — {bibcode: pdf_filename_or_empty}

    For bibcodes already on disk → "downloaded" immediately.
    For the rest, call download_pdfs() to get exact statuses.
    """
    status_map: dict[str, str] = {}
    pdf_map:    dict[str, str] = {}

    # Check disk first
    already_have: list[str] = []
    need_check:   list[str] = []

    for bib in bibcodes:
        safe = bib.replace("/", "_").replace(":", "_")
        pdf_path = pdf_dir / f"{safe}.pdf"
        if pdf_path.exists():
            status_map[bib] = "downloaded"
            pdf_map[bib]    = pdf_path.name
            already_have.append(bib)
        else:
            need_check.append(bib)

    print(f"\n📂 Already on disk : {len(already_have)}")
    print(f"   Need status check: {len(need_check)}")

    if not need_check:
        return status_map, pdf_map

    # For papers not on disk, attempt download to get accurate status.
    # skip_existing=True so already-downloaded files are not re-fetched.
    print(f"\n🚀 Attempting downloads for {len(need_check)} bibcodes …")
    dl_result = download_pdfs(
        need_check,
        output_dir=str(pdf_dir),
        delay_between_requests=CFG["pipeline"]["delay_between_requests"],
        skip_existing=True,
    )

    for bib in dl_result.get("downloaded", []):
        status_map[bib] = "downloaded"
        safe = bib.replace("/", "_").replace(":", "_")
        pdf_map[bib] = f"{safe}.pdf"

    for bib in dl_result.get("skipped", []):
        # Was already on disk from an earlier check (shouldn't happen here)
        status_map[bib] = "downloaded"
        safe = bib.replace("/", "_").replace(":", "_")
        pdf_map[bib] = f"{safe}.pdf"

    for bib in dl_result.get("bot_protected", []):
        status_map[bib] = "bot_protected"
        pdf_map[bib]    = ""

    for bib in dl_result.get("paywalled", []):
        status_map[bib] = "paywall"
        pdf_map[bib]    = ""

    for bib in dl_result.get("failed", []):
        status_map[bib] = "failed"
        pdf_map[bib]    = ""

    for bib in dl_result.get("no_source", []):
        # Refine: if ADS says open-access but we couldn't get a URL, mark available
        if "EPRINT_PDF" in esources_map.get(bib, ""):
            status_map[bib] = "available"
        else:
            status_map[bib] = "no_source"
        pdf_map[bib] = ""

    # Fill any remaining (shouldn't happen, but be safe)
    for bib in bibcodes:
        if bib not in status_map:
            if "EPRINT_PDF" in esources_map.get(bib, ""):
                status_map[bib] = "available"
            else:
                status_map[bib] = "no_source"
            pdf_map.setdefault(bib, "")

    return status_map, pdf_map


# ── Step 4: assemble and save ─────────────────────────────────────────────────

def build_papers_table() -> pd.DataFrame:
    print("=" * 55)
    print("  Building papers reference table")
    print("=" * 55)

    # 1. Collect bibcodes
    print("\n📋 Collecting bibcodes …")
    bib_sources = collect_bibcodes()
    bibcodes    = sorted(bib_sources.keys())
    print(f"  Total unique bibcodes: {len(bibcodes)}")

    # 2. Fetch ADS metadata
    meta = fetch_ads_metadata(bibcodes)
    esources_map = dict(zip(meta["bibcode"], meta["esources"].fillna("")))

    # 3. Determine statuses
    pdf_dir = cfg_path("pdf_dir")
    pdf_dir.mkdir(parents=True, exist_ok=True)
    status_map, pdf_map = determine_statuses(bibcodes, esources_map, pdf_dir)

    # 4. Assemble final table
    meta["status"]       = meta["bibcode"].map(status_map).fillna("no_source")
    meta["pdf_file"]     = meta["bibcode"].map(pdf_map).fillna("")
    meta["source_table"] = meta["bibcode"].map(
        lambda b: "|".join(sorted(bib_sources.get(b, set())))
    )

    # Drop internal esources column (not needed in the final table)
    final = meta[["bibcode", "doi", "title", "year", "status", "pdf_file", "source_table"]]
    final = final.sort_values(["year", "bibcode"], ascending=[False, True]).reset_index(drop=True)

    # 5. Save
    out_path = cfg_path("papers_table")
    final.to_csv(out_path, index=False)

    # 6. Summary
    print("\n" + "=" * 55)
    print("  papers_table.csv — STATUS SUMMARY")
    print("=" * 55)
    for status, count in final["status"].value_counts().items():
        emoji = {
            "downloaded":    "✅",
            "available":     "📂",
            "bot_protected": "🤖",
            "paywall":       "🔒",
            "failed":        "❌",
            "no_source":     "⚠️ ",
        }.get(status, "  ")
        print(f"  {emoji} {status:<14}: {count}")
    print("-" * 55)
    print(f"  Total            : {len(final)}")
    print(f"\n💾 Saved → {out_path.relative_to(PROJECT_ROOT)}")

    return final


if __name__ == "__main__":
    build_papers_table()
