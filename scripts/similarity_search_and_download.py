"""
Pipeline: abstract similarity search + download.

Steps:
  1. Load seed bibcodes from the previous search table.
  2. Run ADS similar() on each seed (astronomy database only).
  3. Pool results — rank candidate papers by how many seeds they are
     similar to, breaking ties by average similarity score.
  4. Take the top 10 candidates.
  5. Download PDFs (arXiv → ADS_PDF → PUB_PDF fallback).
  6. Save a summary table: bibcode | title | year | seeds_matched |
     avg_score | pdf_file | fallback_url
"""

import sys
import os
import csv
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ads_parser import (
    find_similar_papers,
    download_pdfs,
    get_paper_links,
    get_ads_headers,
    ADS_API_BASE_URL,
    _make_ads_request_with_retry,
)

# ── Config ────────────────────────────────────────────────────────────────────
SEED_TABLE      = os.path.join(os.path.dirname(__file__), "..", "data", "search_download_table.csv")
PDF_DIR         = os.path.join(os.path.dirname(__file__), "..", "data", "pdfs")
OUTPUT_CSV      = os.path.join(os.path.dirname(__file__), "..", "data", "similarity_download_table.csv")
SIMILAR_PER_SEED = 50   # candidates retrieved per seed
TOP_N           = 10    # final papers to keep
DELAY           = 1.5   # seconds between similarity API calls
# ─────────────────────────────────────────────────────────────────────────────


def load_seed_bibcodes(csv_path: str) -> list:
    with open(csv_path, newline="") as f:
        return [row["bibcode"] for row in csv.DictReader(f) if row.get("bibcode")]


def fetch_similar(bibcode: str, headers: dict, max_results: int) -> list:
    """
    Wrapper around find_similar_papers that adds database:astronomy filter.
    Returns list of paper dicts with an added 'score' field.
    """
    import requests, time as _time
    params = {
        "q": f"similar({bibcode}) AND database:astronomy",
        "fl": "bibcode,title,year,pub,score",
        "rows": max_results,
        "sort": "score desc",
    }
    response = _make_ads_request_with_retry(
        f"{ADS_API_BASE_URL}/search/query", headers, params
    )
    if response is None or response.status_code != 200:
        return []
    docs = response.json().get("response", {}).get("docs", [])
    # Remove the seed itself if it shows up
    return [d for d in docs if d.get("bibcode") != bibcode]


def main():
    print("=" * 60)
    print("Abstract Similarity Search & Download Pipeline")
    print("=" * 60)

    # Step 1 — load seeds
    seeds = load_seed_bibcodes(SEED_TABLE)
    print(f"🌱 Seed bibcodes loaded : {len(seeds)}")
    for s in seeds:
        print(f"   {s}")
    print()

    # Step 2 — run similarity search for each seed
    headers = get_ads_headers()
    score_map   = defaultdict(list)   # bibcode -> [scores]
    meta_map    = {}                  # bibcode -> {title, year, pub}

    for i, seed in enumerate(seeds, 1):
        print(f"[{i}/{len(seeds)}] Similarity search for {seed} ...")
        candidates = fetch_similar(seed, headers, SIMILAR_PER_SEED)
        for doc in candidates:
            bib = doc.get("bibcode", "")
            if not bib or bib in seeds:   # skip seeds themselves
                continue
            score_map[bib].append(doc.get("score", 0.0))
            if bib not in meta_map:
                meta_map[bib] = {
                    "title": doc.get("title", [""])[0] if doc.get("title") else "",
                    "year":  doc.get("year", ""),
                    "pub":   doc.get("pub", ""),
                }
        import time
        if i < len(seeds):
            time.sleep(DELAY)

    print(f"\n📊 Unique candidate papers found: {len(score_map)}")

    # Step 3 — rank by (seeds_matched DESC, avg_score DESC)
    ranked = sorted(
        score_map.items(),
        key=lambda kv: (len(kv[1]), sum(kv[1]) / len(kv[1])),
        reverse=True,
    )
    top_bibcodes = [bib for bib, _ in ranked[:TOP_N]]

    print(f"🏆 Top {TOP_N} candidates:")
    for bib in top_bibcodes:
        scores = score_map[bib]
        print(f"   {bib}  seeds={len(scores)}  avg_score={sum(scores)/len(scores):.4f}")
    print()

    # Step 4 — download PDFs
    dl_results = download_pdfs(
        bibcodes=top_bibcodes,
        output_dir=PDF_DIR,
        delay_between_requests=2.0,
        skip_existing=True,
    )

    # Step 5 — collect fallback links for papers without a PDF
    no_pdf = dl_results["no_source"] + dl_results["failed"]
    fallback_links = {}
    if no_pdf:
        print(f"\n🔗 Fetching fallback links for {len(no_pdf)} papers without PDF...")
        for bib in no_pdf:
            fallback_links[bib] = get_paper_links(bib, headers)

    # Step 6 — build and save table
    rows = []
    for bib in top_bibcodes:
        scores   = score_map[bib]
        pdf_file = dl_results["pdf_files"].get(bib, "")
        links    = fallback_links.get(bib, {})
        fallback = (
            links.get("html") or
            links.get("doi")  or
            links.get("pub_pdf") or
            ""
        ) if not pdf_file else ""

        rows.append({
            "bibcode":       bib,
            "title":         meta_map[bib]["title"],
            "year":          meta_map[bib]["year"],
            "seeds_matched": len(scores),
            "avg_score":     round(sum(scores) / len(scores), 6),
            "pdf_file":      pdf_file,
            "fallback_url":  fallback,
        })

    fieldnames = ["bibcode", "title", "year", "seeds_matched", "avg_score",
                  "pdf_file", "fallback_url"]
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Print table
    print()
    print("=" * 100)
    print(f"{'BIBCODE':<25} {'YEAR':<6} {'SEEDS':>6} {'SCORE':>8}  {'PDF / FALLBACK'}")
    print("=" * 100)
    for row in rows:
        source = row["pdf_file"] if row["pdf_file"] else row["fallback_url"] or "—"
        print(f"{row['bibcode']:<25} {str(row['year']):<6} {row['seeds_matched']:>6} "
              f"{row['avg_score']:>8.4f}  {source}")
    print("=" * 100)
    print(f"\n💾 Table saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
