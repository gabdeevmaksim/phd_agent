"""
Pipeline: search ADS by keywords, download PDFs, and produce a summary table.

For each paper:
  - If a PDF was downloaded or already exists  → record the local filename
  - If no downloadable PDF exists             → record the best available link
                                                (HTML, DOI, or PUB_PDF URL)

Output: data/search_download_table.csv
"""

import sys
import os
import csv
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ads_parser import (
    search_all_bibcodes,
    download_pdfs,
    get_paper_links,
    get_ads_headers,
)

# ── Config ────────────────────────────────────────────────────────────────────
KEYWORDS     = ["binary", "eclipsing", "mass", "temperature", "parameter"]
SEARCH_FIELD = "full"
YEAR_FILTER  = "year:[0 TO 2021]"
N_PAPERS     = 10
PDF_DIR      = os.path.join(os.path.dirname(__file__), "..", "data", "pdfs")
OUTPUT_CSV   = os.path.join(os.path.dirname(__file__), "..", "data", "search_download_table.csv")
# ─────────────────────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("Search & Download Pipeline")
    print("=" * 60)
    print(f"Keywords : {KEYWORDS}")
    print(f"Field    : {SEARCH_FIELD}  |  Year: {YEAR_FILTER}")
    print(f"Papers   : first {N_PAPERS}")
    print()

    # Step 1 — search
    all_bibcodes = search_all_bibcodes(
        KEYWORDS,
        search_fields=SEARCH_FIELD,
        extra_filter=YEAR_FILTER,
    )
    bibcodes = all_bibcodes[:N_PAPERS]
    print(f"\n📋 Selected {len(bibcodes)} bibcodes from {len(all_bibcodes):,} total\n")

    # Step 2 — download PDFs
    dl_results = download_pdfs(
        bibcodes=bibcodes,
        output_dir=PDF_DIR,
        delay_between_requests=2.0,
        skip_existing=True,
    )

    # Step 3 — collect fallback links for papers without a PDF
    headers = get_ads_headers()
    no_pdf_bibcodes = dl_results["no_source"] + dl_results["failed"]

    print(f"\n🔗 Fetching fallback links for {len(no_pdf_bibcodes)} papers without PDF...")
    fallback_links: dict = {}
    for bib in no_pdf_bibcodes:
        fallback_links[bib] = get_paper_links(bib, headers)

    # Step 4 — build table
    rows = []
    for bib in bibcodes:
        pdf_file = dl_results["pdf_files"].get(bib, "")
        links = fallback_links.get(bib, {})

        # Best fallback link: prefer html > doi > pub_pdf
        fallback_url = (
            links.get("html") or
            links.get("doi") or
            links.get("pub_pdf") or
            ""
        )

        rows.append({
            "bibcode":      bib,
            "pdf_file":     pdf_file,
            "fallback_url": fallback_url if not pdf_file else "",
        })

    # Step 5 — write CSV
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    fieldnames = ["bibcode", "pdf_file", "fallback_url"]
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Step 6 — print table to console
    print()
    print("=" * 80)
    print(f"{'BIBCODE':<25} {'PDF FILE':<35} {'FALLBACK URL'}")
    print("=" * 80)
    for row in rows:
        print(f"{row['bibcode']:<25} {row['pdf_file']:<35} {row['fallback_url']}")
    print("=" * 80)
    print(f"\n💾 Table saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
