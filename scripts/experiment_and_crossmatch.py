"""
Experiment: Search ADS for papers using AND condition on specific keywords,
then cross-match with WUMaCat bibcodes.

Keywords: binary, eclipsing, mass, temperature, parameter
Search field: full text (AND condition), year < 2021
"""

import sys
import os
import json
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ads_parser import search_all_bibcodes
from src.wordcloud_utils import load_wumacat_bibcodes

KEYWORDS    = ["binary", "eclipsing", "mass", "temperature", "parameter"]
SEARCH_FIELD = "full"
YEAR_FILTER  = "year:[0 TO 2021]"
TARGET       = 10000
DATA_DIR     = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_FILE  = os.path.join(DATA_DIR, "experiment_and_crossmatch_results.json")


def main():
    print("=" * 60)
    print("ADS AND-Keyword Experiment + WUMaCat Cross-Match")
    print("=" * 60)
    print(f"Keywords : {KEYWORDS}")
    print(f"Condition: AND (all keywords must appear in full text)")
    print(f"Year     : {YEAR_FILTER}")
    print(f"Target   : {TARGET} papers")
    print()

    # Step 1 — retrieve bibcodes with year filter
    all_bibcodes = search_all_bibcodes(
        KEYWORDS,
        search_fields=SEARCH_FIELD,
        extra_filter=YEAR_FILTER
    )

    if not all_bibcodes:
        print("❌ No papers found. Exiting.")
        sys.exit(1)

    retrieved_bibcodes = all_bibcodes[:TARGET]
    print(f"\n✂️  Using first {len(retrieved_bibcodes):,} bibcodes (of {len(all_bibcodes):,} total)")

    # Step 2 — load WUMaCat reference set
    wumacat_path = os.path.join(DATA_DIR, "WUMaCat.csv")
    wumacat_bibcodes = load_wumacat_bibcodes(wumacat_path)
    print()

    # Step 3 — cross-match
    retrieved_set = set(retrieved_bibcodes)
    matched     = retrieved_set & wumacat_bibcodes
    not_matched = retrieved_set - wumacat_bibcodes

    precision = len(matched) / len(retrieved_set) * 100 if retrieved_set else 0
    recall    = len(matched) / len(wumacat_bibcodes) * 100 if wumacat_bibcodes else 0

    print("=" * 60)
    print("CROSS-MATCH RESULTS")
    print("=" * 60)
    print(f"  Retrieved bibcodes       : {len(retrieved_set):,}")
    print(f"  WUMaCat bibcodes         : {len(wumacat_bibcodes):,}")
    print(f"  Matched (intersection)   : {len(matched):,}")
    print(f"  Not matched              : {len(not_matched):,}")
    print(f"  Precision (match/ret.)   : {precision:.2f}%")
    print(f"  Recall    (match/wumacat): {recall:.2f}%")
    print("=" * 60)

    # Step 4 — save results
    results = {
        "timestamp": datetime.now().isoformat(),
        "experiment": "AND keyword full-text search + WUMaCat cross-match",
        "parameters": {
            "keywords": KEYWORDS,
            "condition": "AND",
            "search_field": SEARCH_FIELD,
            "year_filter": YEAR_FILTER,
            "target_papers": TARGET,
        },
        "search_results": {
            "total_available": len(all_bibcodes),
            "retrieved": len(retrieved_set),
        },
        "crossmatch": {
            "wumacat_total": len(wumacat_bibcodes),
            "matched": len(matched),
            "not_matched": len(not_matched),
            "precision_pct": round(precision, 4),
            "recall_pct": round(recall, 4),
        },
        "matched_bibcodes": sorted(matched),
        "retrieved_bibcodes": sorted(retrieved_bibcodes),
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n💾 Results saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
