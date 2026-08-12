"""
extract_params.py
-----------------
Run the full parameter extraction pipeline over all PDFs in data/pdfs/
and save results to data/catalogue.csv.

Usage
-----
    source .venv/bin/activate
    python scripts/extract_params.py [--skip-classification] [--verbose]
"""

import argparse
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

with open(PROJECT_ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

def cfg_path(key: str) -> Path:
    return PROJECT_ROOT / CFG["paths"][key]


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract WUMaCat parameters from PDFs.")
    parser.add_argument(
        "--skip-classification",
        action="store_true",
        help="Process all PDFs without the relevance filter.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-paper extraction details.",
    )
    args = parser.parse_args()

    from src.pipeline import process_directory

    process_directory(
        pdf_dir=str(cfg_path("pdf_dir")),
        output_csv=str(cfg_path("catalogue_csv")),
        skip_classification=args.skip_classification,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
