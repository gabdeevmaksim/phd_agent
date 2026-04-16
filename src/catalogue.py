"""
catalogue.py — Multi-paper object catalogue.

Schema
------
  Primary key: (Name, bibcode) — one row per object per paper.
  The same object appearing in N papers produces N rows.
  No merging or conflict resolution is performed here; every
  paper's values are preserved verbatim.

  Conflict detection is available separately via print_conflicts().

Conflict definition
-------------------
  Two values from different papers conflict if they differ by more than
  MAX_SIGMA times their combined uncertainty, OR by more than MAX_FRAC_DIFF
  fractionally when no uncertainties are available.

Usage
-----
    from src.catalogue import ObjectCatalogue

    cat = ObjectCatalogue()
    cat.add_paper("data/pdfs/paper1.pdf")
    cat.add_paper("data/pdfs/paper2.pdf")

    df = cat.to_dataframe()        # flat: one row per (object, paper)
    df.to_csv("data/catalogue.csv", index=False)

    # Inspect conflicts across papers
    cat.print_conflicts()
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.param_extractor import (
    PARAM_SPECS,
    ParamMatch,
    SOURCE_TABLE,
    SOURCE_REGEX,
    SOURCE_COMPUTED,
    SOURCE_MISSING,
)

# ── Conflict thresholds ───────────────────────────────────────────────────────
MAX_SIGMA     = 3.0    # flag if values differ by > 3σ (when both have uncertainty)
MAX_FRAC_DIFF = 0.10   # flag if values differ by > 10% (when no uncertainty)


def _is_conflict(a: ParamMatch, b: ParamMatch) -> bool:
    """True if two values for the same parameter are incompatible."""
    if a.is_missing or b.is_missing:
        return False
    diff = abs(a.value - b.value)
    if a.uncertainty is not None and b.uncertainty is not None:
        combined_sigma = math.sqrt(a.uncertainty**2 + b.uncertainty**2)
        if combined_sigma > 0:
            return (diff / combined_sigma) > MAX_SIGMA
    ref = (abs(a.value) + abs(b.value)) / 2.0
    if ref > 0:
        return (diff / ref) > MAX_FRAC_DIFF
    return False


# ── Main catalogue class ──────────────────────────────────────────────────────

class ObjectCatalogue:
    """
    Accumulates extraction results from multiple papers.
    One row per (object, paper) — no value merging.
    """

    def __init__(self) -> None:
        # Flat list of ObjectRow objects (one per object per paper)
        from src.pipeline import ObjectRow  # local import to avoid circular
        self._rows: List["ObjectRow"] = []
        self._papers: List[str] = []

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def add_paper_result(self, result: "PaperResult") -> None:  # noqa: F821
        """Ingest a PaperResult from pipeline.process_paper()."""
        self._papers.append(result.bibcode)
        for obj_row in result.rows:
            self._rows.append(obj_row)

    def add_paper(
        self,
        pdf_path: str,
        centroid=None,
        skip_classification: bool = False,
    ) -> None:
        """Convenience: run the pipeline on a PDF and ingest the result."""
        from src.pipeline import process_paper
        result = process_paper(pdf_path, centroid=centroid,
                               skip_classification=skip_classification,
                               verbose=False)
        self.add_paper_result(result)

    # ── Output ────────────────────────────────────────────────────────────────

    def to_dataframe(self) -> pd.DataFrame:
        """Return a DataFrame with one row per (object, paper)."""
        rows = [r.to_dict() for r in self._rows]
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def save(self, path: str = "data/catalogue.csv") -> None:
        df = self.to_dataframe()
        df.to_csv(path, index=False)
        print(f"Saved {len(df)} rows ({len(self._papers)} papers) -> {path}")

    def print_conflicts(self) -> None:
        """
        Detect and print parameter conflicts between papers for the same object.
        Two rows for the same object are compared pairwise; a conflict is flagged
        if values differ by >MAX_SIGMA or >MAX_FRAC_DIFF.
        """
        # Group rows by object name
        by_object: Dict[str, list] = defaultdict(list)
        for row in self._rows:
            by_object[row.object_name].append(row)

        any_conflict = False
        for obj_name, obj_rows in by_object.items():
            if len(obj_rows) < 2:
                continue
            for pname, spec in PARAM_SPECS.items():
                # Collect non-missing matches for this param across papers
                matches: List[ParamMatch] = []
                for r in obj_rows:
                    pm = r.params.get(pname)
                    if pm and not pm.is_missing:
                        matches.append(pm)
                # Pairwise conflict check
                for i in range(len(matches)):
                    for j in range(i + 1, len(matches)):
                        if _is_conflict(matches[i], matches[j]):
                            if not any_conflict:
                                print(f"\nConflicts detected:")
                            any_conflict = True
                            a, b = matches[i], matches[j]
                            unit = spec.unit if spec else ""
                            a_unc = f"+-{a.uncertainty}" if a.uncertainty else ""
                            b_unc = f"+-{b.uncertainty}" if b.uncertainty else ""
                            print(f"  {obj_name} | {pname:<8} "
                                  f"{a.value}{a_unc} {unit} [{a.bibcode}]  vs  "
                                  f"{b.value}{b_unc} {unit} [{b.bibcode}]")
        if not any_conflict:
            print("No conflicts detected.")

    def summary(self) -> None:
        """Print a summary of the catalogue."""
        obj_names = {r.object_name for r in self._rows}
        # Count objects appearing in more than one paper
        by_object: Dict[str, set] = defaultdict(set)
        for r in self._rows:
            by_object[r.object_name].add(r.bibcode)
        multi_paper = sum(1 for bibs in by_object.values() if len(bibs) > 1)

        print(f"\nCatalogue summary")
        print(f"   Papers      : {len(self._papers)}")
        print(f"   Objects     : {len(obj_names)}")
        print(f"   Rows        : {len(self._rows)}")
        print(f"   Multi-paper : {multi_paper} objects appear in >1 paper")
