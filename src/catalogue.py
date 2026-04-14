"""
catalogue.py — Multi-paper object catalogue with conflict resolution.

When the same astronomical object appears in several papers, each may report
slightly different parameter values (different data, different models, different
conventions). This module accumulates all extractions and selects the best value
per parameter while flagging genuine conflicts.

Ranking priority (highest first)
---------------------------------
  1. table   + uncertainty   — directly parsed from a solution table, with error bars
  2. regex   + uncertainty   — matched inline in text, with error bars
  3. table   (no unc)        — table value without quoted uncertainty
  4. regex   (no unc)        — inline text, no uncertainty
  5. computed                — derived from other parameters
  6. missing                 — not found

Conflict definition
-------------------
  Two values conflict if they differ by more than MAX_SIGMA times their
  combined uncertainty, OR by more than MAX_FRAC_DIFF fractionally when
  no uncertainties are available.

Usage
-----
    from src.catalogue import ObjectCatalogue

    cat = ObjectCatalogue()
    cat.add_paper("data/pdfs/paper1.pdf")
    cat.add_paper("data/pdfs/paper2.pdf")

    df = cat.to_dataframe()
    df.to_csv("data/catalogue.csv", index=False)

    # Inspect conflicts
    cat.print_conflicts()
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.param_extractor import (
    PARAM_SPECS,
    ParamMatch,
    CategoricalMatch,
    SOURCE_TABLE,
    SOURCE_REGEX,
    SOURCE_COMPUTED,
    SOURCE_MISSING,
)

# ── Conflict thresholds ───────────────────────────────────────────────────────
MAX_SIGMA     = 3.0    # flag if values differ by > 3σ (when both have uncertainty)
MAX_FRAC_DIFF = 0.10   # flag if values differ by > 10% (when no uncertainty)

# Source rank: lower = better
_SOURCE_RANK = {
    SOURCE_TABLE:    0,
    SOURCE_REGEX:    1,
    SOURCE_COMPUTED: 2,
    SOURCE_MISSING:  3,
}


# ── Per-parameter evidence record ────────────────────────────────────────────

@dataclass
class Evidence:
    """All extracted values for one parameter of one object, across all papers."""
    param:     str
    matches:   List[ParamMatch] = field(default_factory=list)
    conflicts: List[Tuple[ParamMatch, ParamMatch]] = field(default_factory=list)

    def add(self, pm: ParamMatch) -> None:
        if pm.is_missing:
            return
        # Check for conflict with existing best value
        best = self.best()
        if best is not None and not best.is_missing:
            if _is_conflict(best, pm):
                self.conflicts.append((best, pm))
        self.matches.append(pm)

    def best(self) -> Optional[ParamMatch]:
        """Return highest-ranked non-missing match."""
        candidates = [m for m in self.matches if not m.is_missing]
        if not candidates:
            return None
        return min(candidates, key=_rank_key)

    def all_bibcodes(self) -> List[str]:
        return list(dict.fromkeys(m.bibcode for m in self.matches if m.bibcode))

    def has_conflict(self) -> bool:
        return len(self.conflicts) > 0


def _rank_key(pm: ParamMatch) -> Tuple[int, int]:
    """Lower = better. Primary: source rank. Secondary: has uncertainty (0) or not (1)."""
    src_rank = _SOURCE_RANK.get(pm.source, 9)
    unc_rank = 0 if pm.uncertainty is not None else 1
    return (src_rank, unc_rank)


def _is_conflict(a: ParamMatch, b: ParamMatch) -> bool:
    """True if two values for the same parameter are incompatible."""
    if a.is_missing or b.is_missing:
        return False
    diff = abs(a.value - b.value)
    # Both have uncertainties → sigma test
    if a.uncertainty is not None and b.uncertainty is not None:
        combined_sigma = math.sqrt(a.uncertainty**2 + b.uncertainty**2)
        if combined_sigma > 0:
            return (diff / combined_sigma) > MAX_SIGMA
    # Fractional difference fallback
    ref = (abs(a.value) + abs(b.value)) / 2.0
    if ref > 0:
        return (diff / ref) > MAX_FRAC_DIFF
    return False


# ── Per-object record ─────────────────────────────────────────────────────────

@dataclass
class CatalogueEntry:
    """All evidence for every parameter of one named object."""
    name:     str
    evidence: Dict[str, Evidence] = field(default_factory=dict)
    cat_evidence: Dict[str, List[CategoricalMatch]] = field(default_factory=dict)

    def add_param(self, pm: ParamMatch) -> None:
        ev = self.evidence.setdefault(pm.param, Evidence(pm.param))
        ev.add(pm)

    def add_categorical(self, cm: CategoricalMatch) -> None:
        self.cat_evidence.setdefault(cm.param, []).append(cm)

    def best_categorical(self, param: str) -> Optional[CategoricalMatch]:
        """Return the most-cited non-missing categorical value."""
        candidates = [c for c in self.cat_evidence.get(param, [])
                      if not c.is_missing]
        if not candidates:
            return None
        # Prefer non-computed, break ties by majority vote
        non_computed = [c for c in candidates if c.source != SOURCE_COMPUTED]
        pool = non_computed if non_computed else candidates
        counts: Dict[str, int] = defaultdict(int)
        for c in pool:
            counts[c.value] += 1
        best_val = max(counts, key=counts.__getitem__)
        return next(c for c in pool if c.value == best_val)

    def to_dict(self) -> dict:
        row: dict = {"Name": self.name}

        for pname, spec in PARAM_SPECS.items():
            ev = self.evidence.get(pname)
            best = ev.best() if ev else None

            if best is None or best.is_missing:
                row[pname]           = float("nan")
                row[f"{pname}_unc"]  = float("nan")
                row[f"{pname}_src"]  = "missing"
                row[f"{pname}_bib"]  = ""
                row[f"{pname}_conflict"] = False
            else:
                row[pname]           = best.value
                row[f"{pname}_unc"]  = best.uncertainty if best.uncertainty is not None else float("nan")
                row[f"{pname}_src"]  = best.source
                row[f"{pname}_bib"]  = best.bibcode
                row[f"{pname}_conflict"] = ev.has_conflict() if ev else False

        for cname in ("Type", "ET", "Solver", "Spots"):
            best_cat = self.best_categorical(cname)
            if best_cat is None:
                row[cname]          = ""
                row[f"{cname}_src"] = "missing"
                row[f"{cname}_bib"] = ""
            else:
                row[cname]          = best_cat.value
                row[f"{cname}_src"] = best_cat.source
                row[f"{cname}_bib"] = best_cat.bibcode

        # All bibcodes that contributed any parameter for this object
        all_bibs = set()
        for ev in self.evidence.values():
            all_bibs.update(ev.all_bibcodes())
        for cms in self.cat_evidence.values():
            all_bibs.update(c.bibcode for c in cms if c.bibcode)
        row["bibcodes"] = "|".join(sorted(all_bibs))

        return row

    def conflicts(self) -> List[Tuple[str, ParamMatch, ParamMatch]]:
        """Return list of (param_name, match_a, match_b) for all conflicts."""
        result = []
        for pname, ev in self.evidence.items():
            for a, b in ev.conflicts:
                result.append((pname, a, b))
        return result


# ── Main catalogue class ──────────────────────────────────────────────────────

class ObjectCatalogue:
    """
    Accumulates extraction results from multiple papers and resolves
    per-object parameter values.
    """

    def __init__(self) -> None:
        self._entries: Dict[str, CatalogueEntry] = {}
        self._papers:  List[str] = []

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def add_paper_result(self, result: "PaperResult") -> None:  # noqa: F821
        """Ingest a PaperResult from pipeline.process_paper()."""
        self._papers.append(result.bibcode)
        for obj_row in result.rows:
            entry = self._entries.setdefault(
                obj_row.object_name,
                CatalogueEntry(obj_row.object_name),
            )
            for pm in obj_row.params.values():
                entry.add_param(pm)
            for cm in obj_row.cats.values():
                entry.add_categorical(cm)

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
        """Return a DataFrame with one row per object, best values selected."""
        rows = [entry.to_dict() for entry in self._entries.values()]
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def save(self, path: str = "data/catalogue.csv") -> None:
        df = self.to_dataframe()
        df.to_csv(path, index=False)
        print(f"💾 Saved {len(df)} objects from {len(self._papers)} papers → {path}")

    def print_conflicts(self) -> None:
        """Print all detected inter-paper conflicts."""
        any_conflict = False
        for name, entry in self._entries.items():
            cfls = entry.conflicts()
            if not cfls:
                continue
            any_conflict = True
            print(f"\n⚠️  Conflicts for {name}:")
            for pname, a, b in cfls:
                spec = PARAM_SPECS.get(pname)
                unit = spec.unit if spec else ""
                a_unc = f"±{a.uncertainty}" if a.uncertainty else ""
                b_unc = f"±{b.uncertainty}" if b.uncertainty else ""
                print(f"   {pname:<8} "
                      f"{a.value}{a_unc} {unit} [{a.bibcode}]  vs  "
                      f"{b.value}{b_unc} {unit} [{b.bibcode}]")
        if not any_conflict:
            print("✅ No conflicts detected.")

    def summary(self) -> None:
        """Print a summary of the catalogue."""
        n_objects = len(self._entries)
        n_conflicts = sum(
            1 for e in self._entries.values() if e.conflicts()
        )
        print(f"\n📚 Catalogue summary")
        print(f"   Papers   : {len(self._papers)}")
        print(f"   Objects  : {n_objects}")
        print(f"   Conflicts: {n_conflicts} objects with conflicting values")
