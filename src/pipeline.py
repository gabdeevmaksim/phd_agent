"""
pipeline.py — End-to-end extraction pipeline for a single PDF.

Orchestration order
-------------------
  1. classify_paper()        — 3-stage relevance filter; skip if not relevant
  2. PaperIndex()            — object extraction + chunking + embedding
  3. extract_table_block()   — parse multi-object parameter tables
  4. extract_params()        — inline regex over semantic query hits
  5. extract_categorical()   — Solver, Type, ET, Spots
  6. fill_missing()          — Tier-2 computed fallbacks + Tier-3 NaN markers
  7. assemble_rows()         — one dict per object, WUMaCat column names

Usage
-----
    from src.pipeline import process_paper

    rows = process_paper("data/pdfs/2016AJ....152..129C.pdf")
    for row in rows:
        print(row)

    # Batch over a directory, writing a CSV
    import pandas as pd
    from pathlib import Path

    all_rows = []
    for pdf in Path("data/pdfs").glob("*.pdf"):
        all_rows.extend(process_paper(str(pdf)))
    pd.DataFrame(all_rows).to_csv("data/extracted_params.csv", index=False)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import fitz

from src.classifier import classify_paper
from src.paper_index import PaperIndex, extract_objects
from src.param_extractor import (
    PARAM_SPECS,
    ParamMatch,
    CategoricalMatch,
    SOURCE_TABLE,
    SOURCE_REGEX,
    extract_params,
    extract_table_block,
    extract_categorical,
    fill_missing,
    best_value,
    param_query_plan,
    extract_from_hits,
    print_results,
    _normalize,
)


# ── Per-paper result ──────────────────────────────────────────────────────────

@dataclass
class ObjectRow:
    """
    Extracted parameters for one object in one paper.
    Maps directly to WUMaCat columns plus provenance metadata.
    """
    pdf_path:   str
    bibcode:    str           # derived from filename (no extension)
    object_name: str
    params:     Dict[str, ParamMatch]      = field(default_factory=dict)
    cats:       Dict[str, CategoricalMatch] = field(default_factory=dict)
    n_mentions: int = 0       # how many times the object appears in the paper

    def to_dict(self) -> dict:
        """Flatten to a single dict with WUMaCat column names."""
        import os
        row = {
            "pdf_path":    self.pdf_path,
            "bibcode":     self.bibcode,
            "Name":        self.object_name,
            "n_mentions":  self.n_mentions,
        }
        # Numerical params
        for pname in PARAM_SPECS:
            pm = self.params.get(pname)
            if pm is None or pm.is_missing:
                row[pname]              = float("nan")
                row[f"{pname}_unc"]     = float("nan")
                row[f"{pname}_src"]     = "missing"
                row[f"{pname}_bibcode"] = ""
            else:
                row[pname]              = pm.value
                row[f"{pname}_unc"]     = pm.uncertainty if pm.uncertainty is not None else float("nan")
                row[f"{pname}_src"]     = pm.source
                row[f"{pname}_bibcode"] = pm.bibcode
        # Categorical params
        for cname in ("Type", "ET", "Solver", "Spots"):
            cm = self.cats.get(cname)
            if cm is None or cm.is_missing:
                row[cname]              = ""
                row[f"{cname}_src"]     = "missing"
                row[f"{cname}_bibcode"] = ""
            else:
                row[cname]              = cm.value
                row[f"{cname}_src"]     = cm.source
                row[f"{cname}_bibcode"] = cm.bibcode
        return row

    def print(self) -> None:
        print(f"\n{'='*60}")
        print(f"  Object : {self.object_name}")
        print(f"  Paper  : {self.bibcode}  ({self.n_mentions} mentions)")
        print(f"{'='*60}")
        print_results(self.params, self.cats)


@dataclass
class PaperResult:
    """All extracted objects and metadata for one paper."""
    pdf_path:   str
    bibcode:    str
    verdict:    str           # "relevant" | "not_relevant" | "uncertain"
    exit_stage: int
    rows:       List[ObjectRow] = field(default_factory=list)
    error:      str = ""

    def to_rows(self) -> List[dict]:
        return [r.to_dict() for r in self.rows]

    def print(self) -> None:
        icons = {"relevant": "✅", "not_relevant": "❌", "uncertain": "⚠️ "}
        icon = icons.get(self.verdict, "?")
        print(f"\n{icon} {self.bibcode} — {self.verdict} (stage {self.exit_stage})")
        if self.error:
            print(f"   Error: {self.error}")
        for row in self.rows:
            row.print()


# ── Core pipeline ─────────────────────────────────────────────────────────────

def process_paper(
    pdf_path: str,
    centroid=None,
    skip_classification: bool = False,
    verbose: bool = True,
    ocr_engine: str = "easyocr",
) -> PaperResult:
    """
    Run the full extraction pipeline on a single PDF.

    Args:
        pdf_path:             Path to the PDF file.
        centroid:             Pre-loaded WUMaCat centroid (loaded if None).
        skip_classification:  If True, skip Stage 1-3 filter and always extract.
        verbose:              Print progress and results.
        ocr_engine:           OCR engine for image-based PDFs: "easyocr" (fast,
                              default) or "nougat" (slow but LaTeX-aware; ~90s/page
                              on CPU, much better for parameter tables).

    Returns:
        PaperResult with one ObjectRow per discovered object.
    """
    import os
    bibcode = os.path.splitext(os.path.basename(pdf_path))[0]

    # ── Stage 0: relevance classification ────────────────────────────────────
    if not skip_classification:
        clf = classify_paper(pdf_path, centroid=centroid)
        if verbose:
            clf.print()
        if clf.verdict == "not_relevant":
            return PaperResult(
                pdf_path=pdf_path, bibcode=bibcode,
                verdict="not_relevant", exit_stage=clf.exit_stage,
            )
        verdict, exit_stage = clf.verdict, clf.exit_stage
    else:
        verdict, exit_stage = "relevant", 0

    # ── Stage 1: detect PDF type + extract full text (OCR if needed) ─────────
    from src.pdf_utils import detect_pdf_type, ocr_pdf

    pdf_info = detect_pdf_type(pdf_path)
    if pdf_info.pdf_type == "image":
        if verbose:
            print(f"\n🖼️  Image-based PDF — running {ocr_engine} on {pdf_info.total_pages} pages …")
        raw_text = ocr_pdf(pdf_path, engine=ocr_engine)
    else:
        doc = fitz.open(pdf_path)
        raw_text = "\n".join(page.get_text() for page in doc)
        doc.close()

    full_text = _normalize(raw_text)

    # ── Stage 2: build paper index (objects + chunks + embeddings) ───────────
    if verbose:
        print(f"\n📄 Building index for {bibcode} …")

    try:
        idx = PaperIndex(pdf_path, verbose=verbose, text=full_text)
    except Exception as e:
        return PaperResult(
            pdf_path=pdf_path, bibcode=bibcode,
            verdict=verdict, exit_stage=exit_stage,
            error=f"PaperIndex failed: {e}",
        )

    if not idx.object_list.objects:
        return PaperResult(
            pdf_path=pdf_path, bibcode=bibcode,
            verdict=verdict, exit_stage=exit_stage,
            error="No astronomical objects found in paper",
        )

    table_data = extract_table_block(full_text, idx.object_list.objects)
    inline_data = extract_params(full_text)
    cats = extract_categorical(full_text)

    # ── Stage 3: semantic query refinement (per object) ───────────────────────
    # For each object, run targeted semantic queries and merge results.
    # Semantic hits are lower priority than table/inline — they catch
    # values that appear only in prose sections.
    query_plan = param_query_plan()

    rows: List[ObjectRow] = []

    for obj_name in idx.object_list.objects:
        # Start with table values (highest fidelity)
        obj_params: Dict[str, List[ParamMatch]] = {
            p: [pm] for p, pm in table_data.get(obj_name, {}).items()
        }

        # Add inline regex hits
        for p, hits in inline_data.items():
            obj_params.setdefault(p, []).extend(hits)

        # Add semantic query hits (object-scoped)
        for pname, queries in query_plan.items():
            for q_text in queries:
                hits = idx.query(q_text, object_name=obj_name, top_k=3)
                partial = extract_from_hits(hits, params=[pname])
                for p, pms in partial.items():
                    obj_params.setdefault(p, []).extend(pms)

        # Fill missing via Tier-2 computation and Tier-3 NaN
        filled_params, filled_cats = fill_missing(obj_params, cats)

        # Stamp bibcode on every match for downstream traceability
        for pm in filled_params.values():
            pm.bibcode = bibcode
        for cm in filled_cats.values():
            cm.bibcode = bibcode

        rows.append(ObjectRow(
            pdf_path=pdf_path,
            bibcode=bibcode,
            object_name=obj_name,
            params=filled_params,
            cats=filled_cats,
            n_mentions=idx.object_list.frequencies.get(obj_name, 0),
        ))

    result = PaperResult(
        pdf_path=pdf_path, bibcode=bibcode,
        verdict=verdict, exit_stage=exit_stage,
        rows=rows,
    )

    if verbose:
        result.print()

    return result


# ── Batch processing ──────────────────────────────────────────────────────────

def process_directory(
    pdf_dir: str,
    output_csv: str = "data/extracted_params.csv",
    skip_classification: bool = False,
    verbose: bool = True,
) -> "pd.DataFrame":
    """
    Run process_paper on every PDF in a directory and save results to CSV.

    Args:
        pdf_dir:              Directory containing PDF files.
        output_csv:           Path for the output CSV file.
        skip_classification:  Skip relevance filter (process all PDFs).
        verbose:              Print per-paper progress.

    Returns:
        pandas DataFrame with all extracted rows.
    """
    import os
    import pandas as pd
    from src.embeddings import load_centroid

    centroid = load_centroid()
    pdf_files = sorted(f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf"))

    all_rows = []
    stats = {"relevant": 0, "not_relevant": 0, "uncertain": 0, "error": 0}

    print(f"\n🔬 Processing {len(pdf_files)} PDFs in {pdf_dir}")

    for fname in pdf_files:
        path = os.path.join(pdf_dir, fname)
        try:
            result = process_paper(
                path, centroid=centroid,
                skip_classification=skip_classification,
                verbose=verbose,
            )
            stats[result.verdict] = stats.get(result.verdict, 0) + 1
            all_rows.extend(result.to_rows())
        except Exception as e:
            print(f"  ❌ {fname}: {e}")
            stats["error"] += 1

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df.to_csv(output_csv, index=False)
        print(f"\n💾 Saved {len(df)} rows ({len(pdf_files)} papers) → {output_csv}")

    print(f"\n📊 Summary:")
    for k, v in stats.items():
        print(f"   {k}: {v}")

    return df
