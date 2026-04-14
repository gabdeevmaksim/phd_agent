"""
classifier.py — 3-stage paper relevance classifier.

Stage 1  │ Keyword scoring (fast, rule-based)
         │   >= 2.0  → RELEVANT     (exit)
         │   <  0.5  → NOT_RELEVANT (exit)
         │   else    → Stage 2
         │
Stage 2  │ Abstract embedding vs WUMaCat centroid
         │   >= 0.840 → RELEVANT     (exit)
         │   <  0.836 → NOT_RELEVANT (exit)
         │   else     → Stage 3
         │
Stage 3  │ Conclusions embedding vs WUMaCat centroid
         │   >= 0.820 → RELEVANT
         │   <  0.820 → NOT_RELEVANT
         │   (no conclusions found → NOT_RELEVANT, flagged)

Usage
-----
    from src.classifier import classify_paper, ClassificationResult
    result = classify_paper("data/pdfs/some_paper.pdf")
    result.print()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
import numpy as np


# ── Stage thresholds ─────────────────────────────────────────────────────────
S1_RELEVANT    = 2.0
S1_IRRELEVANT  = 0.5

S2_RELEVANT    = 0.840
S2_BORDERLINE  = 0.836   # window that triggers Stage 3

S3_RELEVANT    = 0.820
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class StageResult:
    stage: int
    score: float
    verdict: str          # "relevant" | "not_relevant" | "borderline"
    note: str = ""


@dataclass
class ClassificationResult:
    pdf_path:   str
    pdf_type:   str       # "text" | "image"
    verdict:    str       # final: "relevant" | "not_relevant" | "uncertain"
    exit_stage: int       # which stage produced the final verdict (0 = pre-check)
    stages:     list = field(default_factory=list)   # List[StageResult]
    error:      str  = ""

    # ── pretty print ─────────────────────────────────────────────────────────
    def print(self) -> None:
        icons = {"relevant": "✅", "not_relevant": "❌", "uncertain": "⚠️ "}
        icon  = icons.get(self.verdict, "?")
        print(f"\n{'='*60}")
        print(f"{icon}  {self.verdict.upper()}  —  {self.pdf_path}")
        print(f"   PDF type  : {self.pdf_type}")
        print(f"   Exit stage: {self.exit_stage}")
        if self.error:
            print(f"   ⚠️  Error  : {self.error}")
        for s in self.stages:
            status = {"relevant": "✅", "not_relevant": "❌",
                      "borderline": "⚠️ "}.get(s.verdict, "?")
            note = f"  [{s.note}]" if s.note else ""
            print(f"   Stage {s.stage}: score={s.score:.4f}  {status} {s.verdict}{note}")
        print(f"{'='*60}")


# ── Main classifier ───────────────────────────────────────────────────────────

def classify_paper(
    pdf_path: str,
    centroid: Optional[np.ndarray] = None,
) -> ClassificationResult:
    """
    Classify a single PDF through the 3-stage pipeline.

    Args:
        pdf_path: Path to the PDF file.
        centroid: Pre-loaded WUMaCat centroid (loaded from disk if None).

    Returns:
        ClassificationResult with verdict and per-stage details.
    """
    from src.pdf_utils import detect_pdf_type, extract_abstract, extract_conclusions
    from src.relevance_keywords import score_text
    from src.embeddings import load_centroid, similarity_score

    stages: list = []

    # ── Pre-check: is it a text PDF? ─────────────────────────────────────────
    try:
        pdf_info = detect_pdf_type(pdf_path)
    except FileNotFoundError as e:
        return ClassificationResult(pdf_path=pdf_path, pdf_type="unknown",
                                    verdict="uncertain", exit_stage=0,
                                    error=str(e))

    if pdf_info.pdf_type == "image":
        return ClassificationResult(pdf_path=pdf_path, pdf_type="image",
                                    verdict="uncertain", exit_stage=0,
                                    error="Image-based PDF — OCR required before classification")

    # Load centroid once
    if centroid is None:
        centroid = load_centroid()

    # ── Stage 1: keyword scoring ──────────────────────────────────────────────
    try:
        abstract_text = extract_abstract(pdf_path)
        kw_result     = score_text(abstract_text)
        kw_score      = kw_result.score

        # RELEVANT requires both a high score AND at least one system_type hit
        # — prevents generic astronomy terms (orbital period, spectroscopy)
        # from matching non-binary papers
        has_system_type = kw_result.category_scores.get("system_type", 0) > 0

        if kw_score >= S1_RELEVANT and has_system_type:
            s1 = StageResult(1, kw_score, "relevant")
            stages.append(s1)
            return ClassificationResult(pdf_path, "text", "relevant", 1, stages)

        if kw_score < S1_IRRELEVANT:
            s1 = StageResult(1, kw_score, "not_relevant")
            stages.append(s1)
            return ClassificationResult(pdf_path, "text", "not_relevant", 1, stages)

        stages.append(StageResult(1, kw_score, "borderline"))

    except Exception as e:
        stages.append(StageResult(1, 0.0, "borderline", note=f"error: {e}"))

    # ── Stage 2: abstract embedding ───────────────────────────────────────────
    try:
        s2_result = similarity_score(abstract_text, centroid)
        sim2      = s2_result.similarity

        if sim2 >= S2_RELEVANT:
            s2 = StageResult(2, sim2, "relevant")
            stages.append(s2)
            return ClassificationResult(pdf_path, "text", "relevant", 2, stages)

        if sim2 < S2_BORDERLINE:
            s2 = StageResult(2, sim2, "not_relevant")
            stages.append(s2)
            return ClassificationResult(pdf_path, "text", "not_relevant", 2, stages)

        stages.append(StageResult(2, sim2, "borderline"))

    except Exception as e:
        stages.append(StageResult(2, 0.0, "not_relevant", note=f"error: {e}"))
        return ClassificationResult(pdf_path, "text", "not_relevant", 2, stages)

    # ── Stage 3: conclusions embedding ───────────────────────────────────────
    try:
        conclusions = extract_conclusions(pdf_path)

        if conclusions is None:
            stages.append(StageResult(3, 0.0, "not_relevant",
                                      note="no conclusions found — defaulting to not_relevant"))
            return ClassificationResult(pdf_path, "text", "not_relevant", 3, stages)

        s3_result = similarity_score(conclusions, centroid)
        sim3      = s3_result.similarity

        if sim3 >= S3_RELEVANT:
            stages.append(StageResult(3, sim3, "relevant"))
            return ClassificationResult(pdf_path, "text", "relevant", 3, stages)
        else:
            stages.append(StageResult(3, sim3, "not_relevant"))
            return ClassificationResult(pdf_path, "text", "not_relevant", 3, stages)

    except Exception as e:
        stages.append(StageResult(3, 0.0, "uncertain", note=f"error: {e}"))
        return ClassificationResult(pdf_path, "text", "uncertain", 3, stages)


def classify_all(
    pdf_dir: str,
    centroid: Optional[np.ndarray] = None,
    verbose: bool = True,
) -> list:
    """
    Classify all PDFs in a directory.

    Args:
        pdf_dir:  Directory containing PDF files.
        centroid: Pre-loaded WUMaCat centroid (loaded once if None).
        verbose:  Print each result.

    Returns:
        List of ClassificationResult objects.
    """
    import os
    from src.embeddings import load_centroid

    if centroid is None:
        centroid = load_centroid()

    pdf_files = sorted(f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf"))
    results   = []

    for fname in pdf_files:
        path   = os.path.join(pdf_dir, fname)
        result = classify_paper(path, centroid=centroid)
        results.append(result)
        if verbose:
            result.print()

    # Summary
    if verbose:
        relevant     = sum(1 for r in results if r.verdict == "relevant")
        not_relevant = sum(1 for r in results if r.verdict == "not_relevant")
        uncertain    = sum(1 for r in results if r.verdict == "uncertain")
        print(f"\n{'='*60}")
        print(f"SUMMARY: {len(results)} papers")
        print(f"  ✅ relevant     : {relevant}")
        print(f"  ❌ not_relevant : {not_relevant}")
        print(f"  ⚠️  uncertain    : {uncertain}")
        print(f"{'='*60}")

    return results
