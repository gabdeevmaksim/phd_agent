"""
relevance_keywords.py — Stage 1 relevance filter for the paper classification pipeline.

Dictionary design
-----------------
Terms are grouped into thematic categories and weighted 1-3:

  3 — highly specific: nearly exclusive to eclipsing/contact binary parameter studies
  2 — strong indicator: common in the domain, rare outside it
  1 — supporting evidence: appear in related work but also in broader astronomy

Scoring
-------
score = sum over matched terms of:
    weight × log2(1 + count)   <- log-dampens repeated mentions of the same term

Final score is normalised by the square root of the word count so that longer
papers do not get an unfair advantage.

Threshold guidance (tunable):
    score >= 2.0  →  likely relevant      (pass to Stage 2)
    score >= 0.5  →  borderline           (pass to Stage 2 with flag)
    score <  0.5  →  likely not relevant  (discard)
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


# ---------------------------------------------------------------------------
# Keyword dictionary
# Each entry: "term": weight
# ---------------------------------------------------------------------------

KEYWORD_DICT: Dict[str, Dict[str, int]] = {

    # ── System type ──────────────────────────────────────────────────────────
    # Identifies the kind of object the paper studies.
    "system_type": {
        "w uma":                    3,
        "w-uma":                    3,
        "w ursae majoris":          3,
        "overcontact":              3,
        "over-contact":             3,
        "contact binary":           3,
        "contact binaries":         3,
        "a-subtype":                3,
        "w-subtype":                3,
        "eclipsing binary":         2,
        "eclipsing binaries":       2,
        "close binary":             2,
        "close binaries":           2,
        "late-type binary":         2,
        "solar-type binary":        2,
        "short-period binary":      2,
        "binary system":            1,
        "double star":              1,
    },

    # ── Observational methods ─────────────────────────────────────────────────
    # How the paper acquires data — photometry + spectroscopy are both key.
    "observational_methods": {
        "light curve solution":     3,
        "light curve synthesis":    3,
        "photometric solution":     3,
        "photometric analysis":     2,
        "photometric study":        2,
        "radial velocity":          2,
        "radial velocities":        2,
        "spectroscopic orbit":      3,
        "spectroscopic binary":     2,
        "ccd photometry":           2,
        "light curve":              1,
        "light curves":             1,
        "photometry":               1,
        "spectroscopy":             1,
        "minimum times":            2,
        "times of minimum":         2,
        "o-c diagram":              2,
        "period analysis":          2,
        "eclipse timing":           2,
    },

    # ── Physical parameters ───────────────────────────────────────────────────
    # Terms indicating the paper determines physical or orbital parameters.
    "physical_parameters": {
        "mass ratio":               3,
        "fill-out factor":          3,
        "fillout factor":           3,
        "degree of contact":        3,
        "orbital period":           2,
        "period change":            2,
        "period variation":         2,
        "absolute parameters":      3,
        "physical parameters":      2,
        "stellar parameters":       2,
        "effective temperature":    2,
        "temperature ratio":        2,
        "luminosity ratio":         2,
        "component masses":         3,
        "primary mass":             2,
        "secondary mass":           2,
        "semi-major axis":          2,
        "inclination":              1,
        "more massive component":   2,
        "less massive component":   2,
        "primary component":        1,
        "secondary component":      1,
    },

    # ── Analysis tools & methods ──────────────────────────────────────────────
    # Specific software or modelling frameworks used in this domain.
    "analysis_tools": {
        "wilson-devinney":          3,
        "wilson devinney":          3,
        "w-d code":                 3,
        "wd code":                  3,
        "phoebe":                   3,
        "roche lobe":               2,
        "roche geometry":           2,
        "binary maker":             2,
        "period04":                 2,
        "nightfall":                2,
        "simplex algorithm":        1,
        "differential correction":  2,
    },

    # ── Physical processes ────────────────────────────────────────────────────
    # Physical phenomena discussed in the context of contact binaries.
    "physical_processes": {
        "mass transfer":            2,
        "angular momentum loss":    3,
        "angular momentum":         1,
        "thermal contact":          3,
        "shallow contact":          2,
        "deep contact":             2,
        "common envelope":          2,
        "thermal relaxation":       3,
        "magnetic braking":         2,
        "tidal interaction":        2,
        "merging":                  2,
        "coalescence":              2,
        "energy transfer":          2,
        "o'connell effect":         3,
        "spot":                     1,
        "third body":               2,
        "third light":              2,
    },
}

# Flat lookup: term -> (category, weight)
_FLAT: Dict[str, Tuple[str, int]] = {
    term: (cat, weight)
    for cat, terms in KEYWORD_DICT.items()
    for term, weight in terms.items()
}


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

@dataclass
class RelevanceResult:
    score: float
    verdict: str                          # "relevant" | "borderline" | "not_relevant"
    matched_terms: Dict[str, int]         # term -> count
    category_scores: Dict[str, float]     # category -> partial score
    word_count: int
    details: List[str] = field(default_factory=list)

    THRESHOLD_RELEVANT    = 2.0
    THRESHOLD_BORDERLINE  = 0.5

    def __post_init__(self):
        if self.score >= self.THRESHOLD_RELEVANT:
            self.verdict = "relevant"
        elif self.score >= self.THRESHOLD_BORDERLINE:
            self.verdict = "borderline"
        else:
            self.verdict = "not_relevant"


def score_text(text: str) -> RelevanceResult:
    """
    Score a paper text against the keyword dictionary.

    Args:
        text: Full text or abstract of the paper (plain text, not PDF bytes).

    Returns:
        RelevanceResult with score, verdict, and breakdown.
    """
    lower = text.lower()
    word_count = max(len(re.findall(r'\b\w+\b', lower)), 1)

    matched_terms: Dict[str, int] = {}
    category_raw: Dict[str, float] = {cat: 0.0 for cat in KEYWORD_DICT}

    for term, (category, weight) in _FLAT.items():
        # Count non-overlapping occurrences
        count = len(re.findall(re.escape(term), lower))
        if count > 0:
            matched_terms[term] = count
            # Log-dampen repeated mentions: log2(1 + count)
            contribution = weight * math.log2(1 + count)
            category_raw[category] += contribution

    # Normalise total score by sqrt(word_count / 100) so short abstracts
    # and long full texts are comparable
    raw_total = sum(category_raw.values())
    normalised = raw_total / math.sqrt(word_count / 100)

    details = [
        f"{term} ×{cnt} (w={_FLAT[term][1]}, cat={_FLAT[term][0]})"
        for term, cnt in sorted(matched_terms.items(),
                                key=lambda kv: _FLAT[kv[0]][1] * kv[1],
                                reverse=True)
    ]

    return RelevanceResult(
        score=round(normalised, 4),
        verdict="",           # filled by __post_init__
        matched_terms=matched_terms,
        category_scores={k: round(v, 4) for k, v in category_raw.items()},
        word_count=word_count,
        details=details,
    )


def score_pdf(pdf_path: str, abstract_only: bool = False) -> RelevanceResult:
    """
    Score a PDF file directly.

    Requires the PDF to be text-based (use pdf_utils.detect_pdf_type first).

    Args:
        pdf_path:      Path to the PDF.
        abstract_only: If True, only score the first page (proxy for abstract).

    Returns:
        RelevanceResult
    """
    from src.pdf_utils import extract_text
    max_pages = 1 if abstract_only else None
    text = extract_text(pdf_path, max_pages=max_pages)
    return score_text(text)


def print_result(result: RelevanceResult, label: str = "") -> None:
    """Pretty-print a RelevanceResult."""
    icons = {"relevant": "✅", "borderline": "⚠️ ", "not_relevant": "❌"}
    icon  = icons.get(result.verdict, "?")

    header = f" {label}" if label else ""
    print(f"\n{icon} {result.verdict.upper()}{header}  —  score={result.score:.4f}  words={result.word_count}")
    print(f"   Category breakdown:")
    for cat, sc in result.category_scores.items():
        bar = "█" * int(sc * 2)
        print(f"     {cat:<25} {sc:>6.3f}  {bar}")
    if result.matched_terms:
        print(f"   Top matched terms:")
        for line in result.details[:10]:
            print(f"     {line}")
