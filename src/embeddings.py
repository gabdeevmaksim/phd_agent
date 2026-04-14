"""
embeddings.py — Stage 2 semantic similarity filter.

Uses fastembed (ONNX-based, no PyTorch required) with BAAI/bge-small-en-v1.5
to embed paper abstracts and compare them against a pre-computed WUMaCat
reference centroid.

Workflow
--------
1. build_wumacat_centroid()  — run once, saves centroid + per-paper embeddings
2. load_centroid()           — loads the saved centroid at runtime
3. similarity_score(text)    — cosine similarity of new text to centroid
4. classify(text)            — returns verdict + score using threshold

Saved artefacts (data/):
    wumacat_centroid.npy     — mean embedding vector  (384,)
    wumacat_embeddings.npy   — per-paper matrix       (N, 384)
    wumacat_embed_meta.json  — bibcodes + threshold info
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import List, Optional

import numpy as np

MODEL_NAME  = "BAAI/bge-small-en-v1.5"
DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
CENTROID_PATH    = os.path.join(DATA_DIR, "wumacat_centroid.npy")
EMBEDDINGS_PATH  = os.path.join(DATA_DIR, "wumacat_embeddings.npy")
META_PATH        = os.path.join(DATA_DIR, "wumacat_embed_meta.json")
ABSTRACTS_PATH   = os.path.join(DATA_DIR, "wumacat_abstracts.json")

# Cosine similarity thresholds (calibrated against WUMaCat self-similarity)
# Tunable — see calibrate_thresholds()
# Calibrated empirically:
#   WUMaCat self-similarity: min=0.74, p10=0.87, median=0.90
#   W UMa / contact binary papers:                         >= 0.84
#   Exoplanet / unrelated astronomy papers:                ~0.83
#   X-ray catalogs, other unrelated:                       ~0.80-0.82
THRESHOLD_RELEVANT   = 0.840   # clearly in the contact/eclipsing binary domain
THRESHOLD_BORDERLINE = 0.836   # narrow band — anything below is not relevant


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_model():
    """Lazy-load the embedding model."""
    from fastembed import TextEmbedding
    return TextEmbedding(model_name=MODEL_NAME)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two 1-D vectors."""
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def _embed_texts(texts: List[str], batch_size: int = 32) -> np.ndarray:
    """Embed a list of texts. Returns (N, dim) float32 array."""
    model = _get_model()
    embeddings = list(model.embed(texts, batch_size=batch_size))
    return np.array(embeddings, dtype=np.float32)


# ---------------------------------------------------------------------------
# Build & save reference centroid
# ---------------------------------------------------------------------------

def build_wumacat_centroid(abstracts_path: str = ABSTRACTS_PATH,
                           force: bool = False) -> np.ndarray:
    """
    Embed all WUMaCat abstracts and save the centroid to disk.

    Args:
        abstracts_path: Path to wumacat_abstracts.json.
        force:          Re-build even if artefacts already exist.

    Returns:
        Centroid vector (384,).
    """
    if not force and os.path.exists(CENTROID_PATH):
        print(f"✅ Centroid already exists at {CENTROID_PATH}")
        print("   Pass force=True to rebuild.")
        return np.load(CENTROID_PATH)

    print(f"📖 Loading WUMaCat abstracts from {abstracts_path} ...")
    with open(abstracts_path) as f:
        data = json.load(f)
    papers = data["papers"]

    # Build (bibcode, text) pairs — combine title + abstract for richer signal
    bibcodes, texts = [], []
    for bib, paper in papers.items():
        title    = paper.get("title", "")
        abstract = paper.get("abstract", "")
        text = f"{title}. {abstract}".strip()
        if text and text != ".":
            bibcodes.append(bib)
            texts.append(text)

    print(f"🔢 Papers to embed: {len(texts)}")
    print(f"📥 Downloading model '{MODEL_NAME}' if needed ...")

    embeddings = _embed_texts(texts)
    print(f"✅ Embeddings shape: {embeddings.shape}")

    centroid = embeddings.mean(axis=0)
    centroid = centroid / np.linalg.norm(centroid)   # L2-normalise

    # Save artefacts
    os.makedirs(DATA_DIR, exist_ok=True)
    np.save(CENTROID_PATH, centroid)
    np.save(EMBEDDINGS_PATH, embeddings)

    meta = {
        "model": MODEL_NAME,
        "n_papers": len(bibcodes),
        "bibcodes": bibcodes,
        "centroid_path": CENTROID_PATH,
        "embeddings_path": EMBEDDINGS_PATH,
        "threshold_relevant": THRESHOLD_RELEVANT,
        "threshold_borderline": THRESHOLD_BORDERLINE,
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"💾 Saved centroid  → {CENTROID_PATH}")
    print(f"💾 Saved embeddings → {EMBEDDINGS_PATH}")
    print(f"💾 Saved metadata  → {META_PATH}")
    return centroid


# ---------------------------------------------------------------------------
# Load centroid
# ---------------------------------------------------------------------------

def load_centroid() -> np.ndarray:
    """
    Load the pre-computed WUMaCat centroid.
    Raises FileNotFoundError if build_wumacat_centroid() hasn't been run yet.
    """
    if not os.path.exists(CENTROID_PATH):
        raise FileNotFoundError(
            f"Centroid not found at {CENTROID_PATH}. "
            "Run build_wumacat_centroid() first."
        )
    return np.load(CENTROID_PATH)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

@dataclass
class EmbeddingResult:
    similarity: float
    verdict: str       # "relevant" | "borderline" | "not_relevant"
    threshold_relevant: float   = 0.840
    threshold_borderline: float = 0.836

    def __post_init__(self):
        if self.similarity >= self.threshold_relevant:
            self.verdict = "relevant"
        elif self.similarity >= self.threshold_borderline:
            self.verdict = "borderline"
        else:
            self.verdict = "not_relevant"


def similarity_score(text: str,
                     centroid: Optional[np.ndarray] = None) -> EmbeddingResult:
    """
    Compute cosine similarity of a paper text to the WUMaCat centroid.

    Args:
        text:     Abstract or full text of the paper.
        centroid: Pre-loaded centroid (loads from disk if None).

    Returns:
        EmbeddingResult with similarity score and verdict.
    """
    if centroid is None:
        centroid = load_centroid()

    embedding = _embed_texts([text])[0]
    sim = _cosine_similarity(embedding, centroid)

    return EmbeddingResult(similarity=round(sim, 6), verdict="")


def similarity_score_pdf(pdf_path: str,
                         centroid: Optional[np.ndarray] = None) -> EmbeddingResult:
    """
    Score a text-based PDF by comparing its abstract to the WUMaCat centroid.

    Uses extract_abstract() to parse the abstract section from the PDF —
    keeping the comparison like-for-like with the centroid, which was built
    from WUMaCat title + abstract text.

    Args:
        pdf_path: Path to the PDF.
        centroid: Pre-loaded centroid (loads from disk if None).
    """
    from src.pdf_utils import extract_abstract
    text = extract_abstract(pdf_path)
    return similarity_score(text, centroid)


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------

def calibrate_thresholds(n_sample: int = 50) -> dict:
    """
    Compute similarity of WUMaCat papers against their own centroid to
    understand the score distribution and suggest thresholds.

    Args:
        n_sample: Number of papers to sample (None = all).

    Returns:
        dict with percentile statistics.
    """
    embeddings = np.load(EMBEDDINGS_PATH)
    centroid   = np.load(CENTROID_PATH)

    if n_sample:
        idx = np.random.choice(len(embeddings), min(n_sample, len(embeddings)),
                               replace=False)
        sample = embeddings[idx]
    else:
        sample = embeddings

    sims = np.array([_cosine_similarity(e, centroid) for e in sample])

    stats = {
        "min":  float(sims.min()),
        "p10":  float(np.percentile(sims, 10)),
        "p25":  float(np.percentile(sims, 25)),
        "p50":  float(np.percentile(sims, 50)),
        "p75":  float(np.percentile(sims, 75)),
        "p90":  float(np.percentile(sims, 90)),
        "max":  float(sims.max()),
        "mean": float(sims.mean()),
        "std":  float(sims.std()),
    }

    print("📊 WUMaCat self-similarity distribution:")
    print(f"   min={stats['min']:.4f}  p10={stats['p10']:.4f}  "
          f"p25={stats['p25']:.4f}  median={stats['p50']:.4f}")
    print(f"   p75={stats['p75']:.4f}  p90={stats['p90']:.4f}  "
          f"max={stats['max']:.4f}  mean={stats['mean']:.4f}  std={stats['std']:.4f}")
    print()
    print(f"   Suggested thresholds:")
    print(f"     relevant   >= {stats['p25']:.3f}  (bottom quartile of WUMaCat)")
    print(f"     borderline >= {stats['p10']:.3f}  (bottom decile of WUMaCat)")

    return stats


# ---------------------------------------------------------------------------
# Pretty print
# ---------------------------------------------------------------------------

def print_result(result: EmbeddingResult, label: str = "") -> None:
    icons = {"relevant": "✅", "borderline": "⚠️ ", "not_relevant": "❌"}
    icon  = icons.get(result.verdict, "?")
    header = f" {label}" if label else ""
    bar_len = int(result.similarity * 40)
    bar = "█" * bar_len + "░" * (40 - bar_len)
    print(f"{icon} {result.verdict.upper()}{header}")
    print(f"   similarity={result.similarity:.4f}  [{bar}]")
    print(f"   thresholds: relevant>={result.threshold_relevant}  "
          f"borderline>={result.threshold_borderline}")
