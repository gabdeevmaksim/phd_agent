# Progress Summary: Regex + Embeddings Extraction, OCR, and SIMBAD/ADS Object Matching

## 1. Approach — Regex + Embeddings Pipeline

This repo builds an end-to-end pipeline to extract physical parameters for W UMa-type contact binaries from scientific PDFs. The core idea is to combine two complementary signal sources:

1. **Regex-based extraction** (high precision when a value appears in a recognizable format).
2. **Embedding-based semantic retrieval** (robustness when tables/prose are formatted differently or OCR introduces noise).

### 1.1 Regex-based parameter extraction

Implemented in `src/param_extractor.py`:

- `PARAM_SPECS` defines **21 numerical parameters** (e.g., `P`, `q`, `i`, `T1`, `T2`, `M1`, `M2`, `R1`, `R2`, `L1`, `L2`, `a`, `Omega`, `f`, `r1p`, `r2p`, `L3`, `d`, `Age`, plus derived fields like `T2_T1`, and `dPdt`).
- Each parameter has multiple **compiled regex patterns** ordered from more specific to less specific matches.
- Each match is normalized and **range-checked** against physical limits.

Important design details:

- **Heavy normalization** before matching:
  - Unicode subscripts/superscripts (e.g., `T₁` → `T1`)
  - minus sign / OCR minus variants
  - collapsing LaTeX-ish tokens from Nougat output
  - OCR artifact fixes (e.g. `q` misread as `9` or `Q`)
- **Uncertainty parsing**:
  - supports common forms like `± N` or `(N)` styles
- **Tier-2 computed fallbacks** in `fill_missing()`:
  - example: compute `a` from Kepler’s law if possible, compute `T2` from `T2_T1`, etc.

### 1.2 Embedding-based semantic retrieval

Implemented via:

- `src/embeddings.py` (embedding model + centroid)
- `src/paper_index.py` (chunking + indexing)
- `src/classifier.py` (3-stage relevance filter using embeddings)

Key points:

- Embeddings use **`BAAI/bge-small-en-v1.5`** (384-dim).
- A **WUMaCat centroid** is computed from WUMaCat title+abstract pairs (`build_wumacat_centroid`).
- Similarity is measured via cosine similarity to the centroid.
- The relevance classifier is **three stages with early exit**:
  1. keyword relevance (requires system-type hit)
  2. abstract embedding similarity
  3. conclusions embedding similarity (captures papers that discuss results but have abstract noise)

For parameter lookup, `PaperIndex` builds an embedding index over:
- section-aware **text chunks**
- **table rows** (rows become their own retrievable chunks, with table headers prepended for context)

At extraction time, the pipeline performs semantic queries per `(parameter, object)` to retrieve the most likely context chunk(s), and then runs the regex extractor on those contexts.

## 2. Difficulties We Met: Text vs Image PDFs (and Notation Drift)

### 2.1 Text PDFs vs image-based PDFs

`src/pdf_utils.py` detects PDF type using a per-page heuristic:
- pages with “enough characters” are treated as text
- otherwise the page is treated as an image

Then it routes extraction:
- **Text PDFs**: PyMuPDF extraction + regex/embedding pipeline.
- **Image PDFs**: OCR is required before anything else.

OCR approach:

- **EasyOCR** (default): CPU-friendly, page-by-page raster OCR.
- **Nougat**: scientific-PDF-aware OCR (heavier; transformer model; can be much slower).

Practical difficulty:
- OCR errors propagate into both **regex matching** and **embedding similarity**.
- The pipeline therefore uses a redundancy pattern: classifier + index + multiple extraction methods.

### 2.2 Regex fragility due to formatting and scientific notation

Even with normalization, regex-based extraction is brittle because scientific papers are not standardized:

- parameter symbols may appear in many variants (`T₁` vs `T1`, OCR subscripts, etc.)
- same physical quantity may be reported in different algebraic forms:
  - `q` vs `m2/m1` vs `1/q` (and sometimes the convention is flipped)
- tables can be:
  - horizontal multi-object tables
  - vertical single-object blocks
  - prose-embedded “mini tables”
- different journals/publishers produce different PDF layouts, and OCR can reorder/merge tokens.

### 2.3 Data source conflicts: “mentions” vs “about”

We still face a deeper challenge: even object tagging systems can include papers where the object is only discussed as an example or in passing.

That motivates why the pipeline combines:
- lexical evidence (regex/keyword scoring)
- semantic evidence (embeddings)
- a multi-stage classifier that tries to distinguish real “relevance” from superficial mention.

## 3. Mermaid Pipeline Scheme

```mermaid
flowchart TD
  A[PDF] --> B[Detect PDF type]
  B -->|Text| C[PyMuPDF text extraction]
  B -->|Image| D[OCR]
  D --> E[OCR text (EasyOCR/Nougat)]
  C --> F[Classify paper relevance]
  E --> F

  F -->|Not relevant| Z[Stop]
  F -->|Relevant| G[Build PaperIndex]
  G --> H[Extract objects + chunking + embedding]

  H --> I[Parameter extraction]
  I --> J[Table-block parsing]
  I --> K[Inline regex extraction]
  I --> L[Semantic queries via PaperIndex]

  J --> M[Fill missing (Tier-2 computed)]
  K --> M
  L --> M

  M --> N[Write ObjectCatalogue rows]
```

## 4. SIMBAD Name Matching Investigation (and Why “Relevance” Is Still Unclear)

### 4.1 The core question

We wanted to understand whether an object-tagging system (SIMBAD / ADS integrations) can differentiate:
- papers that are genuinely **about** a given object
- vs papers that **mention** the object (as an example, comparison, or application)

### 4.2 What SIMBAD/related systems do

SIMBAD’s indexing strategy (and how ADS object search leverages SIMBAD/NED):

- provides object tagging based on identifier detection and metadata workflows
- does **not** enforce semantic relevance.

From the literature on SIMBAD and related CDS processing:

- SIMBAD records can be created when an object is cited anywhere in a paper’s content.
- **No assessment is made of relevance**:
  - a paper can be entirely devoted to the object, or only give a side mention
  - in both cases it can appear as a reference in SIMBAD

### 4.3 Practical implication for this repo

This explains why our pipeline’s 3-stage classifier is still necessary:

- SIMBAD/ADS help with object identity normalization and can improve recall for discovery.
- But they do not guarantee that the retrieved papers are “about” the object in the way parameter extraction needs.

In other words:
- SIMBAD answers: “Is this object mentioned/tagged in the paper?”
- Our pipeline tries to answer: “Is the paper actually providing extractable parameter evidence for the object?”

### 4.4 Why ADS `object:` search still feels unclear

ADS `object:` queries use SIMBAD/NED object recognition and also search ADS text fields.

However, based on ADS/SIMBAD descriptions, it’s still not a strict relevance filter; it’s primarily a retrieval/filter mechanism.

So whether a paper is:
- fully dedicated to a contact binary system
- or only uses it as a motivating example

remains ambiguous until we inspect the text/conclusions and extract parameters.

