# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PhD Research Agent - ADS Analysis Toolkit for astronomical literature analysis using NASA's Astrophysics Data System (ADS) API. Focuses on binary star systems research, keyword discovery, and bibliometric analysis with specialized support for W UMa contact binary stars.

## Essential Setup

1. **ADS API Token Required**: Create `.env` file with `ADS_API_TOKEN=your_token_here`
   - Obtain token from: https://ui.adsabs.harvard.edu/user/settings/token
   - Never commit this file or expose the token

2. **Installation** (requires Python 3.12, x86_64 macOS):
   ```bash
   python3.12 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
   > **Note:** PyTorch is capped at 2.2.x on Intel macOS (no CPU wheels for 2.4+ on x86_64).
   > `requirements.txt` pins `numpy<2`, `transformers<4.45`, and `albumentations<1.4` to ensure
   > compatibility with nougat-ocr 0.1.17 and torch 2.2.x.

3. **Test Connection**:
   ```bash
   python -c "from src.ads_parser import test_ads_connection; test_ads_connection()"
   ```

## Common Development Commands

### Testing
```bash
# Run all tests
python -m unittest discover -s tests

# Test ADS connection
python -c "from src.ads_parser import test_ads_connection; test_ads_connection()"
```

### Jupyter Notebooks
```bash
# Start Jupyter environment
jupyter notebook notebooks/

# Key notebooks:
# - exact_keyword_experiment.ipynb: Precision keyword matching experiments
# - similarity_experiments.ipynb: Paper similarity and clustering analysis
# - ads_query_analysis.ipynb: Search strategy optimization
```

### Quick Operations
```bash
# Get paper abstract
python -c "from src.ads_parser import get_abstract; get_abstract('2020AJ....159..189L')"

# Download catalogue abstracts
python -c "from src.ads_parser import download_catalogue_abstracts; download_catalogue_abstracts('data/WUMaCat.csv', 'output.json', batch_size=50)"
```

## Architecture

### Core Modules

**`src/ads_parser.py`** - ADS API interface with comprehensive search capabilities:
- **Connection**: `test_ads_connection()` validates API connectivity
- **Single retrieval**: `get_paper_info()`, `get_abstract()` for individual papers
- **Bulk operations**: `get_bulk_paper_info()`, `download_catalogue_abstracts()` for batch processing (50-100 bibcodes per request)
- **Advanced search**: `search_papers_by_keywords()`, `search_all_bibcodes()` with pagination support (handles 2000+ results automatically)
- **Exact matching**: `search_exact_keywords()` for precision searches using `=field:"keyword"` syntax
- **Similarity search**: `find_similar_papers()`, `find_similar_papers_bulk()` using ADS `similar()` function
- **Analysis**: `compare_search_strategies()`, `test_keyword_combination_sizes()` for search optimization
- **PDF download**: `download_pdfs(bibcodes, output_dir)` — arXiv primary, ADS resolver fallback chain
- **Retry logic**: `_make_ads_request_with_retry()` handles rate limits (429/502/503/504) with exponential backoff and jitter
- **Search flags**: `astronomy_only=True` (default) appends `AND database:astronomy`; `open_access_only=True` restricts to `esources:EPRINT_PDF`

**`src/pdf_utils.py`** - PDF format detection, text extraction, and OCR:
- `detect_pdf_type(pdf_path)` → PDFInfo with `pdf_type` ("text" | "image"), page stats, recommended tool
- `detect_all_pdfs(pdf_dir)` → batch detection with summary table
- `extract_text(pdf_path, max_pages)` → full text via PyMuPDF
- `extract_abstract(pdf_path)` → abstract section only (pages 1-2, stops at Introduction/Keywords)
- `extract_conclusions(pdf_path)` → conclusions section (last match, stops at References/Acknowledgements)
- `ocr_pdf(pdf_path, dpi=200, engine="easyocr")` → OCR text for image-based PDFs
  - `engine="easyocr"`: page-by-page raster OCR, CPU-only, works offline
  - `engine="nougat"`: scientific-PDF-aware transformer OCR (downloads ~1 GB model on first use)

**`src/relevance_keywords.py`** - Stage 1 keyword scoring:
- `KEYWORD_DICT` with 5 categories (system_type, parameters, methods, observations, physics), 80+ terms, weights 1-3
- Score formula: `sum(weight × log2(1 + count)) / sqrt(word_count / 100)`
- `score_text(text)` → RelevanceResult(score, verdict, matched_terms, category_scores)
- `score_pdf(pdf_path, abstract_only=False)` → RelevanceResult

**`src/embeddings.py`** - Stage 2/3 semantic similarity (fastembed, ONNX, no PyTorch):
- Model: `BAAI/bge-small-en-v1.5` (384-dim); thresholds: relevant ≥ 0.840, borderline ≥ 0.836
- `build_wumacat_centroid(force=False)` → embeds WUMaCat title+abstract, saves L2-normalised centroid
- `load_centroid()` → loads `data/wumacat_centroid.npy`
- `similarity_score(text, centroid)` → EmbeddingResult(similarity, verdict)
- `similarity_score_pdf(pdf_path, centroid)` → uses `extract_abstract()` for like-for-like comparison
- `calibrate_thresholds(n_sample)` → distribution stats for threshold tuning
- Saved artefacts: `data/wumacat_centroid.npy`, `data/wumacat_embeddings.npy`, `data/wumacat_embed_meta.json`

**`src/classifier.py`** - 3-stage paper relevance pipeline:
- Stage 1: keyword score ≥ 2.0 AND system_type hit → RELEVANT; score < 0.5 → NOT_RELEVANT
- Stage 2: abstract cosine similarity ≥ 0.840 → RELEVANT; < 0.836 → NOT_RELEVANT
- Stage 3: conclusions cosine similarity ≥ 0.820 → RELEVANT; no conclusions → NOT_RELEVANT
- `classify_paper(pdf_path, centroid)` → ClassificationResult(verdict, exit_stage, stages)
- `classify_all(pdf_dir, centroid, verbose)` → List[ClassificationResult] with summary

**`src/paper_index.py`** - In-memory semantic index for parameter extraction:
- Step 1 `extract_objects(pdf_path)` → ObjectList ranked by mention frequency (regex + table-column parsing)
- Step 2 `chunk_paper(pdf_path, objects)` → List[Chunk] split by section headings + pdfplumber table rows
- Step 3 `PaperIndex(pdf_path)` → embeds all chunks, ready for semantic queries
- `PaperIndex.query(question, object_name, top_k, chunk_types)` → List[SearchHit] by cosine similarity
- Object-scoped queries: prepend object name to query and filter chunks containing that object
- Supported object formats: variable stars (EP Cep), V-number (V369 Cep), NGC members, KIC, TIC, OGLE, 2MASS

**`src/param_extractor.py`** - Regex and table-based parameter extraction:
- `PARAM_SPECS`: 21 WUMaCat numerical parameters (P, dPdt, q, i, T1, T2, T2_T1, M1, M2, R1, R2, L1, L2, a, Ω, f, r1p, r2p, L3, d, Age)
  - `T2_T1`: temperature ratio T2/T1; extracted directly when only the ratio is reported; Tier-2: computed from T1+T2; also drives T2 = T1 × T2/T1 when T2 is missing
- `extract_table_block(text, objects)` → per-object dict of ParamMatch from multi-column parameter tables
- `extract_params(text)` → dict of List[ParamMatch] from inline regex over full text
- `extract_categorical(text)` → CategoricalMatch for Type (W/A), ET, Solver (WD/PHOEBE/BM3/NF/WUMA), Spots
- `fill_missing(params, cats)` → Tier-2 computed fallbacks (Kepler, Stefan-Boltzmann, r×a), Tier-3 NaN markers
- Source provenance: `SOURCE_TABLE`, `SOURCE_REGEX`, `SOURCE_COMPUTED`, `SOURCE_MISSING` on every match

**`src/pipeline.py`** - End-to-end single-paper extraction pipeline:
- `process_paper(pdf_path, centroid, skip_classification, verbose)` → PaperResult
- `process_directory(pdf_dir, output_csv, skip_classification, verbose)` → pd.DataFrame
- Flow: classify → PaperIndex → table_block + inline_regex + semantic_queries → fill_missing → stamp bibcode
- Auto-detects image PDFs and routes to EasyOCR before extraction

**`src/catalogue.py`** - Multi-paper object catalogue (flat schema):
- `ObjectCatalogue.add_paper(pdf_path)` / `add_paper_result(result)` → ingest PaperResult
- `ObjectCatalogue.to_dataframe()` → flat DataFrame; primary key is **(Name, bibcode)** — one row per object per paper, no merging
- Conflict detection via `print_conflicts()`: pairwise >3σ or >10% fractional difference across papers for the same object

**`src/wordcloud_utils.py`** - Text mining and visualization utilities:
- **Text processing**: `clean_text()`, `extract_abstracts()`, `extract_titles()` with configurable stopwords
- **Analysis**: `get_word_frequencies()`, `print_top_words()` for frequency analysis
- **Visualization**: `create_wordcloud()` generates high-quality word clouds (1200x600, 300dpi)
- **Export**: `save_word_frequencies()` exports top 100 words to JSON with metadata
- **Keyword extraction**: `get_top_keywords_from_wordclouds()` extracts research keywords excluding generic terms
- **Data loading**: `load_wumacat_bibcodes()` reads reference bibcode sets from CSV

### API Rate Limits & Best Practices

- **Search endpoint**: 5,000 requests/day (primary endpoint used by all search functions)
- **Export endpoint**: 100 requests/day (citation formatting)
- **Batch processing**: Use 50-100 bibcodes per request to minimize API calls
- **Pagination**: Automatically handled for result sets >2000 (ADS single request limit)
- **Rate monitoring**: Check `X-RateLimit-Remaining` header in responses
- **Retry strategy**: Exponential backoff with jitter for transient errors (429/502/503/504)
- **Field selection**: Use `fl` parameter to request only needed fields, reducing payload size

### Data Management

**Key datasets in `data/`**:
- `WUMaCat.csv`: Reference catalogue (688 systems, 424 unique bibcodes)
- `wumacat_abstracts.json`: Complete abstracts and metadata for WUMaCat
- `wumacat_centroid.npy`: Pre-computed WUMaCat embedding centroid (384-dim, gitignored)
- `wumacat_embeddings.npy`: Per-paper embedding matrix (N×384, gitignored)
- `wumacat_embed_meta.json`: Centroid metadata and threshold info
- `*_experiment_results.json`: Systematic keyword and search strategy experiments
- `*_summary.csv`: Statistical summaries of experiment results
- `search_download_table.csv`: bibcode | pdf_file | fallback_url (keyword search output)
- `similarity_download_table.csv`: bibcode | title | year | seeds_matched | avg_score | pdf_file | fallback_url

**Generated outputs**:
- `data/pdfs/`: Downloaded PDF files (gitignored — large binaries)
- `wordclouds/`: Word cloud PNG images (300dpi) and frequency JSON files
- Intermediate saves: Every 5 batches during bulk downloads to prevent data loss

### Scripts

**`scripts/experiment_and_crossmatch.py`** — Keyword search + WUMaCat cross-match experiment
**`scripts/search_and_download.py`** — Keyword search → download PDFs → produce `search_download_table.csv`
**`scripts/similarity_search_and_download.py`** — ADS similarity search from seed papers → top-10 → download

### Classification & Extraction Pipeline

```
PDF
 └─ detect_pdf_type()          text / image
      └─ classify_paper()       3-stage relevance filter
           └─ PaperIndex()      object extraction + chunking + embedding
                └─ .query()     semantic retrieval → top-k chunks
                     └─ param_extractor (TODO)  regex extraction of values
```

Run order for a new paper:
```bash
python -c "
from src.classifier import classify_paper
from src.paper_index import PaperIndex
r = classify_paper('data/pdfs/paper.pdf')
r.print()
if r.verdict == 'relevant':
    idx = PaperIndex('data/pdfs/paper.pdf')
    hits = idx.query('mass ratio q', top_k=5)
"
```

### Notebook Organization

Notebooks follow a standard structure:
1. **Setup cell**: Load `.env`, define paths, set constants (batch sizes, fields)
2. **Data Loading**: Import datasets with validation
3. **Processing**: Apply transformations with progress tracking
4. **Analysis**: Calculate metrics, generate visualizations
5. **Results**: Export findings to `data/` with timestamps

## Key Patterns & Conventions

### ADS API Usage
- Always use `get_ads_headers()` helper for authorization
- Implement retry logic for all network calls using `_make_ads_request_with_retry()`
- Batch queries to reduce API calls (50-100 bibcodes per request recommended)
- Request only necessary fields via `fl` parameter
- Log rate-limit headers and add delays between batches (1-2s)
- Persist intermediate results to disk for resume capability
- Handle missing/invalid bibcodes gracefully, never fail entire batch

### Code Style
- Follow PEP 8 style guidelines
- Add type hints to all new functions: `def func(arg: str) -> Optional[Dict]:`
- Use descriptive variable names reflecting data content
- Prefer vectorized pandas/numpy operations over explicit loops
- Keep network I/O isolated from parsing/transformation logic

### Error Handling
- Validate inputs early (paths exist, token set, bibcodes non-empty)
- Use try-except for network operations with informative error messages
- Print status messages with emoji prefixes for clarity (🔍 🚀 ✅ ❌ ⚠️ 📊)
- Return `None` or empty structures on failure, never raise unhandled exceptions in user-facing functions

### Text Processing
- Use `clean_text()` with domain-aware stopwords from `get_default_stopwords()`
- Remove HTML tags, LaTeX markup, and punctuation
- Filter words by minimum length (default: 3 characters)
- Always save both visualization (PNG) and underlying data (JSON) for reproducibility

### Visualization
- Use matplotlib for fine control, seaborn for statistical defaults
- Save figures with `dpi=300`, `bbox_inches='tight'` for publication quality
- Always include axis labels, titles, and legends
- Prefer colorblind-friendly palettes
- Export to `wordclouds/` or analysis-specific subdirectories

## Important Research Context

### Search Strategy Findings
From experimental data (`keyword_experiment_summary.csv`):
- 2-keyword searches: 67,312 papers, 87.7% WUMaCat coverage (high recall, lower precision)
- 3-keyword searches: 12,660 papers, 87.5% coverage (optimal balance)
- 4-keyword searches: 9,425 papers, 86.3% coverage (highest precision)
- **Recommendation**: 3-4 keyword combinations provide best precision/recall balance

### Exact vs. Fuzzy Matching
- Use `=field:"keyword"` syntax for exact matches (e.g., `=abs:"contact binary"`)
- Supported fields: `title`, `abs` (abstract), `full` (full text), `author`
- Full-text search captures papers where keywords appear in body but not abstract
- Abstract search provides higher precision for targeted literature reviews

### Similarity Search
- ADS `similar()` function uses abstract-based similarity scoring
- Scores are normalized; filter with `min_score` parameter for relevant results
- Sort by `score desc` to get most similar papers first
- Automatically filters out the reference paper from results
- Use `find_similar_papers_bulk()` with delay_between_requests=1.0 for multiple papers

## Testing Strategy

- Place tests in `tests/` directory
- Use `python -m unittest discover -s tests` (pytest not installed by default)
- Test functions include: `test_ads_parser.py`, `test_main.py`
- For new functions, add minimal unit tests with example usage in docstrings
- Test with small bibcode lists before running full batch operations

## File Path Convention

- Always use absolute paths for macOS (darwin) commands when possible
- Input data: `data/` directory (CSV/JSON)
- Output visualizations: `wordclouds/` directory
- Analysis outputs: `data/` with descriptive names and timestamps
- Never commit `.env` file or large raw API response files