"""
paper_index.py — In-memory semantic index for a single astronomy paper.

Pipeline
--------
  Step 1  extract_objects()   — ranked list of astronomical objects (by regex + table)
  Step 2  chunk_paper()       — split full text into section/paragraph chunks
  Step 3  index_paper()       — embed all chunks with fastembed, build query interface

Usage
-----
    from src.paper_index import PaperIndex
    idx = PaperIndex("data/pdfs/2016AJ....152..129C.pdf")
    hits = idx.query("mass ratio q", top_k=5)
    for h in hits:
        print(h)

    # Object-scoped query
    hits = idx.query("period", object_name="V369 Cep", top_k=3)

Supported naming conventions
-----------------------------
  EP Cep, EQ Cep           — variable star prefix (1-2 letters + constellation)
  V369 Cep, V782 Cep       — V-number + constellation
  NGC 188 W1               — cluster member designation
  KIC 12345678             — Kepler Input Catalogue
  TIC 123456789            — TESS Input Catalogue
  OGLE-BLG-ECL-123456      — OGLE survey
  2MASS J123456+123456     — 2MASS
  GSC 1234-5678            — Guide Star Catalogue
  ASAS J123456+1234.5      — ASAS survey
  J1234+5678               — generic J-coordinate name
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional, Dict
import fitz
import numpy as np


# ── IAU constellation abbreviations (for variable star matching) ─────────────
_CONSTELLATIONS = (
    "And|Ant|Aps|Aql|Aqr|Ara|Ari|Aur|Boo|Cae|Cam|Cap|Car|Cas|Cen|Cep|Cet|"
    "Cha|Cir|CMa|CMi|Cnc|Col|Com|CrA|CrB|Crt|Cru|Crv|CVn|Cyg|Del|Dor|Dra|"
    "Equ|Eri|For|Gem|Gru|Her|Hor|Hya|Hyi|Ind|Lac|Leo|Lep|Lib|LMi|Lup|Lyn|"
    "Lyr|Men|Mic|Mon|Mus|Nor|Oct|Oph|Ori|Pav|Peg|Per|Phe|Pic|PsA|Psc|Pup|"
    "Pyx|Ret|Sgr|Sco|Sct|Ser|Sex|Sge|Tau|Tel|TrA|Tri|Tuc|UMa|UMi|Vel|Vir|"
    "Vol|Vul"
)

# ── Regex patterns (order matters — more specific first) ─────────────────────
_PATTERNS: List[tuple] = [
    # Survey / catalogue designations
    ("kepler",  re.compile(r'\bKIC\s+\d{7,10}\b')),
    ("tess",    re.compile(r'\bTIC\s+\d{6,12}\b')),
    ("ogle",    re.compile(r'\bOGLE[-\s][A-Z]{2,4}[-\s][A-Z]{2,4}[-\s]\d{4,8}\b')),
    ("2mass",   re.compile(r'\b2MASS\s+J\d{6,8}[+-]\d{4,7}\b')),
    ("gsc",     re.compile(r'\bGSC\s+\d{4}[-–]\d{3,5}\b')),
    ("asas",    re.compile(r'\bASAS\s+J\d{6}[+-]\d{4}\.\d\b')),
    ("j_coord", re.compile(r'\bJ\d{4,6}[+-]\d{4,6}\b')),

    # Variable stars: 1-2 letter prefix + constellation  (EQ Cep, RW Com, ...)
    ("var_star", re.compile(
        rf'\b(?:A[BCDFGHLMNOPQRSTVWXYZ]|[BEG-RT-WY][A-Z]|[A-Z]{{1}})\s+'
        rf'(?:{_CONSTELLATIONS})\b'
    )),

    # V-number stars: V369 Cep, V1234 Aql, ...
    ("v_number", re.compile(
        rf'\bV\d{{1,4}}\s+(?:{_CONSTELLATIONS})\b'
    )),

    # NGC/IC cluster members: NGC 188 W1, NGC 6791 V8, ...
    ("cluster_member", re.compile(
        r'\bNGC\s*\d+\s+(?:[A-Z]\d+|\d+)\b'
        r'|\bIC\s*\d+\s+(?:[A-Z]\d+|\d+)\b'
    )),

    # Berkeley / Melotte / Trumpler open clusters
    ("open_cluster_member", re.compile(
        r'\b(?:Ber(?:keley)?|Be|Mel|Tr|Cr)\s*\d+\s+(?:V|W|B)?\d+\b',
        re.IGNORECASE
    )),
]

# Short labels we want to ignore (digits only, single letters, common words)
_IGNORE = {
    '', 'a', 'b', 'i', 'v', 'the', 'fig', 'table', 'eq', 'and', 'or',
    'et', 'al', 'vs', 'see', 'for', 'from',
}

# ── Section heading detector ──────────────────────────────────────────────────
# Words that start sentences (not headings)
_SENTENCE_STARTERS = re.compile(
    r'^(?:It|The|We|Our|This|In\s|For\s|As\s|At\s|By\s|From\s|A\s|An\s|'
    r'There|These|Those|If|Although|However|Thus|Since|Because|Note|Here|'
    r'Using|Given|Due)\b',
    re.IGNORECASE,
)

# Characters that indicate math/formula lines
_MATH_CHARS = re.compile(r'[=+\-±×÷∑∫√∝∼≈≤≥<>{}^_]|log|ln\s|\btan\b|\bsin\b|\bcos\b')

# Known section keywords common in astronomy papers
_SECTION_KEYWORDS = re.compile(
    r'^(?:'
    r'Abstract|Introduction|Observations?|Data\b|Reduction|Results?|'
    r'Discussion|Conclusions?|Summary|References?|Acknowledgem|'
    r'Membership|Parameters?|Distance|Stellar|Light.curve|Color|'
    r'Spectroscop|Analysis|Methods?|Photom|Radial.veloc|Period|'
    r'Physical|Orbital|Properties|Characteris|Sample|Catalog|'
    r'Selection|Fitting|Modell?ing|Comparison|Notes?'
    r')',
    re.IGNORECASE,
)


def _is_heading(line: str) -> bool:
    """Heuristic: returns True if the line looks like a section heading."""
    s = line.strip()
    if not s or len(s) < 3 or len(s) > 75:
        return False
    # Must start with capital letter or a 1-2 digit section number
    if not (s[0].isupper() or re.match(r'^\d{1,2}[.\s]', s)):
        return False
    # Reject sentence-like starters
    if _SENTENCE_STARTERS.match(s):
        return False
    # Reject math/formula lines
    if _MATH_CHARS.search(s):
        return False
    # Reject lines with comma or semicolon (run-on text)
    if ',' in s or ';' in s:
        return False
    # Reject lines ending with period or colon
    if s.endswith('.') or s.endswith(':'):
        return False
    # Numbered heading: "1. Introduction", "2.1 Methods"
    # Require exactly 1-2 digits (not a year like 2016)
    if re.match(r'^\d{1,2}[.)]\s*\d{0,2}\.?\s+[A-Z][a-z]', s):
        return True
    # Roman numeral heading: "II. Observations"
    if re.match(r'^[IVX]{1,4}\.?\s+[A-Z]', s):
        return True
    # ALL CAPS: "ABSTRACT", "REFERENCES"
    if re.match(r'^[A-Z][A-Z\s]{3,}$', s):
        return True
    # Known section keywords — only if the whole line is short or title-case
    if _SECTION_KEYWORDS.match(s):
        words = s.split()
        # Accept if line is short (single word or ≤ 4 words) OR fully title-case
        if len(words) <= 4 or all(
            w[0].isupper() or w.lower() in {'and', 'or', 'of', 'the', 'in', 'for', 'a', 'an', 'with'}
            for w in words if w
        ):
            return True
    # Title-case multi-word phrase (2-7 words, every word starts with capital)
    words = s.split()
    if 2 <= len(words) <= 7:
        if all(
            w[0].isupper() or w.lower() in {'and', 'or', 'of', 'the', 'in', 'for', 'a'}
            for w in words if w
        ):
            return True
    return False

# Minimum chunk size to keep (chars)
_MIN_CHUNK_CHARS = 80

# Maximum chars per chunk before it is split further into paragraphs
_MAX_CHUNK_CHARS = 1500


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ObjectList:
    objects: List[str]                          # ranked by frequency
    frequencies: dict                           # name -> count
    source_counts: dict = field(default_factory=dict)  # name -> {"table", "text"}
    raw_matches: dict  = field(default_factory=dict)   # name -> pattern type

    def __len__(self) -> int:
        return len(self.objects)

    def print(self) -> None:
        print(f"  {'OBJECT':<25} {'MENTIONS':>8}  SOURCES")
        print("  " + "-" * 55)
        for obj in self.objects:
            freq = self.frequencies[obj]
            src  = "+".join(sorted(self.source_counts.get(obj, set())))
            print(f"  {obj:<25} {freq:>8}  {src}")


@dataclass
class Chunk:
    """A text chunk from the paper, ready for embedding."""
    chunk_id:    int
    chunk_type:  str          # "section" | "table_row" | "table_caption"
    section:     str          # section heading this chunk belongs to
    text:        str          # actual text content
    object_tags: List[str] = field(default_factory=list)  # objects mentioned
    page:        int = 0


@dataclass
class SearchHit:
    chunk: Chunk
    score: float              # cosine similarity to query

    def __repr__(self) -> str:
        tags = ", ".join(self.chunk.object_tags) if self.chunk.object_tags else "—"
        preview = self.chunk.text[:200].replace("\n", " ")
        return (
            f"[{self.score:.4f}] {self.chunk.chunk_type} | "
            f"§{self.chunk.section[:40]} | objects={tags}\n"
            f"  {preview}…"
        )


# ── Table-row parser ──────────────────────────────────────────────────────────

def _extract_table_ids(text: str) -> List[str]:
    """
    Find table-like blocks and extract first-column values.

    Looks for lines that start with a known object name pattern, preceded by
    a header line containing 'ID' or 'Name' or 'Star'. Returns deduplicated
    first-column entries.
    """
    ids: List[str] = []

    lines = text.split('\n')
    in_table = False

    for line in lines:
        stripped = line.strip()

        # Detect table header
        if re.search(r'\b(?:ID|Name|Star|Object|System)\b', stripped,
                     re.IGNORECASE) and len(stripped) < 120:
            in_table = True
            continue

        if in_table:
            if not stripped:
                in_table = False
                continue

            m = re.match(
                rf'^\s*((?:[A-Z]{{1,2}}\s+(?:{_CONSTELLATIONS})'
                rf'|V\d{{1,4}}\s+(?:{_CONSTELLATIONS})'
                rf'|KIC\s+\d{{7,10}}'
                rf'|TIC\s+\d{{6,12}}'
                rf'|[A-Z0-9]{{3,20}}))\s',
                stripped
            )
            if m:
                name = m.group(1).strip()
                if name.lower() not in _IGNORE:
                    ids.append(name)

    return ids


# ── Full-text regex scanner ───────────────────────────────────────────────────

def _scan_text(text: str) -> dict:
    """
    Scan full text with all regex patterns.
    Returns {matched_name: pattern_type}.
    """
    found: dict = {}
    for ptype, pattern in _PATTERNS:
        for m in pattern.finditer(text):
            name = re.sub(r'\s+', ' ', m.group(0).strip())
            if name.lower() not in _IGNORE:
                found[name] = ptype
    return found


# ── Step 1: Object extraction ─────────────────────────────────────────────────

def extract_objects(pdf_path: str, verbose: bool = True,
                    text: Optional[str] = None) -> ObjectList:
    """
    Extract and rank astronomical objects mentioned in a PDF.

    Args:
        pdf_path: Path to the PDF file.
        verbose:  Print the ranked object list.
        text:     Pre-extracted full text (e.g. from OCR). If None, PyMuPDF
                  is used to extract text directly from the PDF.

    Returns:
        ObjectList with objects ranked by mention frequency.
    """
    if text is None:
        doc = fitz.open(pdf_path)
        full_text = "\n".join(page.get_text() for page in doc)
        doc.close()
    else:
        full_text = text

    # -- Source 1: table ID columns --
    table_ids = _extract_table_ids(full_text)
    table_set = set(table_ids)

    # -- Source 2: full-text regex scan --
    text_matches = _scan_text(full_text)

    # -- Merge and count frequencies --
    freq: Counter = Counter()
    source_counts: dict = {}
    raw_matches:   dict = {}

    all_names = table_set | set(text_matches.keys())

    for name in all_names:
        if not re.search(r'[A-Za-z]', name):
            continue
        if len(name) < 3 or name.lower() in {'w uma', 'w-uma', 'eb', 'sb', 'lc'}:
            continue
        count = len(re.findall(re.escape(name), full_text))
        if count == 0:
            continue
        freq[name] = count
        source_counts[name] = set()
        if name in table_set:
            source_counts[name].add("table")
        if name in text_matches:
            source_counts[name].add("text")
            raw_matches[name] = text_matches[name]

    # -- Deduplicate: remove names that are substrings of a longer name --
    all_found = set(freq.keys())
    to_remove = set()
    for name in all_found:
        for other in all_found:
            if name != other and name in other:
                to_remove.add(name)
                break

    for name in to_remove:
        del freq[name]
        source_counts.pop(name, None)
        raw_matches.pop(name, None)

    ranked = [name for name, _ in freq.most_common()]

    result = ObjectList(
        objects=ranked,
        frequencies=dict(freq),
        source_counts={k: v for k, v in source_counts.items()},
        raw_matches=raw_matches,
    )

    if verbose:
        print(f"\n🔭 Objects found in: {pdf_path}")
        print(f"   Total unique objects: {len(ranked)}")
        result.print()

    return result


# ── Step 2: Text chunking ─────────────────────────────────────────────────────

def _tag_objects(text: str, objects: List[str]) -> List[str]:
    """Return list of object names that appear in the given text."""
    return [obj for obj in objects if re.search(re.escape(obj), text)]


def _split_into_paragraphs(text: str, max_chars: int = _MAX_CHUNK_CHARS) -> List[str]:
    """Split a long text block into paragraph-sized pieces."""
    # Split on double newlines first
    raw_paras = re.split(r'\n{2,}', text)
    result = []
    for para in raw_paras:
        para = para.strip()
        if not para:
            continue
        if len(para) <= max_chars:
            result.append(para)
        else:
            # Hard split at sentence boundaries
            sentences = re.split(r'(?<=[.!?])\s+', para)
            current = ""
            for sent in sentences:
                if len(current) + len(sent) + 1 <= max_chars:
                    current = (current + " " + sent).strip()
                else:
                    if current:
                        result.append(current)
                    current = sent
            if current:
                result.append(current)
    return result


def chunk_paper(pdf_path: str, objects: Optional[List[str]] = None,
                text: Optional[str] = None) -> List[Chunk]:
    """
    Split a PDF into text chunks and table rows.

    Args:
        pdf_path: Path to the PDF file.
        objects:  List of known object names (from extract_objects) for tagging.
                  If None, no object tagging is performed.
        text:     Pre-extracted full text (e.g. from OCR). If None, PyMuPDF
                  is used. When text is provided, it is split as a single block
                  (no per-page attribution).

    Returns:
        List of Chunk objects ordered as they appear in the document.
    """
    chunks: List[Chunk] = []
    chunk_id = 0
    objects = objects or []

    current_section = "preamble"
    current_block: List[str] = []

    def flush_block(section: str, block_lines: List[str], page: int) -> None:
        nonlocal chunk_id
        block_text = "\n".join(block_lines).strip()
        if len(block_text) < _MIN_CHUNK_CHARS:
            return
        for para in _split_into_paragraphs(block_text):
            if len(para) < _MIN_CHUNK_CHARS:
                continue
            tags = _tag_objects(para, objects)
            chunks.append(Chunk(
                chunk_id=chunk_id,
                chunk_type="section",
                section=section,
                text=para,
                object_tags=tags,
                page=page,
            ))
            chunk_id += 1

    if text is not None:
        # Pre-supplied text (e.g. from OCR) — process as one block
        for line in text.split('\n'):
            stripped = line.strip()
            if not stripped:
                current_block.append("")
                continue
            if _is_heading(stripped):
                flush_block(current_section, current_block, 0)
                current_section = stripped.strip()
                current_block = []
                continue
            current_block.append(stripped)
        flush_block(current_section, current_block, 0)
    else:
        doc = fitz.open(pdf_path)
        for page_num, page in enumerate(doc, start=1):
            page_text = page.get_text()
            for line in page_text.split('\n'):
                stripped = line.strip()
                if not stripped:
                    current_block.append("")
                    continue
                if _is_heading(stripped):
                    flush_block(current_section, current_block, page_num)
                    current_section = stripped.strip()
                    current_block = []
                    continue
                current_block.append(stripped)
            # Flush at each page boundary to keep page attribution accurate
            if current_block:
                flush_block(current_section, current_block, page_num)
                current_block = []
        if current_block:
            flush_block(current_section, current_block, len(doc))
        doc.close()

    # ── Table rows via pdfplumber (only for native-text PDFs) ─────────────────
    if text is None:
        try:
            import pdfplumber
            chunks.extend(_extract_table_chunks(pdf_path, objects, start_id=chunk_id))
        except ImportError:
            pass  # pdfplumber optional — table chunks skipped

    return chunks


def _extract_table_chunks(pdf_path: str, objects: List[str],
                           start_id: int = 0) -> List[Chunk]:
    """
    Extract table rows as individual chunks via pdfplumber.

    Each row is stored as a pipe-delimited string:
        "col1 | col2 | col3 | ..."

    Object tagging uses the first column as the primary object reference:
    if the first column matches a known object name, that object is tagged.
    """
    import pdfplumber

    chunks: List[Chunk] = []
    chunk_id = start_id

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            if not tables:
                continue

            for table in tables:
                if not table or len(table) < 2:
                    continue

                # First row = header
                header = [str(c).strip() if c else "" for c in table[0]]
                header_text = " | ".join(header)

                # Emit a caption/header chunk
                if len(header_text) >= _MIN_CHUNK_CHARS:
                    chunks.append(Chunk(
                        chunk_id=chunk_id,
                        chunk_type="table_caption",
                        section="table",
                        text=f"Table columns: {header_text}",
                        object_tags=[],
                        page=page_num,
                    ))
                    chunk_id += 1

                # Emit one chunk per data row
                for row in table[1:]:
                    cells = [str(c).strip() if c else "" for c in row]
                    row_text = " | ".join(cells)
                    if len(row_text) < _MIN_CHUNK_CHARS:
                        continue

                    # Tag by first column match
                    first_cell = cells[0] if cells else ""
                    row_objects = []
                    for obj in objects:
                        if obj in first_cell or obj in row_text:
                            row_objects.append(obj)

                    chunks.append(Chunk(
                        chunk_id=chunk_id,
                        chunk_type="table_row",
                        section="table",
                        text=f"[{header_text}]\n{row_text}",
                        object_tags=row_objects,
                        page=page_num,
                    ))
                    chunk_id += 1

    return chunks


# ── Step 3: Embedding + query index ──────────────────────────────────────────

class PaperIndex:
    """
    Full in-memory semantic index for a single astronomy paper.

    Build once, query many times:
        idx = PaperIndex("path/to/paper.pdf")
        hits = idx.query("orbital period", top_k=5)
        hits = idx.query("mass ratio", object_name="V369 Cep", top_k=3)
    """

    def __init__(self, pdf_path: str, verbose: bool = True,
                 text: Optional[str] = None):
        """
        Args:
            pdf_path: Path to the PDF file.
            verbose:  Print indexing progress.
            text:     Pre-extracted full text (e.g. from OCR for image PDFs).
                      If None, text is extracted directly from the PDF via PyMuPDF.
        """
        self.pdf_path = pdf_path
        self._model = None   # lazy-loaded

        if verbose:
            print(f"\n📄 Indexing: {pdf_path}")

        # Step 1 — object list
        self.object_list: ObjectList = extract_objects(
            pdf_path, verbose=verbose, text=text
        )

        # Step 2 — chunks
        if verbose:
            print(f"\n✂️  Chunking paper …")
        self.chunks: List[Chunk] = chunk_paper(
            pdf_path, objects=self.object_list.objects, text=text
        )
        if verbose:
            n_sec = sum(1 for c in self.chunks if c.chunk_type == "section")
            n_row = sum(1 for c in self.chunks if c.chunk_type == "table_row")
            n_cap = sum(1 for c in self.chunks if c.chunk_type == "table_caption")
            print(f"   Chunks: {len(self.chunks)} total "
                  f"({n_sec} section, {n_row} table rows, {n_cap} captions)")

        # Step 3 — embeddings
        if verbose:
            print(f"\n🔢 Embedding {len(self.chunks)} chunks …")
        self._embeddings: np.ndarray = self._embed_chunks()
        if verbose:
            print(f"   Embeddings shape: {self._embeddings.shape}")
            print(f"✅ Index ready.")

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _get_model(self):
        if self._model is None:
            from fastembed import TextEmbedding
            self._model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
        return self._model

    def _embed_texts(self, texts: List[str]) -> np.ndarray:
        model = self._get_model()
        return np.array(list(model.embed(texts, batch_size=32)), dtype=np.float32)

    def _embed_chunks(self) -> np.ndarray:
        texts = [c.text for c in self.chunks]
        if not texts:
            return np.empty((0, 384), dtype=np.float32)
        return self._embed_texts(texts)

    @staticmethod
    def _cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
        denom = np.linalg.norm(a) * np.linalg.norm(b)
        return float(np.dot(a, b) / denom) if denom > 0 else 0.0

    # ── Public API ───────────────────────────────────────────────────────────

    def query(
        self,
        question: str,
        object_name: Optional[str] = None,
        top_k: int = 5,
        chunk_types: Optional[List[str]] = None,
    ) -> List[SearchHit]:
        """
        Semantic search over the paper's chunks.

        Args:
            question:    Natural-language query, e.g. "mass ratio q".
            object_name: If given, restrict to chunks that mention this object.
            top_k:       Number of top results to return.
            chunk_types: If given, filter to these chunk types
                         (e.g. ["table_row"] to search only tables).

        Returns:
            List of SearchHit sorted by descending cosine similarity.
        """
        if not self.chunks:
            return []

        # Build query string (prepend object name for scoped queries)
        query_text = f"{object_name}: {question}" if object_name else question
        q_vec = self._embed_texts([query_text])[0]

        # Filter candidate chunks
        candidates = list(range(len(self.chunks)))
        if object_name:
            candidates = [
                i for i in candidates
                if object_name in self.chunks[i].object_tags
                or object_name in self.chunks[i].text
            ]
        if chunk_types:
            candidates = [
                i for i in candidates
                if self.chunks[i].chunk_type in chunk_types
            ]

        if not candidates:
            return []

        # Score and rank
        scored = [
            (i, self._cosine_sim(q_vec, self._embeddings[i]))
            for i in candidates
        ]
        scored.sort(key=lambda x: x[1], reverse=True)

        return [
            SearchHit(chunk=self.chunks[i], score=s)
            for i, s in scored[:top_k]
        ]

    def objects(self) -> List[str]:
        """Return the ranked list of objects found in the paper."""
        return self.object_list.objects

    def chunks_for_object(self, object_name: str) -> List[Chunk]:
        """Return all chunks that mention a specific object."""
        return [
            c for c in self.chunks
            if object_name in c.object_tags or object_name in c.text
        ]

    def summary(self) -> None:
        """Print a summary of the index."""
        print(f"\n{'='*60}")
        print(f"  Paper : {self.pdf_path}")
        print(f"  Objects ({len(self.object_list)}): "
              f"{', '.join(self.object_list.objects[:8])}"
              + (" …" if len(self.object_list) > 8 else ""))
        n_sec = sum(1 for c in self.chunks if c.chunk_type == "section")
        n_row = sum(1 for c in self.chunks if c.chunk_type == "table_row")
        print(f"  Chunks: {len(self.chunks)} ({n_sec} text, {n_row} table rows)")
        print(f"  Embeddings: {self._embeddings.shape}")
        print(f"{'='*60}")
