"""
pdf_utils.py — PDF format detection and text extraction utilities.

Pipeline step 1: classify each PDF as 'text' or 'image' (scanned),
and expose the right extraction tool for downstream steps.

Detection logic (per page):
  - Extract text length and image count via PyMuPDF (fitz).
  - A page is 'text' if it has more than MIN_TEXT_CHARS characters.
  - A page is 'image' if it has fewer characters and at least one embedded image.
  - Overall PDF type is decided by the majority of pages.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional
import fitz  # PyMuPDF


# Minimum characters on a page to consider it text-based
MIN_TEXT_CHARS = 100


@dataclass
class PageInfo:
    page_num: int
    text_chars: int
    image_count: int
    page_type: str  # "text" or "image"


@dataclass
class PDFInfo:
    path: str
    pdf_type: str           # "text" or "image"
    total_pages: int
    text_pages: int
    image_pages: int
    pages: List[PageInfo] = field(default_factory=list)
    extraction_tool: str = ""  # recommended tool for this PDF

    @property
    def filename(self) -> str:
        return os.path.basename(self.path)


def detect_pdf_type(pdf_path: str, max_pages: int = 5) -> PDFInfo:
    """
    Detect whether a PDF is text-based or image-based (scanned).

    Inspects up to `max_pages` pages and classifies the document by majority.

    Args:
        pdf_path:  Absolute or relative path to the PDF file.
        max_pages: Number of pages to sample (default 5 — sufficient for most papers).

    Returns:
        PDFInfo with classification and recommended extraction tool.

    Recommended extraction tools:
        text  → PyMuPDF (fitz) or pdfplumber — direct text extraction
        image → OCR required (e.g. pytesseract, easyocr, or a vision LLM)
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    pages_to_check = min(max_pages, total_pages)

    page_infos: List[PageInfo] = []

    for i in range(pages_to_check):
        page = doc[i]
        text = page.get_text()
        text_chars = len(text.strip())
        image_count = len(page.get_images(full=False))

        page_type = "text" if text_chars >= MIN_TEXT_CHARS else "image"

        page_infos.append(PageInfo(
            page_num=i + 1,
            text_chars=text_chars,
            image_count=image_count,
            page_type=page_type,
        ))

    doc.close()

    text_pages  = sum(1 for p in page_infos if p.page_type == "text")
    image_pages = sum(1 for p in page_infos if p.page_type == "image")

    # Majority vote
    pdf_type = "text" if text_pages >= image_pages else "image"
    extraction_tool = (
        "PyMuPDF (fitz) — direct text extraction"
        if pdf_type == "text"
        else "OCR required (pytesseract / easyocr / vision LLM)"
    )

    return PDFInfo(
        path=pdf_path,
        pdf_type=pdf_type,
        total_pages=total_pages,
        text_pages=text_pages,
        image_pages=image_pages,
        pages=page_infos,
        extraction_tool=extraction_tool,
    )


def detect_all_pdfs(pdf_dir: str, verbose: bool = True) -> List[PDFInfo]:
    """
    Detect the type of every PDF in a directory.

    Args:
        pdf_dir: Directory containing PDF files.
        verbose: If True, print a summary table.

    Returns:
        List of PDFInfo objects, one per file.
    """
    pdf_files = sorted(
        f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")
    )

    if not pdf_files:
        print(f"⚠️  No PDF files found in: {pdf_dir}")
        return []

    results: List[PDFInfo] = []

    if verbose:
        print(f"🔍 Detecting PDF types in: {pdf_dir}")
        print(f"   Files found: {len(pdf_files)}\n")
        print(f"  {'FILENAME':<45} {'TYPE':<8} {'PAGES':>6}  TOOL")
        print("  " + "-" * 90)

    for filename in pdf_files:
        path = os.path.join(pdf_dir, filename)
        try:
            info = detect_pdf_type(path)
            results.append(info)
            if verbose:
                icon = "📄" if info.pdf_type == "text" else "🖼️ "
                print(f"  {icon} {filename:<43} {info.pdf_type:<8} {info.total_pages:>6}  {info.extraction_tool}")
        except Exception as e:
            if verbose:
                print(f"  ❌ {filename:<43} ERROR: {e}")

    if verbose:
        text_count  = sum(1 for r in results if r.pdf_type == "text")
        image_count = sum(1 for r in results if r.pdf_type == "image")
        print()
        print(f"  📊 Summary: {text_count} text-based, {image_count} image-based (scanned)")

    return results


def extract_text(pdf_path: str, max_pages: Optional[int] = None) -> str:
    """
    Extract text from a text-based PDF using PyMuPDF.

    Call detect_pdf_type() first to confirm the PDF is text-based.
    For image-based PDFs this will return empty or near-empty strings.

    Args:
        pdf_path:  Path to the PDF file.
        max_pages: If set, only extract text from the first N pages.

    Returns:
        Concatenated text from all (or first N) pages.
    """
    doc = fitz.open(pdf_path)
    pages = list(doc)[:max_pages] if max_pages else list(doc)
    text = "\n".join(page.get_text() for page in pages)
    doc.close()
    return text


def extract_conclusions(pdf_path: str) -> Optional[str]:
    """
    Extract the conclusions section from a text-based PDF.

    Searches for a conclusions header (Conclusions, Summary, Discussion)
    and returns text up to the next major section (References, Appendix, etc.).
    Returns None if no conclusions section is found.

    Args:
        pdf_path: Path to the PDF file.

    Returns:
        Conclusions text, or None if not found.
    """
    import re

    doc = fitz.open(pdf_path)
    full_text = "\n".join(page.get_text() for page in doc)
    doc.close()

    # Patterns that mark the start of conclusions (search from end of doc)
    start_patterns = [
        r'(?i)\b(?:conclusions?\s+and\s+(?:outlook|discussion|summary)|'
        r'summary\s+and\s+conclusions?|'
        r'conclusions?\s+and\s+remarks?|'
        r'concluding\s+remarks?|'
        r'conclusions?|'
        r'summary)\b',
    ]

    # Patterns that mark the end of conclusions
    end_patterns = [
        r'(?i)^\s*(?:acknowledge?ments?|acknowledgements?)',
        r'(?i)^\s*references?\b',
        r'(?i)^\s*appendix\b',
        r'(?i)^\s*(?:data\s+availability|author\s+contributions?)',
        r'(?i)^\s*(?:funding|conflict)',
    ]

    # Find the LAST occurrence of a conclusions header
    start_pos = None
    for pat in start_patterns:
        for m in re.finditer(pat, full_text):
            start_pos = m.end()   # keep updating — want the last match
    if start_pos is None:
        return None

    conclusions_text = full_text[start_pos:]

    # Find the earliest end marker
    earliest_end = len(conclusions_text)
    for pat in end_patterns:
        m = re.search(pat, conclusions_text, re.MULTILINE)
        if m and m.start() < earliest_end:
            earliest_end = m.start()

    result = conclusions_text[:earliest_end].strip()
    return result if len(result) >= 80 else None


def extract_abstract(pdf_path: str) -> str:
    """
    Extract just the abstract section from a text-based PDF.

    Searches pages 1-2 for an "Abstract" header and returns the text
    between it and the next section heading (Introduction, Keywords, etc.).
    Falls back to returning the full first page if no abstract marker is found.

    Args:
        pdf_path: Path to the PDF file.

    Returns:
        Abstract text as a string.
    """
    import re

    # Extract text from first two pages (abstract is always there)
    doc = fitz.open(pdf_path)
    raw = "\n".join(doc[i].get_text() for i in range(min(2, len(doc))))
    doc.close()

    # Patterns that mark the start of the abstract
    start_patterns = [
        r'(?i)\bABSTRACT\b',
        r'(?i)\bSummary\b',
    ]
    # Patterns that mark the end of the abstract / start of next section
    end_patterns = [
        r'(?i)^\s*(?:1[\.\s]|I[\.\s])\s*(?:Introduction|INTRODUCTION)',
        r'(?i)^\s*Keywords?\s*[:\-]',
        r'(?i)^\s*Key\s+words?\s*[:\-]',
        r'(?i)^\s*Subject\s+headings',
        r'(?i)^\s*1\s+Introduction',
    ]

    # Find abstract start
    start_match = None
    for pat in start_patterns:
        m = re.search(pat, raw)
        if m:
            start_match = m
            break

    if start_match is None:
        # No abstract marker — return first page as fallback
        return raw[:2000]

    abstract_start = start_match.end()
    abstract_text  = raw[abstract_start:]

    # Find abstract end
    earliest_end = len(abstract_text)
    for pat in end_patterns:
        m = re.search(pat, abstract_text, re.MULTILINE)
        if m and m.start() < earliest_end:
            earliest_end = m.start()

    abstract = abstract_text[:earliest_end].strip()

    # Sanity check — if extraction is too short, fall back to first page
    if len(abstract) < 100:
        return raw[:2000]

    return abstract
