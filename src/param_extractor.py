"""
param_extractor.py — Deterministic regex extraction of physical parameters
from W UMa / contact-binary light-curve solution papers.

Parameters extracted (matching WUMaCat columns)
------------------------------------------------
  P      — orbital period (days)
  dPdt   — period change rate (days / year, ×10^-7 typical)
  q      — mass ratio  m2/m1
  i      — orbital inclination (degrees)
  T1     — primary effective temperature (K)
  T2     — secondary effective temperature (K)
  M1     — primary mass (solar masses)
  M2     — secondary mass (solar masses)
  R1     — primary radius (solar radii)
  R2     — secondary radius (solar radii)
  L1     — primary luminosity (solar luminosities)
  L2     — secondary luminosity (solar luminosities)
  a      — orbital semi-major axis / separation (solar radii)
  Omega  — dimensionless surface potential Ω₁ = Ω₂
  f      — fill-out (contact) factor
  r1p    — fractional radius of primary (pole)
  r2p    — fractional radius of secondary (pole)
  L3     — third-light fraction (dimensionless, 0-1)
  d      — distance (pc)
  Age    — stellar age (Gyr)

Usage
-----
    from src.param_extractor import extract_params, extract_from_hits

    # From raw text
    results = extract_params(text)

    # From PaperIndex search hits (preferred — pre-filtered relevant chunks)
    from src.paper_index import PaperIndex
    idx = PaperIndex("paper.pdf")
    hits = idx.query("mass ratio q", object_name="V369 Cep", top_k=5)
    results = extract_from_hits(hits, object_name="V369 Cep")

    for name, r in results.items():
        print(r)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ── Shared numeric primitives ─────────────────────────────────────────────────

# Unsigned float: 0.482, 5067, 1.65e-7
_UF = r'(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?'

# Signed float (for dPdt which can be negative)
_SF = r'[+-]?\s*(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?'

# Uncertainty suffix:  ± 0.03  or  (0.03)
_UNC = rf'(?:\s*[±]\s*{_UF}|\s*\(\s*{_UF}\s*\))?'

# Combined value + optional uncertainty
_VAL = rf'({_UF}){_UNC}'

# Scientific notation multiplier: × 10^-7 , ×10−7 , x10-7
_SCI = r'(?:\s*[×x×]\s*10\s*[\^]?\s*[-−]?\d+)?'

# Unicode subscripts → normal subscript chars used in papers
# e.g. T₁ → T1, M₂ → M2
def _normalize(text: str) -> str:
    """Normalize Unicode subscripts/superscripts and minus signs."""
    replacements = {
        '₁': '1', '₂': '2', '₃': '3',
        '¹': '1', '²': '2', '³': '3',
        '−': '-', '–': '-', '\u2212': '-',
        '⊙': 'sun', '☉': 'sun',
        '°': ' deg',
        '\u03a9': 'Omega',   # Ω
        '\u03b1': 'alpha',
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ParamMatch:
    """A single extracted parameter value."""
    param:       str            # parameter name (WUMaCat column)
    value:       float          # extracted numeric value
    uncertainty: Optional[float] = None
    unit:        str  = ""
    raw_text:    str  = ""      # the matched text snippet
    chunk_id:    int  = -1

    def __str__(self) -> str:
        unc = f" ± {self.uncertainty}" if self.uncertainty is not None else ""
        unit = f" {self.unit}" if self.unit else ""
        return (f"  {self.param:<8} = {self.value}{unc}{unit}"
                f"  [{self.raw_text[:60].strip()}]")


# ── Parameter specifications ──────────────────────────────────────────────────

@dataclass
class ParamSpec:
    """Definition of one extractable parameter."""
    param:       str             # WUMaCat column name
    label:       str             # human-readable name
    unit:        str             # physical unit
    valid_range: Tuple[float, float]   # (min, max) for sanity filtering
    patterns:    List[re.Pattern] = field(default_factory=list)
    # semantic query texts for PaperIndex.query()
    queries:     List[str] = field(default_factory=list)


def _compile(raw_patterns: List[str]) -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in raw_patterns]


# ─────────────────────────────────────────────────────────────────────────────
# Parameter catalogue
# ─────────────────────────────────────────────────────────────────────────────

PARAM_SPECS: Dict[str, ParamSpec] = {}

# ── Orbital period P (days, 0.1 – 2.0) ───────────────────────────────────────
PARAM_SPECS["P"] = ParamSpec(
    param="P", label="Orbital period", unit="days",
    valid_range=(0.08, 2.5),
    patterns=_compile([
        # "P = 0.226618 d", "P = 0.2266 days"
        rf'\bP\s*=\s*({_UF})\s*d(?:ay)?s?\b',
        # "orbital period of 0.226618 d"
        rf'orbital\s+period\s+(?:of\s+|is\s+)?({_UF})\s*d(?:ay)?s?',
        # "period P = 0.226618"  (no unit — try with context)
        rf'period\s+P\s*=\s*({_UF})',
        # "P0 = 0.22660118 day"
        rf'\bP0?\s*=\s*({_UF})\s*d(?:ay)?s?\b',
    ]),
    queries=["orbital period P days", "period of the binary"],
)

# ── Period change dP/dt (days/year, stored as ×10^-7 typically) ──────────────
PARAM_SPECS["dPdt"] = ParamSpec(
    param="dPdt", label="Period change rate", unit="d/yr",
    valid_range=(-1e-4, 1e-4),
    patterns=_compile([
        # "dP/dt = -1.43 × 10^-7 d/yr"
        rf'dP\s*/\s*dt\s*=\s*({_SF}\s*[×x]\s*10\s*[\^]?\s*[-−]?\d+)',
        # "\dot{P} = -1.43 × 10^{-7}"
        rf'\\?dot\s*[{{]?\s*P\s*[}}]?\s*=\s*({_SF}\s*[×x]\s*10\s*[\^]?\s*[-−]?\d+)',
        # "dP/dt = -0.014256 (×10^-7 d/yr)"   — raw small value
        rf'dP\s*/\s*dt\s*=\s*({_SF})',
        # "period change rate of -1.43 × 10^-7"
        rf'period\s+change\s+(?:rate\s+)?(?:of\s+)?({_SF}\s*[×x]\s*10\s*[\^]?\s*[-−]?\d+)',
    ]),
    queries=["period change rate dP/dt", "period variation secular change"],
)

# ── Mass ratio q (dimensionless, 0.05 – 1.0) ─────────────────────────────────
PARAM_SPECS["q"] = ParamSpec(
    param="q", label="Mass ratio", unit="",
    valid_range=(0.04, 15.0),   # some papers use q=m2/m1 without constraining q<1
    patterns=_compile([
        # "q = 0.482"  or  "q = 0.482 ± 0.003"
        rf'\bq\s*=\s*({_UF}){_UNC}',
        # "mass ratio q = 0.482"
        rf'mass\s+ratio\s+q\s*=\s*({_UF}){_UNC}',
        # "mass ratio of 0.482"
        rf'mass\s+ratio\s+(?:of\s+|is\s+)({_UF})',
        # "q = m2/m1 = 0.482"
        rf'q\s*=\s*m2\s*/\s*m1\s*=\s*({_UF})',
    ]),
    queries=["mass ratio q m2/m1", "mass ratio light curve solution"],
)

# ── Inclination i (degrees, 20 – 90) ─────────────────────────────────────────
PARAM_SPECS["i"] = ParamSpec(
    param="i", label="Orbital inclination", unit="deg",
    valid_range=(20.0, 90.0),
    patterns=_compile([
        # "i = 82.8°"  or  "i = 82.8 deg"  or  "i = 82.8"
        rf'\bi\s*=\s*({_UF}){_UNC}\s*(?:deg|[°◦])',
        # bare "i = 82.8" — accept without unit but tighter range check
        rf'\bi\s*=\s*({_UF}){_UNC}(?=\s|$|\))',
        # "inclination i = 82.8"
        rf'inclination\s+i\s*=\s*({_UF}){_UNC}',
        # "inclination of 82.8 deg"
        rf'inclination\s+(?:of\s+|is\s+)({_UF})\s*(?:deg|[°◦])?',
        # "i (°) / 82.8" (table format)
        rf'i\s*[(\[][°◦]?[)\]]\s*\n?\s*({_UF})',
    ]),
    queries=["orbital inclination i degrees", "inclination light curve solution"],
)

# ── Primary temperature T1 (K, 3000 – 12000) ─────────────────────────────────
PARAM_SPECS["T1"] = ParamSpec(
    param="T1", label="Primary temperature", unit="K",
    valid_range=(3000.0, 12000.0),
    patterns=_compile([
        # "T1 = 5067 K"  or  "T1 (K) = 5067"
        rf'\bT1\s*(?:\(K\))?\s*=\s*({_UF}){_UNC}\s*K?',
        # "T1\n5600" (table column)
        rf'\bT1\s*(?:\(K\))?\s*\n\s*({_UF})',
        # "T_1 = 5067"  or  "Teff,1 = 5067"
        rf'\bT_?\s*1\s*=\s*({_UF}){_UNC}\s*K?',
        rf'\bTeff[,_]?1\s*=\s*({_UF}){_UNC}\s*K?',
        # "temperature of the primary (component 1) is 5067 K"
        rf'temperature\s+of\s+the\s+primary\s+[^=\n]*=?\s*({_UF})\s*K',
        # "primary temperature of 5067 K"
        rf'primary\s+(?:effective\s+)?temperature\s+(?:of\s+)?({_UF})\s*K',
    ]),
    queries=["primary effective temperature T1 K", "T1 temperature primary component"],
)

# ── Secondary temperature T2 (K, 3000 – 12000) ───────────────────────────────
PARAM_SPECS["T2"] = ParamSpec(
    param="T2", label="Secondary temperature", unit="K",
    valid_range=(3000.0, 12000.0),
    patterns=_compile([
        rf'\bT2\s*(?:\(K\))?\s*=\s*({_UF}){_UNC}\s*K?',
        rf'\bT2\s*(?:\(K\))?\s*\n\s*({_UF})',
        rf'\bT_?\s*2\s*=\s*({_UF}){_UNC}\s*K?',
        rf'\bTeff[,_]?2\s*=\s*({_UF}){_UNC}\s*K?',
        rf'temperature\s+of\s+the\s+secondary\s+[^=\n]*=?\s*({_UF})\s*K',
        rf'secondary\s+(?:effective\s+)?temperature\s+(?:of\s+)?({_UF})\s*K',
    ]),
    queries=["secondary effective temperature T2 K", "T2 temperature secondary component"],
)

# ── Primary mass M1 (M⊙, 0.2 – 5.0) ─────────────────────────────────────────
PARAM_SPECS["M1"] = ParamSpec(
    param="M1", label="Primary mass", unit="Msun",
    valid_range=(0.2, 5.0),
    patterns=_compile([
        # "M1 = 0.79 M⊙"
        rf'\bM1\s*=\s*({_UF}){_UNC}\s*M?(?:sun|⊙|☉)?',
        rf'\bM_?\s*1\s*=\s*({_UF}){_UNC}\s*M?(?:sun|⊙|☉)?',
        # "mass of the primary M1 = 0.79"
        rf'(?:mass\s+of\s+the\s+primary|primary\s+(?:component\s+)?mass)\s+[^=\n]{0,20}=\s*({_UF})',
        rf'\bm1\s*=\s*({_UF}){_UNC}\s*M?(?:sun|⊙|☉)?',
    ]),
    queries=["primary mass M1 solar masses", "stellar mass primary component"],
)

# ── Secondary mass M2 (M⊙, 0.1 – 3.0) ───────────────────────────────────────
PARAM_SPECS["M2"] = ParamSpec(
    param="M2", label="Secondary mass", unit="Msun",
    valid_range=(0.1, 3.0),
    patterns=_compile([
        rf'\bM2\s*=\s*({_UF}){_UNC}\s*M?(?:sun|⊙|☉)?',
        rf'\bM_?\s*2\s*=\s*({_UF}){_UNC}\s*M?(?:sun|⊙|☉)?',
        rf'(?:mass\s+of\s+the\s+secondary|secondary\s+(?:component\s+)?mass)\s+[^=\n]{0,20}=\s*({_UF})',
        rf'\bm2\s*=\s*({_UF}){_UNC}\s*M?(?:sun|⊙|☉)?',
    ]),
    queries=["secondary mass M2 solar masses", "stellar mass secondary component"],
)

# ── Primary radius R1 (R⊙, 0.2 – 5.0) ───────────────────────────────────────
PARAM_SPECS["R1"] = ParamSpec(
    param="R1", label="Primary radius", unit="Rsun",
    valid_range=(0.2, 5.0),
    patterns=_compile([
        rf'\bR1\s*=\s*({_UF}){_UNC}\s*R?(?:sun|⊙|☉)?',
        rf'\bR_?\s*1\s*=\s*({_UF}){_UNC}\s*R?(?:sun|⊙|☉)?',
        rf'(?:radius\s+of\s+the\s+primary|primary\s+(?:component\s+)?radius)\s+[^=\n]{0,20}=\s*({_UF})',
    ]),
    queries=["primary radius R1 solar radii", "stellar radius primary component"],
)

# ── Secondary radius R2 (R⊙, 0.1 – 4.0) ─────────────────────────────────────
PARAM_SPECS["R2"] = ParamSpec(
    param="R2", label="Secondary radius", unit="Rsun",
    valid_range=(0.1, 4.0),
    patterns=_compile([
        rf'\bR2\s*=\s*({_UF}){_UNC}\s*R?(?:sun|⊙|☉)?',
        rf'\bR_?\s*2\s*=\s*({_UF}){_UNC}\s*R?(?:sun|⊙|☉)?',
        rf'(?:radius\s+of\s+the\s+secondary|secondary\s+(?:component\s+)?radius)\s+[^=\n]{0,20}=\s*({_UF})',
    ]),
    queries=["secondary radius R2 solar radii", "stellar radius secondary component"],
)

# ── Primary luminosity L1 (L⊙, 0.001 – 500) ─────────────────────────────────
PARAM_SPECS["L1"] = ParamSpec(
    param="L1", label="Primary luminosity", unit="Lsun",
    valid_range=(0.001, 500.0),
    patterns=_compile([
        rf'\bL1\s*=\s*({_UF}){_UNC}\s*L?(?:sun|⊙|☉)?',
        rf'\bL_?\s*1\s*=\s*({_UF}){_UNC}\s*L?(?:sun|⊙|☉)?',
        rf'(?:luminosity\s+of\s+the\s+primary|primary\s+luminosity)\s+[^=\n]{0,20}=\s*({_UF})',
        # luminosity ratio LV1/(LV1+LV2) = 0.66 → primary fraction × total
        # (stored as fraction here; caller can multiply by total L if known)
        rf'L[VR]?1\s*/\s*\(L[VR]?1\s*\+\s*L[VR]?2\)\s*[=\n]\s*({_UF})',
    ]),
    queries=["primary luminosity L1 solar luminosities", "luminosity ratio LV1"],
)

# ── Secondary luminosity L2 (L⊙, 0.001 – 300) ───────────────────────────────
PARAM_SPECS["L2"] = ParamSpec(
    param="L2", label="Secondary luminosity", unit="Lsun",
    valid_range=(0.001, 300.0),
    patterns=_compile([
        rf'\bL2\s*=\s*({_UF}){_UNC}\s*L?(?:sun|⊙|☉)?',
        rf'\bL_?\s*2\s*=\s*({_UF}){_UNC}\s*L?(?:sun|⊙|☉)?',
        rf'(?:luminosity\s+of\s+the\s+secondary|secondary\s+luminosity)\s+[^=\n]{0,20}=\s*({_UF})',
    ]),
    queries=["secondary luminosity L2 solar luminosities"],
)

# ── Orbital separation a (R⊙, 0.5 – 10.0) ───────────────────────────────────
PARAM_SPECS["a"] = ParamSpec(
    param="a", label="Orbital separation", unit="Rsun",
    valid_range=(0.5, 10.0),
    patterns=_compile([
        # "a = 1.65 R⊙"
        rf'\ba\s*=\s*({_UF}){_UNC}\s*R?(?:sun|⊙|☉)',
        rf'\ba\s*=\s*({_UF}){_UNC}\s*(?=R)',
        # "semi-major axis a = 1.65"
        rf'semi.?major\s+axis\s+a\s*=\s*({_UF}){_UNC}',
        rf'orbital\s+separation\s+[^=\n]{0,20}=\s*({_UF})',
        # "a = 1.65" with context
        rf'separation\s+a\s*=\s*({_UF})',
    ]),
    queries=["orbital separation a solar radii semi-major axis"],
)

# ── Surface potential Omega (dimensionless, 1.5 – 15) ────────────────────────
PARAM_SPECS["Omega"] = ParamSpec(
    param="Omega", label="Surface potential", unit="",
    valid_range=(1.5, 20.0),
    patterns=_compile([
        # "Ω = 2.773" or "Omega = 2.773" or "Ω1 = Ω2 = 2.773"
        rf'(?:Omega|[ΩΩ])\s*[12]?\s*=\s*(?:[ΩΩ]\s*[12]?\s*=\s*)?({_UF}){_UNC}',
        # Unicode Ω in text
        rf'\u03a9\s*[12]?\s*=\s*({_UF}){_UNC}',
        # "surface potential Omega = 2.773"
        rf'surface\s+potential\s+[^=\n]{0,20}=\s*({_UF})',
        # "Ω1 = Ω2\n9.56 ± 0.03" (table, header on one line, value on next)
        rf'(?:Omega|[Ω])\s*1\s*=\s*(?:[Ω]|Omega)\s*2\s*\n\s*({_UF})',
    ]),
    queries=["surface potential Omega contact parameter", "dimensionless potential Roche lobe"],
)

# ── Fill-out factor f (0 – 1) ─────────────────────────────────────────────────
PARAM_SPECS["f"] = ParamSpec(
    param="f", label="Fill-out factor", unit="",
    valid_range=(-0.5, 1.5),
    patterns=_compile([
        # "f = 0.235" or "f = 23.5%"
        rf'\bf\s*=\s*({_UF}){_UNC}(?:\s*%)?',
        # "fill-out factor f = 0.235"
        rf'fill.?out\s+factor\s+f\s*=\s*({_UF})',
        rf'fill.?out\s+(?:factor\s+)?(?:of\s+|is\s+)?({_UF})',
        rf'contact\s+degree\s+[^=\n]{0,20}=\s*({_UF})',
        rf'fillout\s+(?:parameter|factor)\s*[=:]\s*({_UF})',
    ]),
    queries=["fill-out factor contact degree f", "fillout parameter contact binary"],
)

# ── Fractional radii r1p, r2p (0.2 – 0.6) ───────────────────────────────────
PARAM_SPECS["r1p"] = ParamSpec(
    param="r1p", label="Fractional radius (primary, pole)", unit="",
    valid_range=(0.15, 0.65),
    patterns=_compile([
        rf'\br1\s*(?:\(pole\))?\s*=\s*({_UF}){_UNC}',
        rf'\br_?\s*1p?\s*=\s*({_UF}){_UNC}',
        rf'fractional\s+radius\s+(?:of\s+the\s+)?primary\s+[^=\n]{0,20}=\s*({_UF})',
    ]),
    queries=["fractional radius r1 primary pole"],
)

PARAM_SPECS["r2p"] = ParamSpec(
    param="r2p", label="Fractional radius (secondary, pole)", unit="",
    valid_range=(0.15, 0.65),
    patterns=_compile([
        rf'\br2\s*(?:\(pole\))?\s*=\s*({_UF}){_UNC}',
        rf'\br_?\s*2p?\s*=\s*({_UF}){_UNC}',
        rf'fractional\s+radius\s+(?:of\s+the\s+)?secondary\s+[^=\n]{0,20}=\s*({_UF})',
    ]),
    queries=["fractional radius r2 secondary pole"],
)

# ── Third light L3 (0 – 1 fraction) ──────────────────────────────────────────
PARAM_SPECS["L3"] = ParamSpec(
    param="L3", label="Third light", unit="",
    valid_range=(0.0, 1.0),
    patterns=_compile([
        rf'\bL3\s*=\s*({_UF}){_UNC}',
        rf'\bl3\s*=\s*({_UF}){_UNC}',
        rf'third\s+light\s+[^=\n]{0,20}=\s*({_UF})',
        rf'third\s+body\s+contribution\s+[^=\n]{0,20}=\s*({_UF})',
    ]),
    queries=["third light L3 contribution third body"],
)

# ── Distance d (pc, 10 – 50000) ───────────────────────────────────────────────
PARAM_SPECS["d"] = ParamSpec(
    param="d", label="Distance", unit="pc",
    valid_range=(10.0, 50000.0),
    patterns=_compile([
        # "d = 351 pc"
        rf'\bd\s*=\s*({_UF}){_UNC}\s*(?:pc|parsec)',
        # "distance d = 351 pc"
        rf'distance\s+d\s*=\s*({_UF}){_UNC}\s*(?:pc|parsec)?',
        # "distance of 351 pc"
        rf'distance\s+(?:of\s+|is\s+)({_UF})\s*(?:[±]\s*{_UF}\s*)?(?:pc|parsec)',
        # "1800 ± 80 pc"  — bare number followed by pc
        rf'({_UF})\s*(?:[±]\s*{_UF}\s*)?pc\b',
    ]),
    queries=["distance parsec pc", "distance determination photometric"],
)

# ── Age (Gyr, 0.01 – 14) ──────────────────────────────────────────────────────
PARAM_SPECS["Age"] = ParamSpec(
    param="Age", label="Stellar age", unit="Gyr",
    valid_range=(0.01, 14.0),
    patterns=_compile([
        # "age of 10.5 Gyr"
        rf'age\s+(?:of\s+|is\s+)?({_UF}){_UNC}\s*Gyr',
        # "age = 10.5 Gyr"
        rf'\bage\s*=\s*({_UF}){_UNC}\s*Gyr',
        # "10.5 × 10^9 yr" → convert via range check
        rf'age\s+[^=\n]{0,20}=?\s*({_UF})\s*[×x]\s*10\s*[\^]?\s*9\s*(?:yr|y)',
        # "age of 10.5 Gyr"  — looser form
        rf'\bage\s+(?:of\s+)?({_UF})\s*Gyr',
    ]),
    queries=["age Gyr stellar age cluster", "isochrone age Gyr"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Extraction logic
# ─────────────────────────────────────────────────────────────────────────────

def _parse_float(s: str) -> Optional[float]:
    """Parse a float string, return None on failure."""
    try:
        # remove spaces around ×10 scientific notation if present
        s = re.sub(r'\s*[×x]\s*10\s*[\^]?\s*([-−+]?\d+)',
                   lambda m: f"e{m.group(1).replace('−','-')}", s.strip())
        return float(s)
    except (ValueError, TypeError):
        return None


def _extract_uncertainty(text: str, value_end: int) -> Optional[float]:
    """
    Look for an uncertainty immediately after the matched value position.
    Handles '± 0.03', '(0.03)', '+/-0.03'.
    """
    tail = text[value_end:value_end + 30]
    m = re.search(
        rf'^\s*(?:[±]\s*|[+].?[-]\s*|[(]\s*)({_UF})(?:[)])?',
        tail,
    )
    if m:
        return _parse_float(m.group(1))
    return None


def extract_params(
    text: str,
    params: Optional[List[str]] = None,
) -> Dict[str, List[ParamMatch]]:
    """
    Run all regex patterns against a text chunk and return matches.

    Args:
        text:   Text to search (a chunk or full paper section).
        params: If given, only extract these parameter names (WUMaCat columns).
                Default: all parameters.

    Returns:
        Dict mapping param name → list of ParamMatch (one per regex hit).
        Multiple hits can occur; caller should de-duplicate or pick best.
    """
    text_n = _normalize(text)
    results: Dict[str, List[ParamMatch]] = {}
    target_specs = (
        {k: PARAM_SPECS[k] for k in params if k in PARAM_SPECS}
        if params else PARAM_SPECS
    )

    for pname, spec in target_specs.items():
        hits: List[ParamMatch] = []
        for pattern in spec.patterns:
            for m in pattern.finditer(text_n):
                raw_val = m.group(1)
                val = _parse_float(raw_val)
                if val is None:
                    continue
                # Sanity range check
                lo, hi = spec.valid_range
                if not (lo <= val <= hi):
                    continue
                # Try to grab uncertainty from the match itself or right after
                unc = None
                if m.lastindex and m.lastindex >= 2:
                    unc_str = m.group(2) if m.lastindex >= 2 else None
                    if unc_str:
                        unc = _parse_float(unc_str.strip(' ±()'))
                if unc is None:
                    unc = _extract_uncertainty(text_n, m.end())

                # Context snippet
                start = max(0, m.start() - 30)
                raw_text = text_n[start: m.end() + 20].replace('\n', ' ').strip()

                hits.append(ParamMatch(
                    param=pname,
                    value=round(val, 6),
                    uncertainty=round(unc, 6) if unc is not None else None,
                    unit=spec.unit,
                    raw_text=raw_text,
                ))

        if hits:
            results[pname] = hits

    return results


def extract_from_hits(
    hits: list,   # List[SearchHit] from PaperIndex.query()
    params: Optional[List[str]] = None,
    object_name: Optional[str] = None,
) -> Dict[str, List[ParamMatch]]:
    """
    Extract parameters from a list of PaperIndex SearchHit objects.

    Adds chunk_id to each ParamMatch for traceability.

    Args:
        hits:        List of SearchHit from PaperIndex.query().
        params:      If given, restrict to these parameter names.
        object_name: If given, prefer hits whose text mentions the object.

    Returns:
        Dict param_name → List[ParamMatch], merged across all hits.
    """
    merged: Dict[str, List[ParamMatch]] = {}
    for hit in hits:
        chunk_results = extract_params(hit.chunk.text, params=params)
        for pname, matches in chunk_results.items():
            for pm in matches:
                pm.chunk_id = hit.chunk.chunk_id
            merged.setdefault(pname, []).extend(matches)
    return merged


def best_value(matches: List[ParamMatch]) -> Optional[ParamMatch]:
    """
    Pick the most reliable value from a list of matches.

    Preference order:
      1. Match with uncertainty (quantified error bars)
      2. Match appearing in a table_row chunk (chunk_id >= 0 means it came from
         extract_from_hits; raw_text heuristic for table context)
      3. First match otherwise
    """
    if not matches:
        return None
    with_unc = [m for m in matches if m.uncertainty is not None]
    if with_unc:
        return with_unc[0]
    return matches[0]


# ── Table-column parser ───────────────────────────────────────────────────────
#
# Many W UMa papers present results in "long-format" text tables:
#
#   Parameters   | V1 (EP Cep) | V2 (EQ Cep) | V7 (V369 Cep) | ...
#   T1 (K)       |     5600    |     5275     |      5546      | ...
#   T2 (K)       |  5074 ± 17  |   4975 ± 9   |   5088 ± 12   | ...
#   i (°)        | 69.45 ± 0.32|  81.40 ± 0.23|  74.71 ± 0.18 | ...
#
# PyMuPDF extracts this as one value per line within a column:
#
#   T1 (K)\n5600\n5275\n5505\n5582\n5383\n5546\n4780\n5750
#
# This function finds such blocks and maps column positions to object names.

# Mapping from table row labels (normalized) to WUMaCat column names
_TABLE_PARAM_MAP: List[Tuple[re.Pattern, str]] = [
    (re.compile(r'T1\s*\(K\)', re.I),                         'T1'),
    (re.compile(r'T2\s*\(K\)', re.I),                         'T2'),
    (re.compile(r'i\s*\([°◦d]', re.I),                        'i'),
    (re.compile(r'Omega1?\s*=\s*Omega2?', re.I),               'Omega'),
    (re.compile(r'q\s*=\s*m2\s*/\s*m1', re.I),                'q'),
    (re.compile(r'r1\b(?!\s*[/=(])', re.I),                    'r1p'),
    (re.compile(r'r2\b(?!\s*[/=(])', re.I),                    'r2p'),
    (re.compile(r'L[VRI]?1\s*/\s*\(L[VRI]?1', re.I),          'L1'),
    (re.compile(r'f\b(?:\s*\(|$)', re.I),                      'f'),
    (re.compile(r'M1\s*\(M', re.I),                            'M1'),
    (re.compile(r'M2\s*\(M', re.I),                            'M2'),
    (re.compile(r'R1\s*\(R', re.I),                            'R1'),
    (re.compile(r'R2\s*\(R', re.I),                            'R2'),
    (re.compile(r'L1\s*\(L', re.I),                            'L1'),
    (re.compile(r'L2\s*\(L', re.I),                            'L2'),
    (re.compile(r'a\s*\(R', re.I),                             'a'),
    (re.compile(r'P\s*\(d', re.I),                             'P'),
    (re.compile(r'dP\s*/\s*dt', re.I),                         'dPdt'),
    (re.compile(r'L3\b', re.I),                                'L3'),
    (re.compile(r'age\s*\(Gyr', re.I),                         'Age'),
    (re.compile(r'd\s*\(pc', re.I),                            'd'),
]

_VALUE_LINE_RE = re.compile(
    rf'^(?P<val>[+-]?\s*(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)'
    rf'(?:\s*[±]\s*(?P<unc>(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?))?'
    rf'\s*$'
)


def _map_table_label(label: str) -> Optional[str]:
    """Return WUMaCat param name for a table row label, or None."""
    label_n = _normalize(label).strip()
    for pat, pname in _TABLE_PARAM_MAP:
        if pat.search(label_n):
            return pname
    return None


def extract_table_block(
    text: str,
    objects: List[str],
) -> Dict[str, Dict[str, ParamMatch]]:
    """
    Parse a text-formatted parameter table and extract per-object values.

    Looks for a block where one row lists known object names (column headers)
    and subsequent rows each start with a parameter label followed by N value
    lines (one per column).

    Args:
        text:    Full paper text or a section containing the table.
        objects: Ranked list of object names from extract_objects().

    Returns:
        {object_name: {param_name: ParamMatch}}
    """
    text_n = _normalize(text)
    lines = text_n.split('\n')
    n_lines = len(lines)

    # ── Step 1: find a run of consecutive lines each containing one object ────
    # This is much more reliable than a windowed search — it won't fire on
    # object names scattered through prose paragraphs.

    col_positions: Optional[List[Tuple[int, str]]] = None
    header_end_idx: int = -1
    min_needed = max(2, len(objects) // 2)  # require at least half the objects

    for start_idx in range(n_lines):
        run: List[str] = []       # ordered object names in header
        i = start_idx
        while i < n_lines and len(run) < len(objects) + 2:
            line = lines[i].strip()
            # Count objects on this line
            found = [obj for obj in objects if obj in line]
            if len(found) == 1:
                run.append(found[0])
                i += 1
            elif len(found) > 1:
                # Multiple objects on same line — not a one-per-line table header
                break
            else:
                # No object — only allow if run is empty (still scanning)
                if run:
                    break
                i += 1
                if i - start_idx > 5:   # give up if >5 non-object lines before first hit
                    break

        if len(run) >= min_needed:
            col_positions = list(enumerate(run))
            header_end_idx = i
            break

    if col_positions is None:
        return {}

    n_cols = len(col_positions)
    if n_cols == 0:
        return {}

    # ── Step 2: scan rows below the header ────────────────────────────────────
    results: Dict[str, Dict[str, ParamMatch]] = {obj: {} for _, obj in col_positions}
    i = header_end_idx

    while i < n_lines:
        label_line = lines[i].strip()
        pname = _map_table_label(label_line)
        i += 1

        if pname is None:
            continue

        # Validate range spec for this param
        spec = PARAM_SPECS.get(pname)
        if spec is None:
            continue
        lo, hi = spec.valid_range

        # Collect next n_cols value lines
        values_collected = 0
        col_idx = 0
        while i < n_lines and values_collected < n_cols + 3:
            val_line = lines[i].strip()
            m = _VALUE_LINE_RE.match(val_line)
            if m:
                raw_val = m.group('val').replace(' ', '')
                val = _parse_float(raw_val)
                if val is not None and lo <= val <= hi:
                    if col_idx < len(col_positions):
                        _, obj = col_positions[col_idx]
                        unc_str = m.group('unc')
                        unc = _parse_float(unc_str) if unc_str else None
                        results[obj][pname] = ParamMatch(
                            param=pname,
                            value=round(val, 6),
                            uncertainty=round(unc, 6) if unc is not None else None,
                            unit=spec.unit,
                            raw_text=val_line,
                        )
                col_idx += 1
                values_collected += 1
                i += 1
            elif val_line == '' or _map_table_label(val_line) is not None:
                # blank line or next param label — stop collecting
                break
            else:
                # non-value line — skip but keep scanning for this param
                i += 1
                continue

    return results


# ── Query plan: which semantic queries to run for each parameter ──────────────

def param_query_plan() -> Dict[str, List[str]]:
    """Return {param: [query_string, ...]} for all parameters."""
    return {pname: spec.queries for pname, spec in PARAM_SPECS.items()}


# ── Pretty printer ────────────────────────────────────────────────────────────

def print_results(results: Dict[str, List[ParamMatch]], label: str = "") -> None:
    """Print extracted parameters in a table."""
    if label:
        print(f"\n{'='*60}")
        print(f"  {label}")
        print(f"{'='*60}")
    if not results:
        print("  (no parameters extracted)")
        return
    print(f"  {'PARAM':<8} {'VALUE':>12} {'±':>10}  UNIT    SNIPPET")
    print("  " + "-" * 75)
    for pname in PARAM_SPECS:  # iterate in catalogue order
        if pname not in results:
            continue
        bv = best_value(results[pname])
        if bv is None:
            continue
        unc_str = f"{bv.uncertainty:.4g}" if bv.uncertainty is not None else "—"
        snippet = bv.raw_text[:40].replace('\n', ' ')
        print(f"  {pname:<8} {bv.value:>12.4g} {unc_str:>10}  {bv.unit:<7} {snippet}")
