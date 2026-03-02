# backend/scripts/_registry/attach/suffix_engine_v1_8.py
# v1_8 deterministic suffix normalization (tail-only)
# Symmetric: apply to BOTH spine indexing and event addr variants.
# No fuzzy. No heuristics. No guessing.

from __future__ import annotations

import re
from typing import Dict, Tuple, List

def _extract_pid(hit):
    """
    authority_pairs value may be:
      - "MA|property|..." (string)
      - {"property_id": "...", ...} (dict)
      - ("MA|property|...", ...) (tuple/list)
    """
    if hit is None:
        return None
    if isinstance(hit, str):
        return hit
    if isinstance(hit, dict):
        return hit.get("property_id") or hit.get("pid")
    if isinstance(hit, (list, tuple)) and len(hit) > 0:
        return hit[0]
    return None

# ----------------------------
# Canonical single-token suffix map
# (keys should be uppercase)
# ----------------------------
SUFFIX_1: Dict[str, str] = {
    # MA-observed + common USPS-like
    "TE": "TERRACE",
    "TER": "TERRACE",
    "TERR": "TERRACE",
    "TERRACE": "TERRACE",

    "LA": "LANE",
    "LN": "LANE",
    "LANE": "LANE",

    "BL": "BOULEVARD",
    "BLVD": "BOULEVARD",
    "BOULEVARD": "BOULEVARD",
    "BOUL": "BOULEVARD",

    "TR": "TRAIL",
    "TRL": "TRAIL",
    "TRAIL": "TRAIL",

    "WY": "WAY",
    "WAY": "WAY",

    "PLZ": "PLAZA",
    "PLAZA": "PLAZA",

    "SQ": "SQUARE",
    "SQR": "SQUARE",
    "SQUARE": "SQUARE",

    "CTR": "CENTER",
    "CNTR": "CENTER",
    "CENTRE": "CENTER",
    "CENTER": "CENTER",

    "ROW": "ROW",
    "PATH": "PATH",

    "PKWY": "PARKWAY",
    "PARKWY": "PARKWAY",
    "PARKWAY": "PARKWAY",

    "HWY": "HIGHWAY",
    "HIGHWAY": "HIGHWAY",

    "RTE": "ROUTE",
    "RT": "ROUTE",
    "ROUTE": "ROUTE",

    "RD": "ROAD",
    "ROAD": "ROAD",

    "DR": "DRIVE",
    "DRIVE": "DRIVE",

    "ST": "STREET",
    "STREET": "STREET",

    "AVE": "AVENUE",
    "AV": "AVENUE",
    "AVENUE": "AVENUE",

    "CT": "COURT",
    "COURT": "COURT",

    "CIR": "CIRCLE",
    "CIRCLE": "CIRCLE",

    "PL": "PLACE",
    "PLACE": "PLACE",

    "TERMINAL": "TERMINAL",  # keep if you ever see it
    "EXT": "EXTENSION",
    "EXTENSION": "EXTENSION",

    "RAMP": "RAMP",
    "LOOP": "LOOP",
    "BYPASS": "BYPASS",
    "SPUR": "SPUR",
    "FRONTAGE": "FRONTAGE",
}

# ----------------------------
# Canonical multi-token suffix map (tail-only)
# Keys are token tuples (already split by whitespace)
# Values are canonical token tuples (usually 2 tokens)
# ----------------------------
SUFFIX_2: Dict[Tuple[str, str], Tuple[str, str]] = {
    # STATE RD / STATE ROAD → STATE ROAD (canonical 2-token suffix)
    ("STATE", "RD"): ("STATE", "ROAD"),
    ("STATE", "ROAD"): ("STATE", "ROAD"),

    # Examples you called out
    ("FRONTAGE", "RD"): ("FRONTAGE", "ROAD"),
    ("FRONTAGE", "ROAD"): ("FRONTAGE", "ROAD"),

    # Common MA-ish patterns
    ("ACCESS", "RD"): ("ACCESS", "ROAD"),
    ("SERVICE", "RD"): ("SERVICE", "ROAD"),
}

# If you later decide "STATE ROUTE" variants exist, add them explicitly:
# ("STATE","RT") -> ("STATE","ROUTE") etc.

_PUNCT_RE = re.compile(r"[,\.;:]+$")

def _clean_token(tok: str) -> str:
    """
    Very conservative token cleanup:
    - strips trailing punctuation ,.;:
    - keeps internal hyphens (EAST ST-HERON ...)
    - does NOT remove numbers
    """
    t = tok.strip().upper()
    t = _PUNCT_RE.sub("", t)
    return t

def normalize_suffix_tail(street: str) -> str:
    """
    Tail-only suffix normalization.
    Given a street string (already reasonably normalized upstream),
    rewrite ONLY the final suffix tokens (1 or 2 tokens) deterministically.

    Examples:
      "PARK TE"   -> "PARK TERRACE"
      "MAIN LA"   -> "MAIN LANE"
      "OCEAN BL"  -> "OCEAN BOULEVARD"
      "RIVER CTR" -> "RIVER CENTER"
      "I 93 RAMP" -> "I 93 RAMP" (unchanged, but supported if last token is RAMP)
      "FOO STATE RD" -> "FOO STATE ROAD"
    """
    if not street:
        return street

    raw_tokens = street.split()
    if not raw_tokens:
        return street

    tokens = [_clean_token(t) for t in raw_tokens]
    n = len(tokens)

    # Try 2-token suffix first
    if n >= 2:
        last2 = (tokens[-2], tokens[-1])
        if last2 in SUFFIX_2:
            canon2 = SUFFIX_2[last2]
            out = tokens[:-2] + [canon2[0], canon2[1]]
            return " ".join(out)

    # Then 1-token suffix
    last1 = tokens[-1]
    if last1 in SUFFIX_1:
        canon1 = SUFFIX_1[last1]
        out = tokens[:-1] + [canon1]
        return " ".join(out)

    return " ".join(tokens)

def normalize_suffix_tail_tokens(tokens: List[str]) -> List[str]:
    """
    Same logic as normalize_suffix_tail but operates on a token list.
    Useful if your v1_7 code is already token-based.
    """
    if not tokens:
        return tokens
    t = [_clean_token(x) for x in tokens]
    n = len(t)

    if n >= 2:
        last2 = (t[-2], t[-1])
        if last2 in SUFFIX_2:
            canon2 = SUFFIX_2[last2]
            return t[:-2] + [canon2[0], canon2[1]]

    last1 = t[-1]
    if last1 in SUFFIX_1:
        canon1 = SUFFIX_1[last1]
        return t[:-1] + [SUFFIX_1[last1]]

    return t
