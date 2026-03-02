#!/usr/bin/env python3
"""Hampden STEP 2 v1.7.12 - Attach registry events to the MA Property Spine.

Institutional constraints (kept):
- Only uses deterministic town+address keying; no geospatial "best guess".
- Normalizes both sides consistently; emits UNKNOWN when we cannot defend a match.

Upgrades in v1.7.12 (DEEDS win):
- Town cleanup (strip 'Addr', trailing 'Y')
- Address cleanup: collapse whitespace, strip trailing 'Y', remove quotes
- Minimal suffix standardization (ST/RD/AVE/DR/LN/CT/PL/TER/CIR/BLVD/HWY)
- Direction normalization (NORTH->N, SOUTH->S, EAST->E, WEST->W)
- Unit stripping (UNIT/APT/#/STE/SUITE) as a fallback matching method
- Address-range expansion (e.g., 151-153 CATHARINE ST) with safe caps
- Spine index stores a few variants (direct + stripped unit)

Outputs:
- events_attached.ndjson where each input event gets:
  - property_id (when attached)
  - attach_status: ATTACHED_A | UNKNOWN | MISSING_TOWN_OR_ADDRESS
  - attach_method: direct | strip_unit | range_expand
"""

import argparse
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone

# ------------------------ Normalization ------------------------

_TOWN_TRASH_RE = re.compile(r"\b(ADDR|ADDRESS)\b", re.IGNORECASE)
_TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")

DIR_MAP = {
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
}

SUFFIX_MAP = {
    # common
    "AVENUE": "AVE", "AVE": "AVE",
    "STREET": "ST",  "ST": "ST",
    "ROAD": "RD",    "RD": "RD",
    "DRIVE": "DR",   "DR": "DR",
    "LANE": "LN",    "LN": "LN",
    "LA": "LN",      # registry shorthand
    "COURT": "CT",   "CT": "CT",
    "PLACE": "PL",   "PL": "PL",
    "TERRACE": "TER", "TERR": "TER", "TER": "TER",
    "CIRCLE": "CIR", "CIR": "CIR",
    "BOULEVARD": "BLVD", "BLVD": "BLVD",
    "HIGHWAY": "HWY", "HWY": "HWY", "HGY": "HWY",
}

UNIT_RE = re.compile(
    r"\s+(?:UNIT|APT|APARTMENT|STE|SUITE|#)\s*[A-Z0-9\-]+\s*$",
    re.IGNORECASE,
)

RANGE_RE = re.compile(r"^\s*(\d+)\s*[-â€“]\s*(\d+)\s+(.*)$")


STREET_TYPE_ALIASES = {
  "st":  ["street"],
  "street": ["st"],
  "rd":  ["road"],
  "road": ["rd"],
  "dr":  ["drive"],
  "drive": ["dr"],
  "ave": ["avenue"],
  "avenue": ["ave"],
  "blvd": ["boulevard"],
  "boulevard": ["blvd"],
  "ln": ["lane"],
  "lane": ["ln"],
  "ct": ["court"],
  "court": ["ct"],
  "pl": ["place"],
  "place": ["pl"],
  "pkwy": ["parkway"],
  "parkway": ["pkwy"],
  "cir": ["circle"],
  "circle": ["cir"],
  "ter": ["terrace"],
  "terrace": ["ter"],
  "hwy": ["highway"],
  "highway": ["hwy"],
}


import re

RE_RANGE = re.compile(r"\b\d+\s*-\s*\d+\b")
RE_UNIT  = re.compile(r"\b(UNIT|APT|APARTMENT|#|STE|SUITE|FL|FLOOR)\b")
RE_LOT   = re.compile(r"\b(LOT|LOT\s+NO\.?|PAR\s+[A-Z]|PARCEL)\b")
RE_MULTI_ADDR_HINT = re.compile(r"\b(AND|&)\b|\b\d+\s+\w+.*\b\d+\s+\w+", re.IGNORECASE)

def detect_unknown_bucket(town_raw, addr_raw, legal_desc_raw, raw_block):
    """
    Institution-grade diagnostic buckets. NO guessing / no attachment changes.
    Returns an enum-like string.
    """
    tr = (town_raw or "").strip()
    ar = (addr_raw or "").strip().upper()
    ld = (legal_desc_raw or "").strip().upper()
    rb = (raw_block or "").strip().upper()

    if not tr or not ar:
        return "MISSING_TOWN_OR_ADDRESS"

    # Multi-address hints: raw_block often contains multiple address tokens for one deed
    # We keep this conservative: only flag when we see multiple obvious street numbers or separators.
    if rb:
        # two+ street numbers in same block often indicates assemblage/portfolio deed
        nums = re.findall(r"\b\d{1,6}\b", rb)
        if len(nums) >= 2 and (";" in rb or " / " in rb or " & " in rb or " AND " in rb):
            return "MULTI_ADDRESS_IN_ONE_EVENT"

    if RE_RANGE.search(ar):
        return "ADDRESS_RANGE_STYLE"

    if RE_UNIT.search(ar):
        return "UNIT_APT_SUFFIX_PRESENT"

    # Lot/parcel style (no street number but has LOT/PAR)
    if RE_LOT.search(ar) or RE_LOT.search(ld):
        return "LEGAL_DESC_LOT_PARCEL_STYLE"

    return "OTHER_KEY_MISMATCH"
def street_type_alias_candidates(addr_norm: str):
    """
    Deterministic street-type swaps at the LAST token only.
    Example: '315 REGENCY PARK DR' -> '315 REGENCY PARK DRIVE'
    Only returns variants, never returns empty.
    """
    if not addr_norm:
        return []
    parts = addr_norm.strip().split()
    if len(parts) < 2:
        return []
    last = parts[-1].lower()
    if last not in STREET_TYPE_ALIASES:
        return []
    out = []
    for alt in STREET_TYPE_ALIASES[last]:
        out.append(" ".join(parts[:-1] + [alt.upper()]))
    return out
def collapse_ws(s: str) -> str:
    return _WS_RE.sub(" ", s).strip()


# --- v1.7.12 additions: suffix aliases + better unit + compact range parsing ---
_SUFFIX_ALIAS = {
    "LA": ["LN", "LANE"],
    "LN": ["LANE"],
    "HGY": ["HWY", "HIGHWAY"],
    "HWY": ["HIGHWAY"],
    "TERR": ["TER", "TERRACE"],
    "TER": ["TERRACE"],
}

_UNIT_TAIL_RE = re.compile(r"(?:\s+(?:UNIT|APT|APARTMENT|#)\s*[A-Z0-9\-]+)$", re.I)
_UNIT_CODE_TAIL_RE = re.compile(r"(?:\s+[A-Z]{1,2}-\d+)$|(?:\s+\d+[A-Z]$)|(?:\s+[A-Z]\d+$)", re.I)

def strip_unit_more(addr_norm: str) -> str:
    if not addr_norm:
        return ""
    a = addr_norm
    a2 = _UNIT_TAIL_RE.sub("", a).strip()
    if a2 != a:
        return collapse_ws(a2)
    parts = a.split()
    if len(parts) >= 3 and _UNIT_CODE_TAIL_RE.search(a):
        last = parts[-1].upper()
        if last not in SUFFIX_MAP and last not in DIR_MAP:
            a2 = _UNIT_CODE_TAIL_RE.sub("", a).strip()
            return collapse_ws(a2)
    return a

def suffix_alias_candidates(addr_norm: str):
    if not addr_norm:
        return []
    parts = addr_norm.split()
    if len(parts) < 2:
        return []
    last = parts[-1].upper().rstrip(".")
    if last not in _SUFFIX_ALIAS:
        return []
    base = parts[:-1]
    return [" ".join(base + [alt]) for alt in _SUFFIX_ALIAS[last]]

_RANGE_COMPACT_RE = re.compile(r"^(\d+)-(\d+)$")

def expand_range_token(tok: str):
    m = _RANGE_COMPACT_RE.match(tok)
    if not m:
        return None
    a, b = m.group(1), m.group(2)
    start = int(a)
    if len(b) < len(a):
        end = int(a[: len(a)-len(b)] + b)  # "481-4" => 484
    else:
        end = int(b)
    if end < start or (end - start) > 20:
        return None
    return list(range(start, end + 1))

def range_expand_addresses(addr_norm: str):
    if not addr_norm:
        return []
    parts = addr_norm.split()
    nums = expand_range_token(parts[0]) if parts else None
    if not nums:
        return []
    tail = parts[1:]
    return [" ".join([str(n)] + tail) for n in nums]

def clean_town(town_raw: str) -> str:
    if not town_raw:
        return ""
    t = town_raw
    t = t.replace('"', '')
    t = _TRAILING_Y_RE.sub("", t)
    t = _TOWN_TRASH_RE.sub("", t)
    t = collapse_ws(t)
    return t.upper()

def normalize_direction_tokens(tokens):
    if not tokens:
        return tokens
    out = []
    for tok in tokens:
        u = tok.upper()
        if u in DIR_MAP:
            out.append(DIR_MAP[u])
        else:
            out.append(tok)
    return out

def normalize_suffix(tokens):
    if not tokens:
        return tokens
    # Only normalize the final token (street suffix) when it matches known set
    last = tokens[-1].upper()
    if last in SUFFIX_MAP:
        tokens[-1] = SUFFIX_MAP[last]
    return tokens

def clean_addr(addr_raw: str) -> str:
    if not addr_raw:
        return ""
    a = addr_raw
    a = a.replace('"', '')
    a = _TRAILING_Y_RE.sub("", a)
    a = collapse_ws(a)
    # Keep uppercase for consistent keys
    tokens = a.split(" ")
    tokens = normalize_direction_tokens(tokens)
    tokens = normalize_suffix(tokens)
    a2 = " ".join(tokens)
    return a2.upper()

def strip_unit(addr_norm: str) -> str:
    return strip_unit_more(addr_norm)

# legacy (kept for reference)
def _strip_unit_legacy(addr_norm: str) -> str:
    if not addr_norm:
        return ""
    return UNIT_RE.sub("", addr_norm).strip()

def addr_range_candidates(addr_norm: str, max_expand: int = 6):
    """Return list of candidate addresses for a numeric range.

    Always includes the original range string (addr_norm) as first candidate.
    Then adds expanded numeric addresses with the same street tail.

    Safe caps:
      - only expand ranges where (end-start) <= 20
      - limit produced candidates to max_expand total (including original)
    """
    m = RANGE_RE.match(addr_norm)
    if not m:
        return [addr_norm]

    start = int(m.group(1))
    end = int(m.group(2))
    tail = m.group(3).strip()
    if end < start:
        start, end = end, start

    if (end - start) > 20:
        # too wide to safely brute-force
        return [addr_norm]

    # Prefer parity step when it makes sense (odd/odd or even/even)
    step = 2 if (start % 2) == (end % 2) else 1

    nums = list(range(start, end + 1, step))
    # Always ensure endpoints are included
    if start not in nums:
        nums.insert(0, start)
    if end not in nums:
        nums.append(end)

    cands = [addr_norm]
    for n in nums:
        cands.append(f"{n} {tail}".upper())
        if len(cands) >= max_expand:
            break
    # De-dupe while preserving order
    seen = set()
    out = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out

def make_key(town_norm: str, addr_norm: str) -> str:
    return f"{town_norm}|{addr_norm}"

# ------------------------ Reading helpers ------------------------

def iter_ndjson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def resolve_spine_to_ndjson(spine_path: str) -> str:
    """Handles CURRENT pointer JSON that contains a properties_ndjson path."""
    if spine_path.lower().endswith(".ndjson"):
        return spine_path
    # CURRENT pointer json
    with open(spine_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    nd = obj.get("properties_ndjson")
    if not nd:
        raise ValueError("SpinePath is not ndjson and lacks properties_ndjson pointer")
    return nd

# ------------------------ Index build + attach ------------------------

def allowed_towns_from_events(events_dir: str):
    towns = set()
    for fn in os.listdir(events_dir):
        if not fn.endswith("_events.ndjson"):
            continue
        for ev in iter_ndjson(os.path.join(events_dir, fn)):
            pr = ev.get("property_ref") or {}
            t = clean_town(pr.get("town_raw") or pr.get("town") or "")
            if t:
                towns.add(t)
    return towns

def spine_addr_variants(addr_norm: str):
    if not addr_norm:
        return []
    v = [addr_norm]
    su = strip_unit(addr_norm)
    if su and su != addr_norm:
        v.append(su)
    # also store a suffix-normalized variant again (idempotent) - helps if input had non-normalized
    v2 = clean_addr(addr_norm)
    if v2 and v2 not in v:
        v.append(v2)
    # de-dupe
    out = []
    seen = set()
    for x in v:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out[:3]

def build_spine_index(spine_path: str, allowed_towns: set):
    resolved = resolve_spine_to_ndjson(spine_path)

    idx = {}
    seen = 0
    indexed = 0

    for p in iter_ndjson(resolved):
        seen += 1
        town_raw = p.get("town") or p.get("jurisdiction_name") or p.get("municipality") or p.get("city") or p.get("source_city") or ""
        town_norm = clean_town(town_raw)
        if not town_norm or (allowed_towns and town_norm not in allowed_towns):
            continue

        # Primary address field
        addr_raw = p.get("full_address") or p.get("address_label") or p.get("address") or ""
        addr_norm = clean_addr(addr_raw)
        if not addr_norm:
            continue

        pid = p.get("property_id") or p.get("propertyId")
        if not pid:
            continue
        for av, _msuf in _addr_variants(addr_norm):
            k = make_key(town_norm, av)
            # Unique-only indexing:
            # - first property_id sets idx[k]
            # - if a different property_id later appears for same key, mark idx[k] = None (ambiguous)
            if k not in idx:
                idx[k] = pid
            else:
                if idx[k] is None:
                    pass
                elif idx[k] != pid:
                    idx[k] = None
        indexed += 1

    meta = {
        "spine_path_input": spine_path,
        "spine_path_resolved": resolved,
        "spine_rows_seen": seen,
        "spine_rows_indexed": indexed,
        "spine_index_keys": len(idx),
    }
    return idx, meta

# ------------------------ Extra Key Fallback Helpers ------------------------
_EL_STREET_TYPES = set([
    "ST","STREET","RD","ROAD","DR","DRIVE","AVE","AV","AVENUE","BLVD","BOULEVARD","LN","LANE",
    "CT","COURT","PL","PLACE","PKWY","PARKWAY","HWY","HIGHWAY","TER","TERRACE","CIR","CIRCLE",
    "WAY","TRL","TRAIL","EXT","EXTN"
])

def _el_strip_rear_prefix(a: str) -> str:
    a = (a or "").strip().upper()
    if a.startswith("REAR OF "):
        return a[len("REAR OF "):].strip()
    if a.startswith("REAR "):
        return a[len("REAR "):].strip()
    return a

def _el_strip_trailing_street_type(a: str) -> str:
    a = (a or "").strip().upper()
    if not a:
        return a
    parts = a.split()
    if len(parts) >= 3 and parts[-1] in _EL_STREET_TYPES:
        return " ".join(parts[:-1]).strip()
    return a
# ---------------------- End Extra Key Fallback Helpers ----------------------

# ------------------------ Address Variant Helpers (v1.7.18) ------------------------
# Purpose: Generate safe, deterministic address variants to improve exact key matching
# without using fuzzy/nearest logic (keeps pipeline defensible).
#
# IMPORTANT: This does NOT "best guess" an address. It only tries alternate normal forms
# that are logically equivalent (unit/lot/extn removal, directional + street-type aliases,
# optional trailing street-type stripping).

import re

_STREET_TYPES = {
    "ST","STREET","RD","ROAD","AVE","AVENUE","BLVD","BOULEVARD","DR","DRIVE","LN","LANE","CT","COURT",
    "PL","PLACE","TER","TERRACE","HWY","HIGHWAY","WAY","PKWY","PARKWAY","CIR","CIRCLE","SQ","SQUARE",
    "PT","POINT","TRL","TRAIL","RTE","ROUTE"
}

_DIR_TO_ABBR = {
    "NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W",
    "NORTHEAST":"NE","NORTHWEST":"NW","SOUTHEAST":"SE","SOUTHWEST":"SW",
}
_ABBR_TO_DIR = {v:k for k,v in _DIR_TO_ABBR.items()}

_TYPE_ALIASES = {
    "AVENUE":"AVE","AVE":"AVE",
    "BOULEVARD":"BLVD","BLVD":"BLVD",
    "STREET":"ST","ST":"ST",
    "ROAD":"RD","RD":"RD",
    "DRIVE":"DR","DR":"DR",
    "LANE":"LN","LN":"LN",
    "COURT":"CT","CT":"CT",
    "PLACE":"PL","PL":"PL",
    "TERRACE":"TER","TER":"TER",
    "CIRCLE":"CIR","CIR":"CIR",
    "PARKWAY":"PKWY","PKWY":"PKWY",
    "HIGHWAY":"HWY","HWY":"HWY",
    "ROUTE":"RTE","RTE":"RTE",
    # MA quirks seen in assessor strings
    "LA":"LN",
    "PKY":"PKWY"
}

_RE_UNIT = re.compile(r"\b(UNIT|APT|APARTMENT|#)\b.*$", re.I)
_RE_LOT  = re.compile(r"\b(LOT|PAR|PARCEL)\b.*$", re.I)
_RE_REAR = re.compile(r"^\s*REAR(\s+OF)?\s+", re.I)
_RE_EXT  = re.compile(r"\b(EXTN|EXT)\b.*$", re.I)

def _collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).strip()

def _strip_trailing_street_type(s: str) -> str:
    parts = _collapse_spaces(s).split(" ")
    if len(parts) >= 2 and parts[-1].upper().rstrip(".") in _STREET_TYPES:
        return " ".join(parts[:-1]).strip()
    return s

def _apply_dir_aliases(s: str):
    parts = _collapse_spaces(s).split(" ")
    outs = set()
    outs.add(" ".join([_DIR_TO_ABBR.get(p.upper(), p) for p in parts]).strip())
    outs.add(" ".join([_ABBR_TO_DIR.get(p.upper(), p) for p in parts]).strip())
    return [o for o in outs if o]

def _apply_type_aliases(s: str):
    parts = _collapse_spaces(s).split(" ")
    if not parts:
        return []
    outs = set()
    last = parts[-1].upper().rstrip(".")
    if last in _TYPE_ALIASES:
        outs.add(" ".join(parts[:-1] + [_TYPE_ALIASES[last]]).strip())
    outs.add(" ".join([_TYPE_ALIASES.get(p.upper().rstrip("."), p) for p in parts]).strip())
    return [o for o in outs if o]

def _addr_variants(addr_norm: str):
    base = _collapse_spaces((addr_norm or "").upper())
    if not base:
        return
    seen = set()
    def y(v, suf):
        v = _collapse_spaces(v.upper())
        if not v or v in seen:
            return
        seen.add(v)
        yield (v, suf)

    yield from y(base, "")

    v = _RE_REAR.sub("", base)
    if v != base: yield from y(v, "strip_rear")

    v = _RE_UNIT.sub("", base)
    if v != base: yield from y(v, "strip_unit")

    v = _RE_LOT.sub("", base)
    if v != base: yield from y(v, "strip_lot")

    v = _RE_EXT.sub("", base)
    if v != base: yield from y(v, "strip_ext")

    for v in _apply_type_aliases(base):
        if v != base: yield from y(v, "street_type_alias")

    for v in _apply_dir_aliases(base):
        if v != base: yield from y(v, "dir_alias")

    v = _strip_trailing_street_type(base)
    if v != base: yield from y(v, "strip_street_type")

    for seed, seed_suf in [(_RE_UNIT.sub("", base),"strip_unit"), (_RE_LOT.sub("", base),"strip_lot"), (_RE_EXT.sub("", base),"strip_ext")]:
        seed = _collapse_spaces(seed)
        if not seed or seed == base:
            continue
        v2 = _strip_trailing_street_type(seed)
        if v2 != seed: yield from y(v2, seed_suf + "+strip_street_type")
        for v3 in _apply_type_aliases(seed):
            if v3 != seed: yield from y(v3, seed_suf + "+street_type_alias")
        for v3 in _apply_dir_aliases(seed):
            if v3 != seed: yield from y(v3, seed_suf + "+dir_alias")

# ---------------------- End Address Variant Helpers (v1.7.18) ----------------------

def attach_one(ev, spine_idx):
    bucket = "OTHER_KEY_MISMATCH"  # default bucket unless refined below
    pr = ev.get("property_ref") or {}
    town_norm = clean_town(pr.get("town_raw") or pr.get("town") or "")
    addr_norm = clean_addr(pr.get("address_raw") or pr.get("address") or "")

    if not town_norm or not addr_norm:
        return None, "MISSING_TOWN_OR_ADDRESS", None, town_norm, addr_norm, "MISSING_TOWN_OR_ADDRESS"

    # Candidate address list (direct + fallbacks)
    candidates = []

        # ---- Candidate generation (v1.7.13) ----
    # Always start from the best available normalized address, but never allow blank address_norm.
    # addr_norm is expected to already be normalized; if it's blank, fall back to a cleaned raw form.
    if not addr_norm:
        # last-resort: use raw address if present, uppercased/trimmed
        addr_norm = (addr_raw or "").strip().upper()

    candidates = []

    # 1) direct
    candidates.append((addr_norm, "direct"))

    # 1b) street-type alias on direct
    for sa in street_type_alias_candidates(addr_norm):
        if sa and sa != addr_norm:
            candidates.append((sa, "direct+street_type_alias"))

    # 2) unit stripped
    su = strip_unit(addr_norm)
    if su and su != addr_norm:
        candidates.append((su, "strip_unit"))
        for sa in suffix_alias_candidates(su):
            if sa and sa != su:
                candidates.append((sa, "strip_unit+suffix_alias"))
        for ta in street_type_alias_candidates(su):
            if ta and ta != su:
                candidates.append((ta, "strip_unit+street_type_alias"))

    # 3) range expansion (safe: keep original out, add only expansions)
    for c in addr_range_candidates(addr_norm):
        if not c or c == addr_norm:
            continue
        candidates.append((c, "range_expand"))

        # range + suffix alias
        for sa in suffix_alias_candidates(c):
            if sa and sa != c:
                candidates.append((sa, "range_expand+suffix_alias"))

        # range + street-type alias
        for ta in street_type_alias_candidates(c):
            if ta and ta != c:
                candidates.append((ta, "range_expand+street_type_alias"))

        # also try stripping unit after range expansion (rare but safe)
        c_su = strip_unit(c)
        if c_su and c_su != c:
            candidates.append((c_su, "range_expand+strip_unit"))
            for ta in street_type_alias_candidates(c_su):
                if ta and ta != c_su:
                    candidates.append((ta, "range_expand+strip_unit+street_type_alias"))

    # De-dupe while preserving order
    seen = set()
    uniq = []
    for a, m in candidates:
        if not a:
            continue
        k = (a, m)
        if k not in seen:
            seen.add(k)
            uniq.append((a, m))

    # Try candidates
    for a, method in uniq:
        key = make_key(town_norm, a)
        pid = spine_idx.get(key)
        if not pid:
            # Fallback 1: strip REAR/REAR OF prefix (exact key only)
            _a1 = _el_strip_rear_prefix(addr_norm)
            if _a1 and _a1 != addr_norm:
                _k1 = make_key(town_norm, _a1) if 'make_key' in globals() else f"{town_norm}|{_a1}"
                pid = spine_idx.get(_k1)
                if pid:
                    addr_norm = _a1
                    method = (method + "+strip_rear") if method else "direct+strip_rear"
                    key = _k1
        if not pid:
            # Fallback 2: strip trailing street-type token (DR/RD/AVE/etc.) (exact key only)
            _a2 = _el_strip_trailing_street_type(addr_norm)
            if _a2 and _a2 != addr_norm:
                _k2 = make_key(town_norm, _a2) if 'make_key' in globals() else f"{town_norm}|{_a2}"
                pid = spine_idx.get(_k2)
                if pid:
                    addr_norm = _a2
                    method = (method + "+strip_street_type") if method else "direct+strip_street_type"
                    key = _k2
        if pid:
            return pid, "ATTACHED_A", method, town_norm, addr_norm, None
    # bucket refinement: distinguish normalization failure from key mismatch
    if (not addr_norm) or (str(addr_norm).strip() == ""):
        return None, "UNKNOWN", None, town_norm, addr_norm, "ADDRESS_NORM_FAILED"
    return None, "UNKNOWN", None, town_norm, addr_norm, bucket

# ------------------------ Main ------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eventsDir", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    allowed_towns = allowed_towns_from_events(args.eventsDir)
    spine_idx, spine_meta = build_spine_index(args.spine, allowed_towns)

    counts = Counter()
    match_methods = Counter()
    events_total = 0

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as w:
        for fn in os.listdir(args.eventsDir):
            if not fn.endswith("_events.ndjson"):
                continue
            src_path = os.path.join(args.eventsDir, fn)
            for ev in iter_ndjson(src_path):
                events_total += 1
                pid, status, method, town_norm, addr_norm, bucket = attach_one(ev, spine_idx)
                ev["attach"] = {
                    "attach_status": status,
                    "attach_method": method,
                    "town_norm": town_norm,
                    "address_norm": addr_norm,
                    "bucket": bucket,
                }
                if pid:
                    ev["property_id"] = pid
                counts[status] += 1
                if method:
                    match_methods[method] += 1
                w.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "events_dir": os.path.abspath(args.eventsDir),
        "spine": spine_meta,
        "allowed_towns_count": len(allowed_towns),
        "allowed_towns_sample": sorted(list(allowed_towns))[:50],
        "events_total": events_total,
        "attach_status_counts": dict(counts),
        "match_methods": dict(match_methods),
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(f"[done] allowed_towns_count: {len(allowed_towns)}")
    print(f"[done] spine_path_input: {spine_meta['spine_path_input']}")
    print(f"[done] spine_path_resolved: {spine_meta['spine_path_resolved']}")
    print(f"[done] spine_rows_seen: {spine_meta['spine_rows_seen']}")
    print(f"[done] spine_rows_indexed: {spine_meta['spine_rows_indexed']}")
    print(f"[done] spine_index_keys: {spine_meta['spine_index_keys']}")
    print(f"[done] events_total: {events_total}")
    print(f"[done] attach_status_counts: {dict(counts)}")
    if match_methods:
        print(f"[done] match_methods: {dict(match_methods)}")
    print(f"[done] out: {os.path.abspath(args.out)}")
    print(f"[done] audit: {os.path.abspath(args.audit)}")

if __name__ == "__main__":
    main()









