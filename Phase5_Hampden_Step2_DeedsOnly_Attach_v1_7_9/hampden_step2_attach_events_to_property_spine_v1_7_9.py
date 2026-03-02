#!/usr/bin/env python3
"""Hampden STEP 2 v1.7.9 - Attach registry events to the MA Property Spine.

Institutional constraints (kept):
- Only uses deterministic town+address keying; no geospatial "best guess".
- Normalizes both sides consistently; emits UNKNOWN when we cannot defend a match.

Upgrades in v1.7.9 (DEEDS win):
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

RANGE_RE = re.compile(r"^\s*(\d+)\s*[-–]\s*(\d+)\s+(.*)$")

def collapse_ws(s: str) -> str:
    return _WS_RE.sub(" ", s).strip()

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
        town_raw = p.get("town") or p.get("municipality") or p.get("city") or ""
        town_norm = clean_town(town_raw)
        if not town_norm or (allowed_towns and town_norm not in allowed_towns):
            continue

        # Primary address field
        addr_raw = p.get("full_address") or p.get("address") or ""
        addr_norm = clean_addr(addr_raw)
        if not addr_norm:
            continue

        pid = p.get("property_id") or p.get("propertyId")
        if not pid:
            continue

        for av in spine_addr_variants(addr_norm):
            k = make_key(town_norm, av)
            # Don't overwrite first hit (stable)
            if k not in idx:
                idx[k] = pid

        indexed += 1

    meta = {
        "spine_path_input": spine_path,
        "spine_path_resolved": resolved,
        "spine_rows_seen": seen,
        "spine_rows_indexed": indexed,
        "spine_index_keys": len(idx),
    }
    return idx, meta

def attach_one(ev, spine_idx):
    pr = ev.get("property_ref") or {}
    town_norm = clean_town(pr.get("town_raw") or pr.get("town") or "")
    addr_norm = clean_addr(pr.get("address_raw") or pr.get("address") or "")

    if not town_norm or not addr_norm:
        return None, "MISSING_TOWN_OR_ADDRESS", None, town_norm, addr_norm

    # Candidate address list (direct + fallbacks)
    candidates = []

    # 1) direct
    candidates.append((addr_norm, "direct"))

    # 2) unit stripped
    su = strip_unit(addr_norm)
    if su and su != addr_norm:
        candidates.append((su, "strip_unit"))

    # 3) range expansion (includes original range string as first)
    for c in addr_range_candidates(addr_norm):
        if c == addr_norm:
            continue
        candidates.append((c, "range_expand"))
        # also try stripping unit after range expansion (rare but safe)
        c_su = strip_unit(c)
        if c_su and c_su != c:
            candidates.append((c_su, "range_expand"))

    # De-dupe while preserving order
    seen = set()
    uniq = []
    for a, m in candidates:
        k = (a, m)
        if a and k not in seen:
            seen.add(k)
            uniq.append((a, m))

    for a, method in uniq:
        key = make_key(town_norm, a)
        pid = spine_idx.get(key)
        if pid:
            return pid, "ATTACHED_A", method, town_norm, addr_norm

    return None, "UNKNOWN", None, town_norm, addr_norm

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
                pid, status, method, town_norm, addr_norm = attach_one(ev, spine_idx)
                ev["attach"] = {
                    "attach_status": status,
                    "attach_method": method,
                    "town_norm": town_norm,
                    "address_norm": addr_norm,
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
