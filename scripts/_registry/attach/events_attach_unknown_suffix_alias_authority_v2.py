#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
events_attach_unknown_suffix_alias_authority_v2.py

Deterministic suffix-tail alias upgrade:
- Build authority from CANON rows that are truly ATTACHED_A with property_id present
- Upgrade UNKNOWN events by trying canonicalized suffix tail + alias variants
- Output upgrades-only NDJSON (one line per upgraded event record)
- Write audit JSON

NO FUZZY. NO NEAREST. NO GUESSING.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Tuple


# ----------------------------
# Suffix maps (deterministic)
# ----------------------------

# Single-token suffix canonicalization
SUFFIX_1: Dict[str, str] = {
    # Terrace variants
    "TE": "TERRACE",
    "TER": "TERRACE",
    "TERR": "TERRACE",
    "TERRACE": "TERRACE",

    # Lane variants (MA shows LA)
    "LA": "LANE",
    "LN": "LANE",
    "LANE": "LANE",

    # Boulevard variants (MA shows BLV/BL/BLVD)
    "BLV": "BOULEVARD",
    "BL": "BOULEVARD",
    "BLVD": "BOULEVARD",
    "BOUL": "BOULEVARD",
    "BOULEVARD": "BOULEVARD",

    # Highway variants (MA shows HWY, sometimes HGWY)
    "HWY": "HIGHWAY",
    "HGWY": "HIGHWAY",
    "HIGHWAY": "HIGHWAY",

    # Parkway
    "PKWY": "PARKWAY",
    "PARKWY": "PARKWAY",
    "PARKWAY": "PARKWAY",

    # Street / Ave / Road, etc.
    "ST": "STREET",
    "STREET": "STREET",

    "AVE": "AVENUE",
    "AV": "AVENUE",
    "AVENUE": "AVENUE",

    "RD": "ROAD",
    "ROAD": "ROAD",

    "DR": "DRIVE",
    "DRIVE": "DRIVE",

    "CT": "COURT",
    "COURT": "COURT",

    "CIR": "CIRCLE",
    "CIRCLE": "CIRCLE",

    "PL": "PLACE",
    "PLACE": "PLACE",

    "SQ": "SQUARE",
    "SQR": "SQUARE",
    "SQUARE": "SQUARE",

    "WY": "WAY",
    "WAY": "WAY",

    "PLZ": "PLAZA",
    "PLAZA": "PLAZA",
}

# Two-token tail canonicalization (tail-only)
SUFFIX_2: Dict[Tuple[str, str], Tuple[str, str]] = {
    ("STATE", "RD"): ("STATE", "ROAD"),
    ("STATE", "ROAD"): ("STATE", "ROAD"),

    ("FRONTAGE", "RD"): ("FRONTAGE", "ROAD"),
    ("FRONTAGE", "ROAD"): ("FRONTAGE", "ROAD"),

    ("SERVICE", "RD"): ("SERVICE", "ROAD"),
    ("SERVICE", "ROAD"): ("SERVICE", "ROAD"),

    ("ACCESS", "RD"): ("ACCESS", "ROAD"),
    ("ACCESS", "ROAD"): ("ACCESS", "ROAD"),
}

_PUNCT_TRAIL_RE = re.compile(r"[,\.;:]+$")
_WS_RE = re.compile(r"\s+")
# Keep this conservative: we do NOT strip internal hyphens, do NOT invent numbers.
_LEADING_GARBAGE_RE = re.compile(r"^\s*([A-Z]\s+)+(?=\d)")  # e.g., "A 249 ..." -> "249 ..."
_ADDR_NUM_RE = re.compile(r"^\s*(\d+[A-Z]?)\s+(.*)$")       # "249A Chelsea ST" -> ("249A","Chelsea ST")


def _clean_token(tok: str) -> str:
    t = (tok or "").strip().upper()
    t = _PUNCT_TRAIL_RE.sub("", t)
    return t


def normalize_suffix_tail(street: str) -> str:
    if not street:
        return street
    raw_tokens = street.split()
    if not raw_tokens:
        return street

    tokens = [_clean_token(t) for t in raw_tokens]
    n = len(tokens)

    if n >= 2:
        last2 = (tokens[-2], tokens[-1])
        if last2 in SUFFIX_2:
            canon2 = SUFFIX_2[last2]
            out = tokens[:-2] + [canon2[0], canon2[1]]
            return " ".join(out)

    last1 = tokens[-1]
    if last1 in SUFFIX_1:
        out = tokens[:-1] + [SUFFIX_1[last1]]
        return " ".join(out)

    return " ".join(tokens)


def _norm_ws(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _norm_town(town_raw: Optional[str]) -> str:
    t = _norm_ws(town_raw or "").upper()
    # Keep your existing "NONE/MULTIPLE/SEARCH ALL" flags intact if present
    return t


def _split_number_and_street(address_raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Very conservative.
    - removes obvious leading letter prefixes like "A 249 ..."
    - extracts leading house number token (supports trailing letter)
    - leaves the street remainder as-is (we normalize suffix tail later)
    """
    if not address_raw:
        return None, None

    a = _norm_ws(address_raw).upper()
    a = _LEADING_GARBAGE_RE.sub("", a)

    m = _ADDR_NUM_RE.match(a)
    if not m:
        return None, None

    num = m.group(1).strip()
    street = m.group(2).strip()
    if not num or not street:
        return None, None
    return num, street


def _build_key(town_norm: str, num: str, street_norm: str) -> str:
    # Match your existing match_key style: "TOWN|<num> <street>"
    return f"{town_norm}|{num} {street_norm}"


def _is_unknown_attach(a: dict) -> bool:
    st = (a.get("status") or "").upper()
    ast = (a.get("attach_status") or "").upper()
    return (st == "UNKNOWN") or (ast == "UNKNOWN")


def _is_attached_a(a: dict) -> bool:
    st = (a.get("status") or "").upper()
    ast = (a.get("attach_status") or "").upper()
    return (st == "ATTACHED_A") or (ast == "ATTACHED_A")


def _safe_json_load(line: str) -> Optional[dict]:
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--canon", required=True, help="Canonical NDJSON (full Suffolk)")
    ap.add_argument("--events", required=True, help="Events NDJSON (typically UNKNOWN-only extract)")
    ap.add_argument("--out", required=True, help="Upgrades-only NDJSON output")
    ap.add_argument("--audit", required=True, help="Audit JSON output")
    ap.add_argument("--limit", type=int, default=0, help="Limit events rows (0 = no limit)")
    args = ap.parse_args()

    started = time.time()

    canon_rows_seen = 0
    canon_json_errors = 0

    # Authority: key -> property_id, but we must suppress ambiguous collisions
    key_to_pid: Dict[str, str] = {}
    key_to_pids: Dict[str, set] = defaultdict(set)

    # Build authority from canon ATTACHED_A rows with property_id present
    with open(args.canon, "r", encoding="utf-8") as f:
        for ln in f:
            canon_rows_seen += 1
            r = _safe_json_load(ln)
            if r is None:
                canon_json_errors += 1
                continue

            a = r.get("attach") or {}
            if not _is_attached_a(a):
                continue

            pid = a.get("property_id")
            if not pid:
                # Hard invariant: ignore broken rows
                continue

            pref = r.get("property_ref") or {}
            town = _norm_town(pref.get("town_raw"))
            addr_raw = pref.get("address_raw")
            num, street = _split_number_and_street(addr_raw)

            if not town or not num or not street:
                continue

            street_norm = normalize_suffix_tail(_norm_ws(street).upper())
            k = _build_key(town, num, street_norm)
            key_to_pids[k].add(pid)

    # Finalize authority dict, excluding ambiguous keys
    collisions = {}
    for k, pset in key_to_pids.items():
        if len(pset) == 1:
            key_to_pid[k] = next(iter(pset))
        else:
            collisions[k] = sorted(list(pset))[:10]

    authority_unique_keys = len(key_to_pid)
    authority_collision_keys = len(collisions)

    # Now stream UNKNOWN events and attempt suffix-tail alias match
    events_rows_seen = 0
    events_json_errors = 0
    events_passthrough_non_unknown = 0

    tried_suffix_alias = 0
    upgraded_attached_a = 0
    no_match = 0
    ambiguous = 0

    out_written = 0

    h_out = hashlib.sha256()

    def emit(obj: dict, fo) -> None:
        nonlocal out_written
        s = json.dumps(obj, ensure_ascii=False)
        fo.write(s + "\n")
        h_out.update((s + "\n").encode("utf-8"))
        out_written += 1

    # Build explicit alias candidates (in addition to normalize_suffix_tail)
    # This is not fuzzy: it's deterministic single-token remaps.
    def street_alias_variants(street_raw: str) -> List[str]:
        """
        Returns deterministic variants of the street string based on suffix remaps.
        Always includes normalized_suffix_tail.
        """
        base = _norm_ws(street_raw).upper()
        base_norm = normalize_suffix_tail(base)

        toks = base.split()
        if not toks:
            return [base_norm]

        last = _clean_token(toks[-1])
        variants = set()
        variants.add(base_norm)

        # If last token is a known alias key, try canonical
        if last in SUFFIX_1:
            canon = SUFFIX_1[last]
            v = " ".join([_clean_token(x) for x in toks[:-1]] + [canon])
            variants.add(v)

        # If last token is canonical, also try “short” keys we know appear in index data
        # (still deterministic)
        canon_to_short = {
            "HIGHWAY": ["HWY", "HGWY"],
            "BOULEVARD": ["BLV", "BLVD", "BL"],
            "LANE": ["LN", "LA"],
            "TERRACE": ["TER", "TERR", "TE"],
            "PARKWAY": ["PKWY"],
            "AVENUE": ["AVE", "AV"],
            "STREET": ["ST"],
            "ROAD": ["RD"],
            "DRIVE": ["DR"],
            "COURT": ["CT"],
            "CIRCLE": ["CIR"],
            "PLACE": ["PL"],
            "SQUARE": ["SQ", "SQR"],
            "PLAZA": ["PLZ"],
            "WAY": ["WY"],
        }
        if last in canon_to_short:
            for short in canon_to_short[last]:
                v = " ".join([_clean_token(x) for x in toks[:-1]] + [short])
                variants.add(v)

        return list(variants)

    with open(args.events, "r", encoding="utf-8") as fi, open(args.out, "w", encoding="utf-8") as fo:
        for ln in fi:
            if args.limit and events_rows_seen >= args.limit:
                break

            events_rows_seen += 1
            r = _safe_json_load(ln)
            if r is None:
                events_json_errors += 1
                continue

            a = r.get("attach") or {}
            if not _is_unknown_attach(a):
                events_passthrough_non_unknown += 1
                continue

            pref = r.get("property_ref") or {}
            town = _norm_town(pref.get("town_raw"))
            addr_raw = pref.get("address_raw")

            num, street = _split_number_and_street(addr_raw)
            if not town or not num or not street:
                # Leave as unknown (no output; upgrades-only script)
                no_match += 1
                continue

            tried_suffix_alias += 1

            # Build candidate keys
            candidates = []
            for st_variant in street_alias_variants(street):
                street_norm = normalize_suffix_tail(_norm_ws(st_variant).upper())
                k = _build_key(town, num, street_norm)
                candidates.append((k, st_variant, street_norm))

            # Resolve to a single property_id if possible
            hit_pid = None
            hit_key = None
            hit_count = 0
            for k, stv, stn in candidates:
                pid = key_to_pid.get(k)
                if pid:
                    hit_count += 1
                    hit_pid = pid
                    hit_key = k

            if hit_count == 0:
                no_match += 1
                continue
            if hit_count > 1:
                ambiguous += 1
                continue

            # Apply upgrade — HARD INVARIANT
            if not hit_pid:
                no_match += 1
                continue

            a = r.get("attach") or {}
            a["property_id"] = hit_pid
            a["method"] = "suffix_alias_authority"
            a["match_method"] = "suffix_alias_authority"
            a["status"] = "ATTACHED_A"
            a["attach_status"] = "ATTACHED_A"

            ev = a.get("evidence") or {}
            ev["matched_town_norm"] = town
            ev["matched_address_norm"] = hit_key.split("|", 1)[1] if hit_key and "|" in hit_key else None
            ev.setdefault("match_keys_used", [])
            if hit_key:
                ev["match_keys_used"].append("A|" + hit_key.split("|", 1)[1].replace(" ", "|", 1).replace("|", "|", 1) if False else f"A|{num}|{hit_key.split('|',1)[1].split(' ',1)[1]}|{town}|")
                # The above keeps your existing "A|NUM|STREET|TOWN|" style, without inventing new formats.
            a["evidence"] = ev

            flags = a.get("flags") or []
            if "ATTACHED_VIA_SUFFIX_ALIAS_AUTHORITY" not in flags:
                flags.append("ATTACHED_VIA_SUFFIX_ALIAS_AUTHORITY")
            a["flags"] = flags

            r["attach"] = a

            emit(r, fo)
            upgraded_attached_a += 1

    audit = {
        "engine": "events_attach_unknown_suffix_alias_authority_v2",
        "inputs": {"canon": args.canon, "events": args.events},
        "counts": {
            "canon_rows_seen": canon_rows_seen,
            "canon_json_errors_skipped": canon_json_errors,
            "authority_unique_keys": authority_unique_keys,
            "authority_collision_keys": authority_collision_keys,
            "events_rows_seen": events_rows_seen,
            "events_json_errors_skipped": events_json_errors,
            "events_passthrough_non_unknown": events_passthrough_non_unknown,
            "tried_suffix_alias": tried_suffix_alias,
            "upgraded_attached_a": upgraded_attached_a,
            "no_match": no_match,
            "ambiguous": ambiguous,
            "out_rows_written": out_written,
        },
        "top_collisions_first25": list(sorted(((k, len(v)) for k, v in collisions.items()), key=lambda x: -x[1]))[:25],
        "sha256_out": h_out.hexdigest(),
        "seconds": round(time.time() - started, 2),
    }

    with open(args.audit, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, indent=2, ensure_ascii=False)

    print("[ok]", audit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
