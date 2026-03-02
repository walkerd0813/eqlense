#!/usr/bin/env python3
"""Hampden STEP 2 v1.7.5
Attach registry events to the canonical Property Spine using a conservative, confidence-gated locator:
  key = TOWN_NORM | ADDRESS_NORM

Key fixes in v1.7.5
- Normalizes BOTH sides consistently:
  - collapse whitespace
  - strip trailing 'Y' markers
  - remove 'Addr' artifacts from towns (e.g., 'HOLYOKE Addr' -> 'HOLYOKE')
  - minimal street-suffix standardization (AVENUE->AVE, STREET->ST, etc.)
- Avoids poisoned spine keys:
  - skips address_label values that look like a mailing/cross-city label (commas, embedded city/zip)
- Reads spine safely whether it's JSON array or NDJSON.

Outputs NDJSON with attach_status + attach evidence.

NOTE: This is intentionally conservative (ATTACHED_A only). Anything else remains UNKNOWN.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, UTC
from typing import Dict, Iterable, Iterator, Optional, Tuple

# ------------------------
# Normalization helpers
# ------------------------

_RE_WS = re.compile(r"\s+")
_RE_TRAIL_Y = re.compile(r"\s+Y\s*$")
_RE_TOWN_ADDR_ARTIFACT = re.compile(r"\bADDR\b", re.IGNORECASE)

_SUFFIX_MAP = {
    "AVENUE": "AVE",
    "AVE": "AVE",
    "STREET": "ST",
    "ST": "ST",
    "ROAD": "RD",
    "RD": "RD",
    "DRIVE": "DR",
    "DR": "DR",
    "LANE": "LN",
    "LN": "LN",
    "COURT": "CT",
    "CT": "CT",
    "PLACE": "PL",
    "PL": "PL",
    "TERRACE": "TER",
    "TER": "TER",
    "CIRCLE": "CIR",
    "CIR": "CIR",
}

_SUFFIX_RE = re.compile(r"\\b(" + "|".join(sorted(_SUFFIX_MAP.keys(), key=len, reverse=True)) + r")\\b", re.IGNORECASE)


def _as_str(x) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    # guard against dict/list being passed accidentally
    try:
        return str(x)
    except Exception:
        return ""


def collapse_ws(s: str) -> str:
    s = _as_str(s)
    s = s.replace("\u00A0", " ")
    s = _RE_WS.sub(" ", s).strip()
    return s


def normalize_town(town: str) -> str:
    s = collapse_ws(town).upper()
    if not s:
        return ""

    # remove the common PDF artifacts
    s = _RE_TRAIL_Y.sub("", s).strip()
    s = _RE_TOWN_ADDR_ARTIFACT.sub("", s)

    # collapse again after removals
    s = collapse_ws(s)

    # some samples show 'HAMPDEN COUNTY' as a "town" line; keep it (it will simply not match spine)
    return s


def normalize_address(addr: str) -> str:
    s = collapse_ws(addr).upper()
    if not s:
        return ""

    # strip trailing Y marker
    s = _RE_TRAIL_Y.sub("", s).strip()

    # remove extra punctuation we know shows up as separators
    s = s.replace("\t", " ")

    # standardize suffixes (minimal)
    def _suffix_sub(m: re.Match) -> str:
        tok = m.group(1)
        return _SUFFIX_MAP.get(tok.upper(), tok.upper())

    s = _SUFFIX_RE.sub(_suffix_sub, s)

    # collapse whitespace again
    s = collapse_ws(s)

    return s


def looks_poisoned_address_label(s: str) -> bool:
    """Skip address_label values that are clearly not a property street address.

    Examples:
      "RIGHT OF WAY, NEW BEDFORD, MA 01516" (contains commas + another city/zip)
    """
    s = _as_str(s)
    if not s:
        return True

    if "," in s:
        return True

    # address labels with embedded ZIP (5 digits) often mean cross-city labels
    if re.search(r"\b\d{5}(?:-\d{4})?\b", s):
        return True

    # labels that look like just state
    if collapse_ws(s).upper() in {"MA", "MASSACHUSETTS"}:
        return True

    return False


# ------------------------
# Spine reading/indexing
# ------------------------

def iter_spine_rows(spine_path: str) -> Iterator[dict]:
    """Yield spine property objects from either:
    - JSON array file
    - NDJSON file

    The canonical spine you use is a JSON array.
    """
    with open(spine_path, "r", encoding="utf-8") as f:
        # peek first non-ws
        head = ""
        while True:
            ch = f.read(1)
            if not ch:
                return
            if not ch.isspace():
                head = ch
                break

        f.seek(0)
        if head == "[":
            data = json.load(f)
            if isinstance(data, list):
                for obj in data:
                    if isinstance(obj, dict):
                        yield obj
            return

        # NDJSON fallback
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def pick_best_spine_address(p: dict) -> Tuple[str, str, str]:
    """Return (addr_path, addr_key, addr_raw)."""
    # Prefer full_address when present
    for key in ("full_address", "address_full", "address", "street_address"):
        v = p.get(key)
        if isinstance(v, str) and collapse_ws(v):
            return "full_address", key, v

    # Some records have nested address objects; try common structures
    addr_obj = p.get("address")
    if isinstance(addr_obj, dict):
        for key in ("full", "label", "full_address", "street"):
            v = addr_obj.get(key)
            if isinstance(v, str) and collapse_ws(v):
                return "address", key, v

    # address_label is last resort — but often poisoned
    v = p.get("address_label")
    if isinstance(v, str) and collapse_ws(v) and (not looks_poisoned_address_label(v)):
        return "address_label", "address_label", v

    return "", "", ""


def build_spine_index(spine_path: str, allowed_towns: set[str]) -> Tuple[Dict[str, str], dict]:
    idx: Dict[str, str] = {}

    rows_seen = 0
    rows_indexed = 0
    skipped_missing = 0
    skipped_town = 0
    skipped_poisoned = 0

    # for audit samples
    samples = []

    for p in iter_spine_rows(spine_path):
        rows_seen += 1

        property_id = p.get("property_id")
        town_raw = _as_str(p.get("town"))
        town_norm = normalize_town(town_raw)

        if not town_norm:
            skipped_missing += 1
            continue

        # Hampden-only indexing: only index towns we saw in the events (normalized)
        if allowed_towns and town_norm not in allowed_towns:
            skipped_town += 1
            continue

        addr_path, addr_key, addr_raw = pick_best_spine_address(p)
        if not addr_raw:
            skipped_missing += 1
            continue

        if addr_key == "address_label" and looks_poisoned_address_label(addr_raw):
            skipped_poisoned += 1
            continue

        addr_norm = normalize_address(addr_raw)
        if not addr_norm:
            skipped_missing += 1
            continue

        key = f"{town_norm}|{addr_norm}"
        if key not in idx and isinstance(property_id, str) and property_id:
            idx[key] = property_id
            rows_indexed += 1

        if len(samples) < 10:
            samples.append({
                "property_id": property_id,
                "town_path": "town",
                "town_key": "town",
                "town_raw": town_raw,
                "addr_path": addr_path,
                "addr_key": addr_key,
                "addr_raw": addr_raw,
                "key": key,
            })

    meta = {
        "spine_rows_seen": rows_seen,
        "spine_rows_indexed": rows_indexed,
        "spine_index_keys": len(idx),
        "skipped_missing": skipped_missing,
        "skipped_town_not_allowed": skipped_town,
        "skipped_poisoned_address_label": skipped_poisoned,
        "spine_key_examples": samples,
    }

    return idx, meta


# ------------------------
# Events helpers
# ------------------------

def iter_events(events_dir: str) -> Iterator[Tuple[str, dict]]:
    for fn in os.listdir(events_dir):
        if not fn.endswith("_events.ndjson"):
            continue
        path = os.path.join(events_dir, fn)
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if isinstance(e, dict):
                    yield fn, e


def extract_locator(e: dict) -> Tuple[str, str, str, str]:
    """Return (town_raw, addr_raw, town_norm, addr_norm)."""
    pr = e.get("property_ref")
    if isinstance(pr, dict):
        town_raw = _as_str(pr.get("town_raw"))
        addr_raw = _as_str(pr.get("address_raw"))
    else:
        town_raw = ""
        addr_raw = ""

    town_norm = normalize_town(town_raw)
    addr_norm = normalize_address(addr_raw)
    return town_raw, addr_raw, town_norm, addr_norm


def build_allowed_towns_from_events(events_dir: str) -> set[str]:
    towns = set()
    for _, e in iter_events(events_dir):
        _, _, tnorm, _ = extract_locator(e)
        if tnorm:
            towns.add(tnorm)
    return towns


# ------------------------
# Main
# ------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="Events dir (NDJSON files)")
    ap.add_argument("--spine", required=True, help="Property spine path (JSON array or NDJSON)")
    ap.add_argument("--out", required=True, help="Output NDJSON")
    ap.add_argument("--audit", required=True, help="Audit JSON")
    args = ap.parse_args()

    created_at = datetime.now(UTC).replace(microsecond=0).isoformat()

    print(f"[start] Hampden STEP 2 v1.7.5 attach (Hampden-only indexing + normalization fixes)")

    allowed_towns = build_allowed_towns_from_events(args.events)
    print(f"[done] allowed_towns_count: {len(allowed_towns)}")

    spine_idx, spine_meta = build_spine_index(args.spine, allowed_towns)
    print(f"[done] spine_rows_seen: {spine_meta['spine_rows_seen']}")
    print(f"[done] spine_index_keys: {spine_meta['spine_index_keys']}")
    print(f"[done] spine_rows_indexed: {spine_meta['spine_rows_indexed']}")

    counts = {"ATTACHED_A": 0, "UNKNOWN": 0, "MISSING_TOWN_OR_ADDRESS": 0}
    samples_missing = []
    samples_unmatched = []

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as out_f:
        for src, e in iter_events(args.events):
            town_raw, addr_raw, town_norm, addr_norm = extract_locator(e)

            if not town_norm or not addr_norm:
                counts["MISSING_TOWN_OR_ADDRESS"] += 1
                counts["UNKNOWN"] += 1
                if len(samples_missing) < 15:
                    samples_missing.append({
                        "src": src,
                        "event_id": e.get("event_id"),
                        "event_type": e.get("event_type"),
                        "town_raw": town_raw,
                        "address_raw": addr_raw,
                    })
                e["attach_status"] = "UNKNOWN"
                e["attach"] = {
                    "property_id": None,
                    "attach_method": "town_address_exact",
                    "attach_confidence": 0.0,
                    "attach_key": None,
                    "attach_as_of_utc": created_at,
                }
                out_f.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue

            key = f"{town_norm}|{addr_norm}"
            pid = spine_idx.get(key)

            if pid:
                counts["ATTACHED_A"] += 1
                e["attach_status"] = "ATTACHED_A"
                e["attach"] = {
                    "property_id": pid,
                    "attach_method": "town_address_exact",
                    "attach_confidence": 1.0,
                    "attach_key": key,
                    "attach_as_of_utc": created_at,
                }
            else:
                counts["UNKNOWN"] += 1
                e["attach_status"] = "UNKNOWN"
                e["attach"] = {
                    "property_id": None,
                    "attach_method": "town_address_exact",
                    "attach_confidence": 0.0,
                    "attach_key": key,
                    "attach_as_of_utc": created_at,
                }
                if len(samples_unmatched) < 15:
                    samples_unmatched.append({
                        "src": src,
                        "event_id": e.get("event_id"),
                        "event_type": e.get("event_type"),
                        "town_norm": town_norm,
                        "address_norm": addr_norm,
                    })

            out_f.write(json.dumps(e, ensure_ascii=False) + "\n")

    audit = {
        "created_at": created_at + "Z",
        "events_dir": os.path.abspath(args.events),
        "spine_path": os.path.abspath(args.spine),
        "allowed_towns_count": len(allowed_towns),
        "allowed_towns_sample": sorted(list(allowed_towns))[:25],
        "counts": counts,
        "spine_meta": spine_meta,
        "samples": {
            "missing_locator": samples_missing,
            "unmatched_locator": samples_unmatched,
        },
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print(f"[done] attach_status_counts: {counts}")
    print(f"[done] audit: {args.audit}")
    print(f"[done] out: {args.out}")
    print("[next] If MORTGAGE still 0 attached, rerun rebuild mortgage events into _events_v1_4 then rerun this attach.")


if __name__ == "__main__":
    main()
