#!/usr/bin/env python3
"""
Market Radar — Liquidity P01 (v0_1)

Goal
- Compute MLS liquidity-style metrics per (zip, asset_bucket, window_days).

Inputs
- Exploded MLS ZIP rollup NDJSON (one row per zip+bucket+window).

Outputs
- NDJSON rows with liquidity metrics (DOM + pending/UA + withdrawals/off-market) when present.
- Audit JSON with coverage + missing-field counts.
- SHA-256 of output file.

Design notes (doctrine-safe)
- Observed metrics only. No advice, no prediction.
- If a field is not present in the rollup shape, emit null and record missing counters.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
from typing import Dict, Any, Iterator, Optional, Tuple

ZIP_RE = re.compile(r"^\d{5}$")

def is_valid_zip(z: Any) -> bool:
    if z is None:
        return False
    s = str(z).strip()
    return bool(ZIP_RE.match(s)) and s != "00000"

def norm_bucket(b: Any) -> Optional[str]:
    if b is None:
        return None
    s = str(b).strip().upper()
    # Accept both MLS-style buckets and our canonical
    if s in ("SF", "SINGLE", "SINGLE_FAMILY", "SINGLE FAMILY"):
        return "SF"
    if s in ("MF", "MULTI", "MULTI_FAMILY", "MULTI FAMILY", "2F", "3F", "4F"):
        return "MF"
    if s in ("CONDO", "CONDOMINIUM"):
        return "CONDO"
    if s in ("LAND", "LOT"):
        return "LAND"
    if s == "OTHER":
        return "OTHER"
    if s == "UNKNOWN":
        return "UNKNOWN"
    # if input is already like "condo" (lowercase from MLS property_type), keep as-is but upper it
    return s

def ndjson_iter(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            yield json.loads(line)

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def pick_first(d: Dict[str, Any], keys) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None

def coerce_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        if isinstance(x, (int,)):
            return int(x)
        if isinstance(x, float):
            # allow floats that are actually ints
            return int(x)
        s = str(x).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None

def coerce_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def safe_ratio(n: Optional[float], d: Optional[float]) -> Optional[float]:
    if n is None or d is None:
        return None
    if d <= 0:
        return None
    return n / d

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="Exploded MLS rollup NDJSON (zip+bucket+window)")
    ap.add_argument("--out", required=True, help="Output NDJSON")
    ap.add_argument("--audit", required=True, help="Audit JSON")
    ap.add_argument("--as_of", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    as_of_date = args.as_of.strip()
    built_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    audit: Dict[str, Any] = {
        "built_at": built_at,
        "as_of_date": as_of_date,
        "inputs": {"infile": args.infile},
        "mls_scan": {
            "rows_in": 0,
            "rows_written": 0,
            "skipped_bad_zip": 0,
            "skipped_bad_bucket": 0,
            "skipped_bad_window": 0,
            "key_dupes_seen": 0,
            "missing_dom": 0,
            "missing_pending": 0,
            "missing_active": 0,
            "missing_withdrawn": 0,
            "missing_off_market": 0,
        },
        "field_coverage": {
            "dom_keys_seen": {},
            "pending_keys_seen": {},
            "withdrawn_keys_seen": {},
            "off_market_keys_seen": {},
            "active_keys_seen": {},
        }
    }

    # field candidates (robust to rollup schema variations)
    DOM_KEYS = [
        "dom_median", "median_dom", "days_on_market_median", "median_days_on_market",
        "dom_p50", "days_on_market_p50", "dom", "days_on_market"
    ]
    PENDING_KEYS = [
        "mls_pending", "pending", "mls_under_agreement", "under_agreement", "mls_uag", "uag"
    ]
    WITHDRAWN_KEYS = [
        "mls_withdrawn", "withdrawn", "mls_temp_off_market", "temp_off_market", "temporarily_off_market"
    ]
    OFFMARKET_KEYS = [
        "mls_off_market", "off_market", "mls_cancelled", "cancelled", "canceled", "mls_expired", "expired"
    ]
    ACTIVE_KEYS = [
        "mls_active", "active", "active_listings", "inventory_active"
    ]

    seen_keys = set()
    out_rows = []
    key_seen = set()

    for r in ndjson_iter(args.infile):
        audit["mls_scan"]["rows_in"] += 1

        z = r.get("zip") or (r.get("geo") or {}).get("zip")
        b = r.get("asset_bucket") or r.get("bucket") or r.get("property_type")
        w = r.get("window_days")

        if not is_valid_zip(z):
            audit["mls_scan"]["skipped_bad_zip"] += 1
            continue
        b2 = norm_bucket(b)
        if not b2:
            audit["mls_scan"]["skipped_bad_bucket"] += 1
            continue
        w_int = coerce_int(w)
        if w_int is None or w_int <= 0:
            audit["mls_scan"]["skipped_bad_window"] += 1
            continue

        key = (str(z), b2, w_int)
        if key in key_seen:
            audit["mls_scan"]["key_dupes_seen"] += 1
            # keep first, skip dup
            continue
        key_seen.add(key)

        inv = r.get("inventory") or {}
        met = r.get("metrics") or {}

        dom_raw = pick_first(met, DOM_KEYS)
        if dom_raw is None:
            dom_raw = pick_first(inv, DOM_KEYS)
        dom = coerce_float(dom_raw)

        pending_raw = pick_first(inv, PENDING_KEYS)
        if pending_raw is None:
            pending_raw = pick_first(met, PENDING_KEYS)
        pending = coerce_int(pending_raw)

        withdrawn_raw = pick_first(inv, WITHDRAWN_KEYS)
        if withdrawn_raw is None:
            withdrawn_raw = pick_first(met, WITHDRAWN_KEYS)
        withdrawn = coerce_int(withdrawn_raw)

        offm_raw = pick_first(inv, OFFMARKET_KEYS)
        if offm_raw is None:
            offm_raw = pick_first(met, OFFMARKET_KEYS)
        off_market = coerce_int(offm_raw)

        active_raw = pick_first(inv, ACTIVE_KEYS)
        if active_raw is None:
            active_raw = pick_first(met, ACTIVE_KEYS)
        active = coerce_int(active_raw)

        # coverage counters
        if dom is None:
            audit["mls_scan"]["missing_dom"] += 1
        if pending is None:
            audit["mls_scan"]["missing_pending"] += 1
        if withdrawn is None:
            audit["mls_scan"]["missing_withdrawn"] += 1
        if off_market is None:
            audit["mls_scan"]["missing_off_market"] += 1
        if active is None:
            audit["mls_scan"]["missing_active"] += 1

        # compute derived liquidity ratios (null-safe)
        pending_to_active = safe_ratio(pending, active) if pending is not None else None

        pending_share = None
        if pending is not None and active is not None:
            denom = pending + active
            pending_share = safe_ratio(pending, denom)

        withdrawal_pressure = None
        if withdrawn is not None and active is not None:
            denom = withdrawn + active
            withdrawal_pressure = safe_ratio(withdrawn, denom)

        off_market_pressure = None
        if off_market is not None and active is not None:
            denom = off_market + active
            off_market_pressure = safe_ratio(off_market, denom)

        doc = {
            "pillar": "liquidity",
            "p": "P01",
            "layer": "mls",
            "as_of_date": as_of_date,
            "zip": str(z),
            "asset_bucket": b2,
            "window_days": w_int,
            "metrics": {
                "dom_median": dom,
                "mls_active": active,
                "mls_pending_or_uag": pending,
                "mls_withdrawn_or_temp_off_market": withdrawn,
                "mls_off_market_or_cancelled_or_expired": off_market,
                "pending_to_active_ratio": round(pending_to_active, 6) if pending_to_active is not None else None,
                "pending_share": round(pending_share, 6) if pending_share is not None else None,
                "withdrawal_pressure": round(withdrawal_pressure, 6) if withdrawal_pressure is not None else None,
                "off_market_pressure": round(off_market_pressure, 6) if off_market_pressure is not None else None,
            }
        }

        out_rows.append(doc)
        audit["mls_scan"]["rows_written"] += 1

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for doc in out_rows:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    audit["output"] = {"out": args.out, "rows_written": audit["mls_scan"]["rows_written"], "sha256": sha256_file(args.out)}
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] Liquidity P01 complete.")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
