#!/usr/bin/env python3
# build_price_discovery_p01_v0_1.py
# Purpose: Price Discovery Integrity (P01) for Market Radar, provenance-safe.
# Inputs: CURRENT_MARKET_RADAR_UNIFIED_ZIP.ndjson (or explicit unified ndjson)
# Output: zip_price_discovery__p01_v0_1.ndjson (+ audit json)
#
# Notes:
# - Uses only already-rollup'd MLS and Deeds window metrics. Does NOT touch raw listings/events.
# - Computes gap between Deeds median consideration and MLS median sale price per (zip,bucket,window).
# - Adds data sufficiency + confidence flags (no over-claiming).
#
# PowerShell-safe / Windows-safe.

import argparse, json, os, hashlib, datetime, math, re
from typing import Any, Dict, Iterator, Optional, Tuple

ZIP_RE = re.compile(r"^\d{5}$")

def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

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

def is_good_zip(z: Any) -> bool:
    if not z:
        return False
    z = str(z).strip()
    if z == "00000":
        return False
    return bool(ZIP_RE.match(z))

def norm_bucket(b: Any) -> Optional[str]:
    if not b:
        return None
    b = str(b).strip().upper()
    # accept both "CONDO" and "condo" etc.
    if b in ("SF","MF","CONDO","LAND","OTHER","UNKNOWN"):
        return b
    # try common aliases
    if b in ("SINGLE","SINGLE_FAMILY","SINGLE FAMILY"):
        return "SF"
    if b in ("MULTI","MULTIFAMILY","MULTI FAMILY"):
        return "MF"
    if b in ("COND","CONDOMINIUM"):
        return "CONDO"
    return b

def get_path(d: Dict[str, Any], *path: str) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

def get_mls_closed_sales(mls: Dict[str, Any]) -> Optional[int]:
    # exploded MLS rollup stores window stats under qa.windows
    for p in [
        ("qa","windows","mls_closed_sales"),
        ("windows","mls_closed_sales"),
        ("metrics","mls_closed_sales"),
        ("metrics","closed_sales"),
    ]:
        v = get_path(mls, *p)
        if isinstance(v, int):
            return v
        if isinstance(v, float) and v.is_integer():
            return int(v)
    return None

def get_mls_median_sale_price(mls: Dict[str, Any]) -> Optional[float]:
    for p in [
        ("qa","windows","median_sale_price"),
        ("windows","median_sale_price"),
        ("metrics","median_sale_price"),
        ("metrics","medianSalePrice"),
    ]:
        v = get_path(mls, *p)
        if isinstance(v, (int,float)) and v is not None:
            return float(v)
    return None

def get_deeds_arms_length(deeds: Dict[str, Any]) -> Optional[int]:
    v = get_path(deeds, "metrics", "deeds_arms_length")
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return None

def get_deeds_cons_median(deeds: Dict[str, Any]) -> Optional[float]:
    v = get_path(deeds, "metrics", "consideration_median")
    if isinstance(v, (int,float)) and v is not None:
        return float(v)
    return None

def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--unified", required=True, help="Unified ZIP NDJSON (Layer D). Typically CURRENT_MARKET_RADAR_UNIFIED_ZIP.ndjson")
    ap.add_argument("--out", required=True, help="Output NDJSON")
    ap.add_argument("--audit", required=True, help="Audit JSON")
    ap.add_argument("--as_of", required=True, help="YYYY-MM-DD")
    ap.add_argument("--min_samples", type=int, default=3, help="Minimum sample count for 'sufficient' flags")
    args = ap.parse_args()

    built_at = utc_now_iso()
    as_of_date = str(args.as_of).strip()

    audit = {
        "built_at": built_at,
        "as_of_date": as_of_date,
        "inputs": {"unified": args.unified},
        "config": {"min_samples": args.min_samples},
        "scan": {
            "rows_in": 0,
            "rows_written": 0,
            "skipped_bad_zip": 0,
            "skipped_bad_bucket": 0,
            "skipped_bad_window": 0,
            "missing_mls_price": 0,
            "missing_deeds_price": 0,
            "missing_mls_samples": 0,
            "missing_deeds_samples": 0,
            "key_dupes_seen": 0,
        }
    }

    seen = set()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    wrote = 0

    with open(args.out, "w", encoding="utf-8") as out_f:
        for row in ndjson_iter(args.unified):
            audit["scan"]["rows_in"] += 1

            z = row.get("zip") or get_path(row, "geo", "zip") or get_path(row, "mls", "zip") or get_path(row, "deeds", "zip")
            b = row.get("asset_bucket") or row.get("bucket") or get_path(row, "mls", "asset_bucket") or get_path(row, "deeds", "asset_bucket")
            w = row.get("window_days") or row.get("window") or get_path(row, "mls", "window_days") or get_path(row, "deeds", "window_days")

            if not is_good_zip(z):
                audit["scan"]["skipped_bad_zip"] += 1
                continue
            b = norm_bucket(b)
            if not b:
                audit["scan"]["skipped_bad_bucket"] += 1
                continue
            if not isinstance(w, int):
                try:
                    w = int(w)
                except Exception:
                    audit["scan"]["skipped_bad_window"] += 1
                    continue
            if w not in (30, 90, 180, 365):
                audit["scan"]["skipped_bad_window"] += 1
                continue

            key = (str(z), str(b), int(w))
            if key in seen:
                audit["scan"]["key_dupes_seen"] += 1
                continue
            seen.add(key)

            mls = row.get("mls") or {}
            deeds = row.get("deeds") or {}

            mls_price = get_mls_median_sale_price(mls)
            deeds_price = get_deeds_cons_median(deeds)

            mls_n = get_mls_closed_sales(mls)
            deeds_n = get_deeds_arms_length(deeds)

            if mls_price is None:
                audit["scan"]["missing_mls_price"] += 1
            if deeds_price is None:
                audit["scan"]["missing_deeds_price"] += 1
            if mls_n is None:
                audit["scan"]["missing_mls_samples"] += 1
            if deeds_n is None:
                audit["scan"]["missing_deeds_samples"] += 1

            # Derived metrics (only if both prices exist)
            gap_abs = None
            gap_pct = None
            if (mls_price is not None) and (deeds_price is not None) and mls_price > 0:
                gap_abs = float(deeds_price) - float(mls_price)
                gap_pct = gap_abs / float(mls_price)

            # Sufficiency + confidence
            mls_sufficient = (mls_n is not None and mls_n >= args.min_samples)
            deeds_sufficient = (deeds_n is not None and deeds_n >= args.min_samples)
            sufficient = bool(mls_sufficient and deeds_sufficient and (gap_pct is not None))

            # Integrity score: 1.0 best, penalize magnitude of gap.
            # 0.0 at >= 25% divergence by default (conservative).
            integrity_score = None
            if gap_pct is not None:
                integrity_score = 1.0 - clamp(abs(gap_pct) / 0.25, 0.0, 1.0)

            flags = []
            if not mls_sufficient:
                flags.append("MLS_THIN")
            if not deeds_sufficient:
                flags.append("DEEDS_THIN")
            if gap_pct is None:
                flags.append("MISSING_PRICE")
            elif abs(gap_pct) >= 0.25:
                flags.append("DIVERGENCE_GT_25PCT")
            elif abs(gap_pct) >= 0.15:
                flags.append("DIVERGENCE_GT_15PCT")

            doc = {
                "as_of_date": as_of_date,
                "zip": str(z),
                "asset_bucket": str(b),
                "window_days": int(w),
                "metrics": {
                    "mls_median_sale_price": mls_price,
                    "mls_closed_sales": mls_n,
                    "deeds_consideration_median": deeds_price,
                    "deeds_arms_length": deeds_n,
                    "deeds_vs_mls_gap_abs": gap_abs,
                    "deeds_vs_mls_gap_pct": gap_pct,
                    "price_discovery_integrity_score": integrity_score,
                    "sufficient_samples": sufficient,
                },
                "flags": flags,
                "provenance": {
                    "inputs": {
                        "unified": os.path.normpath(args.unified),
                    },
                    "method": "gap_between_mls_median_sale_price_and_deeds_consideration_median",
                    "note": "Derived from rollup metrics only; does not infer beyond observed medians."
                }
            }

            out_f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            wrote += 1

    audit["scan"]["rows_written"] = wrote
    audit["output"] = {"out": args.out, "rows_written": wrote, "sha256": sha256_file(args.out)}

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] Price Discovery P01 complete.")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
