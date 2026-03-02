#!/usr/bin/env python3
"""
Market Radar - Velocity P01 (ZIP x bucket x window)

Definition (doctrine-safe):
- Velocity is turnover intensity: arms-length transfer count relative to property stock.
- Numerator: deeds_arms_length from Layer B deeds rollup.
- Denominator: property_stock_total from ZIP stock artifact.
- Output is window-true (0..w) and optionally annualized for comparability.

This script does not predict or advise; it reports observed activity for a window.

References (conceptual):
- Turnover rate is transactions relative to housing stock (central-bank / industry definitions).
- "per 1,000 homes" presentation is common in reporting.

Inputs (expected):
- deeds rollup NDJSON rows keyed by: zip, asset_bucket, window_days
  with metrics.deeds_arms_length (and optionally deeds_total etc.)
- stock NDJSON rows keyed by: zip, asset_bucket
  with property_stock_total (or stock_total / property_stock)

Output:
- NDJSON rows keyed by: zip, asset_bucket, window_days, as_of_date
  metrics.velocity.* and coverage.* and qa.*
"""
from __future__ import annotations
import argparse, datetime, json, os, re, hashlib
from typing import Dict, Any, Iterator, Tuple

ZIP_RE = re.compile(r"^\d{5}$")  # raw string avoids escape warnings

def ndjson_iter(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def is_valid_zip(z: Any) -> bool:
    if z is None:
        return False
    zs = str(z).strip()
    return bool(ZIP_RE.match(zs)) and zs != "00000"

def norm_bucket(b: Any) -> str | None:
    if b is None:
        return None
    s = str(b).strip().upper()
    # normalize known variants
    if s in ("CONDO", "CONDOS", "CONDOMINIUM", "CONDOMINIUMS"):
        return "CONDO"
    if s in ("SF", "SINGLE", "SINGLE_FAMILY", "SINGLE FAMILY", "SFR"):
        return "SF"
    if s in ("MF", "MULTI", "MULTI_FAMILY", "MULTI FAMILY", "2-4", "2-4 FAMILY"):
        return "MF"
    if s in ("LAND", "VACANT", "VACANT_LAND", "VACANT LAND", "LOT"):
        return "LAND"
    if s in ("OTHER",):
        return "OTHER"
    if s in ("UNKNOWN", "UNK", ""):
        return "UNKNOWN"
    # pass-through but keep upper
    return s

def safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None

def build_stock_map(stock_path: str) -> Tuple[Dict[Tuple[str,str], int], Dict[str, Any]]:
    stock: Dict[Tuple[str,str], int] = {}
    audit = {
        "rows_in": 0,
        "rows_used": 0,
        "skipped_bad_zip": 0,
        "skipped_bad_bucket": 0,
        "skipped_missing_stock": 0,
        "distinct_keys": 0,
    }
    for r in ndjson_iter(stock_path):
        audit["rows_in"] += 1
        z = r.get("zip") or (r.get("geo") or {}).get("zip")
        b = r.get("asset_bucket") or r.get("bucket") or r.get("assetBucket")
        if not is_valid_zip(z):
            audit["skipped_bad_zip"] += 1
            continue
        b = norm_bucket(b)
        if not b or b == "UNKNOWN":
            audit["skipped_bad_bucket"] += 1
            continue
        # allow a few possible field names
        n = (
            r.get("property_stock_total")
            or r.get("stock_total")
            or r.get("property_stock")
            or (r.get("metrics") or {}).get("property_stock_total")
            or (r.get("metrics") or {}).get("stock_total")
        )
        try:
            n_int = int(n)
        except Exception:
            audit["skipped_missing_stock"] += 1
            continue
        if n_int < 0:
            audit["skipped_missing_stock"] += 1
            continue
        stock[(str(z).zfill(5), b)] = n_int
        audit["rows_used"] += 1

    audit["distinct_keys"] = len(stock)
    return stock, audit

def sample_grade(n_events: int) -> str:
    # simple, explainable gating for reliability
    if n_events >= 25:
        return "A"
    if n_events >= 10:
        return "B"
    if n_events >= 3:
        return "C"
    return "D"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deeds_rollup", required=True, help="Layer B deeds rollup ndjson (zip x bucket x window)")
    ap.add_argument("--stock", required=True, help="ZIP stock ndjson (zip x bucket)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--as_of", required=True, help="YYYY-MM-DD")
    ap.add_argument("--min_stock", type=int, default=30, help="min stock required to compute velocity (else mark insufficient)")
    ap.add_argument("--annualize", action="store_true", help="include annualized turnover (365/w)")
    args = ap.parse_args()

    as_of = datetime.date.fromisoformat(args.as_of)

    stock_map, stock_audit = build_stock_map(args.stock)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    out_rows = 0
    deeds_rows_in = 0
    deeds_rows_used = 0
    skipped_no_stock = 0
    skipped_bad_zip = 0
    skipped_bad_bucket = 0
    skipped_bad_window = 0
    missing_al = 0

    key_dupes = 0
    seen_keys = set()

    with open(args.out, "w", encoding="utf-8") as out:
        for r in ndjson_iter(args.deeds_rollup):
            deeds_rows_in += 1
            z = r.get("zip") or (r.get("geo") or {}).get("zip")
            b = r.get("asset_bucket") or r.get("bucket") or r.get("assetBucket")
            w = r.get("window_days") or r.get("window") or (r.get("meta") or {}).get("window_days")

            if not is_valid_zip(z):
                skipped_bad_zip += 1
                continue
            b = norm_bucket(b)
            if not b or b == "UNKNOWN":
                skipped_bad_bucket += 1
                continue
            try:
                w_int = int(w)
            except Exception:
                skipped_bad_window += 1
                continue
            if w_int <= 0:
                skipped_bad_window += 1
                continue

            stock = stock_map.get((str(z).zfill(5), b))
            if stock is None:
                skipped_no_stock += 1
                continue

            metrics = r.get("metrics") or {}
            al = metrics.get("deeds_arms_length")
            try:
                al_int = int(al) if al is not None else None
            except Exception:
                al_int = None
            if al_int is None:
                missing_al += 1
                al_int = 0

            # core rates
            turnover_rate = (al_int / stock) if stock > 0 else None
            per_1000 = (1000.0 * al_int / stock) if stock > 0 else None

            annualized = None
            if args.annualize and stock > 0:
                annualized = (al_int / stock) * (365.0 / float(w_int))

            suff = (stock >= args.min_stock)
            grade = sample_grade(al_int)

            key = (str(z).zfill(5), b, w_int)
            if key in seen_keys:
                key_dupes += 1
            else:
                seen_keys.add(key)

            doc = {
                "layer": "velocity",
                "version": "p01_v0_1",
                "as_of_date": as_of.isoformat(),
                "zip": str(z).zfill(5),
                "asset_bucket": b,
                "window_days": w_int,
                "metrics": {
                    "velocity": {
                        "deeds_arms_length": al_int,
                        "turnover_rate": round(turnover_rate, 8) if turnover_rate is not None else None,
                        "turnover_per_1000": round(per_1000, 4) if per_1000 is not None else None,
                        "turnover_annualized": round(annualized, 8) if annualized is not None else None,
                    }
                },
                "coverage": {
                    "property_stock_total": stock,
                    "min_stock_required": args.min_stock,
                },
                "qa": {
                    "stock_sufficient": bool(suff),
                    "sample_grade": grade,
                    "note": "Annualized values are for comparability; interpret with sample_grade.",
                },
                "provenance": {
                    "inputs": {
                        "deeds_rollup": args.deeds_rollup,
                        "stock": args.stock,
                    }
                }
            }
            out.write(json.dumps(doc, ensure_ascii=False) + "\n")
            out_rows += 1
            deeds_rows_used += 1

    audit = {
        "built_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "as_of_date": as_of.isoformat(),
        "inputs": {
            "deeds_rollup": args.deeds_rollup,
            "stock": args.stock,
        },
        "config": {
            "min_stock": args.min_stock,
            "annualize": bool(args.annualize),
        },
        "stock_load": stock_audit,
        "deeds_scan": {
            "rows_in": deeds_rows_in,
            "rows_used": deeds_rows_used,
            "skipped_no_stock": skipped_no_stock,
            "skipped_bad_zip": skipped_bad_zip,
            "skipped_bad_bucket": skipped_bad_bucket,
            "skipped_bad_window": skipped_bad_window,
            "missing_deeds_arms_length": missing_al,
            "key_dupes_seen": key_dupes,
        },
        "output": {
            "out": args.out,
            "rows_written": out_rows,
            "sha256": sha256_file(args.out) if os.path.exists(args.out) else None,
        }
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] build velocity P01")
    print(json.dumps({k: audit[k] for k in ("as_of_date","inputs","config","deeds_scan","output")}, indent=2))

if __name__ == "__main__":
    main()
