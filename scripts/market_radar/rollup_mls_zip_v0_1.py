#!/usr/bin/env python3
"""
Market Radar v0.1 — MLS-only ZIP rollups (inventory-aware absorption)

Inputs:
  - MLS normalized listings NDJSON (streaming)

Computes, by ZIP + propertyType:
  - Active inventory snapshot as-of date
  - Closed sales counts within windows (30/90/180/365) based on dates.saleDate
  - Absorption_30d = sales_30d / active_inventory
  - Months of inventory (MOI) = active_inventory / sales_30d * (30/window_days)

Notes:
  - "Active inventory" is as-of snapshot:
      status == "active"
      listDate <= as_of
      offMarketDate is None or offMarketDate > as_of
  - "Closed sales" are:
      saleDate within (as_of - window_days, as_of]
      and pricing.salePrice is numeric > 0
  - This is MLS-only. Deeds can be added as an extra channel later.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple


def parse_iso_date(s: Optional[str]) -> Optional[dt.date]:
    if not s or not isinstance(s, str):
        return None
    # Accept YYYY-MM-DD or ISO timestamps with Z
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return dt.date.fromisoformat(s[:10])
    except Exception:
        return None
    return None


def as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        if math.isfinite(float(x)):
            return float(x)
        return None
    if isinstance(x, str):
        t = x.strip().replace(",", "").replace("$", "")
        if t == "":
            return None
        try:
            v = float(t)
            if math.isfinite(v):
                return v
        except Exception:
            return None
    return None


def median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    vals_sorted = sorted(vals)
    n = len(vals_sorted)
    mid = n // 2
    if n % 2 == 1:
        return float(vals_sorted[mid])
    return float((vals_sorted[mid - 1] + vals_sorted[mid]) / 2.0)


def safe_get(d: Dict[str, Any], path: List[str]) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def iter_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mls", required=True, help="Path to MLS normalized listings NDJSON")
    ap.add_argument("--out", required=True, help="Output NDJSON rollup (ZIP rows)")
    ap.add_argument("--audit", required=True, help="Audit JSON output")
    ap.add_argument("--asof", required=True, help="As-of date YYYY-MM-DD")
    ap.add_argument("--windows", default="30,90,180,365", help="Comma list of window days")
    ap.add_argument("--status_active", default="active", help="MLS status string considered active")
    args = ap.parse_args()

    mls_path = args.mls
    out_path = args.out
    audit_path = args.audit
    asof = dt.date.fromisoformat(args.asof)
    windows = [int(x.strip()) for x in args.windows.split(",") if x.strip()]

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(audit_path) or ".", exist_ok=True)

    # rollups keyed by (zip, propertyType)
    # store only what we need
    roll: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # audit counters
    audit = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mls_in": mls_path,
        "as_of": args.asof,
        "windows_days": windows,
        "rows_scanned": 0,
        "rows_bad_json": 0,
        "rows_missing_zip": 0,
        "rows_missing_propertyType": 0,
        "rows_active_asof": 0,
        "rows_closed_with_saleDate": 0,
        "rows_closed_with_salePrice": 0,
        "rows_skipped_status_unknown": 0,
        "notes": [
            "Active inventory is computed as-of snapshot using status/listDate/offMarketDate.",
            "Closed sales use dates.saleDate within window and pricing.salePrice > 0.",
        ],
    }

    def ensure_bucket(z: str, pt: str) -> Dict[str, Any]:
        k = (z, pt)
        if k not in roll:
            roll[k] = {
                "as_of": args.asof,
                "geo": {"level": "ZIP", "zip": z},
                "property_type": pt,
                "inventory": {"mls_active": 0},
                "windows": {f"{w}d": {"mls_closed_sales": 0, "median_sale_price": None} for w in windows},
                "_prices": {f"{w}d": [] for w in windows},   # internal
                "_active_list_prices": [],                  # internal
                "metrics": {},                              # filled later
            }
        return roll[k]

    # streaming scan
    with open(mls_path, "r", encoding="utf-8") as f:
        for line in f:
            audit["rows_scanned"] += 1
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                audit["rows_bad_json"] += 1
                continue

            # required dims
            z = safe_get(r, ["address", "zip"])
            if not z or not isinstance(z, str) or z.strip() == "":
                audit["rows_missing_zip"] += 1
                continue
            z = z.strip()

            pt = r.get("propertyType")
            if not pt or not isinstance(pt, str):
                audit["rows_missing_propertyType"] += 1
                continue
            pt = pt.strip()

            status = r.get("status")
            status = status.strip().lower() if isinstance(status, str) else None

            dates = r.get("dates") if isinstance(r.get("dates"), dict) else {}
            list_date = parse_iso_date(dates.get("listDate"))
            off_market_date = parse_iso_date(dates.get("offMarketDate"))
            sale_date = parse_iso_date(dates.get("saleDate"))

            pricing = r.get("pricing") if isinstance(r.get("pricing"), dict) else {}
            list_price = as_float(pricing.get("listPrice"))
            sale_price = as_float(pricing.get("salePrice"))

            bucket = ensure_bucket(z, pt)

            # Active snapshot as-of
            # status must be active; list_date must exist and be <= asof
            # off_market_date if exists must be > asof
            if status == args.status_active.lower():
                if list_date and list_date <= asof and (off_market_date is None or off_market_date > asof):
                    bucket["inventory"]["mls_active"] += 1
                    audit["rows_active_asof"] += 1
                    if isinstance(list_price, (int, float)) and list_price and list_price > 0:
                        bucket["_active_list_prices"].append(float(list_price))
                # else: not active as-of window
            elif status is None:
                audit["rows_skipped_status_unknown"] += 1

            # Closed sales windows (sale_date + sale_price)
            if sale_date:
                audit["rows_closed_with_saleDate"] += 1
                if isinstance(sale_price, (int, float)) and sale_price and sale_price > 0:
                    audit["rows_closed_with_salePrice"] += 1
                    for w in windows:
                        start = asof - dt.timedelta(days=w)
                        # window is (start, asof] inclusive of asof
                        if start < sale_date <= asof:
                            keyw = f"{w}d"
                            bucket["windows"][keyw]["mls_closed_sales"] += 1
                            bucket["_prices"][keyw].append(float(sale_price))

    # finalize medians + metrics
    for (z, pt), b in roll.items():
        # median active list price (optional, useful for context)
        active_med = median(b["_active_list_prices"])
        if active_med is not None:
            b["inventory"]["median_active_list_price"] = active_med
        else:
            b["inventory"]["median_active_list_price"] = None

        # medians per window + absorption/moI
        for w in windows:
            keyw = f"{w}d"
            b["windows"][keyw]["median_sale_price"] = median(b["_prices"][keyw])

        active = b["inventory"]["mls_active"]
        sales30 = b["windows"].get("30d", {}).get("mls_closed_sales", 0)

        # True absorption uses 30d by default (standard)
        b["metrics"]["absorption_30d"] = (sales30 / active) if active > 0 else None
        # months of inventory (30d): active / sales30 * 1 month
        b["metrics"]["months_of_inventory_30d"] = (active / sales30) if sales30 > 0 else None

        # remove internal price caches
        b.pop("_prices", None)
        b.pop("_active_list_prices", None)

    # write NDJSON
    wrote = 0
    with open(out_path, "w", encoding="utf-8") as out:
        for _, b in sorted(roll.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            out.write(json.dumps(b, ensure_ascii=False) + "\n")
            wrote += 1

    audit["zip_type_rows_written"] = wrote
    audit["distinct_zips"] = len(set(k[0] for k in roll.keys()))
    audit["distinct_property_types"] = len(set(k[1] for k in roll.keys()))
    audit["output"] = out_path

    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:", out_path)
    print("[done] audit:", audit_path)
    print("zip_type_rows:", wrote, "distinct_zips:", audit["distinct_zips"])


if __name__ == "__main__":
    main()
