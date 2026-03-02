#!/usr/bin/env python3
"""
Market Radar v0.2 — Registry Deeds ZIP overlay (Hampden)

Purpose:
  Build ZIP-level counts (and optional median prices) from registry deed events,
  using property_id -> zip from the statewide property spine.

Efficiency:
  - First pass events: collect unique property_ids needed
  - Pass spine once: build map only for those property_ids
  - Second pass events: roll up to ZIP windows

Inputs:
  - Hampden deeds arms-length NDJSON (CURRENT)
  - Property spine NDJSON (phase4 canonical)

Outputs:
  - ZIP overlay NDJSON + audit JSON

Notes:
  - This overlay is ZIP-only (not segmented by propertyType) because the spine sample shows no propertyType.
  - We use recording.recording_date as the event date.
  - We count by arms_length.class: ARMS_LENGTH | NON_ARMS_LENGTH | UNKNOWN
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import statistics
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


def parse_iso_date(s: Optional[str]) -> Optional[dt.date]:
    if not s or not isinstance(s, str):
        return None
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
        try:
            v = float(x)
            return v
        except Exception:
            return None
    if isinstance(x, str):
        t = x.strip().replace(",", "").replace("$", "")
        if not t:
            return None
        try:
            return float(t)
        except Exception:
            return None
    return None


def median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return float(statistics.median(sorted(vals)))


def iter_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="Hampden deeds armslen NDJSON (CURRENT)")
    ap.add_argument("--spine", required=True, help="Property spine NDJSON")
    ap.add_argument("--out", required=True, help="Output NDJSON (ZIP overlay rows)")
    ap.add_argument("--audit", required=True, help="Audit JSON output")
    ap.add_argument("--asof", required=True, help="As-of date YYYY-MM-DD")
    ap.add_argument("--windows", default="30,90,180,365", help="Comma list of window days")
    ap.add_argument("--require_attached", action="store_true", help="Count only attached events (attach_status ATTACHED_*)")
    args = ap.parse_args()

    asof = dt.date.fromisoformat(args.asof)
    windows = [int(x.strip()) for x in args.windows.split(",") if x.strip()]
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.audit) or ".", exist_ok=True)

    audit: Dict[str, Any] = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "events_in": args.events,
        "spine_in": args.spine,
        "as_of": args.asof,
        "windows_days": windows,
        "require_attached": bool(args.require_attached),
        "events_scanned_pass1": 0,
        "events_scanned_pass2": 0,
        "events_missing_property_id": 0,
        "events_no_zip_from_spine": 0,
        "events_missing_recording_date": 0,
        "events_out_of_window": 0,
        "events_counted": 0,
        "unique_property_ids_needed": 0,
        "spine_rows_scanned": 0,
        "spine_hits": 0,
        "zip_rows_written": 0,
    }

    # ---- PASS 1: collect needed property_ids ----
    needed: Set[str] = set()

    for e in iter_ndjson(args.events):
        audit["events_scanned_pass1"] += 1

        if args.require_attached:
            attach_status = e.get("attach_status") or (e.get("attach") or {}).get("attach_status")
            if not (isinstance(attach_status, str) and attach_status.upper().startswith("ATTACHED")):
                continue

        pid = e.get("property_id") or (e.get("attach") or {}).get("property_id")
        if not pid or not isinstance(pid, str):
            audit["events_missing_property_id"] += 1
            continue
        needed.add(pid)

    audit["unique_property_ids_needed"] = len(needed)

    # ---- PASS 1b: scan spine once, build pid->zip only for needed ----
    pid_to_zip: Dict[str, str] = {}

    for p in iter_ndjson(args.spine):
        audit["spine_rows_scanned"] += 1
        pid = p.get("property_id")
        if not pid or not isinstance(pid, str):
            continue
        if pid not in needed:
            continue
        z = p.get("zip")
        if z and isinstance(z, str) and z.strip():
            pid_to_zip[pid] = z.strip()
            audit["spine_hits"] += 1
        # small speed-up: early exit if we've matched all
        if audit["spine_hits"] >= audit["unique_property_ids_needed"]:
            break

    # ---- PASS 2: rollup to ZIP ----
    # structure: per zip -> per window -> counts & prices
    roll: Dict[str, Dict[str, Any]] = {}

    def ensure_zip(z: str) -> Dict[str, Any]:
        if z not in roll:
            roll[z] = {
                "as_of": args.asof,
                "geo": {"level": "ZIP", "zip": z},
                "windows": {f"{w}d": {
                    "registry_total": 0,
                    "registry_arms": 0,
                    "registry_non_arms": 0,
                    "registry_unknown": 0,
                    "median_price_arms": None,
                    "_arms_prices": [],
                } for w in windows},
                "meta": {
                    "source": "registry_deeds_hampden",
                    "date_field": "recording.recording_date",
                    "notes": "ZIP-only overlay (not segmented by propertyType).",
                }
            }
        return roll[z]

    for e in iter_ndjson(args.events):
        audit["events_scanned_pass2"] += 1

        if args.require_attached:
            attach_status = e.get("attach_status") or (e.get("attach") or {}).get("attach_status")
            if not (isinstance(attach_status, str) and attach_status.upper().startswith("ATTACHED")):
                continue

        pid = e.get("property_id") or (e.get("attach") or {}).get("property_id")
        if not pid or not isinstance(pid, str):
            continue

        z = pid_to_zip.get(pid)
        if not z:
            audit["events_no_zip_from_spine"] += 1
            continue

        rec = e.get("recording") if isinstance(e.get("recording"), dict) else {}
        d = parse_iso_date(rec.get("recording_date"))
        if not d:
            audit["events_missing_recording_date"] += 1
            continue

        # classify
        al = e.get("arms_length") if isinstance(e.get("arms_length"), dict) else {}
        cls = al.get("class")
        if isinstance(cls, str):
            cls = cls.upper()
        else:
            cls = "UNKNOWN"

        # price (arms median)
        cons = e.get("consideration") if isinstance(e.get("consideration"), dict) else {}
        amt = cons.get("amount")
        if amt is None:
            ts = e.get("transaction_semantics") if isinstance(e.get("transaction_semantics"), dict) else {}
            amt = ts.get("price_amount")
        price = as_float(amt)

        # window check(s)
        counted_any = False
        for w in windows:
            start = asof - dt.timedelta(days=w)
            if start < d <= asof:
                counted_any = True
                bucket = ensure_zip(z)["windows"][f"{w}d"]
                bucket["registry_total"] += 1
                if cls == "ARMS_LENGTH":
                    bucket["registry_arms"] += 1
                    if price and price > 0:
                        bucket["_arms_prices"].append(float(price))
                elif cls == "NON_ARMS_LENGTH":
                    bucket["registry_non_arms"] += 1
                else:
                    bucket["registry_unknown"] += 1

        if not counted_any:
            audit["events_out_of_window"] += 1
            continue

        audit["events_counted"] += 1

    # finalize medians
    for z, row in roll.items():
        for w in windows:
            keyw = f"{w}d"
            prices = row["windows"][keyw].pop("_arms_prices", [])
            row["windows"][keyw]["median_price_arms"] = median(prices)

    # write ndjson
    wrote = 0
    with open(args.out, "w", encoding="utf-8") as out:
        for z in sorted(roll.keys()):
            out.write(json.dumps(roll[z], ensure_ascii=False) + "\n")
            wrote += 1

    audit["zip_rows_written"] = wrote
    audit["distinct_zips"] = wrote
    audit["pid_zip_map_size"] = len(pid_to_zip)

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("zips:", wrote, "events_counted:", audit["events_counted"], "pid_zip_map:", len(pid_to_zip))


if __name__ == "__main__":
    main()
