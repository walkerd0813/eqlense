#!/usr/bin/env python3
"""
Market Radar Layer B (DEEDS) ZIP rollup v0_3
- Streams NDJSON (events + spine)
- County-generic
- Two-pass join:
    Pass 1: collect property_ids from deeds events (optionally county-filtered; optionally require attach_status A/B)
    Pass 2: scan property spine to build lookup {property_id -> {zip, bucket}}
    Pass 3: roll up to (zip, bucket, window_days) with cumulative window counting (30/90/180/365)

Key fixes vs earlier drafts:
- CUMULATIVE windows (an event inside 30d is also inside 90/180/365) => NO "break"
- ZIP hygiene gate: zip must match ^\d{5}$ and not "00000"
- Audit: events_counted_unique + window_increments (so audits don't lie)

Output rows (NDJSON):
{
  "layer": "deeds",
  "as_of_date": "YYYY-MM-DD",
  "window_days": 30|90|180|365,
  "zip": "#####",
  "asset_bucket": "SF|MF|CONDO|LAND|UNKNOWN",
  "metrics": {
     "deeds_total": int,
     "deeds_arms_length": int,
     "deeds_non_arms_length": int,
     "deeds_unknown_arms_length": int,
     "consideration_sum": float,
     "consideration_median": float|null
  }
}
"""

import argparse
import datetime as dt
import json
import os
import re
from collections import defaultdict

ZIP_RE = re.compile(r"^\d{5}$")

DEFAULT_WINDOWS = [30, 90, 180, 365]

def _is_valid_zip(z: str) -> bool:
    if not z:
        return False
    z = str(z).strip()
    if not ZIP_RE.match(z):
        return False
    if z == "00000":
        return False
    return True

def ndjson_iter(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def _parse_date_any(x):
    """
    Accepts:
      - YYYY-MM-DD
      - ISO datetime starting with YYYY-MM-DD
      - MM-DD-YYYY or MM/DD/YYYY
      - 'MM-DD-YYYY  HH:MM:SSa' (registry-style) -> uses date portion
    Returns dt.date or None.
    """
    if not x:
        return None
    s = str(x).strip()
    if not s:
        return None

    # ISO / YYYY-MM-DD...
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return dt.date.fromisoformat(s[:10])
    except Exception:
        pass

    # MM-DD-YYYY or MM/DD/YYYY
    m = re.match(r"^\s*(\d{1,2})[/-](\d{1,2})[/-](\d{4})", s)
    if m:
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return dt.date(yy, mm, dd)
        except Exception:
            return None

    return None

def _median(nums):
    if not nums:
        return None
    s = sorted(nums)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (float(s[mid - 1]) + float(s[mid])) / 2.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deeds", required=True, help="Deeds NDJSON (armslen enriched), ideally already attached to property_id")
    ap.add_argument("--spine", required=True, help="Property spine NDJSON (must contain property_id + zip + asset_bucket)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--as_of", default=None, help="YYYY-MM-DD; default=today (local)")
    ap.add_argument("--county", default=None, help="Optional filter: only include events where ev.county == this")
    ap.add_argument("--require_attached_ab", action="store_true", help="If set: only consider deeds with attach_status ATTACHED_A or ATTACHED_B")
    ap.add_argument("--windows", default=None, help="Comma list like 30,90,180,365 (default)")
    args = ap.parse_args()

    as_of = dt.date.today() if not args.as_of else dt.date.fromisoformat(args.as_of)

    windows = DEFAULT_WINDOWS
    if args.windows:
        windows = [int(x.strip()) for x in str(args.windows).split(",") if x.strip()]
    windows = sorted(set(windows))

    audit = {
        "built_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "as_of_date": as_of.isoformat(),
        "inputs": {"deeds": args.deeds, "spine": args.spine},
        "windows": windows,
        "pass1": {},
        "pass2": {},
        "pass3": {},
        "output": {}
    }

    # PASS 1: collect property_ids present in deeds
    prop_ids = set()
    deeds_rows = 0
    deeds_kept = 0
    deeds_missing_property_id = 0
    deeds_skipped_attach = 0
    deeds_skipped_county = 0

    for ev in ndjson_iter(args.deeds):
        deeds_rows += 1

        if args.county and ev.get("county") != args.county:
            deeds_skipped_county += 1
            continue

        pid = ev.get("property_id") or (ev.get("attach") or {}).get("property_id")
        if not pid:
            deeds_missing_property_id += 1
            continue

        if args.require_attached_ab:
            attach_status = ev.get("attach_status") or (ev.get("attach") or {}).get("status") or (ev.get("attach") or {}).get("attach_status")
            if attach_status not in ("ATTACHED_A", "ATTACHED_B"):
                deeds_skipped_attach += 1
                continue

        prop_ids.add(pid)
        deeds_kept += 1

    audit["pass1"] = {
        "deeds_rows": deeds_rows,
        "deeds_kept": deeds_kept,
        "deeds_skipped_county": deeds_skipped_county,
        "deeds_skipped_attach": deeds_skipped_attach,
        "deeds_missing_property_id": deeds_missing_property_id,
        "unique_property_ids": len(prop_ids)
    }

    # PASS 2: scan spine and build lookup for these property_ids
    lookup = {}  # pid -> {"zip": "#####", "bucket": "..."}
    spine_rows = 0
    spine_hits = 0
    spine_missing_zip = 0
    spine_missing_bucket = 0
    spine_bad_zip = 0

    for row in ndjson_iter(args.spine):
        spine_rows += 1
        pid = row.get("property_id")
        if not pid or pid not in prop_ids:
            continue

        z = row.get("zip") or (row.get("address") or {}).get("zip") or (row.get("property_ref") or {}).get("zip")
        b = row.get("asset_bucket") or row.get("assetBucket") or (row.get("asset") or {}).get("bucket")

        if not z:
            spine_missing_zip += 1
            continue

        z5 = str(z).strip()
        # preserve leading zeros if numeric
        if z5.isdigit() and len(z5) < 5:
            z5 = z5.zfill(5)

        if not _is_valid_zip(z5):
            spine_bad_zip += 1
            continue

        if not b:
            spine_missing_bucket += 1
            b = "UNKNOWN"

        lookup[pid] = {"zip": z5, "bucket": str(b)}
        spine_hits += 1

        if spine_hits >= len(prop_ids):
            break

    audit["pass2"] = {
        "spine_rows_scanned": spine_rows,
        "lookup_hits": spine_hits,
        "lookup_size": len(lookup),
        "spine_missing_zip": spine_missing_zip,
        "spine_bad_zip": spine_bad_zip,
        "spine_missing_bucket": spine_missing_bucket
    }

    # PASS 3: roll up deeds into (zip, bucket, window)
    def win_key(days): return f"{days}d"

    counts = defaultdict(lambda: defaultdict(lambda: {
        "deeds_total": 0,
        "deeds_arms_length": 0,
        "deeds_non_arms_length": 0,
        "deeds_unknown_arms_length": 0,
        "consideration_sum": 0.0,
        "consideration_median_samples": [],
    }))

    events_counted_unique = 0
    window_increments = 0
    skipped_no_lookup = 0
    skipped_no_recording_date = 0
    skipped_future_dated = 0
    skipped_bad_zip = 0
    skipped_county = 0
    skipped_attach = 0

    for ev in ndjson_iter(args.deeds):
        if args.county and ev.get("county") != args.county:
            skipped_county += 1
            continue

        if args.require_attached_ab:
            attach_status = ev.get("attach_status") or (ev.get("attach") or {}).get("status") or (ev.get("attach") or {}).get("attach_status")
            if attach_status not in ("ATTACHED_A", "ATTACHED_B"):
                skipped_attach += 1
                continue

        pid = ev.get("property_id") or (ev.get("attach") or {}).get("property_id")
        if not pid:
            continue

        if pid not in lookup:
            skipped_no_lookup += 1
            continue

        rec = ev.get("recording") or {}
        d = _parse_date_any(
            rec.get("recording_date")
            or rec.get("recording_date_raw")
            or rec.get("recorded_at_raw")
            or rec.get("recorded_at")
        )
        if not d:
            skipped_no_recording_date += 1
            continue

        age_days = (as_of - d).days
        if age_days < 0:
            skipped_future_dated += 1
            continue

        z = lookup[pid]["zip"]
        b = lookup[pid]["bucket"]

        if not _is_valid_zip(z):
            skipped_bad_zip += 1
            continue

        al = (ev.get("arms_length") or {}).get("class")
        amt = None
        cons = ev.get("consideration") or {}
        if cons.get("amount") is not None:
            amt = cons.get("amount")
        else:
            ts = ev.get("transaction_semantics") or {}
            if ts.get("price_amount") is not None:
                amt = ts.get("price_amount")

        counted_any = False
        for w in windows:
            if age_days <= w:
                k = win_key(w)
                row = counts[(z, b)][k]
                row["deeds_total"] += 1
                if al == "ARMS_LENGTH":
                    row["deeds_arms_length"] += 1
                elif al == "NON_ARMS_LENGTH":
                    row["deeds_non_arms_length"] += 1
                else:
                    row["deeds_unknown_arms_length"] += 1

                if amt is not None and isinstance(amt, (int, float)) and amt > 0:
                    row["consideration_sum"] += float(amt)
                    row["consideration_median_samples"].append(float(amt))

                counted_any = True
                window_increments += 1

        if counted_any:
            events_counted_unique += 1

    # write output
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    wrote = 0
    with open(args.out, "w", encoding="utf-8") as out:
        for (z, b), winmap in counts.items():
            for w in windows:
                k = win_key(w)
                r = winmap.get(k)
                if not r:
                    continue
                med = _median(r["consideration_median_samples"])

                doc = {
                    "layer": "deeds",
                    "as_of_date": as_of.isoformat(),
                    "window_days": w,
                    "zip": z,
                    "asset_bucket": b,
                    "metrics": {
                        "deeds_total": r["deeds_total"],
                        "deeds_arms_length": r["deeds_arms_length"],
                        "deeds_non_arms_length": r["deeds_non_arms_length"],
                        "deeds_unknown_arms_length": r["deeds_unknown_arms_length"],
                        "consideration_sum": round(r["consideration_sum"], 2),
                        "consideration_median": med,
                    }
                }
                out.write(json.dumps(doc, ensure_ascii=False) + "\n")
                wrote += 1

    audit["pass3"] = {
        "events_counted_unique": events_counted_unique,
        "window_increments": window_increments,
        "skipped_no_lookup": skipped_no_lookup,
        "skipped_no_recording_date": skipped_no_recording_date,
        "skipped_future_dated": skipped_future_dated,
        "skipped_bad_zip": skipped_bad_zip,
        "skipped_county": skipped_county,
        "skipped_attach": skipped_attach
    }
    audit["output"] = {"out": args.out, "rows_written": wrote}

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] rollup_deeds_zip_v0_3")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
