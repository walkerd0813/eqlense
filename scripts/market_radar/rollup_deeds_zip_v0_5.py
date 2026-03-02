#!/usr/bin/env python3
"""
rollup_deeds_zip_v0_5.py

Market Radar Layer B (Deeds) rollup to (zip, asset_bucket, window_days)

Key fixes vs v0_4:
- DOES NOT depend on bucket fields inside the Phase4 spine rows.
- Loads ZIP from spine, and asset_bucket from a separate attachment artifact:
    publicData/properties/_attached/phase5_asset_bucket_v1/asset_bucket__v1_1.ndjson (or newer)

ZIP hygiene:
- ZIP must match ^\d{5}$ and not "00000"

No fuzzy logic, no inference beyond deterministic mapping already captured in asset_bucket attachment.
"""

import argparse
import datetime
import json
import os
import re
from collections import defaultdict

ZIP_RE = re.compile(r"^\d{5}$")

def is_valid_zip(z: str) -> bool:
    if not z:
        return False
    z = str(z).strip()
    return bool(ZIP_RE.match(z)) and z != "00000"

def ndjson_iter(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def parse_date_any(v):
    if not v:
        return None
    s = str(v).strip()
    # common forms:
    # 2024-12-31
    # 12-31-2024
    # 12/31/2024
    # 12-31-2024  8:58:00a
    # 2024-12-31T00:00:00Z
    for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    # iso-like
    try:
        return datetime.date.fromisoformat(s[:10])
    except Exception:
        return None

def get_attach_status(ev: dict):
    # dual-shape
    if isinstance(ev.get("attach"), dict):
        st = ev["attach"].get("attach_status") or ev["attach"].get("status")
        if st:
            return st
    return ev.get("attach_status") or (ev.get("attach") or {}).get("attach_status")

def get_property_id(ev: dict):
    return ev.get("property_id") or (ev.get("attach") or {}).get("property_id")

def normalize_bucket(b: str) -> str:
    if not b:
        return None
    s = str(b).strip().upper()
    # allow common variants
    if s in ("SF", "SINGLE_FAMILY", "SINGLE FAMILY", "SFR"):
        return "SF"
    if s in ("MF", "MULTI_FAMILY", "MULTI FAMILY", "2F", "3F", "4F"):
        return "MF"
    if s in ("CONDO", "COND", "CONDOMINIUM", "CONDOMINIUMS"):
        return "CONDO"
    if s in ("LAND", "VACANT", "VACANT LAND"):
        return "LAND"
    if s in ("OTHER", "COMMERCIAL", "MIXED", "INDUSTRIAL"):
        return "OTHER"
    if s == "UNKNOWN":
        return "UNKNOWN"
    # keep unknown bucket labels out; upstream should normalize
    return "OTHER"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deeds", required=True, help="NDJSON deed events (arms-length classified)")
    ap.add_argument("--spine", required=True, help="Phase4 property spine NDJSON (for ZIP lookup)")
    ap.add_argument("--asset_buckets", required=True, help="asset_bucket attachment NDJSON (property_id -> asset_bucket)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--as_of", required=True, help="YYYY-MM-DD")
    ap.add_argument("--county", default=None)
    ap.add_argument("--windows", default="30,90,180,365")
    ap.add_argument("--require_attached_ab", action="store_true", help="Keep only ATTACHED_A / ATTACHED_B events")
    args = ap.parse_args()

    as_of = datetime.date.fromisoformat(args.as_of)
    windows = [int(x) for x in str(args.windows).split(",") if x.strip()]
    windows = sorted(set(windows))

    audit = {
        "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "as_of_date": as_of.isoformat(),
        "inputs": {
            "deeds": args.deeds,
            "spine": args.spine,
            "asset_buckets": args.asset_buckets,
        },
        "windows": windows,
    }

    # PASS 1: read deeds, filter county + attach, collect candidate property_ids
    deeds_rows = 0
    kept = 0
    skipped_county = 0
    skipped_attach = 0
    missing_property_id = 0
    property_ids = set()

    for ev in ndjson_iter(args.deeds):
        deeds_rows += 1
        if args.county and ev.get("county") != args.county:
            skipped_county += 1
            continue
        if args.require_attached_ab:
            st = get_attach_status(ev)
            if st not in ("ATTACHED_A", "ATTACHED_B"):
                skipped_attach += 1
                continue
        pid = get_property_id(ev)
        if not pid:
            missing_property_id += 1
            continue
        property_ids.add(pid)
        kept += 1

    audit["pass1"] = {
        "deeds_rows": deeds_rows,
        "deeds_kept": kept,
        "deeds_skipped_county": skipped_county,
        "deeds_skipped_attach": skipped_attach,
        "deeds_missing_property_id": missing_property_id,
        "unique_property_ids": len(property_ids),
    }

    # PASS 2: scan spine once, build lookup pid -> zip (hygienic)
    spine_rows = 0
    lookup_zip = {}
    spine_missing_zip = 0
    spine_bad_zip = 0

    for row in ndjson_iter(args.spine):
        spine_rows += 1
        pid = row.get("property_id")
        if not pid or pid not in property_ids:
            continue
        z = row.get("zip")
        if not z:
            spine_missing_zip += 1
            continue
        if not is_valid_zip(z):
            spine_bad_zip += 1
            continue
        lookup_zip[pid] = str(z).strip()

    audit["pass2"] = {
        "spine_rows_scanned": spine_rows,
        "lookup_hits": len(property_ids),
        "lookup_zip_size": len(lookup_zip),
        "spine_missing_zip": spine_missing_zip,
        "spine_bad_zip": spine_bad_zip,
    }

    # PASS 2b: read asset_bucket attachment, keep only pids in lookup_zip
    lookup_bucket = {}
    bucket_unknown = 0
    bucket_missing = 0
    bucket_rows = 0
    bucket_source_counts = defaultdict(int)
    bucket_examples_other = []
    bucket_examples_unknown = []

    needed = set(lookup_zip.keys())
    for r in ndjson_iter(args.asset_buckets):
        bucket_rows += 1
        pid = r.get("property_id")
        if not pid or pid not in needed:
            continue
        b = normalize_bucket(r.get("asset_bucket"))
        src = r.get("source") or "unknown"
        bucket_source_counts[src] += 1
        if not b or b == "UNKNOWN":
            bucket_unknown += 1
            if len(bucket_examples_unknown) < 10:
                bucket_examples_unknown.append(str(r.get("asset_bucket")))
            continue
        lookup_bucket[pid] = b
        if b == "OTHER" and len(bucket_examples_other) < 10:
            bucket_examples_other.append(str(r.get("asset_bucket")))

    # anything in needed without a bucket row is missing
    bucket_missing = len(needed) - (len(lookup_bucket) + bucket_unknown)

    audit["pass2b"] = {
        "asset_bucket_rows_scanned": bucket_rows,
        "needed_pids": len(needed),
        "lookup_bucket_size": len(lookup_bucket),
        "bucket_unknown": bucket_unknown,
        "bucket_missing": bucket_missing,
        "bucket_source_counts_top": dict(sorted(bucket_source_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]),
        "samples": {
            "other_bucket_examples": bucket_examples_other,
            "unknown_bucket_examples": bucket_examples_unknown,
        }
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
    skipped_bucket_unknown = 0
    skipped_county2 = 0
    skipped_attach2 = 0

    seen_event_ids = set()

    for ev in ndjson_iter(args.deeds):
        if args.county and ev.get("county") != args.county:
            skipped_county2 += 1
            continue
        if args.require_attached_ab:
            st = get_attach_status(ev)
            if st not in ("ATTACHED_A", "ATTACHED_B"):
                skipped_attach2 += 1
                continue

        pid = get_property_id(ev)
        if not pid:
            skipped_no_lookup += 1
            continue
        z = lookup_zip.get(pid)
        if not z:
            skipped_no_lookup += 1
            continue
        if not is_valid_zip(z):
            skipped_bad_zip += 1
            continue
        b = lookup_bucket.get(pid)
        if not b:
            skipped_bucket_unknown += 1
            continue

        rec = ev.get("recording") or {}
        d = parse_date_any(rec.get("recording_date") or rec.get("recording_date_raw") or rec.get("recorded_at_raw") or rec.get("recorded_at"))
        if not d:
            skipped_no_recording_date += 1
            continue

        age_days = (as_of - d).days
        if age_days < 0:
            skipped_future_dated += 1
            continue

        # arms-length class
        al = (ev.get("arms_length") or {}).get("class")
        # consideration amount
        amt = None
        cons = ev.get("consideration") or {}
        if cons.get("amount") is not None:
            amt = cons.get("amount")
        else:
            ts = ev.get("transaction_semantics") or {}
            if ts.get("price_amount") is not None:
                amt = ts.get("price_amount")

        # unique counting (best-effort)
        eid = ev.get("event_id")
        if eid and eid not in seen_event_ids:
            seen_event_ids.add(eid)
            events_counted_unique += 1

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

                window_increments += 1
                break  # counted into smallest satisfied window

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
                samples = sorted(r["consideration_median_samples"])
                if samples:
                    mid = len(samples) // 2
                    median = samples[mid] if len(samples) % 2 == 1 else (samples[mid-1] + samples[mid]) / 2.0
                else:
                    median = None

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
                        "consideration_median": median,
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
        "skipped_bucket_unknown_or_missing": skipped_bucket_unknown,
        "skipped_county": skipped_county2,
        "skipped_attach": skipped_attach2,
    }
    audit["output"] = {"out": args.out, "rows_written": wrote}

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows_written:", wrote)

if __name__ == "__main__":
    main()
