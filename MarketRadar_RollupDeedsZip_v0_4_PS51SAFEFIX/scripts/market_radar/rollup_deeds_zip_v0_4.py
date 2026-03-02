#!/usr/bin/env python3
"""
Market Radar - Layer B (Deeds) ZIP rollup (v0_4)

Fixes vs v0_3:
- Counts deeds cumulatively into ALL satisfied windows (30/90/180/365).
- ZIP hygiene gate: zip must match ^\d{5}$ and not "00000".
- Bucket derivation from Phase4 spine: derives asset_bucket (SF/MF/CONDO/LAND) primarily from assessor_best fields.
- Adds audit bucket_source_counts and bucket_unknown/skips to prevent silent shrinkage.
"""

import argparse
import json
import os
import re
from collections import defaultdict, Counter
from datetime import date, datetime

ZIP_RE = re.compile(r"^\d{5}$")

WINDOWS_DEFAULT = [30, 90, 180, 365]

def ndjson_iter(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            yield json.loads(line)

def _parse_date_any(x):
    if not x:
        return None
    s = str(x).strip()
    # already YYYY-MM-DD
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        pass
    # common MM-DD-YYYY
    for fmt in ("%m-%d-%Y", "%m/%d/%Y", "%m-%d-%y", "%m/%d/%y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            continue
    # recorded_at_raw often has datetime-ish
    for fmt in ("%m-%d-%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).date()
        except Exception:
            continue
    return None

def _is_valid_zip(z):
    if z is None:
        return False
    z = str(z).strip()
    if not ZIP_RE.match(z):
        return False
    if z == "00000":
        return False
    return True

def _norm_bucket_text(s: str):
    s = (s or "").strip().lower()
    if not s:
        return None
    # quick text mapping
    if "condo" in s or "condominium" in s:
        return "CONDO"
    if "single" in s and "family" in s:
        return "SF"
    if "two" in s and "family" in s:
        return "MF"
    if "three" in s and "family" in s:
        return "MF"
    if "multi" in s and "family" in s:
        return "MF"
    if "apartment" in s:
        return "MF"
    if "vacant" in s or "land" in s:
        return "LAND"
    # common abbreviations
    if s in ("sf", "single_family", "single family", "singlefamily"):
        return "SF"
    if s in ("mf", "multi_family", "multi family", "multifamily"):
        return "MF"
    if s in ("condo", "condominium"):
        return "CONDO"
    if s in ("land", "vacant"):
        return "LAND"
    return None

def _bucket_from_use_code(use_code):
    """
    Heuristic mapping for MA assessor use codes (best-effort).
    We only need broad buckets: SF / MF / CONDO / LAND.
    """
    if use_code is None:
        return None
    try:
        uc = int(str(use_code).strip())
    except Exception:
        return None

    # Common MA DOR style codes (not guaranteed across all towns, but helpful)
    if uc == 101:
        return "SF"
    if uc == 102:
        return "CONDO"
    if uc in (103,):
        return "SF"  # mobile home-ish; treat as SF for now
    if uc in (104, 105, 106, 107, 108, 109, 111, 112, 113, 114):
        return "MF"
    if uc in (130, 131, 132, 133):
        return "LAND"

    # broader guesses
    if 100 <= uc < 110:
        return "SF"
    if 110 <= uc < 120:
        return "MF"
    if 120 <= uc < 140:
        return "LAND"

    return None

def derive_bucket_from_spine_row(row, bucket_source_counts: Counter):
    """
    Return (bucket, source_key) where bucket is one of SF/MF/CONDO/LAND or None.
    """
    # 1) assessor_best is our primary source
    ab = row.get("assessor_best") or {}
    if isinstance(ab, dict):
        # Try use_code variants
        for k in ("use_code", "useCode", "land_use_code", "landuse_code", "property_use_code", "use"):
            if k in ab and ab.get(k) not in (None, "", 0):
                b = _bucket_from_use_code(ab.get(k))
                if b:
                    bucket_source_counts[f"assessor_best.{k}"] += 1
                    return b, f"assessor_best.{k}"

        # Try descriptive variants
        for k in ("use_desc", "use_description", "land_use", "landuse", "property_class", "class", "style", "property_type"):
            if k in ab and ab.get(k):
                b = _norm_bucket_text(str(ab.get(k)))
                if b:
                    bucket_source_counts[f"assessor_best.{k}"] += 1
                    return b, f"assessor_best.{k}"

        # Try units-based heuristic if present
        units = None
        for k in ("num_units", "units", "unit_count", "living_units"):
            if k in ab and ab.get(k) not in (None, ""):
                try:
                    units = int(float(ab.get(k)))
                except Exception:
                    units = None
                if units is not None:
                    break
        if units is not None and units > 0:
            if units == 1:
                bucket_source_counts["assessor_best.units"] += 1
                return "SF", "assessor_best.units"
            if units >= 2:
                bucket_source_counts["assessor_best.units"] += 1
                return "MF", "assessor_best.units"

    # 2) fallbacks: (rare) top-level fields if they ever appear
    for k in ("asset_bucket", "bucket", "property_type"):
        if row.get(k):
            b = _norm_bucket_text(str(row.get(k))) or _bucket_from_use_code(row.get(k))
            if b:
                bucket_source_counts[f"top.{k}"] += 1
                return b, f"top.{k}"

    return None, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deeds", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--as_of", required=True, help="YYYY-MM-DD")
    ap.add_argument("--county", default=None)
    ap.add_argument("--windows", default=",".join(str(x) for x in WINDOWS_DEFAULT))
    ap.add_argument("--require_attached_ab", action="store_true", help="only count events with ATTACHED_A or ATTACHED_B")
    args = ap.parse_args()

    as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
    windows = [int(x.strip()) for x in str(args.windows).split(",") if x.strip()]
    windows = sorted(set(windows))

    audit = {
        "built_at": datetime.utcnow().isoformat() + "Z",
        "as_of_date": as_of.isoformat(),
        "inputs": {"deeds": args.deeds, "spine": args.spine},
        "windows": windows,
        "pass1": {},
        "pass2": {},
        "pass3": {},
        "output": {}
    }

    # PASS 1: scan deeds, keep candidate property_ids
    deeds_rows = deeds_kept = 0
    deeds_skipped_county = deeds_skipped_attach = deeds_missing_property_id = 0
    pids = set()

    for ev in ndjson_iter(args.deeds):
        deeds_rows += 1
        if args.county and ev.get("county") != args.county:
            deeds_skipped_county += 1
            continue

        if args.require_attached_ab:
            status = (ev.get("attach") or {}).get("attach_status") or (ev.get("attach_status"))
            if status not in ("ATTACHED_A", "ATTACHED_B"):
                deeds_skipped_attach += 1
                continue

        pid = ev.get("property_id") or (ev.get("attach") or {}).get("property_id")
        if not pid:
            deeds_missing_property_id += 1
            continue

        pids.add(pid)
        deeds_kept += 1

    audit["pass1"] = {
        "deeds_rows": deeds_rows,
        "deeds_kept": deeds_kept,
        "deeds_skipped_county": deeds_skipped_county,
        "deeds_skipped_attach": deeds_skipped_attach,
        "deeds_missing_property_id": deeds_missing_property_id,
        "unique_property_ids": len(pids),
    }

    # PASS 2: build lookup(pid -> {zip, bucket}) from spine for those pids only
    spine_rows_scanned = 0
    lookup_hits = 0
    spine_missing_zip = 0
    spine_bad_zip = 0
    spine_missing_bucket = 0
    bucket_source_counts = Counter()

    lookup = {}

    for row in ndjson_iter(args.spine):
        spine_rows_scanned += 1
        pid = row.get("property_id")
        if not pid or pid not in pids:
            continue
        lookup_hits += 1

        z = row.get("zip")
        if not z:
            spine_missing_zip += 1
            continue
        if not _is_valid_zip(z):
            spine_bad_zip += 1
            continue

        b, src = derive_bucket_from_spine_row(row, bucket_source_counts)
        if not b:
            spine_missing_bucket += 1
            continue

        lookup[pid] = {"zip": str(z).strip(), "bucket": b}

    audit["pass2"] = {
        "spine_rows_scanned": spine_rows_scanned,
        "lookup_hits": lookup_hits,
        "lookup_size": len(lookup),
        "spine_missing_zip": spine_missing_zip,
        "spine_bad_zip": spine_bad_zip,
        "spine_missing_bucket": spine_missing_bucket,
        "bucket_source_counts_top": bucket_source_counts.most_common(20),
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
            status = (ev.get("attach") or {}).get("attach_status") or (ev.get("attach_status"))
            if status not in ("ATTACHED_A", "ATTACHED_B"):
                skipped_attach += 1
                continue

        pid = ev.get("property_id") or (ev.get("attach") or {}).get("property_id")
        if not pid or pid not in lookup:
            skipped_no_lookup += 1
            continue

        rec = ev.get("recording") or {}
        d = _parse_date_any(rec.get("recording_date") or rec.get("recording_date_raw") or rec.get("recorded_at_raw"))
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
                samples = sorted(r["consideration_median_samples"])
                if samples:
                    mid = len(samples)//2
                    median = samples[mid] if len(samples)%2==1 else (samples[mid-1]+samples[mid])/2.0
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
        "skipped_county": skipped_county,
        "skipped_attach": skipped_attach,
    }
    audit["output"] = {"out": args.out, "rows_written": wrote}

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows_written:", wrote)
    print("[done] rollup complete.")

if __name__ == "__main__":
    main()
