#!/usr/bin/env python3
"""
Market Radar - ZIP stock denominator (v0_1)

Builds a parcel/property stock denominator by (zip, asset_bucket) from:
- Spine NDJSON (zip comes from spine row)
- Asset bucket attachment NDJSON (bucket comes from attachment; keyed by property_id)

Outputs NDJSON rows:
{
  "layer": "stock",
  "as_of_date": "YYYY-MM-DD",
  "zip": "#####",
  "asset_bucket": "SF|MF|CONDO|LAND|OTHER",
  "metrics": {"property_stock_total": <int>},
  "coverage": {...},
  "provenance": {...}
}

Rules:
- ZIP hygiene: must match ^\\d{5}$ and not "00000"
- Bucket is taken ONLY from asset_bucket attachment; UNKNOWN/missing is excluded but counted in audit.
"""
import argparse, json, os, re, datetime
from collections import defaultdict

ZIP_RE = re.compile(r"^\d{5}$")

def is_valid_zip(z):
    if z is None:
        return False
    z = str(z).strip()
    return bool(ZIP_RE.match(z)) and z != "00000"

def norm_bucket(b):
    if b is None:
        return None
    b = str(b).strip().upper()
    if b in ("SF","MF","CONDO","LAND","OTHER","UNKNOWN"):
        return b
    if b in ("SINGLE","SINGLE_FAMILY","SINGLE FAMILY","SFH"):
        return "SF"
    if b in ("MULTI","MULTIFAMILY","MULTI_FAMILY","MULTI FAMILY"):
        return "MF"
    if b in ("COND","CONDOMINIUM"):
        return "CONDO"
    if b in ("LOT","VACANT","VACANT_LAND","VACANT LAND"):
        return "LAND"
    return "OTHER"

def ndjson_iter(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spine", required=True)
    ap.add_argument("--asset_buckets", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--as_of", default=None)
    args = ap.parse_args()

    as_of_date = args.as_of or datetime.date.today().isoformat()

    audit = {
        "built_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of_date": as_of_date,
        "inputs": {"spine": args.spine, "asset_buckets": args.asset_buckets},
    }

    # PASS 1: buckets map
    bucket_by_pid = {}
    bucket_rows = 0
    bucket_missing_pid = 0
    bucket_counts = defaultdict(int)
    bucket_unknown_rows = 0

    for r in ndjson_iter(args.asset_buckets):
        bucket_rows += 1
        pid = r.get("property_id")
        if not pid:
            bucket_missing_pid += 1
            continue
        b = norm_bucket(r.get("asset_bucket"))
        if b is None:
            bucket_unknown_rows += 1
            continue
        bucket_by_pid[pid] = b
        bucket_counts[b] += 1
        if b == "UNKNOWN":
            bucket_unknown_rows += 1

    audit["pass1_load_buckets"] = {
        "asset_bucket_rows_scanned": bucket_rows,
        "bucket_map_size": len(bucket_by_pid),
        "bucket_missing_property_id": bucket_missing_pid,
        "bucket_counts": dict(sorted(bucket_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "bucket_unknown_rows": bucket_unknown_rows,
    }

    # PASS 2: scan spine -> counts
    counts = defaultdict(int)
    spine_rows = 0
    spine_missing_pid = 0
    spine_missing_zip = 0
    spine_bad_zip = 0
    spine_missing_bucket = 0
    spine_bucket_unknown = 0

    for row in ndjson_iter(args.spine):
        spine_rows += 1
        pid = row.get("property_id")
        if not pid:
            spine_missing_pid += 1
            continue

        z = row.get("zip")
        if z is None or str(z).strip() == "":
            spine_missing_zip += 1
            continue
        z = str(z).strip()
        if not is_valid_zip(z):
            spine_bad_zip += 1
            continue

        b = bucket_by_pid.get(pid)
        if not b:
            spine_missing_bucket += 1
            continue
        if b == "UNKNOWN":
            spine_bucket_unknown += 1
            continue

        counts[(z, b)] += 1

    audit["pass2_scan_spine"] = {
        "spine_rows_scanned": spine_rows,
        "spine_missing_property_id": spine_missing_pid,
        "spine_missing_zip": spine_missing_zip,
        "spine_bad_zip": spine_bad_zip,
        "spine_missing_bucket": spine_missing_bucket,
        "spine_bucket_unknown": spine_bucket_unknown,
        "distinct_zip_bucket_pairs": len(counts),
    }

    # write out
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    wrote = 0
    with open(args.out, "w", encoding="utf-8") as out:
        for (z, b), n in sorted(counts.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            doc = {
                "layer": "stock",
                "as_of_date": as_of_date,
                "zip": z,
                "asset_bucket": b,
                "metrics": {"property_stock_total": int(n)},
                "coverage": {"zip_hygiene": r'zip must match ^\d{5}$ and not 00000', "excluded_bucket_unknown": True},
                "provenance": {"spine_source": os.path.basename(args.spine), "asset_bucket_source": os.path.basename(args.asset_buckets)},
            }
            out.write(json.dumps(doc, ensure_ascii=False) + "\n")
            wrote += 1

    audit["output"] = {"out": args.out, "rows_written": wrote}

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] build_zip_stock_spine_v0_1")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
