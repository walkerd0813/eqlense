import argparse
import json
import os
from datetime import datetime, timezone
from collections import defaultdict

def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mls", required=True, help="Layer A MLS rollup ndjson")
    ap.add_argument("--deeds", required=True, help="Layer B Deeds rollup ndjson")
    ap.add_argument("--out", required=True, help="Output ndjson")
    ap.add_argument("--audit", required=True, help="Audit json")
    ap.add_argument("--asof", default=None, help="as_of_date YYYY-MM-DD (default: today UTC)")
    args = ap.parse_args()

    as_of_date = args.asof or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    built_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Index rows by (zip, asset_bucket, window_days)
    idx = {}
    audit = {
        "built_at": built_at,
        "as_of_date": as_of_date,
        "inputs": {"mls": args.mls, "deeds": args.deeds},
        "rows_in": {"mls": 0, "deeds": 0},
        "rows_out": 0,
        "distinct_keys": 0,
        "coverage": {"only_mls": 0, "only_deeds": 0, "both": 0},
        "dupes": {"mls": 0, "deeds": 0},
        "missing_key_fields": {"mls": 0, "deeds": 0},
    }

    def k_from_row(r):
        z = r.get("zip")
        b = r.get("asset_bucket")
        w = r.get("window_days")
        if not z or not b or w is None:
            return None
        return (str(z), str(b), int(w))

    # Load MLS
    mls_map = {}
    for r in read_ndjson(args.mls):
        audit["rows_in"]["mls"] += 1
        k = k_from_row(r)
        if k is None:
            audit["missing_key_fields"]["mls"] += 1
            continue
        if k in mls_map:
            audit["dupes"]["mls"] += 1
        mls_map[k] = r

    # Load Deeds
    deeds_map = {}
    for r in read_ndjson(args.deeds):
        audit["rows_in"]["deeds"] += 1
        k = k_from_row(r)
        if k is None:
            audit["missing_key_fields"]["deeds"] += 1
            continue
        if k in deeds_map:
            audit["dupes"]["deeds"] += 1
        deeds_map[k] = r

    keys = sorted(set(mls_map.keys()) | set(deeds_map.keys()))
    audit["distinct_keys"] = len(keys)

    out_rows = []
    for k in keys:
        z, b, w = k
        m = mls_map.get(k)
        d = deeds_map.get(k)

        has_mls = m is not None
        has_deeds = d is not None

        if has_mls and has_deeds:
            audit["coverage"]["both"] += 1
        elif has_mls:
            audit["coverage"]["only_mls"] += 1
        else:
            audit["coverage"]["only_deeds"] += 1

        row = {
            "zip": z,
            "asset_bucket": b,
            "window_days": w,
            "as_of_date": as_of_date,

            "inputs": {
                "layerA_mls": {
                    "source_file": args.mls,
                    "source_kind": "rollup_zip",
                    "version": "v0_1"
                },
                "layerB_deeds": {
                    "source_file": args.deeds,
                    "source_kind": "rollup_zip",
                    "version": "v0_2"
                }
            },

            "coverage": {
                "has_mls": has_mls,
                "has_deeds": has_deeds,
                "mls_row_count": 1 if has_mls else 0,
                "deeds_row_count": 1 if has_deeds else 0
            },

            # Carry through exactly; do NOT transform fields
            "mls": {
                "metrics": (m.get("metrics") if has_mls else {}),
                "qa": (m.get("qa") if has_mls else {})
            },
            "deeds": {
                "metrics": (d.get("metrics") if has_deeds else {}),
                "qa": (d.get("qa") if has_deeds else {})
            },

            # Reserved: only safe placeholders
            "unified": {
                "metrics": {
                    "notes": "Layer D is provenance-safe container; indicators built later"
                }
            },

            "meta": {
                "schema_version": "market_radar.zip_unified.v0_1",
                "built_at": built_at
            }
        }

        out_rows.append(row)

    write_ndjson(args.out, out_rows)
    audit["rows_out"] = len(out_rows)

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] build_zip_unified_v0_1")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
