import argparse
import json
import os
from datetime import datetime, timezone

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

def _first_present(r, candidates):
    for c in candidates:
        if c in r and r.get(c) not in (None, "", []):
            return r.get(c)
    return None

def _coerce_int(x):
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None

def k_from_row(r, kind, audit):
    """
    Build key (zip, asset_bucket, window_days) from varying shapes.
    We DO NOT transform values beyond basic coercions; this is just for joining.
    """
    # zip candidates
    z = _first_present(r, ["zip", "address.zip", "addressZip"])  # dotted paths not supported; kept for doc
    if z is None:
        # try nested address dict if present
        addr = r.get("address") or {}
        if isinstance(addr, dict):
            z = addr.get("zip") or addr.get("zipcode")
    if z is None:
        audit["missing_key_fields"][kind] += 1
        return None

    # bucket/type candidates
    b = _first_present(r, ["asset_bucket", "assetBucket", "asset_type", "assetType", "property_type", "propertyType"])
    if b is None:
        # sometimes rollups stash it under meta or qa
        meta = r.get("meta") or {}
        if isinstance(meta, dict):
            b = meta.get("asset_bucket") or meta.get("asset_type") or meta.get("propertyType")
    if b is None:
        audit["missing_key_fields"][kind] += 1
        return None

    # window candidates
    w = _first_present(r, ["window_days", "windowDays", "window", "days", "lookback_days"])
    w = _coerce_int(w)
    if w is None:
        # sometimes stored in meta
        meta = r.get("meta") or {}
        if isinstance(meta, dict):
            w = _coerce_int(meta.get("window_days") or meta.get("window"))
    if w is None:
        audit["missing_key_fields"][kind] += 1
        return None

    return (str(z).strip(), str(b).strip(), int(w))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mls", required=True)
    ap.add_argument("--deeds", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--asof", default=None)
    args = ap.parse_args()

    as_of_date = args.asof or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    built_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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
        "key_preview": {
            "mls_first_key": None,
            "deeds_first_key": None
        }
    }

    mls_map = {}
    for r in read_ndjson(args.mls):
        audit["rows_in"]["mls"] += 1
        k = k_from_row(r, "mls", audit)
        if k is None:
            continue
        if audit["key_preview"]["mls_first_key"] is None:
            audit["key_preview"]["mls_first_key"] = {"zip": k[0], "bucket": k[1], "window_days": k[2]}
        if k in mls_map:
            audit["dupes"]["mls"] += 1
        mls_map[k] = r

    deeds_map = {}
    for r in read_ndjson(args.deeds):
        audit["rows_in"]["deeds"] += 1
        k = k_from_row(r, "deeds", audit)
        if k is None:
            continue
        if audit["key_preview"]["deeds_first_key"] is None:
                       audit["key_preview"]["deeds_first_key"] = {"zip": k[0], "bucket": k[1], "window_days": k[2]}
        if k in deeds_map:
            audit["dupes"]["deeds"] += 1
        deeds_map[k] = r

    keys = sorted(set(mls_map.keys()) | set(deeds_map.keys()))
    audit["distinct_keys"] = len(keys)

    out_rows = []
    for (z, b, w) in keys:
        m = mls_map.get((z, b, w))
        d = deeds_map.get((z, b, w))
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
                "layerA_mls": {"source_file": args.mls, "source_kind": "rollup_zip"},
                "layerB_deeds": {"source_file": args.deeds, "source_kind": "rollup_zip"}
            },

            "coverage": {
                "has_mls": has_mls,
                "has_deeds": has_deeds,
                "mls_row_count": 1 if has_mls else 0,
                "deeds_row_count": 1 if has_deeds else 0
            },

            # carry-through, provenance-safe
            "mls": {
                "metrics": (m.get("metrics") if has_mls else {}),
                "qa": (m.get("qa") if has_mls else {})
            },
            "deeds": {
                "metrics": (d.get("metrics") if has_deeds else {}),
                "qa": (d.get("qa") if has_deeds else {})
            },

            "unified": {
                "metrics": {
                    "notes": "Layer D container only; indicators computed later"
                }
            },

            "meta": {
                "schema_version": "market_radar.zip_unified.v0_2",
                "built_at": built_at
            }
        }
        out_rows.append(row)

    write_ndjson(args.out, out_rows)
    audit["rows_out"] = len(out_rows)

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] build_zip_unified_v0_2")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
