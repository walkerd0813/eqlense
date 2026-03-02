#!/usr/bin/env python3
import argparse, json, os, hashlib, datetime
from typing import Dict, Tuple, Any, List

DEFAULT_CANON = ["SINGLE_FAMILY","CONDO","MF_2_4","MF_5_PLUS","LAND"]

ALIASES = {
    "SFR": "SINGLE_FAMILY",
    "SINGLE": "SINGLE_FAMILY",
    "SINGLE_FAMILY": "SINGLE_FAMILY",
    "CONDO": "CONDO",
    "MF": "MF",
    "MULTI": "MF",
    "MULTIFAMILY": "MF",
    "LAND": "LAND",
}

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def norm_bucket(b: Any) -> str:
    if b is None:
        return ""
    s = str(b).strip()
    if not s:
        return ""
    s = s.upper()
    return ALIASES.get(s, s)

def make_unknown_row(template_row: Dict[str, Any], bucket: str, reason: str) -> Dict[str, Any]:
    out = dict(template_row)
    out["asset_bucket"] = bucket
    inds = out.get("indicators") or {}
    new_inds = {k: {"state": "UNKNOWN", "reason": reason, "value": None} for k in inds.keys()}
    out["indicators"] = new_inds
    out["inputs_snapshot"] = {
        "deeds_arms_length": None,
        "stock_parcels": None,
        "mls_closed": None,
        "note": "UNSUPPORTED_BUCKET placeholder row (row completeness guarantee)"
    }
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--buckets", default=",".join(DEFAULT_CANON))
    args = ap.parse_args()

    canon = [b.strip().upper() for b in args.buckets.split(",") if b.strip()]
    if not canon:
        canon = DEFAULT_CANON

    key_to_rows: Dict[Tuple[str,int,str], Dict[str, Dict[str, Any]]] = {}
    key_to_template: Dict[Tuple[str,int,str], Dict[str, Any]] = {}
    scan_in = 0

    with open(args.infile, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            scan_in += 1
            z_raw = str(o.get("zip") or "")
            z = z_raw.zfill(5) if z_raw.isdigit() else z_raw
            w = int(o.get("window_days") or 0)
            as_of = str(o.get("as_of_date") or args.as_of)
            b = norm_bucket(o.get("asset_bucket"))
            o["asset_bucket"] = b

            key = (z, w, as_of)
            if key not in key_to_rows:
                key_to_rows[key] = {}
                key_to_template[key] = o
            if b and b not in key_to_rows[key]:
                key_to_rows[key][b] = o

    rows: List[Dict[str, Any]] = []
    written = 0
    added = 0

    for key, bucket_map in key_to_rows.items():
        template = key_to_template[key]
        mf_row = bucket_map.get("MF")
        for b in canon:
            if b in bucket_map:
                rows.append(bucket_map[b]); written += 1
            else:
                if b == "MF_2_4" and mf_row is not None:
                    aliased = dict(mf_row)
                    aliased["asset_bucket"] = "MF_2_4"
                    rows.append(aliased); written += 1; added += 1
                else:
                    rows.append(make_unknown_row(template, b, "UNSUPPORTED_BUCKET"))
                    written += 1; added += 1

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for o in rows:
            f.write(json.dumps(o, ensure_ascii=False) + "\n")

    sha = sha256_file(args.out)
    audit = {
        "built_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "state": args.state,
        "as_of_date": args.as_of,
        "infile": os.path.abspath(args.infile),
        "out": os.path.abspath(args.out),
        "sha256": sha,
        "scan": {"rows_in": scan_in, "groups": len(key_to_rows)},
        "canonical_buckets": canon,
        "rows_written": written,
        "rows_added": added,
        "notes": [
            "MF_5_PLUS is placeholder UNKNOWN until upstream supports 5+ segmentation.",
            "MF legacy bucket may be aliased to MF_2_4 (label-only) when present."
        ]
    }
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps({"ok": True, "out": os.path.abspath(args.out), "audit": os.path.abspath(args.audit), "sha256": sha}, indent=2))

if __name__ == "__main__":
    main()
