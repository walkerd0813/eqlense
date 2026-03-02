#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, os, re
import datetime as dt
from typing import Any, Optional, Tuple
from collections import Counter

BUCKETS = {"SF","CONDO","MF_2_4","MF_5P","LAND","COMMERCIAL","OTHER","UNKNOWN"}

def dget(d: Any, path: str) -> Any:
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur

def parse_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        if not s:
            return None
        s = re.sub(r"[^\d]", "", s)
        return int(s) if s else None
    except Exception:
        return None

def code_prefix(code: int) -> str:
    s = str(code)
    if len(s) >= 3:
        return s[:3]
    return s[:2]

def bucket_from_use_code_prefix(use_code: Optional[int]) -> Tuple[str,str,str]:
    if use_code is None:
        return ("UNKNOWN", "C", "missing_use_code")

    p3 = code_prefix(use_code)

    if p3 == "101":
        return ("SF", "A", "use_code_prefix:101")
    if p3 == "102":
        return ("CONDO", "A", "use_code_prefix:102")
    if p3 == "104":
        return ("MF_2_4", "A", "use_code_prefix:104")
    if p3 == "105":
        return ("MF_5P", "A", "use_code_prefix:105")
    if p3 == "930":
        return ("LAND", "B", "use_code_prefix:930")

    if p3 in ("130","131","132","133","134","135","136","137","138","139"):
        return ("COMMERCIAL", "B", f"use_code_prefix:{p3}")
    if str(use_code).startswith("13"):
        return ("COMMERCIAL", "C", "use_code_startswith:13")

    return ("OTHER", "C", "unmapped_use_code")

def extract_use_code_from_best(row: dict) -> Tuple[Optional[int], str, Any]:
    ab = row.get("assessor_best") if isinstance(row.get("assessor_best"), dict) else {}
    raw = dget(ab, "structure.use_code.value")
    return (parse_int(raw), "assessor_best.structure.use_code.value", raw)

def extract_use_code_from_by_source(row: dict) -> Tuple[Optional[int], str, Any]:
    src = row.get("assessor_by_source")
    if not isinstance(src, dict):
        return (None, "assessor_by_source", None)

    # assessor_by_source is typically {sourceName: payloadDict}
    # We scan in stable key order so it's deterministic.
    for k in sorted(src.keys()):
        payload = src.get(k)
        if isinstance(payload, dict):
            raw = dget(payload, "structure.use_code.value")
            u = parse_int(raw)
            if u is not None:
                return (u, f"assessor_by_source.{k}.structure.use_code.value", raw)
    return (None, "assessor_by_source", None)

def extract_use_code_from_fallback_fields(row: dict) -> Tuple[Optional[int], str, Any]:
    ff = row.get("assessor_fallback_fields")
    if not isinstance(ff, dict):
        return (None, "assessor_fallback_fields", None)

    # common fallback keys (keep extendable)
    cand_keys = [
        "use_code", "usecode", "use_code_value",
        "land_use_code", "landuse", "lu_code",
        "property_class", "class_code", "use"
    ]
    for key in cand_keys:
        if key in ff:
            raw = ff.get(key)
            u = parse_int(raw)
            if u is not None:
                return (u, f"assessor_fallback_fields.{key}", raw)

    # last resort: scan for anything that looks like a code
    for k, v in ff.items():
        if "use" in str(k).lower() and v is not None:
            u = parse_int(v)
            if u is not None:
                return (u, f"assessor_fallback_fields.{k}", v)

    return (None, "assessor_fallback_fields", None)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.audit) or ".", exist_ok=True)

    audit = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "infile": args.infile,
        "out": args.out,
        "rows": 0,
        "use_code_present": 0,
        "use_code_missing": 0,
        "source_counts": {},
        "bucket_counts": {b:0 for b in sorted(BUCKETS)},
        "confidence_counts": {"A":0,"B":0,"C":0},
        "top_use_codes": {},
        "top_prefixes": {},
    }

    use_ctr = Counter()
    prefix_ctr = Counter()
    source_ctr = Counter()

    with open(args.out, "w", encoding="utf-8") as w:
        with open(args.infile, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                audit["rows"] += 1

                u, src_path, raw = extract_use_code_from_best(row)
                if u is None:
                    u, src_path, raw = extract_use_code_from_by_source(row)
                if u is None:
                    u, src_path, raw = extract_use_code_from_fallback_fields(row)

                source_ctr[src_path] += 1

                if u is None:
                    audit["use_code_missing"] += 1
                else:
                    audit["use_code_present"] += 1
                    use_ctr[u] += 1
                    prefix_ctr[code_prefix(u)] += 1

                bucket, conf, reason = bucket_from_use_code_prefix(u)

                row["asset_bucket"] = bucket
                row["asset_bucket_confidence"] = conf
                row["asset_bucket_source"] = src_path
                row["asset_bucket_reason"] = reason
                row["asset_bucket_evidence"] = {
                    "use_code_raw": raw,
                    "use_code": u,
                    "use_code_prefix": code_prefix(u) if u is not None else None,
                }

                audit["bucket_counts"][bucket] += 1
                audit["confidence_counts"][conf] += 1

                w.write(json.dumps(row, ensure_ascii=False) + "\n")

    audit["top_use_codes"] = dict(use_ctr.most_common(30))
    audit["top_prefixes"] = dict(prefix_ctr.most_common(30))
    audit["source_counts"] = dict(source_ctr.most_common(30))

    with open(args.audit, "w", encoding="utf-8") as af:
        json.dump(audit, af, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows:", audit["rows"], "use_code_present:", audit["use_code_present"], "use_code_missing:", audit["use_code_missing"])
    print("source_counts top:", list(audit["source_counts"].items())[:8])
    print("top buckets:", sorted(audit["bucket_counts"].items(), key=lambda x: -x[1])[:10])

if __name__ == "__main__":
    main()
