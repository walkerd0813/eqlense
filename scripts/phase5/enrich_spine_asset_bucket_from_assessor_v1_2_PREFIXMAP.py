#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, os, re
import datetime as dt
from typing import Any, Dict, Optional, Tuple
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
    # preserve leading family by length:
    # 4-digit: use first 3 as family (1010 -> "101")
    # 3-digit: use first 3 (101 -> "101")
    # else: use first 2 as broader family
    s = str(code)
    if len(s) >= 3:
        return s[:3]
    return s[:2]

def bucket_from_use_code_prefix(use_code: Optional[int]) -> Tuple[str,str,str]:
    """
    Returns: (bucket, confidence, reason)
    Prefix families derived from your observed top codes:
      101* => SF
      102* => CONDO
      104* => MF_2_4
      105* => MF_5P
      93*  => LAND  (based on 930/9300 prominence; treat as land family for now)
      13*  => COMMERCIAL (130/132 families)
    Everything else: OTHER (or UNKNOWN if no code)
    """
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

    # Broader “13*” commercial catch, but keep it lower confidence
    if str(use_code).startswith("13"):
        return ("COMMERCIAL", "C", "use_code_startswith:13")

    return ("OTHER", "C", "unmapped_use_code")

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
        "bucket_counts": {b:0 for b in sorted(BUCKETS)},
        "confidence_counts": {"A":0,"B":0,"C":0},
        "reason_counts": {},
        "top_use_codes": {},
        "top_prefixes": {},
    }

    use_ctr = Counter()
    prefix_ctr = Counter()
    reason_ctr = Counter()

    with open(args.out, "w", encoding="utf-8") as w:
        with open(args.infile, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                audit["rows"] += 1

                ab = row.get("assessor_best") if isinstance(row.get("assessor_best"), dict) else {}

                use_code_raw = dget(ab, "structure.use_code.value")
                use_code = parse_int(use_code_raw)

                if use_code is None:
                    audit["use_code_missing"] += 1
                else:
                    audit["use_code_present"] += 1
                    use_ctr[use_code] += 1
                    prefix_ctr[code_prefix(use_code)] += 1

                bucket, conf, reason = bucket_from_use_code_prefix(use_code)
                reason_ctr[reason] += 1

                row["asset_bucket"] = bucket
                row["asset_bucket_confidence"] = conf
                row["asset_bucket_source"] = "assessor_best.structure.use_code.value"
                row["asset_bucket_reason"] = reason
                row["asset_bucket_evidence"] = {
                    "use_code_raw": use_code_raw,
                    "use_code": use_code,
                    "use_code_prefix": code_prefix(use_code) if use_code is not None else None,
                }

                audit["bucket_counts"][bucket] += 1
                audit["confidence_counts"][conf] += 1

                w.write(json.dumps(row, ensure_ascii=False) + "\n")

    audit["top_use_codes"] = dict(use_ctr.most_common(30))
    audit["top_prefixes"] = dict(prefix_ctr.most_common(30))
    audit["reason_counts"] = dict(reason_ctr.most_common(30))

    with open(args.audit, "w", encoding="utf-8") as af:
        json.dump(audit, af, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows:", audit["rows"], "use_code_present:", audit["use_code_present"], "use_code_missing:", audit["use_code_missing"])
    print("top prefixes:", list(audit["top_prefixes"].items())[:10])
    print("top buckets:", sorted(audit["bucket_counts"].items(), key=lambda x: -x[1])[:10])

if __name__ == "__main__":
    main()
