#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, os, re
import datetime as dt
from typing import Any, Dict, Optional, Tuple

BUCKETS = {"SF","CONDO","MF_2_4","MF_5P","LAND","COMMERCIAL","OTHER","UNKNOWN"}

def norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s

def dget(d: Any, path: str) -> Any:
    """Dot-path getter, safe for dicts."""
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

def bucket_from_use_code(use_code: Optional[int]) -> Tuple[str,str]:
    """
    MA assessor-style heuristic mapping.
    We start with a conservative default:
      100-199: Residential (often SF/Condo/MF)
      200-299: Apartments / MF (often 3+ units)
      300-399: Vacant land
      400-499: Commercial
      500-699: Industrial / utilities / special
    This may need tuning after we see distributions, but it will immediately stop "all OTHER".
    """
    if use_code is None:
        return ("UNKNOWN", "C")

    # Common MA use-code banding (heuristic)
    if 100 <= use_code <= 199:
        # residential: without unit count we default to SF with B confidence
        return ("SF", "B")
    if 200 <= use_code <= 299:
        # apartment / multifamily
        return ("MF_5P", "B")
    if 300 <= use_code <= 399:
        return ("LAND", "A")
    if 400 <= use_code <= 499:
        return ("COMMERCIAL", "A")
    if 500 <= use_code <= 699:
        return ("COMMERCIAL", "B")

    return ("OTHER", "C")

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
        "filled": 0,
        "unknown": 0,
        "bucket_counts": {b:0 for b in sorted(BUCKETS)},
        "confidence_counts": {"A":0,"B":0,"C":0},
        "use_code_present": 0,
        "use_code_missing": 0,
        "top_use_codes": {},
    }

    # lightweight counter for use_code distribution
    from collections import Counter
    use_ctr = Counter()

    with open(args.out, "w", encoding="utf-8") as w:
        with open(args.infile, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                audit["rows"] += 1

                ab = row.get("assessor_best") if isinstance(row.get("assessor_best"), dict) else {}

                # nested fields you showed
                use_code_raw = dget(ab, "structure.use_code.value")
                use_code = parse_int(use_code_raw)

                # optional: sometimes unit counts appear in structure (not shown yet), keep hooks
                units_raw = dget(ab, "structure.units.value") or dget(ab, "structure.unit_count.value") or dget(ab, "structure.res_units.value")
                units = parse_int(units_raw)

                # Try to upgrade SF->CONDO/MF if unit evidence exists
                bucket, conf = bucket_from_use_code(use_code)
                if units is not None:
                    if units >= 5:
                        bucket, conf = ("MF_5P", "A")
                    elif 2 <= units <= 4:
                        bucket, conf = ("MF_2_4", "A")
                    elif units == 1 and bucket == "SF":
                        conf = "A"

                if use_code is not None:
                    audit["use_code_present"] += 1
                    use_ctr[use_code] += 1
                else:
                    audit["use_code_missing"] += 1

                row["asset_bucket"] = bucket
                row["asset_bucket_confidence"] = conf
                row["asset_bucket_source"] = "assessor_best.structure.use_code.value"
                row["asset_bucket_evidence"] = {
                    "use_code_raw": use_code_raw,
                    "use_code": use_code,
                    "units_raw": units_raw,
                    "units": units,
                    "assessor_best_keys": sorted(list(ab.keys()))[:20]
                }

                audit["bucket_counts"][bucket] += 1
                audit["confidence_counts"][conf] += 1

                if bucket == "UNKNOWN":
                    audit["unknown"] += 1
                else:
                    audit["filled"] += 1

                w.write(json.dumps(row, ensure_ascii=False) + "\n")

    audit["top_use_codes"] = dict(use_ctr.most_common(30))

    with open(args.audit, "w", encoding="utf-8") as af:
        json.dump(audit, af, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows:", audit["rows"], "use_code_present:", audit["use_code_present"], "use_code_missing:", audit["use_code_missing"])
    print("top_use_codes:", list(audit["top_use_codes"].items())[:10])
    print("top buckets:", sorted(audit["bucket_counts"].items(), key=lambda x: -x[1])[:10])

if __name__ == "__main__":
    main()
