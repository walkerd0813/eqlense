#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from typing import Any, Dict, Optional, Tuple

# ---- Canonical buckets ----
BUCKETS = {"SF","CONDO","MF_2_4","MF_5P","LAND","COMMERCIAL","OTHER","UNKNOWN"}

def norm_str(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s

def pick_first(d: Dict[str, Any], keys) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    return None

def bucket_from_tokens(use_code: str, use_desc: str, prop_class: str, prop_type: str, style: str, units: Optional[int]) -> Tuple[str,str,str,Dict[str,Any]]:
    """
    Returns: (bucket, confidence, source, evidence)
    Confidence:
      A = clear mapping (units or explicit condo/commercial/land)
      B = good heuristic (keywords)
      C = weak heuristic
    """
    tokens = " | ".join([use_code, use_desc, prop_class, prop_type, style])
    t = norm_str(tokens)

    evidence = {
        "use_code": use_code, "use_desc": use_desc,
        "property_class": prop_class, "property_type": prop_type,
        "style": style, "units": units
    }

    # --- strongest signals first ---
    if units is not None:
        try:
            u = int(units)
            if u >= 5:
                return ("MF_5P", "A", "assessor_units", evidence)
            if 2 <= u <= 4:
                return ("MF_2_4", "A", "assessor_units", evidence)
            if u == 1:
                # could be SF or condo; decide by keywords
                if "CONDO" in t or "CONDOMINIUM" in t:
                    return ("CONDO", "A", "assessor_units+keyword", evidence)
                return ("SF", "B", "assessor_units", evidence)
        except Exception:
            pass

    # keywords for condo
    if "CONDO" in t or "CONDOMINIUM" in t:
        return ("CONDO", "A", "assessor_keyword", evidence)

    # land / vacant
    if "VACANT" in t or "LAND" in t or "LOT" in t or "UNIMPROVED" in t:
        return ("LAND", "A", "assessor_keyword", evidence)

    # commercial
    if any(k in t for k in ["COMMERCIAL","RETAIL","OFFICE","INDUSTRIAL","WAREHOUSE","MIXED USE","MIXED-USE","STORE","SHOP","RESTAURANT","HOTEL","MOTEL"]):
        return ("COMMERCIAL", "A", "assessor_keyword", evidence)

    # multifamily
    if any(k in t for k in ["MULTI","MULTIFAMILY","MULTI-FAMILY","3-FAM","THREE FAMILY","4-FAM","FOUR FAMILY","2-FAM","TWO FAMILY","DUPLEX","TRIPLEX","QUAD"]):
        # try to split 2-4 vs 5+ if hinted
        if any(k in t for k in ["5","6","7","8","9","10","ELEVEN","TWELVE","APARTMENT","APT BLDG","APARTMENTS"]):
            return ("MF_5P", "B", "assessor_keyword", evidence)
        return ("MF_2_4", "B", "assessor_keyword", evidence)

    # single family
    if any(k in t for k in ["SINGLE FAMILY","SINGLE-FAMILY","SF","1-FAM","ONE FAMILY","RANCH","COLONIAL","CAPE","SPLIT LEVEL","SPLIT-LEVEL"]):
        return ("SF", "B", "assessor_keyword", evidence)

    # if we got here, weak/unknown
    if t:
        return ("OTHER", "C", "assessor_weak", evidence)
    return ("UNKNOWN", "C", "missing_assessor_fields", evidence)

def extract_assessor_fields(row: Dict[str,Any]) -> Dict[str,Any]:
    best = row.get("assessor_best") if isinstance(row.get("assessor_best"), dict) else {}
    fb   = row.get("assessor_fallback_fields") if isinstance(row.get("assessor_fallback_fields"), dict) else {}

    use_code = norm_str(pick_first(best, ["use_code","useCode","land_use_code","landUseCode","lu_code","class_code","classCode"]) or
                        pick_first(fb,   ["use_code","useCode","land_use_code","landUseCode","lu_code","class_code","classCode"]))
    use_desc = norm_str(pick_first(best, ["use_desc","useDesc","land_use","landUse","lu_desc","class_desc","classDesc","use"]) or
                        pick_first(fb,   ["use_desc","useDesc","land_use","landUse","lu_desc","class_desc","classDesc","use"]))
    prop_class = norm_str(pick_first(best, ["property_class","propertyClass","prop_class","class","propertyclass"]) or
                          pick_first(fb,   ["property_class","propertyClass","prop_class","class","propertyclass"]))
    prop_type  = norm_str(pick_first(best, ["property_type","propertyType","prop_type","type"]) or
                          pick_first(fb,   ["property_type","propertyType","prop_type","type"]))
    style      = norm_str(pick_first(best, ["style","building_style","buildingStyle"]) or
                          pick_first(fb,   ["style","building_style","buildingStyle"]))

    units = pick_first(best, ["units","num_units","unit_count","res_units","total_units"]) or \
            pick_first(fb,   ["units","num_units","unit_count","res_units","total_units"])

    return {
        "use_code": use_code,
        "use_desc": use_desc,
        "property_class": prop_class,
        "property_type": prop_type,
        "style": style,
        "units": units,
    }

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
        "bucket_counts": {b:0 for b in sorted(BUCKETS)},
        "confidence_counts": {"A":0,"B":0,"C":0},
        "filled": 0,
        "unknown": 0,
    }

    with open(args.out, "w", encoding="utf-8") as w:
        with open(args.infile, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                audit["rows"] += 1

                a = extract_assessor_fields(row)
                bucket, conf, src, ev = bucket_from_tokens(
                    a["use_code"], a["use_desc"], a["property_class"], a["property_type"], a["style"], a["units"]
                )

                row["asset_bucket"] = bucket
                row["asset_bucket_confidence"] = conf
                row["asset_bucket_source"] = src
                row["asset_bucket_evidence"] = ev

                audit["bucket_counts"][bucket] += 1
                audit["confidence_counts"][conf] += 1
                if bucket == "UNKNOWN":
                    audit["unknown"] += 1
                else:
                    audit["filled"] += 1

                w.write(json.dumps(row, ensure_ascii=False) + "\n")

    with open(args.audit, "w", encoding="utf-8") as af:
        json.dump(audit, af, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows:", audit["rows"], "filled:", audit["filled"], "unknown:", audit["unknown"])
    print("top buckets:", sorted(audit["bucket_counts"].items(), key=lambda x: -x[1])[:10])

if __name__ == "__main__":
    main()
