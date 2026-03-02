import json, argparse, os, datetime
from typing import Any, Dict, Optional

# Asset bucket vocabulary (locked):
# SF | MF | CONDO | LAND | OTHER | UNKNOWN

def norm(s: Any) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    return s.upper() if s else None

def getv(d: Any, *path: str) -> Any:
    """Safely get nested value-wrapped fields (e.g., {"value": ...})."""
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    if isinstance(cur, dict) and "value" in cur:
        return cur.get("value")
    return cur

def normalize_use_code(raw: Any) -> Optional[str]:
    """Normalize MA DOR property type / use codes (101, 0101, 1010 -> 101)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s2 = "".join(ch for ch in s if ch.isdigit())
    if not s2:
        return None
    s2 = s2.lstrip("0") or "0"
    if len(s2) == 4 and s2.endswith("0"):
        s2 = s2[:3]
    if len(s2) > 3:
        s2 = s2[:3]
    return s2

def bucket_from_use_code(use_code: Optional[str]) -> Optional[str]:
    """Map MA DOR Property Type Classification Codes -> buckets."""
    if not use_code:
        return None
    try:
        c = int(use_code)
    except Exception:
        return None

    # Residential (MA DOR examples):
    if c == 101:
        return "SF"
    if c == 102:
        return "CONDO"
    if c == 103:
        return "SF"  # mobile home -> SF bucket for market grouping
    if c in (104, 105, 109):
        return "MF"
    if 111 <= c <= 112:
        return "MF"
    if 130 <= c <= 132:
        return "LAND"

    return "OTHER"

MLS_MAP = {
    "SINGLE FAMILY": "SF",
    "SF": "SF",
    "CONDO": "CONDO",
    "TOWNHOUSE": "CONDO",
    "MULTI-FAMILY": "MF",
    "MULTIFAMILY": "MF",
    "2 FAMILY": "MF",
    "3 FAMILY": "MF",
    "4 FAMILY": "MF",
    "LAND": "LAND",
}

def main():
    ap = argparse.ArgumentParser(description="Attach asset_bucket to property_id from spine assessor fields.")
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--max_rows", type=int, default=0, help="Stop after N rows (0=all) for quick tests.")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    audit = {
        "built_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "inputs": {"spine": args.spine},
        "total": 0,
        "resolved_non_unknown": 0,
        "by_source": {},
        "by_bucket": {},
        "unknown": 0,
        "samples": {
            "assessor_use_code_value_examples": [],
            "other_use_code_examples": []
        }
    }

    def inc(m: Dict[str, int], k: str):
        m[k] = m.get(k, 0) + 1

    wrote = 0
    with open(args.spine, "r", encoding="utf-8") as f, open(args.out, "w", encoding="utf-8") as out:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            audit["total"] += 1
            if args.max_rows and audit["total"] >= args.max_rows:
                break

            pid = row.get("property_id")
            if not pid:
                continue

            bucket = None
            source = "unknown"
            confidence = 0.0

            # 1) Assessor use_code (value-wrapped)
            abest = row.get("assessor_best") or {}
            raw_uc = getv(abest, "structure", "use_code")
            uc = normalize_use_code(raw_uc)
            if raw_uc is not None and len(audit["samples"]["assessor_use_code_value_examples"]) < 10:
                audit["samples"]["assessor_use_code_value_examples"].append(str(raw_uc))

            b = bucket_from_use_code(uc)
            if b:
                bucket = b
                source = "assessor_use_code"
                confidence = 0.95 if b in ("SF","MF","CONDO","LAND") else 0.70
                if b == "OTHER" and uc and len(audit["samples"]["other_use_code_examples"]) < 10:
                    audit["samples"]["other_use_code_examples"].append(uc)

            # 2) MLS property_type (if present)
            if not bucket:
                mls = norm((row.get("mls") or {}).get("property_type"))
                if mls in MLS_MAP:
                    bucket = MLS_MAP[mls]
                    source = "mls"
                    confidence = 0.85

            # 3) Zoning fallback (very coarse)
            if not bucket:
                zcat = norm((row.get("zoning") or {}).get("zoning_category"))
                if zcat == "RESIDENTIAL":
                    bucket = "SF"
                    source = "zoning_fallback"
                    confidence = 0.40

            if not bucket:
                bucket = "UNKNOWN"
                audit["unknown"] += 1
            else:
                if bucket != "UNKNOWN":
                    audit["resolved_non_unknown"] += 1

            inc(audit["by_bucket"], bucket)
            inc(audit["by_source"], source)

            out.write(json.dumps({
                "property_id": pid,
                "asset_bucket": bucket,
                "source": source,
                "confidence": confidence,
            }, ensure_ascii=False) + "\n")
            wrote += 1

    audit["output"] = {"out": args.out, "rows_written": wrote}

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] asset bucket attachment v1_1")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
