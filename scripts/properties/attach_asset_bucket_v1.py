import json, argparse, os

def norm(s):
    return str(s).strip().upper() if s else None
    
def getv(d, *path):
    """Safely get nested .value-aware fields from assessor schema."""
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    # unwrap {"value": ...}
    if isinstance(cur, dict) and "value" in cur:
        return cur.get("value")
    return cur

ASSESSOR_MAP = {
    "SINGLE FAMILY": "SF",
    "TWO FAMILY": "MF",
    "THREE FAMILY": "MF",
    "FOUR FAMILY": "MF",
    "MULTI FAMILY": "MF",
    "CONDOMINIUM": "CONDO",
    "VACANT LAND": "LAND",
}

MLS_MAP = {
    "SINGLE FAMILY": "SF",
    "SF": "SF",
    "CONDO": "CONDO",
    "TOWNHOUSE": "CONDO",
    "MULTI-FAMILY": "MF",
    "2 FAMILY": "MF",
    "3 FAMILY": "MF",
    "4 FAMILY": "MF",
    "LAND": "LAND",
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    audit = {
        "total": 0,
        "resolved": 0,
        "by_source": {},
        "unknown": 0
    }

    def hit(src):
        audit["by_source"][src] = audit["by_source"].get(src, 0) + 1

    with open(args.spine, "r", encoding="utf-8") as f, \
         open(args.out, "w", encoding="utf-8") as out:

        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            audit["total"] += 1

            pid = row.get("property_id")
            if not pid:
                continue

            bucket = None
            source = None
            confidence = None

            # 1. Assessor (value-wrapped schema)
abest = row.get("assessor_best") or {}

use_code = getv(abest, "structure", "use_code")  # unwraps .value
use_code = str(use_code).strip() if use_code is not None else None

bucket = None
source = None
confidence = None

# V1 rulepack: start with strong known codes; expand as we observe more.
if use_code == "101":
    bucket = "SF"
    source = "assessor_use_code"
    confidence = 0.95
elif use_code is not None:
    # lightweight heuristic: 10x family residential band (town-dependent but useful)
    if use_code.startswith("10"):
        # if we can't distinguish, keep it OTHER_RES for now? No—use UNKNOWN to avoid fake precision.
        bucket = "UNKNOWN"
        source = "assessor_use_code_unmapped"
        confidence = 0.20

            # 2. MLS
            if not bucket:
                mls = norm((row.get("mls") or {}).get("property_type"))
                if mls in MLS_MAP:
                    bucket = MLS_MAP[mls]
                    source = "mls"
                    confidence = 0.85

            # 3. Zoning fallback
            if not bucket:
                zcat = norm((row.get("zoning") or {}).get("zoning_category"))
                if zcat == "RESIDENTIAL":
                    bucket = "SF"
                    source = "zoning_fallback"
                    confidence = 0.40

            if not bucket:
                bucket = "UNKNOWN"
                source = "unknown"
                confidence = 0.0
                audit["unknown"] += 1
            else:
                audit["resolved"] += 1
                hit(source)

            out.write(json.dumps({
                "property_id": pid,
                "asset_bucket": bucket,
                "source": source,
                "confidence": confidence
            }) + "\n")

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] asset bucket attachment v1")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()

