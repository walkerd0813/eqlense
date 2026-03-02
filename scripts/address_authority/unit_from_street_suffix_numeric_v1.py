#!/usr/bin/env python3
import re, json, argparse, hashlib, datetime
from datetime import timezone
from collections import Counter

RE_TRAIL_NUM = re.compile(r"^(?P<base>.+?)\s+(?P<num>\d{1,4})\s*$", re.I)
RE_GUARD_ROUTE = re.compile(r"\b(ROUTE|RTE|RT|HWY|HIGHWAY)\b", re.I)
RE_GUARD_ORDINAL = re.compile(r"\b(1ST|2ND|3RD|[4-9]TH)\b", re.I)
RE_GUARD_END_ORDINAL = re.compile(r"\b\d+(ST|ND|RD|TH)\b\s*$", re.I)

def utc_now():
    return datetime.datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def is_empty(x):
    return x is None or (isinstance(x, str) and x.strip() == "")

def should_skip(street_name: str) -> bool:
    if not street_name:
        return True
    s = street_name.strip()
    if RE_GUARD_ROUTE.search(s):
        return True
    if RE_GUARD_ORDINAL.search(s):
        return True
    if RE_GUARD_END_ORDINAL.search(s):
        return True
    return False

def transform_row(r, stats: Counter):
    street_no = (r.get("street_no") or "").strip()
    street_name = (r.get("street_name") or "").strip()
    unit = r.get("unit")

    # Gate condition: unit empty + street_no present + street_name ends with numeric token
    if is_empty(street_no) or is_empty(street_name) or not is_empty(unit):
        return r, False

    if should_skip(street_name):
        return r, False

    m = RE_TRAIL_NUM.match(street_name)
    if not m:
        return r, False

    num = int(m.group("num"))
    if num < 1 or num > 9999:
        return r, False

    base = m.group("base").strip()
    if not base:
        return r, False

    # Preserve raw fields (only set if missing/empty)
    if is_empty(r.get("street_name_raw")):
        r["street_name_raw"] = r.get("street_name")
    if is_empty(r.get("full_address_raw")):
        r["full_address_raw"] = r.get("full_address")

    # Apply transform
    r["street_name"] = base
    r["unit"] = str(num)

    # Optional: keep unit_type unknown unless you have evidence elsewhere
    if "unit_type" not in r:
        r["unit_type"] = None

    # Track transforms/anomalies (additive, non-breaking)
    r.setdefault("address_transforms", [])
    r.setdefault("address_anomalies", [])
    r["address_transforms"].append("UNIT_FROM_STREET_SUFFIX_NUMERIC_v1")
    r["address_anomalies"].append("ADDR_UNIT_EMBEDDED_IN_STREETNAME")

    r.setdefault("address_transform_meta", {})
    r["address_transform_meta"]["unit_from_street_suffix_numeric"] = {
        "version": "v1",
        "at": utc_now(),
        "from_street_name": street_name,
        "to_street_name": base,
        "unit": str(num),
        "rule": "unit null && street_name endswith space+digits && not route/ordinal",
    }

    stats["rows_changed"] += 1
    return r, True

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    stats = Counter()
    stats["rows_scanned"] = 0
    stats["rows_changed"] = 0

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            stats["rows_scanned"] += 1
            r = json.loads(line)
            r2, _changed = transform_row(r, stats)
            fout.write(json.dumps(r2, ensure_ascii=False) + "\n")

    audit = {
        "engine_id": "address_authority.UNIT_FROM_STREET_SUFFIX_NUMERIC_v1",
        "ran_at": utc_now(),
        "infile": args.infile,
        "infile_sha256": sha256_file(args.infile),
        "out": args.out,
        "audit": args.audit,
        "stats": dict(stats),
        "guards": {
            "skip_if_contains": ["ROUTE|RTE|RT|HWY|HIGHWAY", "1ST|2ND|3RD|4TH..9TH", "endswith ordinal like 12TH"],
            "numeric_range": "1..9999",
        },
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps({"done": True, **audit["stats"]}, indent=2))

if __name__ == "__main__":
    main()