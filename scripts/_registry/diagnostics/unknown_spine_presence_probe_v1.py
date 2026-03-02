#!/usr/bin/env python
import argparse, json, os, re, hashlib
from collections import Counter

UNIT_PATTERNS = [
    re.compile(r"\b(UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR|NO)\s*([A-Z0-9\-]+)\b", re.IGNORECASE),
    re.compile(r"\#\s*([A-Z0-9\-]+)\b", re.IGNORECASE),
]

def clean_spaces(s: str) -> str:
    return " ".join((s or "").strip().split())

def strip_unit(addr: str) -> str:
    a = addr or ""
    for pat in UNIT_PATTERNS:
        a = pat.sub("", a)
    return clean_spaces(a.replace("  ", " "))

def norm_town(s: str) -> str:
    return clean_spaces(s).upper()

def norm_addr(s: str) -> str:
    s = clean_spaces(s).upper()
    # light punctuation normalization (keep hyphen for ranges)
    s = re.sub(r"[\,\.\;]", "", s)
    s = clean_spaces(s)
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="NDJSON events file")
    ap.add_argument("--spine", required=True, help="NDJSON spine file")
    ap.add_argument("--out", required=True, help="JSON report")
    ap.add_argument("--sample_out", required=True, help="NDJSON sample of misses")
    ap.add_argument("--max_miss_samples", type=int, default=500)
    args = ap.parse_args()

    # 1) Build spine base-key set: TOWN|ADDRESS (base only)
    spine_keys = set()
    spine_seen = 0
    spine_kept = 0

    with open(args.spine, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            spine_seen += 1
            r = json.loads(ln)
            town = norm_town((r.get("town") or r.get("town_raw") or "").strip())
            addr = None

            # try common paths
            aobj = r.get("address") or {}
            if isinstance(aobj, dict):
                addr = aobj.get("street") or aobj.get("address_raw") or aobj.get("street_name") or None

            # fallback: some spines store a prebuilt key
            if not addr:
                addr = r.get("address_raw") or r.get("address") if isinstance(r.get("address"), str) else None

            if not town or not addr:
                continue

            addr_base = norm_addr(strip_unit(str(addr)))
            if not addr_base:
                continue

            spine_keys.add(f"{town}|{addr_base}")
            spine_kept += 1

    # 2) Scan UNKNOWN events and test presence
    c = Counter()
    miss_samples = 0
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.sample_out), exist_ok=True)

    with open(args.sample_out, "w", encoding="utf-8") as fo, open(args.events, "r", encoding="utf-8") as fe:
        for ln in fe:
            ln = ln.strip()
            if not ln:
                continue
            r = json.loads(ln)
            a = r.get("attach") or {}
            if (a.get("status") or "").strip().upper() != "UNKNOWN":
                continue

            pr = r.get("property_ref") or {}
            town = norm_town(pr.get("town_raw") or "")
            addr = pr.get("address_raw") or ""
            if not town or not addr:
                c["unknown_missing_town_or_addr"] += 1
                continue

            addr_base = norm_addr(strip_unit(addr))
            key = f"{town}|{addr_base}"
            c["unknown_total"] += 1

            if key in spine_keys:
                c["unknown_base_found_in_spine"] += 1
            else:
                c["unknown_base_not_found_in_spine"] += 1
                if miss_samples < args.max_miss_samples:
                    r["_probe"] = {"base_key": key, "addr_base": addr_base}
                    fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                    miss_samples += 1

    report = {
        "spine_rows_seen": spine_seen,
        "spine_keys_built": spine_kept,
        "unknown_total": c.get("unknown_total", 0),
        "unknown_base_found_in_spine": c.get("unknown_base_found_in_spine", 0),
        "unknown_base_not_found_in_spine": c.get("unknown_base_not_found_in_spine", 0),
        "unknown_missing_town_or_addr": c.get("unknown_missing_town_or_addr", 0),
        "miss_sample_rows_written": miss_samples,
        "counts": dict(c),
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
