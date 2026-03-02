#!/usr/bin/env python3
import argparse, json, re
from collections import Counter, defaultdict

def parse_house_no(addr: str):
    if not addr: return None
    s = str(addr).strip().upper()
    # common leading patterns like "0 " or "REAR "
    s = re.sub(r"^(REAR|REAR\s+OF|REAR-OF)\s+", "", s)
    # grab first numeric run
    m = re.match(r"^(\d+)", s)
    if m: return int(m.group(1))
    # handle "2B ..." -> 2
    m = re.match(r"^(\d+)[A-Z]\b", s)
    if m: return int(m.group(1))
    # handle hyphen range "19-21 ..." -> 19 (caller decides)
    m = re.match(r"^(\d+)\s*-\s*(\d+)", s)
    if m: return int(m.group(1))
    return None

def get_addr(rec: dict):
    # try the common fields we’ve seen in samples
    for k in ("addr","address","address_raw","address_text"):
        v = rec.get(k)
        if isinstance(v,str) and v.strip():
            return v
    # property_ref / property_ref_guess
    pr = rec.get("property_ref") or {}
    if isinstance(pr, dict):
        for k in ("addr","address","address_raw"):
            v = pr.get(k)
            if isinstance(v,str) and v.strip():
                return v
    # address_candidates[0]
    ac = rec.get("address_candidates")
    if isinstance(ac, list) and ac:
        c0 = ac[0]
        if isinstance(c0, dict):
            for k in ("addr","address","address_raw","text","raw"):
                v = c0.get(k)
                if isinstance(v,str) and v.strip():
                    return v
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--max_samples", type=int, default=25)
    args = ap.parse_args()

    why = Counter()
    town_missing = 0
    addr_missing = 0
    non_string_addr = 0
    still_unknown = 0

    bucket = defaultdict(list)

    with open(args.in_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                rec=json.loads(line)
            except Exception:
                continue

            if rec.get("attach_status") != "UNKNOWN":
                continue
            still_unknown += 1

            town = rec.get("town") or rec.get("recording",{}).get("town")
            if not town:
                town_missing += 1
                why["NO_TOWN"] += 1
                continue

            addr = get_addr(rec)
            if addr is None:
                addr_missing += 1
                why["NO_ADDR_FIELD"] += 1
                continue
            if not isinstance(addr, str):
                non_string_addr += 1
                why["ADDR_NOT_STRING"] += 1
                continue

            hn = parse_house_no(addr)
            if hn is None:
                why["NO_NUM_PARSE"] += 1
                bkey = "NO_NUM_PARSE"
            else:
                # split into a few common types
                if re.search(r"\b(LOT|PAR|PARCEL|PAR A|PAR B)\b", addr.upper()):
                    bkey = "HAS_NUM_BUT_PARCEL_STYLE"
                elif re.search(r"\b(UNIT|APT|#)\b", addr.upper()):
                    bkey = "HAS_NUM_HAS_UNIT"
                elif re.search(r"^\d+\s*-\s*\d+", addr.strip()):
                    bkey = "HAS_NUM_RANGE"
                else:
                    bkey = "HAS_NUM_STANDARD"
                why[bkey] += 1

            if len(bucket[bkey]) < args.max_samples:
                bucket[bkey].append({
                    "event_id": rec.get("event_id"),
                    "town": town,
                    "addr": addr,
                    "docno_raw": rec.get("docno_raw") or rec.get("recording",{}).get("document_number_raw"),
                })

    out = {
        "meta": {
            "tool": "hampden_axis2_unknown_diag_v1_36_0",
            "input": args.in_path,
            "still_unknown": still_unknown,
            "town_missing": town_missing,
            "addr_missing": addr_missing,
            "non_string_addr": non_string_addr
        },
        "why_counts": dict(why),
        "samples": dict(bucket)
    }

    with open(args.out_path, "w", encoding="utf-8") as w:
        json.dump(out, w, indent=2)

    print("[summary] still_unknown:", still_unknown)
    print("[top] why_counts:", dict(why.most_common(12)))

if __name__ == "__main__":
    main()
