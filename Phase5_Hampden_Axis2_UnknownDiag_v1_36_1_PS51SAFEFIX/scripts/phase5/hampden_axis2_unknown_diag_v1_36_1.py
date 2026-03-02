#!/usr/bin/env python3
# hampden_axis2_unknown_diag_v1_36_1.py
# Reads Axis2 ndjson and summarizes remaining UNKNOWNs with robust town extraction.
import argparse, json, os, re, sys
from collections import Counter, defaultdict

def get_nested(d, path):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur

def first_str(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def extract_town(rec):
    # Try common locations (keep extending safely)
    candidates = []
    candidates.append(rec.get("town"))
    candidates.append(rec.get("town_raw"))
    candidates.append(get_nested(rec, "recording.town"))
    candidates.append(get_nested(rec, "recording.municipality"))
    candidates.append(get_nested(rec, "property_ref.town"))
    candidates.append(get_nested(rec, "meta.town"))
    t = first_str(*candidates)
    if not t:
        return None, "NO_TOWN"
    return t.upper(), "OK"

def extract_addr(rec):
    return first_str(rec.get("addr"), rec.get("address"), get_nested(rec, "recording.address"))

def classify_unknown(rec):
    # Conservative buckets only; we DO NOT attach here.
    addr = extract_addr(rec) or ""
    a = addr.upper()
    if not a.strip():
        return "NO_ADDR"
    if re.search(r"\b(LOT|PAR|PARCEL|REAR|REAR OF|REAR-OF)\b", a):
        return "PARCEL_STYLE"
    if re.search(r"\b(UNIT|APT|APARTMENT|#)\b", a):
        return "HAS_UNIT"
    if re.search(r"\b\d+\s*-\s*\d+\b", a) or re.search(r"\b\d+\s*&\s*\d+\b", a):
        return "HAS_RANGE"
    if re.search(r"\b0\s+[A-Z]", a) or a.startswith("0 "):
        return "ZERO_NUM"
    # standard number?
    if re.search(r"^\s*\d+[A-Z]?\b", a):
        return "STANDARD_NUM"
    return "NO_NUM"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--max_samples", type=int, default=20)
    args = ap.parse_args()

    why_counts = Counter()
    town_status = Counter()
    unknown_class = Counter()
    samples = defaultdict(list)

    total = 0
    unknown_rows = 0

    with open(args.inp, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                rec = json.loads(line)
            except Exception:
                continue

            st = rec.get("attach_status")
            # IMPORTANT: only treat UNKNOWN as unknown
            if st != "UNKNOWN":
                continue

            unknown_rows += 1
            town, town_flag = extract_town(rec)
            town_status[town_flag] += 1

            # prefer existing why, but also compute a structural class
            why = rec.get("why") or rec.get("match_method") or "UNKNOWN"
            why = str(why).upper()
            why_counts[why] += 1

            cls = classify_unknown(rec)
            unknown_class[cls] += 1

            key = f"{town_flag}|{cls}|{why}"
            if len(samples[key]) < args.max_samples:
                samples[key].append({
                    "event_id": rec.get("event_id"),
                    "town": town,
                    "addr": extract_addr(rec),
                    "why": rec.get("why"),
                    "match_method": rec.get("match_method"),
                    "attachments_n": rec.get("attachments_n"),
                    "docno_raw": rec.get("docno_raw"),
                })

    out_obj = {
        "in": args.inp,
        "total_rows": total,
        "unknown_rows_by_attach_status": unknown_rows,
        "town_status": dict(town_status),
        "why_counts": dict(why_counts.most_common()),
        "unknown_class_counts": dict(unknown_class.most_common()),
        "sample_keys": list(samples.keys())[:50],
        "samples": {k: v for k, v in samples.items()},
        "notes": [
            "Counts unknowns strictly where attach_status == 'UNKNOWN'.",
            "Town extraction checks: town, town_raw, recording.town, recording.municipality, property_ref.town, meta.town.",
            "Class buckets are conservative and do not attach anything."
        ]
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as w:
        json.dump(out_obj, w, indent=2)

    print("[summary] total_rows:", total)
    print("[summary] unknown_rows (attach_status==UNKNOWN):", unknown_rows)
    print("[top] town_status:", dict(town_status))
    print("[top] unknown_class_counts:", dict(unknown_class.most_common(10)))
    print("[top] why_counts:", dict(why_counts.most_common(10)))
    print("[done] wrote:", args.out)

if __name__ == "__main__":
    main()
