#!/usr/bin/env python3
"""Post-attach bucket report for Hampden DEED-only attaches.

Reads an attached NDJSON and prints:
- totals by attach_status
- top UNKNOWN reasons (if present)
- samples for remaining UNKNOWN and MISSING locator

This script is intentionally lightweight and read-only.
"""

import argparse, json, os
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--attached", required=True, help="Path to events_attached*.ndjson")
    ap.add_argument("--maxSamples", type=int, default=8)
    args = ap.parse_args()

    path = os.path.abspath(args.attached)
    status = Counter()
    reasons = Counter()
    samples = defaultdict(list)

    total = 0
    for row in iter_ndjson(path):
        total += 1
        st = row.get("attach", {}).get("status") or row.get("attach_status") or "UNKNOWN"
        status[st] += 1
        if st == "UNKNOWN":
            r = row.get("attach", {}).get("reason") or row.get("attach_reason") or "UNKNOWN"
            reasons[r] += 1
            if len(samples[r]) < args.maxSamples:
                pr = row.get("property_ref", {}) or {}
                rec = row.get("recording", {}) or {}
                doc = rec.get("document_number") or rec.get("document_number_raw") or row.get("document", {}).get("document_number")
                samples[r].append({
                    "doc": doc,
                    "town_raw": pr.get("town_raw") or pr.get("town") or "",
                    "addr_raw": pr.get("address_raw") or pr.get("address") or "",
                    "town_norm": pr.get("town_norm") or "",
                    "addr_norm": pr.get("address_norm") or "",
                })
        elif st == "MISSING_TOWN_OR_ADDRESS":
            if len(samples[st]) < args.maxSamples:
                pr = row.get("property_ref", {}) or {}
                samples[st].append({
                    "town_raw": pr.get("town_raw") or pr.get("town") or "",
                    "addr_raw": pr.get("address_raw") or pr.get("address") or "",
                })

    print("=== Post-Attach Summary ===")
    print({"attached": path, "total": total, "status": dict(status)})
    if reasons:
        print("\n=== UNKNOWN reasons (top 20) ===")
        for k, v in reasons.most_common(20):
            print(f"{k}: {v}")
        print("\n=== Samples ===")
        for k, arr in samples.items():
            if not arr:
                continue
            print(f"\n--- {k} ---")
            for s in arr:
                print(s)

if __name__ == "__main__":
    main()
