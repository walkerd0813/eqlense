
#!/usr/bin/env python3
"""
Phase 5B — Deeds → Property Spine Attachment (v1)

Input:
  registry_deeds_canonical_v2__ALL_DEDUPED.ndjson
  properties spine ndjson (parcel-anchored)

Output:
  deeds_attached_v1.ndjson

Rules:
- Deeds are events, not property truth
- Attach with confidence grades
- UNKNOWN is valid
"""

import json, hashlib, sys, argparse
from datetime import datetime

def load_ndjson(path):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def normalize_addr(addr):
    if not addr:
        return None
    return addr.upper().replace('.', '').replace(',', '').strip()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--deeds', required=True)
    ap.add_argument('--properties', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    # Build address → property index
    prop_by_addr = {}
    prop_by_parcel = {}

    for p in load_ndjson(args.properties):
        if p.get('property_id'):
            if p.get('normalized_address'):
                prop_by_addr[normalize_addr(p['normalized_address'])] = p
            if p.get('parcel_id'):
                prop_by_parcel[p['parcel_id']] = p

    attached = 0
    unknown = 0

    with open(args.out, 'w', encoding='utf-8') as out:
        for d in load_ndjson(args.deeds):
            attach = {
                "event_id": d["event_id"],
                "event_type": "DEED",
                "property_id": None,
                "parcel_id": None,
                "attach_method": None,
                "attach_confidence": "UNKNOWN",
                "attach_timestamp": datetime.utcnow().isoformat() + "Z"
            }

            addr = normalize_addr(d.get("property", {}).get("address"))
            if addr and addr in prop_by_addr:
                p = prop_by_addr[addr]
                attach.update({
                    "property_id": p.get("property_id"),
                    "parcel_id": p.get("parcel_id"),
                    "attach_method": "ADDRESS_MATCH",
                    "attach_confidence": "A"
                })
                attached += 1
            else:
                unknown += 1

            out.write(json.dumps({**d, "attachment": attach}) + "\n")

    print("[done] attached:", attached, "unknown:", unknown)

if __name__ == "__main__":
    main()
