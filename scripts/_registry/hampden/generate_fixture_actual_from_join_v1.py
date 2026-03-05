#!/usr/bin/env python3
"""Generate a compact actual.json from a join NDJSON (for fixture diffing).

Usage:
  python generate_fixture_actual_from_join_v1.py --join <join_ndjson> --out <actual.json>
"""
import argparse
import json

def read_ndjson(path):
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out

def compact_event(ev):
    m = ev.get("meta") or {}
    rec = ev.get("recording") or {}
    parties = ev.get("parties") or {}
    return {
        "page_index": int(m.get("page_index") or 0),
        "record_index": int(m.get("record_index") or 0),
        "inst_raw": rec.get("inst_raw") or rec.get("instrument_number") or None,
        "book_page_raw": rec.get("book_page_raw") or None,
        "property_refs": ev.get("property_refs") or [],
        "parties": parties.get("parties_raw") or parties.get("parties") or [],
        "descr_loc_raw": ev.get("descr_loc_raw") or None,
        "consideration_raw": (ev.get("consideration") or {}).get("amount_raw") or None,
        "stitch": m.get("stitch") or None,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--join", required=True, help="join NDJSON input")
    ap.add_argument("--out", required=True, help="actual.json output path")
    args = ap.parse_args()

    rows = read_ndjson(args.join)
    out = [compact_event(r) for r in rows]

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"events": out}, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(out)} events to {args.out}")

if __name__ == '__main__':
    main()
