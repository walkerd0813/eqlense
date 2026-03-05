#!/usr/bin/env python3
"""Compact an events NDJSON into fixture-style actual.json

Usage:
  python compact_events_for_fixture_v1.py --in <events.ndjson> --out <actual.json>
"""
import argparse
import json

def read_ndjson(path):
    out = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def compact_event(ev):
    m = ev.get('meta') or {}
    rec = ev.get('recording') or ev.get('recording') or {}
    parties = ev.get('parties') or {}
    return {
        'page_index': int(m.get('page_index') or 0),
        'record_index': int(m.get('record_index') or 0),
        'inst_raw': rec.get('inst_raw') or rec.get('instrument_number') or None,
        'book_page_raw': rec.get('book_page_raw') or None,
        'property_refs': ev.get('property_refs') or [],
        'parties': parties.get('parties_raw') or parties.get('parties') or [],
        'descr_loc_raw': ev.get('descr_loc') or ev.get('descr_loc_raw') or None,
        'consideration_raw': (ev.get('consideration') or {}).get('amount_raw') or ev.get('consideration_raw') or None,
        'stitch': (m.get('stitch') or None),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='in_path', required=True)
    ap.add_argument('--out', dest='out_path', required=True)
    args = ap.parse_args()

    rows = read_ndjson(args.in_path)
    out = [compact_event(r) for r in rows]

    with open(args.out_path, 'w', encoding='utf-8') as f:
        json.dump({'events': out}, f, indent=2, ensure_ascii=False)

    print(f'Wrote {len(out)} events to {args.out_path}')

if __name__ == '__main__':
    main()
