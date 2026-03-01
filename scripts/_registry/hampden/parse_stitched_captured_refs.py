#!/usr/bin/env python3
"""Parse Town/Addr from stitched `captured_top_lines` and add property_refs."""
from __future__ import annotations

import argparse
import json
import os
import re
from typing import List

TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.IGNORECASE)
TOWN_ADDR_RE = re.compile(r"Town:\s*(?P<town>.+?)\s+Addr:\s*(?P<addr>.+)$", re.IGNORECASE)

def load_ndjson(path: str) -> List[dict]:
    out = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            out.append(json.loads(s))
    return out

def write_ndjson(path: str, rows: List[dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

def extract_from_lines(lines: List[str]):
    for ln in lines:
        m = TOWN_ADDR_RE.search(ln)
        if m:
            town = m.group('town').strip()
            addr = m.group('addr').strip()
            # strip trailing OCR 'Y'
            town = TRAILING_Y_RE.sub('', town).strip()
            addr = TRAILING_Y_RE.sub('', addr).strip()
            return town, addr
    return None, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--stitched', required=True)
    ap.add_argument('--out', required=False)
    args = ap.parse_args()

    stitched_path = args.stitched
    if not os.path.exists(stitched_path):
        raise SystemExit(f'stitched not found: {stitched_path}')

    rows = load_ndjson(stitched_path)
    patched = 0

    for ev in rows:
        # look for stitch info in meta or top-level
        stitch = ev.get('meta', {}).get('stitch') or ev.get('stitch')
        if not stitch:
            continue
        lines = stitch.get('captured_top_lines') or []
        if not lines:
            continue

        town, addr = extract_from_lines(lines)
        if not town or not addr:
            # fallback: search evidence.lines_clean
            evidence = ev.get('evidence', {}).get('lines_clean') or []
            town, addr = extract_from_lines(evidence)
        if not town or not addr:
            continue

        # if property_refs missing or empty, add primary
        refs = ev.get('property_refs')
        if not isinstance(refs, list) or len(refs) == 0:
            ev['property_refs'] = [{'ref_index': 0, 'town': town, 'address_raw': addr, 'unit_hint': None, 'ref_role': 'PRIMARY'}]
            patched += 1

    out_path = args.out or stitched_path
    write_ndjson(out_path, rows)
    print(f"[done] stitched_in={stitched_path} patched={patched} out={out_path}")

if __name__ == '__main__':
    main()
