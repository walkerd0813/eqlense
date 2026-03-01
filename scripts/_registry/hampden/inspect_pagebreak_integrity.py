#!/usr/bin/env python3
"""
inspect_pagebreak_integrity.py

Scan a pipeline work_root for pagebreak candidate cases and produce a before/after report
for stitched vs crosschunk-patched stitched files.

Usage:
  python inspect_pagebreak_integrity.py --work_root <WORK_ROOT> [--stitch_py <stitcher.py>] [--out report.json]

"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
from typing import Dict, List, Optional

RE_CHUNK = re.compile(r"^p(?P<start>\d{5})_p(?P<end>\d{5})$")

def load_ndjson(path: str) -> List[dict]:
    out: List[dict] = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            out.append(json.loads(line))
    return out

def list_chunks(work_root: str) -> Dict[int, str]:
    m: Dict[int, str] = {}
    for name in os.listdir(work_root):
        p = os.path.join(work_root, name)
        if not os.path.isdir(p):
            continue
        mm = RE_CHUNK.match(name)
        if not mm:
            continue
        s = int(mm.group('start'))
        m[s] = p
    return m

def load_stitch_module(stitch_py: Optional[str]):
    if not stitch_py:
        return None
    spec = importlib.util.spec_from_file_location('stitch_mod', stitch_py)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod

def build_events_by_page(events: List[dict]) -> Dict[int, List[dict]]:
    by: Dict[int, List[dict]] = {}
    for ev in events:
        p = int(ev.get('meta', {}).get('page_index', -1))
        by.setdefault(p, []).append(ev)
    return by

def event_key(ev: dict) -> str:
    return f"{int(ev['meta']['page_index'])}|{int(ev['meta']['record_index'])}"

def refs_count(ev: dict) -> int:
    return len(ev.get('property_refs') or [])

def parties_count(ev: dict) -> int:
    return len((ev.get('parties') or {}).get('parties_raw') or [])

TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.IGNORECASE)

def primary_ref_summary(ev: dict) -> Optional[str]:
    refs = ev.get('property_refs') or []
    if not refs:
        return None
    p0 = refs[0]
    town = p0.get('town') or ''
    addr = (p0.get('address_raw') or '').strip()
    return f"{town} | {addr}".strip()

def has_trailing_y(ev: dict) -> bool:
    refs = ev.get('property_refs') or []
    if not refs:
        return False
    addr = refs[0].get('address_raw') or ''
    return bool(TRAILING_Y_RE.search(str(addr)))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--work_root', required=True)
    ap.add_argument('--stitch_py', required=False)
    ap.add_argument('--out', default=None)
    ap.add_argument('--limit', type=int, default=50, help='Max rows to print')
    args = ap.parse_args()

    chunks = list_chunks(args.work_root)
    if not chunks:
        raise SystemExit('No chunk folders found under work_root')

    stitch_mod = load_stitch_module(args.stitch_py) if args.stitch_py else None
    event_missing_fn = None
    if stitch_mod is not None:
        event_missing_fn = getattr(stitch_mod, 'event_missing_addr', None) or getattr(stitch_mod, 'event_missing_refs', None)

    report = {'work_root': args.work_root, 'chunks': []}
    rows_out = []

    for s in sorted(chunks.keys()):
        chunk_dir = chunks[s]
        stitched_path = None
        for fn in os.listdir(chunk_dir):
            if fn.endswith('__STITCHED_v1.ndjson') and fn.startswith('events__'):
                stitched_path = os.path.join(chunk_dir, fn)
                break
        if not stitched_path:
            continue
        patched_path = stitched_path.replace('__STITCHED_v1.ndjson', '__STITCHED_v1__CROSSCHUNK_PATCHED_v1.ndjson')

        stitched = load_ndjson(stitched_path)
        stitched_idx = {event_key(ev): ev for ev in stitched}
        patched = None
        if os.path.exists(patched_path):
            patched = load_ndjson(patched_path)
            patched_idx = {event_key(ev): ev for ev in patched}
        else:
            patched_idx = {}

        # Determine candidate pages using stitcher fn if available, otherwise fall back to stitched last-event refs==0
        by_page = build_events_by_page(stitched)
        candidate_pages = []
        for p in sorted(by_page.keys()):
            last_ev = by_page[p][-1]
            missing = False
            try:
                if callable(event_missing_fn):
                    missing = bool(event_missing_fn(last_ev))
                else:
                    missing = refs_count(last_ev) == 0
            except Exception:
                missing = refs_count(last_ev) == 0
            if missing:
                candidate_pages.append(p)

        chunk_report = {'chunk': os.path.basename(chunk_dir), 'stitched': os.path.basename(stitched_path), 'patched_exists': os.path.exists(patched_path), 'candidates': []}

        # For each candidate page, produce before/after summary
        for p in candidate_pages:
            last_ev = by_page[p][-1]
            key = event_key(last_ev)
            before = {
                'page': p,
                'record_index': int(last_ev['meta']['record_index']),
                'refs_before': refs_count(last_ev),
                'parties_before': parties_count(last_ev),
                'primary_before': primary_ref_summary(last_ev),
                'trailing_y_before': has_trailing_y(last_ev),
            }

            after_ev = patched_idx.get(key)
            after = None
            if after_ev is not None:
                after = {
                    'refs_after': refs_count(after_ev),
                    'parties_after': parties_count(after_ev),
                    'primary_after': primary_ref_summary(after_ev),
                    'trailing_y_after': has_trailing_y(after_ev),
                }

            # contamination heuristic: compare primary_before to the next event's primary in stitched
            contamination = False
            next_ev = None
            # try to find next event in stitched list after last_ev
            try:
                idx = stitched.index(last_ev)
                if idx + 1 < len(stitched):
                    next_ev = stitched[idx+1]
                    if primary_ref_summary(last_ev) and primary_ref_summary(next_ev) and primary_ref_summary(last_ev) == primary_ref_summary(next_ev):
                        contamination = True
            except Exception:
                pass

            rec = {'chunk': os.path.basename(chunk_dir), 'candidate_page': p, 'key': key, 'before': before, 'after': after, 'contamination': contamination}
            chunk_report['candidates'].append(rec)
            rows_out.append(rec)

        report['chunks'].append(chunk_report)

    out_path = args.out or os.path.join(args.work_root, 'qa__TB_STITCH__PAGEBREAK_INTEGRITY.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)

    # Print concise table
    print(f"[done] report={out_path} candidates={len(rows_out)}\n")
    header = ['chunk', 'page', 'rec_idx', 'refs_before', 'refs_after', 'primary_before', 'primary_after', 'trailingY_before', 'trailingY_after', 'contam']
    print('\t'.join(header))
    for r in rows_out[:args.limit]:
        b = r['before']
        a = r.get('after') or {}
        row = [r['chunk'], str(r['candidate_page']), str(b.get('record_index') or ''), str(b.get('refs_before') or 0), str(a.get('refs_after') or ''), str(b.get('primary_before') or ''), str(a.get('primary_after') or ''), str(b.get('trailing_y_before') or False), str(a.get('trailing_y_after') or ''), str(r.get('contamination') or False)]
        print('\t'.join(row))

if __name__ == '__main__':
    main()
