#!/usr/bin/env python3
"""Verify stitched transactions in fixture actual.json against continuation targets.

Produces a concise report for each stitched event: page/record, refs, parties,
descr_loc_raw, consideration, and overlap with candidate continuation events.
"""
import json
from pathlib import Path
from typing import List, Dict, Tuple

ACTUAL = Path(r"c:\seller-app\backend\fixtures\pagebreak\deed_pagebreak_case_01\actual.json")

def load():
    with open(ACTUAL, 'r', encoding='utf-8-sig') as f:
        return json.load(f).get('events', [])

def key(ev):
    return (int(ev.get('page_index') or 0), int(ev.get('record_index') or 0))

def refs_set(ev) -> set:
    out = set()
    for r in ev.get('property_refs') or []:
        town = (r.get('town') or '').strip().upper()
        addr = (r.get('address_raw') or '').strip().upper()
        out.add((town, addr))
    return out

def parties_list(ev) -> List[str]:
    p = ev.get('parties') or []
    # parties may be list[str] or list[dict]
    out = []
    for item in p:
        if isinstance(item, dict):
            out.append((item.get('name_raw') or '').strip())
        else:
            out.append(str(item).strip())
    return out

def find_candidates(events: List[dict], into_page: int) -> List[dict]:
    return [e for e in events if int(e.get('page_index') or 0) == into_page]

def report():
    events = load()
    idx: Dict[Tuple[int,int], dict] = {key(e): e for e in events}

    stitched = [e for e in events if (e.get('stitch') or {}).get('did_stitch')]
    if not stitched:
        print('No stitched events found')
        return

    for ev in stitched:
        st = ev.get('stitch') or {}
        from_page = int(st.get('from_page') or ev.get('page_index'))
        into_page = int(st.get('into_page') or (from_page + 1))
        rec = int(ev.get('record_index') or 0)

        print('---')
        print(f'Stitched event: page={from_page} rec={rec} -> into_page={into_page}')
        print('Refs (stitched):')
        for r in ev.get('property_refs') or []:
            print('  -', (r.get('town'), r.get('address_raw')))
        print('Parties (stitched):')
        for p in parties_list(ev):
            print('  -', p)
        print('descr_loc_raw:', ev.get('descr_loc_raw'))
        print('consideration_raw:', ev.get('consideration_raw'))

        # find candidates on into_page and compare
        cands = find_candidates(events, into_page)
        if not cands:
            print('No candidate events found on into_page')
            continue
        print(f'Found {len(cands)} candidate(s) on into_page {into_page}:')
        s_refs = refs_set(ev)
        for c in cands:
            crec = int(c.get('record_index') or 0)
            print(f'  Candidate rec={crec}:')
            print('    Refs (target):')
            for r in c.get('property_refs') or []:
                print('     -', (r.get('town'), r.get('address_raw')))
            ov = s_refs.intersection(refs_set(c))
            print('    Overlap refs:', len(ov), '->', ov)
            print('    Parties (target):')
            for p in parties_list(c):
                print('     -', p)
            print('    descr_loc_raw:', c.get('descr_loc_raw'))
            print('    consideration_raw:', c.get('consideration_raw'))

        # quick quality checks
        if any(refs_set(c) & s_refs for c in cands):
            print('Result: property_refs correctly continued/merged')
        else:
            print('Result: NO property_refs overlap — possible failure')

        # check trailing Y in stitched captured_top_lines
        captured = st.get('captured_top_lines') or []
        y_artifacts = [ln for ln in captured if ln.strip().endswith(' Y')]
        print('Captured top lines count:', len(captured), 'Trailing Y lines:', len(y_artifacts))

if __name__ == '__main__':
    report()
