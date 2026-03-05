#!/usr/bin/env python3
import json
from pathlib import Path
from collections import defaultdict

CHUNK_DIR = Path('fixtures/hampden_run_0_400')
STITCHED = CHUNK_DIR / 'stitched.ndjson'
JOINED = CHUNK_DIR / 'joined_fix.ndjson'
ROWCTX = CHUNK_DIR / 'rowctx.ndjson'
OUT = CHUNK_DIR / 'INSPECT' / 'chunk_verifier_337_336-338.json'

pages = set([336,337,338])

def read_ndjson(p):
    with open(p, 'r', encoding='utf-8') as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)

def refs_set(ev):
    out = set()
    for r in ev.get('property_refs') or []:
        town = (r.get('town') or '').strip().upper()
        addr = (r.get('address_raw') or '').strip().upper()
        out.add((town, addr))
    return out

stitched_map = {}
for ev in read_ndjson(STITCHED):
    m = ev.get('meta') or {}
    p = m.get('page_index')
    r = m.get('record_index')
    if p in pages:
        stitched_map[(p, r)] = ev

joined_map = defaultdict(list)
for ev in read_ndjson(JOINED):
    m = ev.get('meta') or {}
    p = m.get('page_index')
    r = m.get('record_index')
    if p in pages:
        joined_map[(p, r)].append(ev)

rowctx_map = defaultdict(list)
for rc in read_ndjson(ROWCTX):
    p = rc.get('page_index')
    r = rc.get('record_index')
    if p in pages:
        rowctx_map[(p, r)].append(rc)

report = []
for (p,r), stitched in sorted(stitched_map.items()):
    s_refs = refs_set(stitched)
    # find joined candidates at same page, same record index and neighbors
    joined_cands = []
    for rr in range(max(1, r-2), r+3):
        joined_cands.extend(joined_map.get((p, rr), []))
    cand_summ = []
    for j in joined_cands:
        j_refs = refs_set(j)
        overlap = len(s_refs & j_refs)
        rc = j.get('rowctx') or {}
        cand_summ.append({
            'inst': (j.get('recording') or {}).get('inst_raw') or j.get('inst_raw'),
            'page_index': (j.get('meta') or {}).get('page_index'),
            'record_index': (j.get('meta') or {}).get('record_index'),
            'refs': list(j_refs)[:2],
            'overlap_with_stitched': overlap,
            'rowctx': rc,
        })
    report.append({'stitched_page': p, 'stitched_record': r, 'stitched_refs': list(s_refs), 'candidates': cand_summ})

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(report, indent=2), encoding='utf-8')
print('wrote', OUT)
