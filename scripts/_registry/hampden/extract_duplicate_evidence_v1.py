"""
Scan `fixtures/hampden_run_0_400/joined.ndjson`, group events by recording.inst_raw,
and write evidence bundles for any inst with more than one joined event to
`fixtures/hampden_run_0_400/duplicate_evidence/<inst>.json`.

Usage: run from repo root (script will use workspace-relative paths)
"""
import os, json, io
from collections import defaultdict

ROOT = os.path.join(os.path.dirname(__file__), '..', '..', '..')
ROOT = os.path.abspath(ROOT)
CHUNK_DIR = os.path.join(ROOT, 'fixtures', 'hampden_run_0_400')
JOINED_ND = os.path.join(CHUNK_DIR, 'joined.ndjson')
OUT_DIR = os.path.join(CHUNK_DIR, 'duplicate_evidence')
os.makedirs(OUT_DIR, exist_ok=True)

# Defensive: fallback filenames
if not os.path.exists(JOINED_ND):
    print('ERROR: joined.ndjson not found at', JOINED_ND)
    raise SystemExit(1)

# Group joined events by instrument id
groups = defaultdict(list)
count = 0
with io.open(JOINED_ND, 'r', encoding='utf8') as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except Exception:
            # skip malformed lines but log first few
            count += 1
            continue
        inst = None
        try:
            inst = (ev.get('recording') or {}).get('inst_raw')
        except Exception:
            inst = None
        if not inst:
            inst = '<<NO_INST_{}_{}>>'.format(ev.get('meta',{}).get('page_index','?'), ev.get('meta',{}).get('record_index','?'))
        groups[str(inst)].append(ev)
        count += 1

print('scanned joined.ndjson rows=', count, 'unique_insts=', len(groups))

# Find duplicates (more than one joined event)
dups = {k:v for k,v in groups.items() if len(v) > 1}
print('found duplicate inst groups=', len(dups))

# Write per-inst evidence files (limit per-inst to first 200 events to avoid giant files)
for inst, evs in sorted(dups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
    safe_inst = inst.replace('/', '_').replace('\\','_').replace(' ','_')[:150]
    outp = os.path.join(OUT_DIR, f'{safe_inst}.json')
    bundle = {
        'inst': inst,
        'count': len(evs),
        'sample_size': min(len(evs), 200),
        'events': evs[:200]
    }
    with io.open(outp, 'w', encoding='utf8') as of:
        json.dump(bundle, of, indent=2, ensure_ascii=False)

print('wrote evidence files to', OUT_DIR)

# Write a summary JSON
summary = {
    'scanned_rows': count,
    'unique_insts': len(groups),
    'duplicate_inst_count': len(dups),
    'evidence_dir': os.path.relpath(OUT_DIR, ROOT)
}
with io.open(os.path.join(CHUNK_DIR, 'duplicate_evidence_summary.json'), 'w', encoding='utf8') as sf:
    json.dump(summary, sf, indent=2)

print('WROTE', os.path.join(CHUNK_DIR, 'duplicate_evidence_summary.json'))
