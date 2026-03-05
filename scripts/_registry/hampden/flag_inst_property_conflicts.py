#!/usr/bin/env python3
import json
from pathlib import Path
from collections import defaultdict

joined = Path('fixtures/hampden_run_0_400/joined.ndjson')
out_csv = Path('fixtures/hampden_run_0_400/INSPECT/inst_property_conflicts.csv')

if not joined.exists():
    print('joined.ndjson not found at', joined)
    raise SystemExit(1)

mapping = defaultdict(set)  # (inst,page) -> set of (town,address)
example_rows = defaultdict(list)
with joined.open(encoding='utf-8') as f:
    for line in f:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        inst = str((obj.get('recording') or {}).get('inst_raw') or obj.get('inst_raw') or '')
        meta = obj.get('meta') or {}
        page = meta.get('page_index')
        props = obj.get('property_refs') or []
        if props:
            p0 = props[0]
            town = (p0.get('town') or '').strip()
            addr = (p0.get('address_raw') or '').strip()
            mapping[(inst, page)].add((town, addr))
            example_rows[(inst, page)].append({'town': town, 'addr': addr, 'meta': meta, 'evidence': obj.get('evidence')})

conflicts = []
for (inst, page), refs in mapping.items():
    if len(refs) > 1:
        conflicts.append({'inst': inst, 'page': page, 'count': len(refs), 'refs': list(refs)})

# write CSV-like output
out_lines = ["inst,page,count,refs"]
for c in sorted(conflicts, key=lambda x: (int(x['inst']) if x['inst'].isdigit() else x['inst'], x['page'] or -1)):
    refs_str = '|'.join([f"{t}:{a}" for t,a in c['refs']])
    out_lines.append(f"{c['inst']},{c['page']},{c['count']},{refs_str}")

out_csv.write_text('\n'.join(out_lines), encoding='utf-8')
print('conflicts_found:', len(conflicts))
if conflicts:
    print('example:', conflicts[:5])
