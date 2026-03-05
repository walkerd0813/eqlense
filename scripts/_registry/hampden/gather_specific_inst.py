import sys, json, os
from pathlib import Path

joined = Path('fixtures/hampden_run_0_400/joined.ndjson')
out_dir = Path('fixtures/hampden_run_0_400/suspect_evidence')
insts = set(sys.argv[1:])
if not insts:
    print('Usage: gather_specific_inst.py INST1 [INST2 ...]')
    raise SystemExit(1)
if not joined.exists():
    print('ERROR: joined.ndjson not found at', joined)
    raise SystemExit(1)

os.makedirs(out_dir, exist_ok=True)
found = {}
with joined.open(encoding='utf-8') as f:
    for line in f:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        inst = str(obj.get('recording', {}).get('inst_raw') or obj.get('inst_raw') or '')
        if inst in insts:
            found.setdefault(inst, []).append(obj)

for inst, records in found.items():
    out_path = out_dir / f"{inst}.json"
    with out_path.open('w', encoding='utf-8') as w:
        json.dump(records, w, indent=2)

print('insts_requested:', sorted(insts))
print('evidence_files_written:', len(found))
if missing := sorted(insts - set(found.keys())):
    print('missing_in_joined:', missing)
else:
    print('all insts present in joined output')
