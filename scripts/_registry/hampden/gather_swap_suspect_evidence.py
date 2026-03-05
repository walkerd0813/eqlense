import csv, json, os
from pathlib import Path

inspect_csv = Path('fixtures/hampden_run_0_400/INSPECT/swap_suspects.csv')
joined_ndjson = Path('fixtures/hampden_run_0_400/joined_norm.ndjson')
out_dir = Path('fixtures/hampden_run_0_400/suspect_evidence')

if not inspect_csv.exists():
    print('ERROR: swap_suspects.csv not found at', inspect_csv)
    raise SystemExit(1)
if not joined_ndjson.exists():
    print('ERROR: joined_norm.ndjson not found at', joined_ndjson)
    raise SystemExit(1)

insts = set()
with inspect_csv.open(newline='') as f:
    reader = csv.DictReader(f)
    for r in reader:
        inst = (r.get('inst_raw') or '').strip()
        if inst:
            insts.add(inst)

os.makedirs(out_dir, exist_ok=True)
found = {}
with joined_ndjson.open(encoding='utf-8') as jf:
    for line in jf:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        inst = str(obj.get('inst_raw') or '')
        if inst in insts:
            found.setdefault(inst, []).append(obj)

for inst, records in found.items():
    out_path = out_dir / f"{inst}.json"
    with out_path.open('w', encoding='utf-8') as w:
        json.dump(records, w, indent=2)

print('insts_in_inspect:', len(insts))
print('evidence_files_written:', len(found))
if missing := sorted(insts - set(found.keys())):
    print('missing_in_joined:', len(missing), missing[:10])
else:
    print('all insts present in joined output')
