import json, sys, os
from collections import Counter

spine_ndjson = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20000

FIELDS = ["address_norm","address","address_raw","address_full","site_address"]

type_counts = Counter()
dict_keys = Counter()
examples = []

seen = 0
with open(spine_ndjson, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except Exception:
            continue
        seen += 1
        # pick first present field among candidates
        val = None
        which = None
        for k in FIELDS:
            if k in r and r[k] is not None:
                val = r[k]; which = k; break
        if which is None:
            continue

        t = type(val).__name__
        type_counts[(which, t)] += 1

        if isinstance(val, dict):
            for kk in list(val.keys())[:50]:
                dict_keys[(which, kk)] += 1
            if len(examples) < 5:
                examples.append({"field": which, "keys": list(val.keys())[:25], "sample": val})

        if seen >= N:
            break

print("rows_scanned:", seen)
print("\nType counts (field,type)->count:")
for (k,t),c in type_counts.most_common(25):
    print(f"  {k:12s} {t:10s} {c}")

print("\nTop dict keys:")
for (k,kk),c in dict_keys.most_common(25):
    print(f"  {k:12s} {kk:20s} {c}")

print("\nExamples:")
for ex in examples:
    print(json.dumps(ex, indent=2)[:2000])
