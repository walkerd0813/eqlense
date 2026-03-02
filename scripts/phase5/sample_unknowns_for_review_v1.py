import json, random, sys
from collections import defaultdict

IN_FILE = sys.argv[1]
SAMPLE_SIZE = int(sys.argv[2]) if len(sys.argv) > 2 else 50

buckets = defaultdict(list)

with open(IN_FILE, "r", encoding="utf-8") as f:
    for line in f:
        ev = json.loads(line)
        attach = ev.get("attach", {})
        status = attach.get("attach_status", "UNKNOWN")
        method = attach.get("match_method") or "no_method"
        key = f"{status}|{method}"
        buckets[key].append(ev)

print(f"[info] loaded {sum(len(v) for v in buckets.values())} triage rows")
print(f"[info] buckets: {len(buckets)}")

# allocate samples evenly
keys = list(buckets.keys())
per_bucket = max(1, SAMPLE_SIZE // max(1, len(keys)))

picked = []

for k in keys:
    rows = buckets[k]
    take = min(per_bucket, len(rows))
    picked.extend(random.sample(rows, take))

# trim if we overshot
picked = picked[:SAMPLE_SIZE]

print(f"[info] showing {len(picked)} samples\n")

for i, ev in enumerate(picked, 1):
    pr = ev.get("property_ref", {})
    rec = ev.get("recording", {})
    att = ev.get("attach", {})

    print("=" * 80)
    print(f"Sample #{i}")
    print(f"event_id      : {ev.get('event_id')}")
    print(f"town          : {pr.get('town_norm') or pr.get('town_raw')}")
    print(f"address_raw   : {pr.get('address_raw')}")
    print(f"address_norm  : {pr.get('address_norm')}")
    print(f"book/page     : {rec.get('book')} / {rec.get('page')}")
    print(f"attach_status : {att.get('attach_status')}")
    print(f"match_method  : {att.get('match_method')}")
    print(f"attach_scope  : {att.get('attach_scope')}")
    print(f"is_multi      : {pr.get('primary_is_multi')}")
    if pr.get("multi_address"):
        print("multi_address :")
        for a in pr["multi_address"]:
            print(f"  - {a}")
