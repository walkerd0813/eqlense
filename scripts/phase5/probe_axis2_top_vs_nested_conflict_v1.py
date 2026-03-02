import argparse, json
from collections import Counter

ap = argparse.ArgumentParser()
ap.add_argument("--in", dest="inp", required=True)
args = ap.parse_args()

c = Counter()
changed = 0
fuzzy_rows = 0
both_present = 0

with open(args.inp, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)

        top = (r.get("attach_status"), r.get("match_method"))
        nested = (r.get("attach", {}).get("attach_status"), r.get("attach", {}).get("match_method"))

        if "attach" in r:
            both_present += 1

        c[top] += 1

        mm = (r.get("match_method") or "").lower()
        if "fuzzy" in mm:
            fuzzy_rows += 1
            if nested[0] not in (None, "UNKNOWN"):
                changed += 1

print("top counts:", c.most_common(12))
print("both_present:", both_present)
print("fuzzy_rows:", fuzzy_rows)
print("fuzzy rows where nested was not UNKNOWN:", changed)
