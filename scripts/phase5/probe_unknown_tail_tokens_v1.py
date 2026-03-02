import json, re
from collections import Counter

PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_3.ndjson"

def it(p):
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

suffix_last = Counter()
towns = Counter()
kinds = Counter()

for ev in it(PATH):
    a = ev.get("attach", {})
    if (a.get("attach_status") or "").upper() != "UNKNOWN":
        continue

    pr = ev.get("property_ref", {})
    addr = (pr.get("address_norm") or pr.get("address_raw") or "").upper().strip()
    town = (pr.get("town_norm") or pr.get("town_raw") or "").upper().strip()

    towns[town] += 1

    toks = addr.split()
    if toks:
        suffix_last[toks[-1]] += 1

    if re.match(r"^\d+\s*-\s*\d+\s+", addr):
        kinds["range_like"] += 1
    elif re.match(r"^\d+[A-Z]\b", addr):
        kinds["alpha_suffix_num (2B/10A)"] += 1
    elif re.match(r"^\d+\s+", addr):
        kinds["simple_num"] += 1
    else:
        kinds["no_num"] += 1

print("UNKNOWN towns top10:", towns.most_common(10))
print("UNKNOWN last-token top20:", suffix_last.most_common(20))
print("UNKNOWN kind:", dict(kinds))
