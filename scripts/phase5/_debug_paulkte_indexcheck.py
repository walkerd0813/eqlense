import json
from collections import defaultdict

spine = r"publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"
outp  = r"publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_28_FIXED.ndjson"

target = "MA|registry|deed|hampden|e046db4b3ac0c3038f86d91d"  # PAULK TER event

# 1) load the output row
row = None
for line in open(outp, "r", encoding="utf-8"):
    r = json.loads(line)
    if r.get("event_id") == target:
        row = r
        break

print("FOUND_OUT", bool(row))
if row:
    print("OUT town=", row.get("town"), "addr=", row.get("addr"))
    print("OUT attach=", row.get("attach"))

# 2) build tiny spine indexes for SPRINGFIELD only
town = "SPRINGFIELD"

idx_full = defaultdict(list)   # (town, FULL_ADDRESS_UPPER) -> [property_id...]
idx_sn   = defaultdict(list)   # (town, street_no, STREET_NAME_UPPER) -> [property_id...]

for line in open(spine, "r", encoding="utf-8"):
    s = json.loads(line)
    t = str(s.get("town") or "").strip().upper()
    if t != town:
        continue

    fa = str(s.get("full_address") or "").strip().upper()
    sn = str(s.get("street_no") or "").strip()
    st = str(s.get("street_name") or "").strip().upper()

    if fa:
        idx_full[(t, fa)].append(s.get("property_id"))
    if sn and st:
        idx_sn[(t, sn, st)].append(s.get("property_id"))

key_full = ("SPRINGFIELD", "86 PAULK TE")
key_sn   = ("SPRINGFIELD", "86", "PAULK TE")

print("SPINE has key_full =", idx_full.get(key_full))
print("SPINE has key_sn   =", idx_sn.get(key_sn))
