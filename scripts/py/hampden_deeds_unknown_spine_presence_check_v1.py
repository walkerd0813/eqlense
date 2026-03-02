import json, re, collections
from datetime import datetime, timezone

ATTACHED = r"C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_v1_7_8\events_attached_DEED_ONLY_v1_7_8.ndjson"
SPINE_PTR = r"C:\seller-app\backend\publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"

def collapse_ws(s): 
    return re.sub(r"\s+"," ",(s or "").strip())

def norm_town(t):
    t = collapse_ws(t).upper()
    t = re.sub(r"\bADDR\b","",t,flags=re.I)
    t = collapse_ws(t)
    t = re.sub(r"\s+Y\s*$","",t,flags=re.I)
    return t

def norm_addr(a):
    a = collapse_ws(a).upper()
    a = re.sub(r"\s+Y\s*$","",a,flags=re.I)
    return collapse_ws(a)

# resolve spine pointer
with open(SPINE_PTR,"r",encoding="utf-8") as f:
    ptr = json.load(f)
spine_ndjson = ptr["properties_ndjson"]

# build a compact set of keys from UNKNOWNs only (so we don't index whole spine unnecessarily)
unknown_keys = set()
unknown_rows = []
with open(ATTACHED,"r",encoding="utf-8") as f:
    for line in f:
        if not line.strip(): 
            continue
        ev = json.loads(line)
        if ev.get("event_type") != "DEED": 
            continue
        if (ev.get("attach") or {}).get("status") != "UNKNOWN":
            continue
        pr = ev.get("property_ref") or {}
        t = norm_town(pr.get("town_raw") or pr.get("town") or "")
        a = norm_addr(pr.get("address_raw") or pr.get("address") or "")
        if t and a:
            k = f"{t}|{a}"
            unknown_keys.add(k)
            if len(unknown_rows) < 2000:  # cap memory
                unknown_rows.append(k)

print("[info] unknown_keys:", len(unknown_keys))

# now scan spine and count which unknown keys exist in spine exactly
hits = set()
with open(spine_ndjson,"r",encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        p = json.loads(line)
        t = norm_town(p.get("town") or "")
        a = norm_addr(p.get("full_address") or p.get("address") or "")
        if not t or not a:
            continue
        k = f"{t}|{a}"
        if k in unknown_keys:
            hits.add(k)

print("[done] unknown_keys_found_in_spine_exact:", len(hits))
print("[done] unknown_keys_missing_from_spine_exact:", len(unknown_keys) - len(hits))

# show a few missing examples
missing = [k for k in list(unknown_keys) if k not in hits][:20]
print("[sample_missing_keys]", missing[:20])
