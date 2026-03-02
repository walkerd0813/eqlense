import json, re, collections

sp = r"""C:\seller-app\backend\publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"""
ev = r"""C:\seller-app\backend\publicData\registry\hampden\_events_DEED_ONLY_v1\deed_events.ndjson"""
outp = r"""C:\seller-app\backend\publicData\registry\hampden\_audit\spine_index_collision_audit_HAMPDEN_ONLY_v1_7_18.json"""

def town_norm(x):
    return (x or "").strip().upper()

# read towns from events
towns=set()
with open(ev,"r",encoding="utf-8") as f:
    for line in f:
        o=json.loads(line)
        t = town_norm(o.get("town_norm") or o.get("town_raw") or o.get("town") or "")
        if t: towns.add(t)

# variants similar to what we're using (enough to detect collisions)
UNIT_RE = re.compile(r"\s+(UNIT|APT|APARTMENT|#)\s+.*$", re.I)
STTYPE_RE = re.compile(r"\s+(ST|STREET|RD|ROAD|AVE|AVENUE|DR|DRIVE|LN|LANE|BLVD|BOULEVARD|CT|COURT|PL|PLACE|TER|TERRACE|HWY|HIGHWAY|PKWY|PARKWAY)\.?$", re.I)

def variants(a):
    a = (a or "").strip().upper()
    if not a: return []
    v=[a]
    v.append(UNIT_RE.sub("", a).strip())
    v.append(STTYPE_RE.sub("", a).strip())
    out=[]
    for x in v:
        x=" ".join(x.split())
        if x and x not in out: out.append(x)
    return out

seen={}
collisions=0
collide_keys=collections.Counter()
rows_seen=0
keys_generated=0

with open(sp,"r",encoding="utf-8") as f:
    for line in f:
        rows_seen += 1
        o=json.loads(line)
        t = town_norm(o.get("town") or o.get("town_norm") or "")
        if t not in towns:
            continue
        addr = (o.get("full_address") or o.get("address") or "").strip().upper()
        pid = o.get("property_id") or ""
        if not t or not addr or not pid:
            continue
        for av in variants(addr):
            k=f"{t}|{av}"
            keys_generated += 1
            if k in seen and seen[k] != pid:
                collisions += 1
                collide_keys[k] += 1
            else:
                seen[k] = pid

audit = {
  "towns_count": len(towns),
  "rows_seen": rows_seen,
  "keys_generated": keys_generated,
  "unique_keys": len(seen),
  "collision_events": collisions,
  "top_colliding_keys": [{"key":k,"count":c} for k,c in collide_keys.most_common(50)]
}
open(outp,"w",encoding="utf-8").write(json.dumps(audit, indent=2))
print(json.dumps(audit, indent=2))

