import json, collections, sys
sp = r"""C:\seller-app\backend\publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"""
outp = r"""C:\seller-app\backend\publicData\registry\hampden\_audit\spine_index_collision_audit_v1_7_18.json"""

seen = {}
collisions = 0
collide_keys = collections.Counter()

def variants(a):
    # match the *basic* ones we know are used (unit strip + street-type strip)
    a = (a or "").strip().upper()
    if not a: return []
    v = [a]
    # strip " UNIT ..." etc
    import re
    v.append(re.sub(r"\s+(UNIT|APT|APARTMENT|#)\s+.*$", "", a).strip())
    # strip trailing street type token
    v.append(re.sub(r"\s+(ST|STREET|RD|ROAD|AVE|AVENUE|DR|DRIVE|LN|LANE|BLVD|BOULEVARD|CT|COURT|PL|PLACE|TER|TERRACE|HWY|HIGHWAY|PKWY|PARKWAY)\.?$", "", a).strip())
    # unique, non-empty
    out=[]
    for x in v:
        x=" ".join(x.split())
        if x and x not in out: out.append(x)
    return out

total=0
indexed=0
for line in open(sp,"r",encoding="utf-8"):
    o=json.loads(line)
    town=(o.get("town") or "").strip().upper()
    addr=(o.get("full_address") or "").strip().upper()
    pid=o.get("property_id") or ""
    total += 1
    if not town or not addr or not pid: 
        continue
    for av in variants(addr):
        k=f"{town}|{av}"
        if k in seen and seen[k]!=pid:
            collisions += 1
            collide_keys[k]+=1
        else:
            seen[k]=pid
        indexed += 1

top = collide_keys.most_common(50)
audit = {
    "rows_seen": total,
    "keys_generated": indexed,
    "unique_keys": len(seen),
    "collision_events": collisions,
    "top_colliding_keys": [{"key":k,"count":c} for k,c in top]
}
open(outp,"w",encoding="utf-8").write(json.dumps(audit, indent=2))
print(json.dumps(audit, indent=2))

