import json, re, collections

sp = r"""C:\seller-app\backend\publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"""
ev = r"""C:\seller-app\backend\publicData\registry\hampden\_events_DEED_ONLY_v1\deed_events.ndjson"""
outp = r"""C:\seller-app\backend\publicData\registry\hampden\_audit\spine_index_collision_audit_HAMPDEN_ONLY_v1_7_18.json"""

def norm(x): return (x or "").strip().upper()

TOWN_KEYS = ["town_norm","town_raw","town","city","municipality","muni","jurisdiction"]

def extract_town(o):
    # 1) top-level
    for k in TOWN_KEYS:
        v = o.get(k)
        if isinstance(v,str) and v.strip():
            return norm(v)

    # 2) common nests
    for p in ["property","address","registry","document","meta","source"]:
        d = o.get(p)
        if isinstance(d, dict):
            for k in TOWN_KEYS:
                v = d.get(k)
                if isinstance(v,str) and v.strip():
                    return norm(v)

    # 3) address_candidates
    ac = o.get("address_candidates")
    if isinstance(ac, list):
        for c in ac:
            if isinstance(c, dict):
                for k in TOWN_KEYS:
                    v = c.get(k)
                    if isinstance(v,str) and v.strip():
                        return norm(v)
                # sometimes it's nested again
                a = c.get("address")
                if isinstance(a, dict):
                    for k in TOWN_KEYS:
                        v = a.get(k)
                        if isinstance(v,str) and v.strip():
                            return norm(v)
    return ""

# build towns set from events
towns=set()
with open(ev,"r",encoding="utf-8") as f:
    for line in f:
        o=json.loads(line)
        t = extract_town(o)
        if t: towns.add(t)

print("TOWNS FOUND:", len(towns))
if len(towns) <= 2:
    # dump a couple sample lines to help debug if still wrong
    with open(ev,"r",encoding="utf-8") as f:
        for i,line in enumerate(f):
            if i>4: break
            o=json.loads(line)
            print("SAMPLE", i, "town_extracted=", extract_town(o), "keys=", list(o.keys())[:25])

# variant logic (for collision detection only)
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
        t = norm(o.get("town") or o.get("town_norm") or o.get("municipality") or "")
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

