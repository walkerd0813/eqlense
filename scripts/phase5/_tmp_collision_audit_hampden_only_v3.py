import json, re, collections

SP = r"C:\seller-app\backend\publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"
EV = r"C:\seller-app\backend\publicData\registry\hampden\_events_DEED_ONLY_v1\deed_events.ndjson"
OUT = r"C:\seller-app\backend\publicData\registry\hampden\_audit\spine_index_collision_audit_HAMPDEN_ONLY_v3.json"

def norm(x): return (x or "").strip().upper()

def extract_town(ev):
    pr = ev.get("property_ref")
    if isinstance(pr, dict):
        t = pr.get("town_raw") or pr.get("town") or pr.get("town_norm")
        return norm(t) if isinstance(t,str) else ""
    return ""

def extract_addr(ev):
    pr = ev.get("property_ref")
    if isinstance(pr, dict):
        a = pr.get("address_raw") or pr.get("address") or pr.get("addr_raw")
        if isinstance(a,str) and a.strip():
            return " ".join(a.strip().upper().split())
    return ""

# 1) towns from events
towns=set()
sample=[]
with open(EV,"r",encoding="utf-8") as f:
    for line in f:
        ev=json.loads(line)
        t=extract_town(ev)
        if t:
            towns.add(t)
            if len(sample)<8:
                sample.append({"town":t,"addr":extract_addr(ev),"event_id":ev.get("event_id")})
print("TOWNS FOUND:", len(towns))
print("TOWN SAMPLE:", sample)

# 2) simple variants (collision audit only)
UNIT_RE = re.compile(r"\s+(UNIT|APT|APARTMENT|#)\s+.*$", re.I)
STTYPE_RE = re.compile(r"\s+(ST|STREET|RD|ROAD|AVE|AVENUE|DR|DRIVE|LN|LANE|BLVD|BOULEVARD|CT|COURT|PL|PLACE|TER|TERRACE|HWY|HIGHWAY|PKWY|PARKWAY)\.?$", re.I)

def variants(a):
    a=(a or "").strip().upper()
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
rows_kept=0
keys_generated=0

# NOTE: spine town key names vary; try common ones
def spine_town(o):
    return norm(o.get("town") or o.get("town_norm") or o.get("municipality") or o.get("city") or "")

def spine_addr(o):
    return (o.get("full_address") or o.get("address") or o.get("addr_norm") or "").strip().upper()

with open(SP,"r",encoding="utf-8") as f:
    for line in f:
        rows_seen += 1
        o=json.loads(line)
        t=spine_town(o)
        if t not in towns:
            continue
        rows_kept += 1
        addr = spine_addr(o)
        pid = o.get("property_id") or ""
        if not t or not addr or not pid:
            continue
        for av in variants(addr):
            k=f"{t}|{av}"
            keys_generated += 1
            prev = seen.get(k)
            if prev and prev != pid:
                collisions += 1
                collide_keys[k] += 1
            else:
                seen[k]=pid

audit={
  "towns_count": len(towns),
  "rows_seen": rows_seen,
  "rows_kept_in_towns": rows_kept,
  "keys_generated": keys_generated,
  "unique_keys": len(seen),
  "collision_events": collisions,
  "top_colliding_keys": [{"key":k,"count":c} for k,c in collide_keys.most_common(50)]
}
open(OUT,"w",encoding="utf-8").write(json.dumps(audit, indent=2))
print(json.dumps(audit, indent=2))

