import json, re
from collections import defaultdict

CUR = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
INP = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_4.ndjson"

RE_MULTI_SPACE = re.compile(r"\s+")
RE_TRAIL_Y = re.compile(r"\s+Y\s*$")
RE_UNIT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|NO\.|STE|SUITE|FL|FLOOR)\b.*$", re.I)
RE_LOT  = re.compile(r"\b(?:LOT|PAR|PARCEL)\b.*$", re.I)

def norm(s):
    s = (s or "").upper()
    s = RE_TRAIL_Y.sub("", s)
    s = RE_MULTI_SPACE.sub(" ", s).strip()
    return s

def strip_unit_lot(s):
    s = RE_UNIT.sub("", s).strip()
    s = RE_LOT.sub("", s).strip()
    return s

def parse(addr):
    a = strip_unit_lot(norm(addr))
    m = re.match(r"^(\d+)\s+(.+)$", a)
    if not m: return None, None, a
    return m.group(1), norm(m.group(2)), a

def iter_ndjson(p):
    with open(p,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if line: yield json.loads(line)

# resolve spine path
ptr = json.load(open(CUR,'r',encoding='utf-8'))
spine_path = ptr["properties_ndjson"]

# build town|street_no -> list of street_name samples
bucket = defaultdict(list)
for row in iter_ndjson(spine_path):
    town = norm(row.get("town"))
    no = (row.get("street_no") or "").strip()
    name = norm(row.get("street_name"))
    if town and no and name:
        if len(bucket[f"{town}|{no}"]) < 25:
            bucket[f"{town}|{no}"].append(name)

printed = 0
for ev in iter_ndjson(INP):
    a = ev.get("attach",{})
    if (a.get("attach_status") or "").upper() != "UNKNOWN":
        continue
    pr = ev.get("property_ref",{})
    town = norm(pr.get("town_norm") or pr.get("town_raw"))
    addr = pr.get("address_norm") or pr.get("address_raw") or ""
    no, street_name, cleaned = parse(addr)
    if not no:
        continue
    # only focus on the "no_match" pile
    if (a.get("why") or "") not in ("no_match",""):
        continue

    key = f"{town}|{no}"
    samples = bucket.get(key, [])
    print(json.dumps({
        "event_id": ev.get("event_id"),
        "town": town,
        "addr_norm": norm(addr),
        "parsed": {"street_no": no, "street_name": street_name},
        "spine_street_name_samples_same_no": samples[:10]
    }, ensure_ascii=False))
    printed += 1
    if printed >= 50:
        break

print("printed", printed)
