import json, re
from collections import Counter, defaultdict

IN_ND = r"""publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_28.ndjson"""
SPINE = r"""publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"""
OUT = r"""publicData\_audit\registry\hampden_axis2_nomatch_diagnostics_v1.json"""

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            yield json.loads(line)

def norm(s):
    if s is None: return ""
    s = s.upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s

# very small, very safe suffix alias map for diagnostics (should mirror your runtime intent)
SUF = {
    "LN":"LN","LANE":"LN","LA":"LN",
    "RD":"RD","ROAD":"RD",
    "DR":"DR","DRIVE":"DR",
    "ST":"ST","STREET":"ST",
    "AVE":"AVE","AV":"AVE","AVENUE":"AVE",
    "BLVD":"BLVD","BOULEVARD":"BLVD",
    "CT":"CT","COURT":"CT",
    "TERR":"TERR","TER":"TERR","TE":"TERR","TERRACE":"TERR",
    "CIR":"CIR","CIRCLE":"CIR","CI":"CIR"
}

def norm_street(raw):
    toks = norm(raw).split()
    if not toks: return "", False
    last = toks[-1]
    alias = False
    if last in SUF:
        new_last = SUF[last]
        if new_last != last: alias = True
        toks[-1] = new_last
    return " ".join(toks), alias

UNIT_RE = re.compile(r"\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b", re.I)

def parse_addr(raw):
    raw = norm(raw)
    if not raw:
        return None
    # pull unit segment (very rough)
    unit = None
    m = re.search(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE|#)\s*([A-Z0-9\-]+)\b", raw)
    if m:
        unit = m.group(1)

    # leading number or range
    m2 = re.match(r"^(\d+)(?:\s*-\s*(\d+))?\s+(.*)$", raw)
    if not m2:
        return {"street_no": None, "range": False, "street_raw": raw, "unit": unit}
    a = int(m2.group(1))
    b = m2.group(2)
    rest = m2.group(3)
    is_range = b is not None
    return {"street_no": a, "range": is_range, "street_raw": rest, "unit": unit}

# Build a light spine index: town -> street_no -> set(street_norm)
sp = defaultdict(lambda: defaultdict(set))

for r in iter_ndjson(SPINE):
    town = norm(r.get("town") or r.get("town_norm") or "")
    if not town: 
        continue
    try:
        sn = r.get("street_no")
        if sn is None: 
            continue
        sn = int(sn)
        if sn <= 0: 
            continue
    except Exception:
        continue

    street = r.get("street_name") or ""
    street_norm, _ = norm_street(street)
    if street_norm:
        sp[town][sn].add(street_norm)

def edit1(a,b):
    # cheap distance for diagnostics: allow <=1 (insert/delete/substitute)
    if a==b: return 0
    la, lb = len(a), len(b)
    if abs(la-lb)>1: return 2
    # substitution or insert/delete
    # classic O(n) check for <=1
    i=j=0
    diff=0
    while i<la and j<lb:
        if a[i]==b[j]:
            i+=1; j+=1
        else:
            diff += 1
            if diff>1: return 2
            if la>lb: i+=1
            elif lb>la: j+=1
            else: i+=1; j+=1
    if i<la or j<lb: diff += 1
    return diff

counts = Counter()
examples = defaultdict(list)

for e in iter_ndjson(IN_ND):
    att = e.get("attach") or {}
    if att.get("attach_status") != "UNKNOWN" or att.get("match_method") != "no_match":
        continue

    town = norm((e.get("town") or "") or (e.get("property_ref") or {}).get("town_raw") or "")
    addr_raw = (e.get("addr") or "") or (e.get("property_ref") or {}).get("address_raw") or ""
    pa = parse_addr(addr_raw)

    if not town:
        counts["missing_town"] += 1
        continue

    if pa is None or not addr_raw:
        counts["missing_addr"] += 1
        continue

    if pa["street_no"] is None:
        counts["no_num"] += 1
        if len(examples["no_num"]) < 10:
            examples["no_num"].append({"town":town,"addr":addr_raw,"event_id":e.get("event_id")})
        continue

    if pa["range"]:
        counts["range_no"] += 1
        if len(examples["range_no"]) < 10:
            examples["range_no"].append({"town":town,"addr":addr_raw,"event_id":e.get("event_id")})

    street_norm, alias = norm_street(pa["street_raw"])
    has_unit = bool(pa["unit"]) or bool(UNIT_RE.search(addr_raw))
    if has_unit:
        counts["has_unit"] += 1

    sn = pa["street_no"]
    cands = sorted(list(sp[town].get(sn, [])))

    if not cands:
        counts["no_spine_candidates_same_no"] += 1
        if len(examples["no_spine_candidates_same_no"]) < 10:
            examples["no_spine_candidates_same_no"].append({"town":town,"addr":addr_raw,"event_id":e.get("event_id")})
        continue

    # compare against candidates for <=1 edit (strict)
    best = []
    for c in cands:
        d = edit1(street_norm, c)
        if d <= 1:
            best.append((d,c))
    best.sort()

    if not best:
        counts["spine_has_same_no_but_no_close_street"] += 1
        continue

    # unique best candidate?
    d0 = best[0][0]
    tops = [c for d,c in best if d==d0]
    if len(tops)==1:
        counts["fuzzy_unique_candidate"] += 1
        if len(examples["fuzzy_unique_candidate"]) < 25:
            examples["fuzzy_unique_candidate"].append({
                "town":town,"addr":addr_raw,"street_norm":street_norm,"candidate":tops[0],"dist":d0,"event_id":e.get("event_id")
            })
    else:
        counts["fuzzy_ambiguous_multi"] += 1

report = {
    "in": IN_ND,
    "spine": SPINE,
    "summary": dict(counts),
    "examples": examples
}

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

print("[ok] wrote", OUT)
print("summary:", dict(counts))
