import json, re
from collections import defaultdict, Counter

CUR = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
CAND = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k.ndjson"

def load_spine_path():
    with open(CUR, "r", encoding="utf-8") as f:
        ptr = json.load(f)
    p = ptr.get("properties_ndjson")
    if not p:
        raise RuntimeError("CURRENT pointer missing properties_ndjson")
    return p

RE_MULTI_SPACE = re.compile(r"\s+")
RE_TRAIL_Y = re.compile(r"\s+Y\s*$")
RE_UNIT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|NO\.|STE|SUITE|FL|FLOOR)\b.*$", re.I)
RE_LOT  = re.compile(r"\b(?:LOT|PAR|PARCEL)\b.*$", re.I)

SUFFIX_MAP = {
  " LA":" LN",
  " LN":" LN",
  " TERR":" TER",
  " TER":" TER",
  " PKY":" PKWY",
  " PKWY":" PKWY",
  " BLVD":" BLVD",
  " AVE":" AVE",
  " ST":" ST",
  " RD":" RD",
  " DR":" DR",
  " CIR":" CIR",
  " CT":" CT",
  " WAY":" WAY",
  " PL":" PL",
}

def norm_full(a: str) -> str:
    if not a: return ""
    s = a.upper()
    s = RE_TRAIL_Y.sub("", s)
    s = RE_MULTI_SPACE.sub(" ", s).strip()
    return s

def strip_unit_lot(a: str) -> str:
    s = RE_UNIT.sub("", a).strip()
    s = RE_LOT.sub("", s).strip()
    return s

def norm_core(a: str) -> str:
    s = norm_full(a)
    s = strip_unit_lot(s)
    for k,v in SUFFIX_MAP.items():
        if s.endswith(k):
            s = s[: -len(k)] + v
            break
    tokens = s.split()
    if len(tokens) >= 2 and tokens[-1] in {"ST","RD","AVE","BLVD","DR","LN","LA","TER","TERR","PKY","PKWY","CT","CIR","WAY","PL"}:
        s_nosuf = " ".join(tokens[:-1])
    else:
        s_nosuf = s
    return s_nosuf

def split_num_and_rest(a: str):
    s = norm_core(a)
    m = re.match(r"^(?P<num>\d+)\s+(?P<rest>.+)$", s)
    if not m:
        return None, s
    return m.group("num"), m.group("rest")

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            yield json.loads(line)

def main():
    spine_path = load_spine_path()

    idx_full = {}                 # TOWN|FULL -> property_id
    idx_core = defaultdict(list)  # TOWN|NUM|REST -> [property_id...]

    for row in iter_ndjson(spine_path):
        town = (row.get("town_norm") or row.get("town") or row.get("address", {}).get("town_norm") or "").upper().strip()
        addr = row.get("address_norm") or row.get("address") or row.get("address_full") or row.get("address_line1") or row.get("address", {}).get("address_norm")
        if isinstance(addr, dict):
            addr = addr.get("value") or addr.get("text") or ""
        if not town or not addr:
            continue

        pid = row.get("property_id") or row.get("id") or row.get("parcel_id") or None
        if not pid:
            continue

        addr_full = norm_full(str(addr))
        idx_full[f"{town}|{addr_full}"] = pid

        num, rest = split_num_and_rest(str(addr))
        if num and rest:
            idx_core[f"{town}|{num}|{rest}"].append(pid)

    stats = Counter()
    examples = []

    for ev in iter_ndjson(CAND):
        town = (ev.get("property_ref", {}).get("town_norm") or "").upper().strip()
        addr = ev.get("property_ref", {}).get("address_norm") or ev.get("property_ref", {}).get("address_raw") or ""
        amt  = ev.get("consideration", {}).get("amount")
        if not town or not addr:
            continue

        full_key = f"{town}|{norm_full(str(addr))}"
        if full_key in idx_full:
            stats["already_full_match"] += 1
            continue

        num, rest = split_num_and_rest(str(addr))
        if not num or not rest:
            stats["no_num"] += 1
            continue

        core_key = f"{town}|{num}|{rest}"
        hits = idx_core.get(core_key, [])
        if len(hits) == 1:
            stats["core_unique_match"] += 1
            if len(examples) < 30:
                examples.append({
                    "event_id": ev.get("event_id"),
                    "town": town,
                    "addr": str(addr),
                    "amount": amt,
                    "core_key": core_key,
                    "property_id": hits[0]
                })
        elif len(hits) > 1:
            stats["core_collision"] += 1
        else:
            stats["core_no_match"] += 1

    print("=== SPINE PROBE (axis2_candidates_ge_10k) ===")
    print(dict(stats))
    print("\n--- up to 30 examples of core_unique_match ---")
    for ex in examples:
        print(json.dumps(ex, ensure_ascii=False))

if __name__ == "__main__":
    main()
