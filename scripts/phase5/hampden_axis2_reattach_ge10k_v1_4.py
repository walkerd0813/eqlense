import json, re, os
from collections import defaultdict, Counter

CUR = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
CAND = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_3.ndjson"

OUT  = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_4.ndjson"
AUD  = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_4.json"

RE_MULTI_SPACE = re.compile(r"\s+")
RE_TRAIL_Y = re.compile(r"\s+Y\s*$")
RE_UNIT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|NO\.|STE|SUITE|FL|FLOOR)\b.*$", re.I)
RE_LOT  = re.compile(r"\b(?:LOT|PAR|PARCEL)\b.*$", re.I)

KNOWN_SUFFIX = {"ST","RD","AVE","AV","BLVD","BL","DR","LN","LA","TER","TERR","CT","CIR","WAY","PL","PKWY","PKY"}

# extra aliasing beyond v1_3
SUFFIX_ALIAS = {
  "BLVD":"BL",
  "AVE":"AV",
  "TERR":"TER",
  "PKWY":"PKY",
  "LN":"LA",   # allow lane/lane-abbrev pairing both ways if encountered
  "LA":"LN",
}

def load_spine_path():
    with open(CUR, "r", encoding="utf-8") as f:
        ptr = json.load(f)
    p = ptr.get("properties_ndjson")
    if not p:
        raise RuntimeError("CURRENT pointer missing properties_ndjson")
    return p

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

def parse_addr(addr_raw: str):
    s = norm_full(addr_raw)
    s = strip_unit_lot(s)
    toks = s.split()
    if not toks:
        return None, None, s
    m = re.match(r"^(\d+)\s+(.+)$", s)
    if not m:
        return None, None, s
    street_no = m.group(1)
    street_name = m.group(2).strip()
    return street_no, street_name, s

def alias_suffix(street_name: str):
    toks = street_name.split()
    if not toks:
        return street_name
    last = toks[-1]
    if last in SUFFIX_ALIAS:
        toks[-1] = SUFFIX_ALIAS[last]
        return " ".join(toks)
    return street_name

def strip_trailing_unit_token_if_obvious(street_name: str):
    # If it looks like "... ST A" or "... RD 2" or "... AVE 305" -> drop final token.
    toks = street_name.split()
    if len(toks) < 2:
        return street_name
    last = toks[-1]
    prev = toks[-2]
    if prev in KNOWN_SUFFIX and re.match(r"^(?:[A-Z]|[0-9]+|[0-9]+[A-Z])$", last):
        return " ".join(toks[:-1])
    return street_name

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            yield json.loads(line)

def build_spine_indexes(spine_path: str):
    # Indexes:
    # 1) town|full_address -> property_id (direct)
    # 2) town|street_no|street_name -> [property_id...] (exact)
    idx_full = {}
    idx_street = defaultdict(list)

    for row in iter_ndjson(spine_path):
        town = (row.get("town") or "").upper().strip()
        full_address = row.get("full_address")
        street_no = row.get("street_no")
        street_name = row.get("street_name")

        if town and full_address:
            idx_full[f"{town}|{norm_full(str(full_address))}"] = row.get("property_id")

        if town and street_no and street_name:
            k = f"{town}|{str(street_no).strip()}|{norm_full(str(street_name))}"
            idx_street[k].append(row.get("property_id"))

    return idx_full, idx_street

def try_match(town, addr_norm, idx_full, idx_street):
    # Attempt ladder:
    # A) full address exact
    # B) street_no + street_name exact
    # C) street_no + suffix alias (BLVD->BL, AVE->AV, TERR->TER, etc) unique-only
    # D) street_no + strip trailing unit token unique-only (only if prev token is suffix)
    # E) (optional combo) alias + strip unit unique-only
    addr_norm = norm_full(addr_norm)

    # A
    k_full = f"{town}|{addr_norm}"
    pid = idx_full.get(k_full)
    if pid:
        return ("ATTACHED_A", pid, "axis2_full_address_exact", k_full)

    # parse street parts
    street_no, street_name, _ = parse_addr(addr_norm)
    if not street_no or not street_name:
        return ("UNKNOWN", None, "no_num", k_full)

    street_name_n = norm_full(street_name)

    # B
    k_st = f"{town}|{street_no}|{street_name_n}"
    hits = idx_street.get(k_st, [])
    if len(hits) == 1 and hits[0]:
        return ("ATTACHED_A", hits[0], "axis2_street_unique_exact", f"{town}|{street_no} {street_name_n}")
    if len(hits) > 1:
        return ("UNKNOWN", None, "collision_street", f"{town}|{street_no} {street_name_n}")

    # C alias suffix
    street_alias = norm_full(alias_suffix(street_name_n))
    if street_alias != street_name_n:
        k2 = f"{town}|{street_no}|{street_alias}"
        hits2 = idx_street.get(k2, [])
        if len(hits2) == 1 and hits2[0]:
            return ("ATTACHED_A", hits2[0], "axis2_street_unique_suffix_alias2", f"{town}|{street_no} {street_alias}")
        if len(hits2) > 1:
            return ("UNKNOWN", None, "collision_suffix_alias2", f"{town}|{street_no} {street_alias}")

    # D strip trailing unit-ish token
    street_stripped = norm_full(strip_trailing_unit_token_if_obvious(street_name_n))
    if street_stripped != street_name_n:
        k3 = f"{town}|{street_no}|{street_stripped}"
        hits3 = idx_street.get(k3, [])
        if len(hits3) == 1 and hits3[0]:
            return ("ATTACHED_A", hits3[0], "axis2_street_unique_strip_trailing_unit", f"{town}|{street_no} {street_stripped}")
        if len(hits3) > 1:
            return ("UNKNOWN", None, "collision_strip_trailing_unit", f"{town}|{street_no} {street_stripped}")

    # E combo alias + strip
    street_combo = norm_full(strip_trailing_unit_token_if_obvious(street_alias))
    if street_combo != street_name_n:
        k4 = f"{town}|{street_no}|{street_combo}"
        hits4 = idx_street.get(k4, [])
        if len(hits4) == 1 and hits4[0]:
            return ("ATTACHED_A", hits4[0], "axis2_street_unique_alias2_then_strip_unit", f"{town}|{street_no} {street_combo}")
        if len(hits4) > 1:
            return ("UNKNOWN", None, "collision_combo", f"{town}|{street_no} {street_combo}")

    return ("UNKNOWN", None, "no_match", f"{town}|{addr_norm}")

def main():
    spine_path = load_spine_path()
    idx_full, idx_street = build_spine_indexes(spine_path)

    stats = Counter()
    out_rows = 0

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    os.makedirs(os.path.dirname(AUD), exist_ok=True)

    with open(OUT, "w", encoding="utf-8") as w:
        for ev in iter_ndjson(CAND):
            a = ev.get("attach", {})
            # only try to improve UNKNOWNs; keep prior attachments as-is
            if (a.get("attach_status") or "").upper() == "UNKNOWN":
                pr = ev.get("property_ref", {})
                town = (pr.get("town_norm") or pr.get("town_raw") or "").upper().strip()
                addr = (pr.get("address_norm") or pr.get("address_raw") or "").strip()

                if town and addr:
                    st, pid, method, key = try_match(town, addr, idx_full, idx_street)
                    if st == "ATTACHED_A" and pid:
                        a["attach_status"] = "ATTACHED_A"
                        a["property_id"] = pid
                        a["match_method"] = method
                        a["match_key"] = key
                        a.pop("why", None)
                        stats["single_upgraded_to_attached"] += 1
                    else:
                        # preserve why signal but make it more specific
                        why = method if method.startswith("collision") else "no_match"
                        a["why"] = why
                        if method.startswith("collision"):
                            stats["single_still_unknown__collision"] += 1
                        elif method == "no_num":
                            stats["single_still_unknown__no_num"] += 1
                        else:
                            stats["single_still_unknown__no_match"] += 1
                else:
                    a["why"] = "missing_town_or_addr"
                    stats["single_still_unknown__missing_fields"] += 1

                ev["attach"] = a
            else:
                stats["already_attached_or_not_unknown"] += 1

            w.write(json.dumps(ev, ensure_ascii=False) + "\n")
            out_rows += 1

    audit = {
        "script": "hampden_axis2_reattach_ge10k_v1_4.py",
        "in": CAND,
        "out": OUT,
        "spine": spine_path,
        "stats": dict(stats),
        "out_rows": out_rows,
        "notes": [
            "Deterministic-only. Unique-only joins.",
            "Adds suffix aliasing (BLVD->BL, AVE->AV, TERR->TER, PKWY->PKY) and strips trailing unit-ish tokens when preceded by street suffix."
        ]
    }
    with open(AUD, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("=== AXIS2 REATTACH (>=10k) v1_4 ===")
    print(json.dumps({"out": OUT, "audit": AUD, "stats": dict(stats)}, ensure_ascii=False))

if __name__ == "__main__":
    main()
