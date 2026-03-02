import json, re, os
from collections import defaultdict, Counter

CUR = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
INP = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_4.ndjson"

OUT = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_5.ndjson"
AUD = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_5.json"

RE_MULTI_SPACE = re.compile(r"\s+")
RE_PUNCT = re.compile(r"[^\w\s]")
RE_TRAIL_Y = re.compile(r"\s+Y\s*$")
RE_UNIT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|NO\.|STE|SUITE|FL|FLOOR)\b.*$", re.I)
RE_LOT  = re.compile(r"\b(?:LOT|PAR|PARCEL)\b.*$", re.I)

SUFFIX_ALIAS = {
  "BLVD": {"BL", "BLVD"},
  "PKWY": {"PKY", "PKWY"},
  "TERR": {"TERR", "TER", "TERRACE"},
  "TER": {"TER", "TERR", "TERRACE"},
  "LN": {"LN","LA","LANE"},
  "RD": {"RD","ROAD"},
  "ST": {"ST","STREET"},
  "AVE": {"AVE","AV","AVENUE"},
  "DR": {"DR","DRIVE"},
  "CT": {"CT","COURT"},
  "CIR": {"CIR","CI","CIRCLE"},
  "PL": {"PL","PLACE"},
  "WAY": {"WAY"},
}

CANON_SUFFIX = {
  "BL":"BLVD","BLVD":"BLVD",
  "PKY":"PKWY","PKWY":"PKWY",
  "TERRACE":"TERR","TERR":"TERR","TER":"TERR",
  "LA":"LN","LANE":"LN","LN":"LN",
  "ROAD":"RD","RD":"RD",
  "STREET":"ST","ST":"ST",
  "AVENUE":"AVE","AV":"AVE","AVE":"AVE",
  "DRIVE":"DR","DR":"DR",
  "COURT":"CT","CT":"CT",
  "CIRCLE":"CIR","CI":"CIR","CIR":"CIR",
  "PLACE":"PL","PL":"PL",
  "WAY":"WAY",
}

def norm(s: str) -> str:
    s = (s or "").upper()
    s = RE_TRAIL_Y.sub("", s)
    s = RE_PUNCT.sub(" ", s)              # remove punctuation deterministically
    s = RE_MULTI_SPACE.sub(" ", s).strip()
    return s

def strip_unit_lot(s: str) -> str:
    s = RE_UNIT.sub("", s).strip()
    s = RE_LOT.sub("", s).strip()
    return s

def split_num_and_rest(addr: str):
    a = strip_unit_lot(norm(addr))
    m = re.match(r"^(\d+)\s+(.+)$", a)
    if not m:
        return None, None, a
    return m.group(1), m.group(2), a

def street_from_full(full_addr: str, street_no: str):
    # full_addr may already include street_no; remove it deterministically
    s = strip_unit_lot(norm(full_addr))
    if street_no:
        s2 = re.sub(rf"^\s*{re.escape(str(street_no))}\s+", "", s)
        s = s2.strip()
    return s

def canon_suffix_tokens(tokens):
    if not tokens: return tokens
    last = tokens[-1]
    if last in CANON_SUFFIX:
        tokens = tokens[:-1] + [CANON_SUFFIX[last]]
    return tokens

def variants(street: str):
    # returns a set of deterministic street variants
    out = set()
    s = strip_unit_lot(norm(street))
    if not s:
        return out
    toks = s.split()
    toks = canon_suffix_tokens(toks)
    base = " ".join(toks)
    out.add(base)

    # suffix alias: if last token has alias set, generate each canonicalized alias
    if toks:
        last = toks[-1]
        # normalize last to canonical key bucket
        canon_last = CANON_SUFFIX.get(last, last)
        alias_set = SUFFIX_ALIAS.get(canon_last)
        if alias_set:
            for a in alias_set:
                a = CANON_SUFFIX.get(a, a)
                out.add(" ".join(toks[:-1] + [a]))

    # no-suffix variant
    if len(toks) >= 2 and toks[-1] in set(CANON_SUFFIX.values()):
        out.add(" ".join(toks[:-1]))

    # singularize non-suffix tokens (safe-ish fallback) e.g., HILLS->HILL, WOODS->WOOD
    toks2 = toks[:]
    for i in range(len(toks2)):
        t = toks2[i]
        if t.endswith("S") and len(t) >= 4 and t not in set(CANON_SUFFIX.values()):
            toks2[i] = t[:-1]
    out.add(" ".join(toks2))
    if len(toks2) >= 2 and toks2[-1] in set(CANON_SUFFIX.values()):
        out.add(" ".join(toks2[:-1]))

    return {x for x in out if x}

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            yield json.loads(line)

def main():
    ptr = json.load(open(CUR, "r", encoding="utf-8"))
    spine_path = ptr.get("properties_ndjson")
    if not spine_path or not os.path.exists(spine_path):
        raise RuntimeError("Could not resolve spine ndjson path from CURRENT pointer JSON.")

    # Build indexes
    idx_full = {}  # town|FULL_ADDRESS -> property_id
    idx_no_street = defaultdict(list)  # town|no|street_variant -> [property_id...]

    spine_rows = 0
    for row in iter_ndjson(spine_path):
        spine_rows += 1
        town = norm(row.get("town"))
        pid = row.get("property_id") or row.get("id") or row.get("parcel_id")
        if not town or not pid:
            continue

        street_no = (row.get("street_no") or "").strip()
        full_addr = row.get("full_address") or ""
        if full_addr:
            fa = norm(full_addr)
            idx_full[f"{town}|{fa}"] = pid

        if street_no and full_addr:
            st = street_from_full(full_addr, street_no)
            for v in variants(st):
                idx_no_street[f"{town}|{street_no}|{v}"].append(pid)

    stats = Counter()
    os.makedirs(os.path.dirname(AUD).replace("\\","/"), exist_ok=True)

    out_count = 0
    with open(OUT, "w", encoding="utf-8") as w:
        for ev in iter_ndjson(INP):
            a = ev.get("attach", {}) or {}
            status = (a.get("attach_status") or "").upper()

            # pass through already-attached
            if status and status != "UNKNOWN":
                stats["already_attached_or_not_unknown"] += 1
                w.write(json.dumps(ev, ensure_ascii=False) + "\n")
                out_count += 1
                continue

            pr = ev.get("property_ref", {}) or {}
            town = norm(pr.get("town_norm") or pr.get("town_raw"))
            addr = pr.get("address_norm") or pr.get("address_raw") or ""

            no, rest, cleaned = split_num_and_rest(addr)
            if not town or not addr:
                stats["unknown_missing_town_or_addr"] += 1
                w.write(json.dumps(ev, ensure_ascii=False) + "\n")
                out_count += 1
                continue

            if not no:
                # keep UNKNOWN
                a["attach_status"] = "UNKNOWN"
                a["why"] = "no_num"
                ev["attach"] = a
                stats["single_still_unknown__no_num"] += 1
                w.write(json.dumps(ev, ensure_ascii=False) + "\n")
                out_count += 1
                continue

            # 1) direct full_address exact
            full_key = f"{town}|{norm(addr)}"
            pid = idx_full.get(full_key)
            if pid:
                a.update({
                    "attach_scope": "SINGLE",
                    "attach_status": "ATTACHED_A",
                    "property_id": pid,
                    "match_method": "axis2_full_address_exact",
                    "match_key": full_key,
                })
                a["evidence"] = (a.get("evidence") or {})
                a["evidence"].update({
                    "join_method": "town+full_address OR town+street_no+street_from_full unique_only (+suffix-alias/+singular fallback)",
                    "join_basis": "axis2_fulladdr_or_streetfromfull_v1_5",
                })
                ev["attach"] = a
                stats["single_upgraded_to_attached"] += 1
                w.write(json.dumps(ev, ensure_ascii=False) + "\n")
                out_count += 1
                continue

            # 2) town|no|street_from_full variants (unique-only)
            hits = []
            # the candidate street is rest already (not including the number)
            cand_street = rest or cleaned
            for v in variants(cand_street):
                k = f"{town}|{no}|{v}"
                hits.extend(idx_no_street.get(k, []))

            # unique the hits
            hits = list(dict.fromkeys(hits))
            if len(hits) == 1:
                a.update({
                    "attach_scope": "SINGLE",
                    "attach_status": "ATTACHED_A",
                    "property_id": hits[0],
                    "match_method": "axis2_street_from_full_unique",
                    "match_key": f"{town}|{no}|{norm(cand_street)}",
                })
                a["evidence"] = (a.get("evidence") or {})
                a["evidence"].update({
                    "join_method": "town+full_address OR town+street_no+street_from_full unique_only (+suffix-alias/+singular fallback)",
                    "join_basis": "axis2_fulladdr_or_streetfromfull_v1_5",
                })
                ev["attach"] = a
                stats["single_upgraded_to_attached"] += 1
            elif len(hits) > 1:
                a["attach_status"] = "UNKNOWN"
                a["why"] = "collision"
                stats["single_still_unknown__collision"] += 1
            else:
                a["attach_status"] = "UNKNOWN"
                a["why"] = "no_match"
                stats["single_still_unknown__no_match"] += 1

            ev["attach"] = a
            w.write(json.dumps(ev, ensure_ascii=False) + "\n")
            out_count += 1

    audit = {
        "script": os.path.basename(__file__),
        "in": INP,
        "out": OUT,
        "spine": spine_path,
        "out_rows": out_count,
        "stats": dict(stats),
    }
    with open(AUD, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print("=== AXIS2 REATTACH (>=10k) v1_5 ===")
    print(json.dumps({"out": OUT, "audit": AUD, "stats": dict(stats)}, ensure_ascii=False))

if __name__ == "__main__":
    main()
