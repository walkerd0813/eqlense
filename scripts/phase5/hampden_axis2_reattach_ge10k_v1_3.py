import json, re, os
from collections import defaultdict, Counter

CUR  = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
CAND = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k.ndjson"

OUT_NDJSON = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_3.ndjson"
OUT_AUDIT  = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_3.json"

RE_MULTI_SPACE = re.compile(r"\s+")
RE_TRAIL_Y = re.compile(r"\s+Y\s*$")
RE_PUNCT_KEEP = re.compile(r"[^A-Z0-9 #\-\/]")

RE_UNIT_TAIL = re.compile(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR)\b\s*([A-Z0-9\-]+)\b.*$", re.I)
RE_HASH_UNIT = re.compile(r"#\s*([A-Z0-9\-]+)\b.*$")
RE_LOT_TAIL  = re.compile(r"\b(?:LOT|PAR|PARCEL)\b.*$", re.I)

RE_RANGE = re.compile(r"^(?P<a>\d+)\s*-\s*(?P<b>\d+)\s+(?P<rest>.+)$")
RE_NUM_REST = re.compile(r"^(?P<num>[0-9A-Z]+)\s+(?P<rest>.+)$")

# Canonize common suffix words -> abbrev (candidate side)
SUFFIX_CANON = {
  "STREET":"ST", "ST":"ST",
  "ROAD":"RD", "RD":"RD",
  "AVENUE":"AVE", "AVE":"AVE", "AV":"AVE",
  "BOULEVARD":"BLVD", "BLVD":"BLVD",
  "DRIVE":"DR", "DR":"DR",
  "LANE":"LN", "LN":"LN", "LA":"LN",
  "TERRACE":"TER", "TERR":"TER", "TER":"TER",
  "PARKWAY":"PKWY", "PKWY":"PKWY", "PKY":"PKWY",
  "COURT":"CT", "CT":"CT",
  "CIRCLE":"CIR", "CIR":"CIR",
  "PLACE":"PL", "PL":"PL",
  "WAY":"WAY",
  "HIGHWAY":"HWY", "HWY":"HWY",
  "TRAIL":"TRL", "TRL":"TRL",
}

# Spine sometimes truncates suffix tokens (e.g., BLVD->BL)
# We'll allow a deterministic alias fallback ONLY on unique-only street matching.
SUFFIX_ALIAS = {
  "BLVD": ["BL"],   # boulevard truncated
  "AVE":  ["AV"],   # avenue truncated
  "PKWY": ["PKY","PK"],  # conservative
  "HWY":  ["HY"],   # conservative
  "TER":  ["TERR"], # sometimes reversed
  "LN":   ["LA"],   # already seen
}

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            yield json.loads(line)

def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).upper()
    s = RE_TRAIL_Y.sub("", s)
    s = RE_PUNCT_KEEP.sub(" ", s)
    s = RE_MULTI_SPACE.sub(" ", s).strip()
    return s

def canon_suffix_last_token(s: str) -> str:
    s = norm_text(s)
    if not s:
        return s
    toks = s.split()
    if len(toks) >= 2:
        last = toks[-1]
        if last in SUFFIX_CANON:
            toks[-1] = SUFFIX_CANON[last]
    return " ".join(toks)

def norm_full_address(s: str) -> str:
    return canon_suffix_last_token(s)

def street_nosuf(s: str) -> str:
    s = canon_suffix_last_token(s)
    toks = s.split()
    if len(toks) >= 2 and toks[-1] in set(SUFFIX_CANON.values()):
        return " ".join(toks[:-1])
    return s

def extract_unit(addr: str):
    if not addr:
        return None
    m = RE_UNIT_TAIL.search(addr)
    if m:
        return norm_text(m.group(1))
    m = RE_HASH_UNIT.search(addr)
    if m:
        return norm_text(m.group(1))
    return None

def strip_unit_and_lot(addr: str) -> str:
    if not addr:
        return ""
    s = norm_text(addr)
    s = RE_LOT_TAIL.sub("", s).strip()
    s = re.sub(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR)\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"#\s*[A-Z0-9\-]+\b.*$", "", s).strip()
    s = RE_MULTI_SPACE.sub(" ", s).strip()
    return s

def parse_candidate_address(addr_raw: str):
    addr0 = norm_text(addr_raw)
    unit = extract_unit(addr0)
    core = strip_unit_and_lot(addr0)

    m = RE_RANGE.match(core)
    if m:
        rest = canon_suffix_last_token(m.group("rest"))
        return {"kind":"range", "a":m.group("a"), "b":m.group("b"), "rest":rest, "unit":unit}

    m = RE_NUM_REST.match(core)
    if not m:
        return {"kind":"no_num", "raw":addr0, "unit":unit}

    street_no = norm_text(m.group("num"))
    rest = canon_suffix_last_token(m.group("rest"))
    return {"kind":"single", "street_no":street_no, "rest":rest, "unit":unit}

def load_spine_path():
    with open(CUR, "r", encoding="utf-8") as f:
        ptr = json.load(f)
    p = ptr.get("properties_ndjson")
    if not p:
        raise RuntimeError("CURRENT pointer missing properties_ndjson")
    return p

def build_spine_indexes(spine_path: str):
    idx_full = {}
    idx_full_collide = set()

    idx_unit = {}
    idx_unit_collide = set()

    idx_street = defaultdict(list)         # town|no|street_name
    idx_street_nosuf = defaultdict(list)   # town|no|street_name(without suffix)

    rows = 0
    for row in iter_ndjson(spine_path):
        rows += 1
        town = norm_text(row.get("town"))
        pid  = row.get("property_id") or row.get("id") or row.get("parcel_id")
        if not town or not pid:
            continue

        full_addr = row.get("full_address")
        if full_addr:
            k = f"{town}|{norm_full_address(full_addr)}"
            if k in idx_full and idx_full[k] != pid:
                idx_full_collide.add(k)
            else:
                idx_full[k] = pid

        street_no = row.get("street_no")
        street_nm = row.get("street_name")
        unit = row.get("unit")

        if street_no and street_nm:
            sn = norm_text(street_no)
            st = norm_text(street_nm)  # IMPORTANT: use spine's street_name as-is (it may be BL, AV, etc.)
            ks = f"{town}|{sn}|{st}"
            idx_street[ks].append(pid)

            st0 = street_nosuf(street_nm)
            ks0 = f"{town}|{sn}|{st0}"
            idx_street_nosuf[ks0].append(pid)

            if unit:
                un = norm_text(unit)
                ku = f"{town}|{sn}|{st}|{un}"
                if ku in idx_unit and idx_unit[ku] != pid:
                    idx_unit_collide.add(ku)
                else:
                    idx_unit[ku] = pid

    for k in idx_full_collide:
        idx_full[k] = None
    for k in idx_unit_collide:
        idx_unit[k] = None

    return (idx_full, idx_unit, idx_street, idx_street_nosuf, rows)

def suffix_alias_variants(street_rest: str):
    """
    Given candidate rest like 'PAGE BLVD', return possible alias variants:
    ['PAGE BL', ...] based on last token aliases.
    """
    s = canon_suffix_last_token(street_rest)
    toks = s.split()
    if len(toks) < 2:
        return []
    last = toks[-1]
    alts = SUFFIX_ALIAS.get(last, [])
    out = []
    for a in alts:
        out.append(" ".join(toks[:-1] + [a]))
    return out

def try_match_single(town: str, addr_raw: str, idx_full, idx_unit, idx_street, idx_street_nosuf):
    townN = norm_text(town)
    if not townN:
        return (None, None, "no_town")

    # 1) full_address exact (normalized)
    fk = f"{townN}|{norm_full_address(addr_raw)}"
    pid = idx_full.get(fk)
    if pid:
        return (pid, "full_address_exact", None)
    if fk in idx_full and idx_full[fk] is None:
        return (None, "full_address_exact", "collision_full")

    parsed = parse_candidate_address(addr_raw)
    if parsed["kind"] == "no_num":
        return (None, None, "no_num")
    if parsed["kind"] == "range":
        return (None, None, "range_unhandled_here")

    sn = parsed["street_no"]
    st = parsed["rest"]          # candidate canonicalized suffix (BLVD etc)
    un = parsed.get("unit")

    # 2) street+unit exact (spine street_name as-is, but candidate st is canonized;
    #    we also try alias variants if needed)
    if un:
        # exact
        ku = f"{townN}|{sn}|{st}|{un}"
        pid2 = idx_unit.get(ku)
        if pid2:
            return (pid2, "street+unit_exact", None)
        if ku in idx_unit and idx_unit[ku] is None:
            return (None, "street+unit_exact", "collision_unit")

        # alias variants
        for stv in suffix_alias_variants(st):
            ku2 = f"{townN}|{sn}|{stv}|{un}"
            pid3 = idx_unit.get(ku2)
            if pid3:
                return (pid3, "street+unit_suffix_alias_unique", None)
            if ku2 in idx_unit and idx_unit[ku2] is None:
                return (None, "street+unit_suffix_alias_unique", "collision_unit")

    # 3) street unique exact (candidate st)
    ks = f"{townN}|{sn}|{st}"
    hits = idx_street.get(ks, [])
    if len(hits) == 1:
        return (hits[0], "street_unique_exact", None)
    if len(hits) > 1:
        return (None, "street_unique_exact", "collision_street")

    # 3b) suffix-alias fallback (unique only)
    for stv in suffix_alias_variants(st):
        ks2 = f"{townN}|{sn}|{stv}"
        hits2 = idx_street.get(ks2, [])
        if len(hits2) == 1:
            return (hits2[0], "street_unique_suffix_alias", None)
        if len(hits2) > 1:
            return (None, "street_unique_suffix_alias", "collision_street")

    # 4) no-suffix fallback (unique only)
    st0 = street_nosuf(st)
    ks0 = f"{townN}|{sn}|{st0}"
    hits0 = idx_street_nosuf.get(ks0, [])
    if len(hits0) == 1:
        return (hits0[0], "street_unique_nosuf", None)
    if len(hits0) > 1:
        return (None, "street_unique_nosuf", "collision_street")

    return (None, None, "no_match")

def ensure_attach_struct(ev):
    if "attach" not in ev or not isinstance(ev["attach"], dict):
        ev["attach"] = {}
    a = ev["attach"]
    a.setdefault("attach_scope", "SINGLE")
    a.setdefault("attach_status", "UNKNOWN")
    a.setdefault("property_id", None)
    a.setdefault("match_method", None)
    a.setdefault("match_key", None)
    a.setdefault("attachments", [])
    a.setdefault("evidence", {})
    return a

def main():
    spine_path = load_spine_path()
    idx_full, idx_unit, idx_street, idx_street_nosuf, spine_rows = build_spine_indexes(spine_path)

    stats = Counter()
    os.makedirs(os.path.dirname(OUT_AUDIT), exist_ok=True)

    out_rows = 0
    with open(OUT_NDJSON, "w", encoding="utf-8") as fout:
        for ev in iter_ndjson(CAND):
            out_rows += 1
            a = ensure_attach_struct(ev)

            town = ev.get("property_ref", {}).get("town_norm") or ev.get("property_ref", {}).get("town_raw") or ""
            addr = ev.get("property_ref", {}).get("address_norm") or ev.get("property_ref", {}).get("address_raw") or ""

            scope = (a.get("attach_scope") or "SINGLE").upper()
            status_before = (a.get("attach_status") or "UNKNOWN").upper()

            if status_before not in {"UNKNOWN", "PARTIAL_MULTI"}:
                stats["skipped_not_unknown_or_partial"] += 1
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            # MULTI: upgrade each attachment deterministically
            if scope == "MULTI" and a.get("attachments"):
                changed = 0
                for att in a["attachments"]:
                    if (att.get("attach_status") or "").upper() == "ATTACHED_A":
                        continue
                    t2 = att.get("town_norm") or town
                    a2 = att.get("address_norm") or att.get("address_raw") or ""
                    pid, method, why = try_match_single(t2, a2, idx_full, idx_unit, idx_street, idx_street_nosuf)
                    if pid:
                        att["attach_status"] = "ATTACHED_A"
                        att["property_id"] = pid
                        att["match_method"] = f"axis2_{method}"
                        att["match_key"] = f"{norm_text(t2)}|{norm_full_address(a2)}"
                        changed += 1
                    else:
                        att["match_method"] = att.get("match_method") or "axis2_no_match"
                        att["match_key"] = att.get("match_key") or f"{norm_text(t2)}|{norm_full_address(a2)}"
                        att["why"] = why

                any_unknown = any((x.get("attach_status") or "").upper() != "ATTACHED_A" for x in a["attachments"])
                a["attach_status"] = "PARTIAL_MULTI" if any_unknown else "ATTACHED_A"
                stats["multi_upgraded_some_attachments" if changed else "multi_no_change"] += 1

                a["evidence"]["join_basis"] = "axis2_suffix_alias_v1_3"
                a["evidence"]["join_method"] = "town+full_address OR town+street_no+street_name(+unit) unique_only (+suffix-alias unique, +nosuf unique)"
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            # RANGE -> endpoints
            parsed = parse_candidate_address(addr)
            if parsed["kind"] == "range":
                townN = norm_text(town)
                rest = parsed["rest"]
                a_num = norm_text(parsed["a"])
                b_num = norm_text(parsed["b"])

                pidA, methodA, whyA = try_match_single(townN, f"{a_num} {rest}", idx_full, idx_unit, idx_street, idx_street_nosuf)
                pidB, methodB, whyB = try_match_single(townN, f"{b_num} {rest}", idx_full, idx_unit, idx_street, idx_street_nosuf)

                a["attach_scope"] = "MULTI"
                a["attachments"] = []
                for num, pidX, methodX, whyX in [(a_num, pidA, methodA, whyA),(b_num, pidB, methodB, whyB)]:
                    a["attachments"].append({
                        "town_norm": townN,
                        "address_norm": f"{num} {rest}",
                        "attach_status": "ATTACHED_A" if pidX else "UNKNOWN",
                        "property_id": pidX,
                        "match_method": f"axis2_{methodX}" if pidX else "axis2_no_match",
                        "match_key": f"{townN}|{norm_full_address(f'{num} {rest}')}",
                        "why": None if pidX else whyX,
                    })

                any_unknown = any((x.get("attach_status") or "").upper() != "ATTACHED_A" for x in a["attachments"])
                a["attach_status"] = "PARTIAL_MULTI" if any_unknown else "ATTACHED_A"
                stats["range_processed"] += 1

                a["property_id"] = None
                a["match_method"] = None
                a["match_key"] = None
                a["evidence"]["join_basis"] = "axis2_suffix_alias_v1_3"
                a["evidence"]["join_method"] = "range_endpoints_deterministic"
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            # SINGLE
            pid, method, why = try_match_single(town, addr, idx_full, idx_unit, idx_street, idx_street_nosuf)
            if pid:
                a["attach_scope"] = "SINGLE"
                a["attach_status"] = "ATTACHED_A"
                a["property_id"] = pid
                a["match_method"] = f"axis2_{method}"
                a["match_key"] = f"{norm_text(town)}|{norm_full_address(addr)}"
                stats["single_upgraded_to_attached"] += 1
            else:
                a["attach_status"] = "UNKNOWN"
                a["match_method"] = a.get("match_method") or "no_match"
                a["match_key"] = a.get("match_key") or f"{norm_text(town)}|{norm_full_address(addr)}"
                a["why"] = why
                stats[f"single_still_unknown__{why}"] += 1

            a["evidence"]["join_basis"] = "axis2_suffix_alias_v1_3"
            a["evidence"]["join_method"] = "town+full_address OR town+street_no+street_name(+unit) unique_only (+suffix-alias unique, +nosuf unique)"
            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
      "candidates_in": CAND,
      "spine_ptr": CUR,
      "spine_rows_indexed": spine_rows,
      "out_ndjson": OUT_NDJSON,
      "stats": dict(stats),
      "notes": [
        "Deterministic-only. No fuzzy, no nearest.",
        "Suffix canon + suffix-alias fallback for spine-truncated tokens (e.g., BLVD->BL).",
        "Unique-only acceptance for street keys and alias keys."
      ]
    }
    with open(OUT_AUDIT, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print("=== AXIS2 REATTACH (>=10k) v1_3 ===")
    print(json.dumps({"out": OUT_NDJSON, "audit": OUT_AUDIT, "stats": dict(stats)}, ensure_ascii=False))

if __name__ == "__main__":
    main()
