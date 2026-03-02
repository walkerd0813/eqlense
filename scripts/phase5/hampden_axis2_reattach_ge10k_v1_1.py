import json, re, os
from collections import defaultdict, Counter

CUR = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
CAND = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k.ndjson"

OUT_NDJSON = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_1.ndjson"
OUT_AUDIT  = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_1.json"

RE_MULTI_SPACE = re.compile(r"\s+")
RE_TRAIL_Y = re.compile(r"\s+Y\s*$")
RE_PUNCT_KEEP = re.compile(r"[^A-Z0-9 #\-\/]")  # keep # - / for unit-ish patterns

RE_UNIT_TAIL = re.compile(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR)\b\s*([A-Z0-9\-]+)\b.*$", re.I)
RE_HASH_UNIT = re.compile(r"#\s*([A-Z0-9\-]+)\b.*$")
RE_LOT_TAIL  = re.compile(r"\b(?:LOT|PAR|PARCEL)\b.*$", re.I)

RE_RANGE = re.compile(r"^(?P<a>\d+)\s*-\s*(?P<b>\d+)\s+(?P<rest>.+)$")
RE_NUM_REST = re.compile(r"^(?P<num>[0-9A-Z]+)\s+(?P<rest>.+)$")

SUFFIX_FIX = {
  " LA":" LN",     # Lane (common in your data)
  " TERR":" TER",
  " PKY":" PKWY",
}

# street suffix canonical set (for stripping / comparison)
SUFFIX_SET = {
  "ST","RD","AVE","AV","BLVD","DR","LN","TER","PKWY","CT","CIR","WAY","PL","HWY","PK","PARK","TRL"
}

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
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

def norm_full_address(s: str) -> str:
    # full_address in spine may contain commas; we normalize hard
    s = norm_text(s)
    for k,v in SUFFIX_FIX.items():
        if s.endswith(k):
            s = s[:-len(k)] + v
            break
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
    # strip LOT/PAR tails first (land descriptors)
    s = RE_LOT_TAIL.sub("", s).strip()
    # strip UNIT tails (keep unit separately)
    s = re.sub(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR)\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"#\s*[A-Z0-9\-]+\b.*$", "", s).strip()
    s = RE_MULTI_SPACE.sub(" ", s).strip()
    return s

def parse_candidate_address(addr_raw: str):
    """
    Returns dict:
      kind: single|range|no_num
      a,b: range ends if range
      street_no: str for single
      street_name: normalized
      unit: normalized or None
      street_key: street_name normalized (with suffix fixes)
    """
    addr0 = norm_text(addr_raw)
    unit = extract_unit(addr0)
    core = strip_unit_and_lot(addr0)

    # apply suffix fixes on the whole string
    for k,v in SUFFIX_FIX.items():
        if core.endswith(k):
            core = core[:-len(k)] + v
            break

    m = RE_RANGE.match(core)
    if m:
        rest = norm_text(m.group("rest"))
        return {"kind":"range", "a":m.group("a"), "b":m.group("b"), "rest":rest, "unit":unit}

    m = RE_NUM_REST.match(core)
    if not m:
        return {"kind":"no_num", "raw":addr0, "unit":unit}

    street_no = norm_text(m.group("num"))
    rest = norm_text(m.group("rest"))
    return {"kind":"single", "street_no":street_no, "rest":rest, "unit":unit}

def normalize_street_name(rest: str) -> str:
    """
    rest is like: 'MAPLE ST' or 'REGENCY PARK DR'
    We keep suffix but normalize some last token forms (LA->LN etc already handled).
    """
    s = norm_text(rest)
    # also normalize TERR->TER, PKY->PKWY if last token
    tokens = s.split()
    if len(tokens) >= 2:
        last = tokens[-1]
        if last == "TERR":
            tokens[-1] = "TER"
        elif last == "PKY":
            tokens[-1] = "PKWY"
        elif last == "LA":
            tokens[-1] = "LN"
    return " ".join(tokens)

def load_spine_path():
    with open(CUR, "r", encoding="utf-8") as f:
        ptr = json.load(f)
    p = ptr.get("properties_ndjson")
    if not p:
        raise RuntimeError("CURRENT pointer missing properties_ndjson")
    return p

def build_spine_indexes(spine_path: str):
    """
    idx_full: town|full_address_norm -> pid or None if collision
    idx_unit: town|street_no|street_name|unit -> pid or None if collision
    idx_street: town|street_no|street_name -> list[pids]
    """
    idx_full = {}
    idx_full_collide = set()

    idx_unit = {}
    idx_unit_collide = set()

    idx_street = defaultdict(list)

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
            st = normalize_street_name(street_nm)
            ks = f"{town}|{sn}|{st}"
            idx_street[ks].append(pid)

            if unit:
                un = norm_text(unit)
                ku = f"{town}|{sn}|{st}|{un}"
                if ku in idx_unit and idx_unit[ku] != pid:
                    idx_unit_collide.add(ku)
                else:
                    idx_unit[ku] = pid

    # purge collisions so we never pick a random one
    for k in idx_full_collide:
        idx_full[k] = None
    for k in idx_unit_collide:
        idx_unit[k] = None

    return (idx_full, idx_unit, idx_street, rows)

def try_match_single(town: str, addr_raw: str, idx_full, idx_unit, idx_street):
    """
    Returns (pid, method, why) where pid may be None.
    """
    town = norm_text(town)
    if not town:
        return (None, None, "no_town")

    # 1) full_address exact (candidate may not include town/zip; still matches if spine full_address is similar)
    fk = f"{town}|{norm_full_address(addr_raw)}"
    pid = idx_full.get(fk)
    if pid:
        return (pid, "full_address_exact", None)
    if fk in idx_full and idx_full[fk] is None:
        return (None, "full_address_exact", "collision_full")

    # 2) components
    parsed = parse_candidate_address(addr_raw)
    if parsed["kind"] == "no_num":
        return (None, None, "no_num")

    if parsed["kind"] == "range":
        # handled elsewhere
        return (None, None, "range_unhandled_here")

    sn = parsed["street_no"]
    st = normalize_street_name(parsed["rest"])
    un = parsed.get("unit")

    if un:
        ku = f"{town}|{sn}|{st}|{un}"
        pid2 = idx_unit.get(ku)
        if pid2:
            return (pid2, "street+unit_exact", None)
        if ku in idx_unit and idx_unit[ku] is None:
            return (None, "street+unit_exact", "collision_unit")

    ks = f"{town}|{sn}|{st}"
    hits = idx_street.get(ks, [])
    if len(hits) == 1:
        return (hits[0], "street_unique_exact", None)
    if len(hits) > 1:
        return (None, "street_unique_exact", "collision_street")

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
    idx_full, idx_unit, idx_street, spine_rows = build_spine_indexes(spine_path)

    stats = Counter()
    out_rows = 0

    os.makedirs(os.path.dirname(OUT_AUDIT), exist_ok=True)

    with open(OUT_NDJSON, "w", encoding="utf-8") as fout:
        for ev in iter_ndjson(CAND):
            out_rows += 1
            a = ensure_attach_struct(ev)

            town = ev.get("property_ref", {}).get("town_norm") or ev.get("property_ref", {}).get("town_raw") or ""
            addr = ev.get("property_ref", {}).get("address_norm") or ev.get("property_ref", {}).get("address_raw") or ""

            scope = (a.get("attach_scope") or "SINGLE").upper()
            status_before = (a.get("attach_status") or "UNKNOWN").upper()

            # Only attempt for UNKNOWN/PARTIAL_MULTI (your axis2 set should be those)
            if status_before not in {"UNKNOWN", "PARTIAL_MULTI"}:
                stats["skipped_not_unknown_or_partial"] += 1
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            # MULTI: try to fix unknown attachments in-place; SINGLE: try to attach
            if scope == "MULTI" and a.get("attachments"):
                changed = 0
                for att in a["attachments"]:
                    if (att.get("attach_status") or "").upper() == "ATTACHED_A":
                        continue
                    t2 = att.get("town_norm") or town
                    a2 = att.get("address_norm") or att.get("address_raw") or ""
                    pid, method, why = try_match_single(t2, a2, idx_full, idx_unit, idx_street)
                    if pid:
                        att["attach_status"] = "ATTACHED_A"
                        att["property_id"] = pid
                        att["match_method"] = f"axis2_{method}"
                        att["match_key"] = f"{norm_text(t2)}|{norm_full_address(a2)}"
                        changed += 1
                    else:
                        # keep unknown; add optional why for audit/debug
                        att["match_method"] = att.get("match_method") or "axis2_no_match"
                        att["match_key"] = att.get("match_key") or f"{norm_text(t2)}|{norm_full_address(a2)}"
                        att["why"] = why

                # recompute overall status
                any_unknown = any((x.get("attach_status") or "").upper() != "ATTACHED_A" for x in a["attachments"])
                a["attach_status"] = "PARTIAL_MULTI" if any_unknown else "ATTACHED_A"
                if changed:
                    stats["multi_upgraded_some_attachments"] += 1
                else:
                    stats["multi_no_change"] += 1

                a["evidence"]["join_basis"] = "axis2_deterministic_components_v1_1"
                a["evidence"]["join_method"] = "town+full_address OR town+street_no+street_name(+unit) unique_only"
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            # SINGLE or MULTI with no attachments list
            # handle ranges deterministically
            parsed = parse_candidate_address(addr)
            if parsed["kind"] == "range":
                townN = norm_text(town)
                rest = normalize_street_name(parsed["rest"])
                a_num = norm_text(parsed["a"])
                b_num = norm_text(parsed["b"])

                pidA, methodA, whyA = try_match_single(townN, f"{a_num} {rest}", idx_full, idx_unit, idx_street)
                pidB, methodB, whyB = try_match_single(townN, f"{b_num} {rest}", idx_full, idx_unit, idx_street)

                a["attach_scope"] = "MULTI"
                a["attachments"] = []
                for num, pidX, methodX, whyX in [
                    (a_num, pidA, methodA, whyA),
                    (b_num, pidB, methodB, whyB),
                ]:
                    att = {
                        "town_norm": townN,
                        "address_norm": f"{num} {rest}",
                        "attach_status": "ATTACHED_A" if pidX else "UNKNOWN",
                        "property_id": pidX,
                        "match_method": f"axis2_{methodX}" if pidX else "axis2_no_match",
                        "match_key": f"{townN}|{norm_full_address(f'{num} {rest}')}",
                        "why": None if pidX else whyX,
                    }
                    a["attachments"].append(att)

                any_unknown = any((x.get("attach_status") or "").upper() != "ATTACHED_A" for x in a["attachments"])
                a["attach_status"] = "PARTIAL_MULTI" if any_unknown else "ATTACHED_A"
                stats["range_processed"] += 1

                a["property_id"] = None
                a["match_method"] = None
                a["match_key"] = None
                a["evidence"]["join_basis"] = "axis2_deterministic_components_v1_1"
                a["evidence"]["join_method"] = "range_endpoints_deterministic"
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            pid, method, why = try_match_single(town, addr, idx_full, idx_unit, idx_street)
            if pid:
                a["attach_scope"] = "SINGLE"
                a["attach_status"] = "ATTACHED_A"
                a["property_id"] = pid
                a["match_method"] = f"axis2_{method}"
                a["match_key"] = f"{norm_text(town)}|{norm_full_address(addr)}"
                stats["single_upgraded_to_attached"] += 1
            else:
                a["attach_status"] = "UNKNOWN"
                a["match_method"] = a.get("match_method") or "axis2_no_match"
                a["match_key"] = a.get("match_key") or f"{norm_text(town)}|{norm_full_address(addr)}"
                a["why"] = why
                stats[f"single_still_unknown__{why}"] += 1

            a["evidence"]["join_basis"] = "axis2_deterministic_components_v1_1"
            a["evidence"]["join_method"] = "town+full_address OR town+street_no+street_name(+unit) unique_only"
            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
      "candidates_in": CAND,
      "spine_ptr": CUR,
      "spine_rows_indexed": spine_rows,
      "out_ndjson": OUT_NDJSON,
      "stats": dict(stats),
      "notes": [
        "Deterministic-only. No fuzzy, no nearest.",
        "Tier A: town+full_address exact (normalized).",
        "Tier B: town+street_no+street_name+unit exact (when unit present).",
        "Tier C: town+street_no+street_name only if UNIQUE in spine.",
        "Ranges expanded to endpoints; attaches each endpoint deterministically."
      ]
    }
    with open(OUT_AUDIT, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print("=== AXIS2 REATTACH (>=10k) v1_1 ===")
    print(json.dumps({"out_rows": out_rows, "out": OUT_NDJSON, "audit": OUT_AUDIT, "stats": dict(stats)}, ensure_ascii=False))
    print(f"[ok] wrote: {OUT_NDJSON}")
    print(f"[ok] audit: {OUT_AUDIT}")

if __name__ == "__main__":
    main()
