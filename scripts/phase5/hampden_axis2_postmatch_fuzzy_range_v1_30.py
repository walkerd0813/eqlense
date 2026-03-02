#!/usr/bin/env python3
"""
Hampden Axis2 PostMatch v1_30 (PS51SAFEFIX)
Conservative post-attach pass that ONLY upgrades rows currently UNKNOWN.

Adds ATTACHED_B via unique-only strategies:
  (1) unit-leading rewrite (numeric unit only): "SANDALWOOD DR UNIT 88" -> "88 SANDALWOOD DR"
  (2) drop-suffix street unique: "REGENCY PARK DR" -> "REGENCY PARK" when unique under (town, street_no)
  (3) <=1-edit fuzzy unique on street name within same (town, street_no)

No nearest, no best-guess, no broad range expansion.
Writes audit counters + sample outputs.

Expected inputs:
 - events NDJSON containing top-level attach fields and nested attach object (if present)
 - spine NDJSON containing property_id + town + address (raw or norm)

This script is intentionally defensive about field names in the spine.
"""
import argparse, json, re, os
from collections import defaultdict, Counter

VERSION = "v1_30"
SCOPE = "AXIS2_POSTMATCH_V1_30"

# canonical suffixes
SUF = {
    "LN":"LN","LANE":"LN","LA":"LN",
    "RD":"RD","ROAD":"RD",
    "DR":"DR","DRIVE":"DR",
    "ST":"ST","STREET":"ST",
    "AVE":"AVE","AV":"AVE","AVENUE":"AVE",
    "BLVD":"BLVD","BOULEVARD":"BLVD",
    "CT":"CT","COURT":"CT",
    "TERR":"TERR","TER":"TERR","TE":"TERR","TERRACE":"TERR",
    "CIR":"CIR","CIRCLE":"CIR","CI":"CIR",
    "PKY":"PKY","PARKWAY":"PKY",
    "PL":"PL","PLACE":"PL",
    "WAY":"WAY",
    "HTS":"HTS","HEIGHTS":"HTS"
}

UNIT_RE = re.compile(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE)\s*([A-Z0-9\-]+)\b", re.I)
HASH_UNIT_RE = re.compile(r"^(.*?)(?:\s+#+\s*([A-Z0-9\-]+))$", re.I)

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def w_ndjson(path, rows_iter):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows_iter:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def norm(s):
    if s is None:
        return ""
    s = str(s).upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def strip_punct(s: str) -> str:
    s = norm(s)
    # keep # for unit parsing, keep hyphen for ranges, otherwise remove punctuation
    s = re.sub(r"[.,;:()]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_street(raw_street: str):
    toks = strip_punct(raw_street).split()
    if not toks:
        return "", False
    last = toks[-1]
    alias = False
    if last in SUF:
        new_last = SUF[last]
        if new_last != last:
            alias = True
        toks[-1] = new_last
    return " ".join(toks), alias

def street_drop_suffix(street_norm: str):
    toks = street_norm.split()
    if len(toks) < 2:
        return street_norm, False
    last = toks[-1]
    # only drop if the last token is a canonical suffix value
    if last in set(SUF.values()):
        return " ".join(toks[:-1]), True
    return street_norm, False

def parse_unit(raw_addr: str):
    raw = strip_punct(raw_addr)
    m = UNIT_RE.search(raw)
    if m:
        u = m.group(1).strip().upper()
        return u if u else None
    # also try trailing # unit
    m2 = HASH_UNIT_RE.match(raw)
    if m2 and m2.group(2):
        u = m2.group(2).strip().upper()
        return u if u else None
    return None

def parse_addr(raw_addr: str):
    raw = strip_punct(raw_addr)
    if not raw:
        return None
    unit = parse_unit(raw)

    # remove unit trailing for street parsing
    raw_no_unit = re.sub(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE)\b.*$", "", raw, flags=re.I).strip()
    raw_no_unit = re.sub(r"\s+#+\s*[A-Z0-9\-]+\s*$", "", raw_no_unit, flags=re.I).strip()

    m = re.match(r"^(\d+)(?:\s*-\s*(\d+))?\s+(.*)$", raw_no_unit)
    if not m:
        return {"street_no": None, "street_no2": None, "is_range": False, "street_raw": raw_no_unit, "unit": unit}
    a = int(m.group(1))
    b = m.group(2)
    rest = m.group(3).strip()
    return {"street_no": a, "street_no2": int(b) if b else None, "is_range": b is not None, "street_raw": rest, "unit": unit}

def edit_leq1(a: str, b: str) -> int:
    """Return 0 if equal, 1 if exactly 1 edit, 2 if >1 edits."""
    if a == b:
        return 0
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return 2
    i = j = 0
    diff = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            diff += 1
            if diff > 1:
                return 2
            if la > lb:
                i += 1
            elif lb > la:
                j += 1
            else:
                i += 1
                j += 1
    if i < la or j < lb:
        diff += 1
    return 1 if diff == 1 else 2

def get_spine_town(row: dict) -> str:
    for k in ("town_norm","town","municipality","city","property_ref_town","property_ref.town_raw","property_ref.town_norm"):
        if k in row and row.get(k):
            return norm(row.get(k))
    # try nested structures
    pr = row.get("property_ref") or {}
    for k in ("town_norm","town_raw","town"):
        if pr.get(k):
            return norm(pr.get(k))
    return ""

def get_spine_address(row: dict) -> str:
    for k in ("address_norm","address","full_address","addr","site_address","property_ref_address","property_ref.address_raw","property_ref.address_norm"):
        if k in row and row.get(k):
            return strip_punct(row.get(k))
    pr = row.get("property_ref") or {}
    for k in ("address_norm","address_raw","address"):
        if pr.get(k):
            return strip_punct(pr.get(k))
    # some spines store address parts
    hn = row.get("house_number") or row.get("street_number")
    st = row.get("street_name") or row.get("street")
    if hn and st:
        return strip_punct(f"{hn} {st}")
    return ""

def get_spine_pid(row: dict) -> str:
    for k in ("property_id","pid","id"):
        if row.get(k):
            return str(row.get(k))
    return ""

def build_spine_indexes(spine_path: str):
    by_town_no = defaultdict(lambda: defaultdict(set))  # (town, no) -> street_norm -> {pid}
    fulladdr = defaultdict(set)  # town|fulladdr -> {pid}
    ctr = Counter()

    for r in iter_ndjson(spine_path):
        pid = get_spine_pid(r)
        if not pid:
            continue
        town = get_spine_town(r)
        addr = get_spine_address(r)
        if not town or not addr:
            ctr["spine_skip_missing_town_or_addr"] += 1
            continue

        fulladdr[f"{town}|{addr}"].add(pid)

        p = parse_addr(addr)
        if not p:
            ctr["spine_skip_unparseable"] += 1
            continue
        sn = p.get("street_no")
        if sn is None:
            ctr["spine_skip_no_num"] += 1
            continue
        street_norm, _ = norm_street(p.get("street_raw") or "")
        if not street_norm:
            ctr["spine_skip_no_street"] += 1
            continue
        by_town_no[(town, int(sn))][street_norm].add(pid)
        ctr["spine_rows_indexed"] += 1

    return by_town_no, fulladdr, ctr

def set_attach(row: dict, pid: str, method: str, why: str, evidence_extra: dict):
    # update BOTH top + nested if present
    top = row
    nested = row.get("nested")
    if isinstance(nested, dict):
        nested_obj = nested
    else:
        nested_obj = None

    for obj in (top, nested_obj):
        if obj is None:
            continue
        obj["attach_scope"] = "SINGLE"
        obj["attach_status"] = "ATTACHED_B"
        obj["property_id"] = pid
        obj["match_method"] = method
        obj["why"] = None
        ev = obj.get("evidence") or {}
        ev.update({
            "postmatch_scope": SCOPE,
            "postmatch_version": VERSION,
            "why_chosen": why,
        })
        if evidence_extra:
            ev.update(evidence_extra)
        obj["evidence"] = ev

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    by_town_no, fulladdr, spine_ctr = build_spine_indexes(args.spine)

    ctr = Counter()
    samples = {"attach_unit_leading": [], "attach_drop_suffix_unique": [], "attach_fuzzy_unique": [], "skip_unhandled": []}

    def pick_unique_pid(pid_set):
        if not pid_set:
            return None
        if len(pid_set) == 1:
            return next(iter(pid_set))
        return None

    def handle_row(row: dict):
        # only act on rows that are UNKNOWN both top+nested (or nested missing)
        top_status = (row.get("attach_status") or "").upper()
        nested = row.get("nested") if isinstance(row.get("nested"), dict) else None
        nested_status = (nested.get("attach_status") or "").upper() if nested else top_status

        if top_status != "UNKNOWN" or nested_status != "UNKNOWN":
            ctr["pass_through"] += 1
            return row

        town = norm(row.get("town") or row.get("property_ref", {}).get("town_raw") or row.get("property_ref", {}).get("town") or "")
        addr_raw = row.get("addr") or row.get("property_ref", {}).get("address_raw") or row.get("property_ref", {}).get("address") or ""
        addr_norm = strip_punct(addr_raw)
        p = parse_addr(addr_norm) if addr_norm else None
        if not p:
            ctr["skip_unhandled"] += 1
            if len(samples["skip_unhandled"]) < 10:
                samples["skip_unhandled"].append({"town": town, "addr": addr_raw})
            return row

        unit = p.get("unit")
        sn = p.get("street_no")
        street_norm, _ = norm_street(p.get("street_raw") or "")

        # (1) unit-leading rewrite numeric only when no street_no but unit exists
        if sn is None and unit and unit.isdigit():
            candidate_addr = strip_punct(f"{unit} {p.get('street_raw')}")
            key = f"{town}|{candidate_addr}"
            pid = pick_unique_pid(fulladdr.get(key, set()))
            if pid:
                set_attach(row, pid, "axis2_unit_leading_numeric_exact_unique", "unit-leading numeric rewrite matched unique full address",
                           {"unit_leading_from": addr_norm, "unit_leading_to": candidate_addr})
                ctr["attach_unit_leading"] += 1
                if len(samples["attach_unit_leading"]) < 10:
                    samples["attach_unit_leading"].append({"town": town, "from": addr_norm, "to": candidate_addr, "pid": pid})
                return row

        # from here we need street_no
        if sn is None:
            ctr["no_num"] += 1
            return row

        key_no = (town, int(sn))
        cand_map = by_town_no.get(key_no)
        if not cand_map:
            ctr["no_spine_candidates_same_no"] += 1
            return row

        # (2) drop suffix unique
        street_drop, dropped = street_drop_suffix(street_norm)
        if dropped:
            pid = pick_unique_pid(cand_map.get(street_drop, set()))
            if pid:
                set_attach(row, pid, "axis2_drop_suffix_unique", "dropped canonical suffix and matched unique street under same town+no",
                           {"street_from": street_norm, "street_to": street_drop})
                ctr["attach_drop_suffix_unique"] += 1
                if len(samples["attach_drop_suffix_unique"]) < 10:
                    samples["attach_drop_suffix_unique"].append({"town": town, "no": sn, "from": street_norm, "to": street_drop, "pid": pid})
                return row

        # (3) <=1 edit fuzzy unique on street name (same town+no)
        # try against both street_norm and drop-suffix version if exists
        targets = [street_norm]
        if street_drop != street_norm:
            targets.append(street_drop)

        best = None  # (dist, street_candidate, pid)
        for t in targets:
            for s_cand, pids in cand_map.items():
                d = edit_leq1(t, s_cand)
                if d <= 1:
                    pid = pick_unique_pid(pids)
                    if not pid:
                        continue
                    if best is None or d < best[0]:
                        best = (d, s_cand, pid)

        if best:
            d, s_cand, pid = best
            set_attach(row, pid, "axis2_street_no_fuzzy_unique_leq1", f"<=1 edit fuzzy unique within same town+no (d={d})",
                       {"street_from": street_norm, "street_to": s_cand, "edit_distance_leq1": d})
            ctr["attach_fuzzy_unique"] += 1
            if len(samples["attach_fuzzy_unique"]) < 10:
                samples["attach_fuzzy_unique"].append({"town": town, "no": sn, "from": street_norm, "to": s_cand, "pid": pid, "d": d})
            return row

        ctr["spine_has_same_no_but_no_close_street"] += 1
        return row

    out_rows = []
    rows_in = 0
    for row in iter_ndjson(args.inp):
        rows_in += 1
        out_rows.append(handle_row(row))

    w_ndjson(args.out, out_rows)

    audit = {
        "scope": SCOPE,
        "version": VERSION,
        "rows_in": rows_in,
        "counters": dict(ctr),
        "spine_index_counters": dict(spine_ctr),
        "samples": samples,
        "in": args.inp,
        "spine": args.spine,
        "out": args.out,
    }
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print("[done] v1_30 postmatch")
    for k in sorted(ctr.keys()):
        print(f"  {k}: {ctr[k]}")
    print("[ok] OUT  ", args.out)
    print("[ok] AUDIT", args.audit)

if __name__ == "__main__":
    main()
