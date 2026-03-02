import argparse, json, re
from collections import defaultdict, Counter

VERSION = "v1_31"
SCOPE = "AXIS2_POSTMATCH_V1_31"

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
    "WAY":"WAY"
}

CANON_SUFFIXES = set(SUF.values())

UNIT_RE = re.compile(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE|#)\s*([A-Z0-9\-]+)\b", re.I)


def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def w_ndjson(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def norm(s):
    if s is None:
        return ""
    s = str(s).upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_street(raw_street: str):
    toks = norm(raw_street).split()
    if not toks:
        return "", False
    last = toks[-1]
    alias = False
    if last in SUF:
        new_last = SUF[last]
        alias = (new_last != last)
        toks[-1] = new_last
    return " ".join(toks), alias


def drop_suffix_if_canonical(street_norm: str):
    toks = norm(street_norm).split()
    if len(toks) < 2:
        return street_norm, False
    last = toks[-1]
    if last in CANON_SUFFIXES:
        return " ".join(toks[:-1]), True
    return street_norm, False


def parse_unit(raw_addr: str):
    m = UNIT_RE.search(norm(raw_addr))
    if not m:
        return None
    u = m.group(1).strip().upper()
    return u if u else None


def parse_addr(raw_addr: str):
    raw = norm(raw_addr)
    if not raw:
        return None

    unit = parse_unit(raw)

    # strip trailing unit tokens for street parsing
    raw_no_unit = re.sub(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE|#)\b.*$", "", raw, flags=re.I).strip()

    # leading number or range
    m = re.match(r"^(\d+)(?:\s*-\s*(\d+))?\s+(.*)$", raw_no_unit)
    if not m:
        return {"street_no": None, "street_no2": None, "is_range": False, "street_raw": raw_no_unit, "unit": unit}

    a = int(m.group(1))
    b = m.group(2)
    rest = m.group(3).strip()

    return {
        "street_no": a,
        "street_no2": int(b) if b else None,
        "is_range": b is not None,
        "street_raw": rest,
        "unit": unit,
    }


def edit_leq1(a: str, b: str) -> int:
    """Return 0 if equal, 1 if exactly one edit away, else 2."""
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
            continue
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


def spine_get_town_addr_pid(r: dict):
    # Prefer pre-normalized columns if present
    town = r.get("town_norm") or r.get("town")
    addr = r.get("address_norm") or r.get("addr_norm") or r.get("address") or r.get("addr")
    pid = r.get("property_id") or r.get("propertyId") or r.get("pid")
    town_n = norm(town)
    addr_n = norm(addr)
    return town_n, addr_n, pid


def build_spine_indexes(spine_path: str):
    by_full = defaultdict(list)          # town|full_addr -> [pid]
    by_no = defaultdict(list)            # town|street_no -> [(street_norm, pid, full_addr_norm)]
    by_exact = defaultdict(list)         # town|street_no|street_norm -> [pid]
    by_exact_nosuf = defaultdict(list)   # town|street_no|street_nosuf -> [pid]

    rows = 0
    for r in iter_ndjson(spine_path):
        town_n, addr_n, pid = spine_get_town_addr_pid(r)
        if not town_n or not addr_n or not pid:
            continue
        rows += 1
        by_full[f"{town_n}|{addr_n}"].append(pid)
        pa = parse_addr(addr_n)
        if not pa:
            continue
        sn = pa.get("street_no")
        if sn is None:
            continue
        street_norm, _ = norm_street(pa.get("street_raw") or "")
        if not street_norm:
            continue
        by_no[f"{town_n}|{sn}"].append((street_norm, pid, addr_n))
        by_exact[f"{town_n}|{sn}|{street_norm}"].append(pid)
        street_nosuf, dropped = drop_suffix_if_canonical(street_norm)
        if dropped:
            by_exact_nosuf[f"{town_n}|{sn}|{street_nosuf}"].append(pid)

    meta = {"rows_indexed": rows}
    return by_full, by_no, by_exact, by_exact_nosuf, meta


def unique_pid(lst):
    if not lst:
        return None
    u = list(dict.fromkeys(lst))
    return u[0] if len(u) == 1 else None


def attempt_match(town_n, addr_raw, by_full, by_no, by_exact, by_exact_nosuf, ctr: Counter):
    pa = parse_addr(addr_raw)
    if not pa:
        ctr["no_match"] += 1
        return None

    unit = pa.get("unit")
    sn1 = pa.get("street_no")
    sn2 = pa.get("street_no2")
    is_range = bool(pa.get("is_range"))

    # Range handling: only endpoints, only small spans (institutionalized)
    if is_range and sn1 is not None and sn2 is not None:
        span = abs(sn2 - sn1)
        if span <= 4:
            # try each endpoint as full address first
            base_street = pa.get("street_raw") or ""
            street_norm, _ = norm_street(base_street)
            for endpoint in (sn1, sn2):
                addr_ep = f"{endpoint} {street_norm}".strip()
                pid = unique_pid(by_full.get(f"{town_n}|{addr_ep}", []))
                if pid:
                    ctr["attach_range_endpoint_full"] += 1
                    return {
                        "pid": pid,
                        "method": "axis2_range_endpoint_full_address_unique",
                        "why": None,
                        "meta": {"street_no_range": True, "range_span": span, "endpoint": endpoint}
                    }
            # fall through to regular parsing using sn1 (conservative)
        else:
            ctr["range_too_large_skip"] += 1
            return None

    # NO_NUM: allow unit-leading numeric like "SANDALWOOD DR UNIT 88"
    if sn1 is None:
        if unit and unit.isdigit():
            sn_try = int(unit)
            street_norm, _ = norm_street(pa.get("street_raw") or "")
            if street_norm:
                pid = unique_pid(by_exact.get(f"{town_n}|{sn_try}|{street_norm}", []))
                if pid:
                    ctr["attach_unit_leading"] += 1
                    return {
                        "pid": pid,
                        "method": "axis2_unit_leading_numeric_as_street_no",
                        "why": None,
                        "meta": {"street_no": sn_try, "unit": unit, "street_no_range": False}
                    }
        ctr["no_num"] += 1
        return None

    # 1) Full address exact unique
    addr_norm = norm(addr_raw)
    pid = unique_pid(by_full.get(f"{town_n}|{addr_norm}", []))
    if pid:
        ctr["attach_full_address_unique"] += 1
        return {"pid": pid, "method": "axis2_full_address_exact", "why": None, "meta": {"street_no_range": False}}

    street_norm, alias_applied = norm_street(pa.get("street_raw") or "")
    if not street_norm:
        ctr["no_match"] += 1
        return None

    # 2) Street+no exact unique
    pid = unique_pid(by_exact.get(f"{town_n}|{sn1}|{street_norm}", []))
    if pid:
        ctr["attach_street_unique_exact"] += 1
        return {"pid": pid, "method": "axis2_street_unique_exact", "why": None, "meta": {"street_no_range": False}}

    # 3) Suffix alias exact unique (counts separately for reporting)
    if alias_applied:
        pid = unique_pid(by_exact.get(f"{town_n}|{sn1}|{street_norm}", []))
        if pid:
            ctr["attach_street_unique_suffix_alias"] += 1
            return {"pid": pid, "method": "axis2_street_unique_suffix_alias", "why": None, "meta": {"street_no_range": False}}

    # 4) Drop suffix if canonical and unique
    street_nosuf, dropped = drop_suffix_if_canonical(street_norm)
    if dropped:
        pid = unique_pid(by_exact_nosuf.get(f"{town_n}|{sn1}|{street_nosuf}", []))
        if pid:
            ctr["attach_drop_suffix_unique"] += 1
            return {"pid": pid, "method": "axis2_drop_suffix_unique", "why": None, "meta": {"street_no_range": False}}

    # 5) Unit exact (only if unit exists and full-address fails): build as "NO STREET #UNIT"
    # We keep this extremely conservative: only match if spine has literal #UNIT in address_norm.
    if unit:
        addr_u = f"{sn1} {street_norm} #{unit}"
        pid = unique_pid(by_full.get(f"{town_n}|{addr_u}", []))
        if pid:
            ctr["attach_street_unit_exact"] += 1
            return {"pid": pid, "method": "axis2_street+unit_exact", "why": None, "meta": {"street_no_range": False, "unit": unit}}

    # 6) Fuzzy unique by town+street_no, edit distance <=1 on street name, must be unique best
    cand = by_no.get(f"{town_n}|{sn1}", [])
    if not cand:
        ctr["no_spine_candidates_same_no"] += 1
        return None

    # compute best edit distance across unique street names
    best = []
    for st_norm, pid_c, full_addr in cand:
        d = edit_leq1(st_norm, street_norm)
        best.append((d, st_norm, pid_c, full_addr))

    best.sort(key=lambda x: x[0])
    if not best:
        ctr["no_match"] += 1
        return None

    d0 = best[0][0]
    if d0 <= 1:
        # ensure uniqueness: only one candidate at distance d0 and next best > d0
        same_d0 = [x for x in best if x[0] == d0]
        if len(same_d0) == 1:
            # also require that pid is unique for that exact candidate street
            pid_hit = same_d0[0][2]
            ctr["attach_fuzzy_unique"] += 1
            return {
                "pid": pid_hit,
                "method": "axis2_street_no_fuzzy_unique_lev1" if d0 == 1 else "axis2_street_no_fuzzy_unique_lev0",
                "why": None,
                "meta": {"street_no": sn1, "street_norm": street_norm, "candidate_street": same_d0[0][1], "lev": d0, "street_no_range": False}
            }
        else:
            ctr["collision"] += 1
            return None

    ctr["spine_has_same_no_but_no_close_street"] += 1
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--spine", dest="spine", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--audit", dest="audit", required=True)
    args = ap.parse_args()

    by_full, by_no, by_exact, by_exact_nosuf, spine_meta = build_spine_indexes(args.spine)

    ctr = Counter()
    out_rows = []

    for r in iter_ndjson(args.inp):
        # Respect existing attachments (never downgrade)
        status = (r.get("attach_status") or r.get("top", {}).get("attach_status") or "").upper()
        if status.startswith("ATTACHED"):
            ctr["pass_through"] += 1
            out_rows.append(r)
            continue

        town_n = norm(r.get("town") or r.get("town_norm"))
        addr = r.get("addr") or r.get("address") or ""
        if not town_n or not addr:
            ctr["no_match"] += 1
            out_rows.append(r)
            continue

        hit = attempt_match(town_n, addr, by_full, by_no, by_exact, by_exact_nosuf, ctr)
        if not hit:
            # leave as UNKNOWN
            r["attach_scope"] = r.get("attach_scope") or "SINGLE"
            r["attach_status"] = "UNKNOWN"
            r["match_method"] = r.get("match_method") or "no_match"
            r["why"] = r.get("why") or "no_match"
            out_rows.append(r)
            continue

        # Upgrade attachment
        r["attach_scope"] = "SINGLE"
        # Fuzzy is Tier B; everything else is Tier A here
        r["attach_status"] = "ATTACHED_B" if "fuzzy" in hit["method"].lower() else "ATTACHED_A"
        r["property_id"] = hit["pid"]
        r["match_method"] = hit["method"]
        r["why"] = hit.get("why")
        r["match_meta"] = hit.get("meta") or {}

        out_rows.append(r)

    w_ndjson(args.out, out_rows)

    audit_obj = {
        "scope": SCOPE,
        "version": VERSION,
        "spine_meta": spine_meta,
        "counts": dict(ctr)
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit_obj, f, ensure_ascii=False, indent=2)

    print(f"[done] {VERSION} postmatch")
    for k in sorted(ctr.keys()):
        print(f"  {k}: {ctr[k]}")
    print(f"[ok] OUT   {args.out}")
    print(f"[ok] AUDIT {args.audit}")


if __name__ == "__main__":
    main()
