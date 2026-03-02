import argparse, json, re
from collections import Counter, defaultdict

VERSION = "v1_32_0"
SCOPE = "AXIS2_POSTMATCH_V1_32_0"

# Canonical suffix map (keep conservative)
SUF = {
    "LN":"LN","LANE":"LN","LA":"LN",
    "RD":"RD","ROAD":"RD",
    "DR":"DR","DRIVE":"DR",
    "ST":"ST","STREET":"ST",
    "AVE":"AVE","AV":"AVE","AVENUE":"AVE",
    "BLVD":"BLVD","BOULEVARD":"BLVD",
    "CT":"CT","COURT":"CT",
    "TERR":"TERR","TER":"TERR","TERRACE":"TERR",
    "CIR":"CIR","CIRCLE":"CIR","CI":"CIR",
    "PKY":"PKY","PARKWAY":"PKY",
    "PL":"PL","PLACE":"PL",
    "WAY":"WAY"
}

UNIT_RE = re.compile(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE|#)\s*([A-Z0-9\-]+)\b", re.I)

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
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

def drop_suffix(street_norm: str):
    toks = norm(street_norm).split()
    if len(toks) < 2:
        return street_norm, False
    last = toks[-1]
    # Only drop if it looks like a canonical suffix
    if last in set(SUF.values()):
        return " ".join(toks[:-1]), True
    return street_norm, False

def parse_unit(raw_addr: str):
    raw = norm(raw_addr)
    m = UNIT_RE.search(raw)
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
        "unit": unit
    }

def edit_leq1(a: str, b: str) -> int:
    # <=1 edit distance (0,1,2 where 2 means >1)
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

def extract_town_address_from_spine(sp):
    # We expect these based on earlier probes: town_norm + address_norm
    town = sp.get("town_norm") or sp.get("town") or sp.get("site_city") or sp.get("city")
    addr = sp.get("address_norm") or sp.get("addr_norm") or sp.get("full_address_norm") or sp.get("address")
    pid = sp.get("property_id") or sp.get("parcel_id") or sp.get("pid")
    return norm(town), norm(addr), pid

def want_keys_from_events(events):
    want_full = set()
    want_no_street = set()

    for r in events:
        if (r.get("attach_status") or "").upper() != "UNKNOWN":
            continue
        addr = r.get("addr") or r.get("address") or ""
        town = norm(r.get("town") or r.get("town_norm") or "")
        if not town:
            continue
        p = parse_addr(addr)
        if not p:
            continue

        # full address key
        want_full.add(f"{town}|{norm(addr)}")

        # street_no based
        if p.get("street_no") is not None:
            st_norm, _ = norm_street(p.get("street_raw") or "")
            if st_norm:
                want_no_street.add(f"{town}|{p['street_no']}|{st_norm}")
                st_nosuf, did = drop_suffix(st_norm)
                if did:
                    want_no_street.add(f"{town}|{p['street_no']}|{st_nosuf}")

        # range endpoints
        if p.get("is_range") and p.get("street_no2") is not None:
            a = p.get("street_no")
            b = p.get("street_no2")
            st_norm, _ = norm_street(p.get("street_raw") or "")
            if st_norm and a is not None and b is not None:
                for n in (a, b):
                    want_no_street.add(f"{town}|{n}|{st_norm}")
                    st_nosuf, did = drop_suffix(st_norm)
                    if did:
                        want_no_street.add(f"{town}|{n}|{st_nosuf}")

    return want_full, want_no_street

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--spine", dest="spine", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    ap.add_argument("--audit", dest="audit", required=True)
    args = ap.parse_args()

    events = list(iter_ndjson(args.inp))

    want_full, want_no_street = want_keys_from_events(events)

    # Build minimal indexes by scanning spine once and only capturing rows we might use.
    by_full = defaultdict(list)      # town|addr_norm -> [property_id]
    by_no_street = defaultdict(list) # town|no|street_norm -> [property_id]

    for sp in iter_ndjson(args.spine):
        town, addr_norm, pid = extract_town_address_from_spine(sp)
        if not town or not pid:
            continue

        # full address key
        if addr_norm:
            kf = f"{town}|{addr_norm}"
            if kf in want_full:
                by_full[kf].append(pid)

        # street_no based
        if addr_norm:
            p = parse_addr(addr_norm)
            if p and p.get("street_no") is not None:
                st_norm, _ = norm_street(p.get("street_raw") or "")
                if st_norm:
                    kn = f"{town}|{p['street_no']}|{st_norm}"
                    if kn in want_no_street:
                        by_no_street[kn].append(pid)
                    st_nosuf, did = drop_suffix(st_norm)
                    if did:
                        kn2 = f"{town}|{p['street_no']}|{st_nosuf}"
                        if kn2 in want_no_street:
                            by_no_street[kn2].append(pid)

    ctr = Counter()
    out_rows = []

    for r in events:
        st = (r.get("attach_status") or "").upper()
        if st != "UNKNOWN":
            ctr["pass_through"] += 1
            out_rows.append(r)
            continue

        town = norm(r.get("town") or r.get("town_norm") or "")
        addr_raw = r.get("addr") or r.get("address") or ""
        addr_norm = norm(addr_raw)
        p = parse_addr(addr_raw)
        if not p:
            ctr["no_parse"] += 1
            out_rows.append(r)
            continue

        # If no leading number, we do NOT guess.
        if p.get("street_no") is None:
            ctr["no_num"] += 1
            out_rows.append(r)
            continue

        st_norm, _ = norm_street(p.get("street_raw") or "")
        if not st_norm:
            ctr["no_street"] += 1
            out_rows.append(r)
            continue

        # 1) Full address exact unique
        kf = f"{town}|{addr_norm}"
        if kf in by_full and len(set(by_full[kf])) == 1:
            pid = list(set(by_full[kf]))[0]
            r["attach_status"] = "ATTACHED_A"
            r["match_method"] = "axis2_full_address_exact_unique"
            r["property_id"] = pid
            ctr["attach_full_exact"] += 1
            out_rows.append(r)
            continue

        # 2) Range endpoints: only attach if BOTH endpoints land on SAME single property.
        if p.get("is_range") and p.get("street_no2") is not None:
            a = p.get("street_no")
            b = p.get("street_no2")
            cands = []
            for n in (a, b):
                kn = f"{town}|{n}|{st_norm}"
                c1 = list(set(by_no_street.get(kn, [])))
                # try drop suffix fallback too
                if not c1:
                    st_nosuf, did = drop_suffix(st_norm)
                    if did:
                        kn2 = f"{town}|{n}|{st_nosuf}"
                        c1 = list(set(by_no_street.get(kn2, [])))
                if len(c1) == 1:
                    cands.append(c1[0])
                else:
                    cands.append(None)

            if cands[0] is not None and cands[1] is not None and cands[0] == cands[1]:
                r["attach_status"] = "ATTACHED_B"
                r["match_method"] = "axis2_range_endpoints_same_property_unique"
                r["property_id"] = cands[0]
                ctr["attach_range_endpoints_same_property"] += 1
                out_rows.append(r)
                continue
            else:
                ctr["range_unresolved"] += 1
                out_rows.append(r)
                continue

        # 3) Strict street_no + street exact unique
        kn = f"{town}|{p['street_no']}|{st_norm}"
        cands = list(set(by_no_street.get(kn, [])))
        if len(cands) == 1:
            r["attach_status"] = "ATTACHED_A"
            r["match_method"] = "axis2_street_no_unique_exact"
            r["property_id"] = cands[0]
            ctr["attach_street_no_unique"] += 1
            out_rows.append(r)
            continue

        # 4) Drop suffix unique (e.g., CHERRY HILL vs CHERRY HILL RD)
        st_nosuf, did = drop_suffix(st_norm)
        if did:
            kn2 = f"{town}|{p['street_no']}|{st_nosuf}"
            c2 = list(set(by_no_street.get(kn2, [])))
            if len(c2) == 1:
                r["attach_status"] = "ATTACHED_B"
                r["match_method"] = "axis2_drop_suffix_unique"
                r["property_id"] = c2[0]
                ctr["attach_drop_suffix_unique"] += 1
                out_rows.append(r)
                continue

        # 5) Fuzzy street (<=1 edit) among same street_no candidates (institutional, no ranges)
        # We only compare within same street_no and town, using the wanted keys we indexed.
        # Gather all candidates for this street_no across known streets (small set).
        pool = []
        prefix = f"{town}|{p['street_no']}|"
        for k, vals in by_no_street.items():
            if k.startswith(prefix):
                street_part = k.split("|", 2)[2]
                for pid in set(vals):
                    pool.append((street_part, pid))

        # de-dupe by pid, keep best (min edit)
        best = {}
        for street_part, pid in pool:
            d = edit_leq1(st_norm, street_part)
            if d <= 1:
                if pid not in best or d < best[pid][0]:
                    best[pid] = (d, street_part)

        if len(best) == 1:
            pid = next(iter(best.keys()))
            r["attach_status"] = "ATTACHED_B"
            r["match_method"] = "axis2_street_no_fuzzy_unique_leq1"
            r["property_id"] = pid
            ctr["attach_fuzzy_unique"] += 1
            out_rows.append(r)
            continue
        elif len(best) > 1:
            ctr["fuzzy_ambiguous_multi"] += 1
            out_rows.append(r)
            continue

        ctr["no_match"] += 1
        out_rows.append(r)

    w_ndjson(args.outp, out_rows)
    audit = {
        "scope": SCOPE,
        "version": VERSION,
        "in": args.inp,
        "spine": args.spine,
        "out": args.outp,
        "counts": dict(ctr)
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(f"[done] {VERSION} postmatch")
    for k, v in ctr.most_common():
        print(f"  {k}: {v}")
    print(f"[ok] OUT   {args.outp}")
    print(f"[ok] AUDIT {args.audit}")

if __name__ == "__main__":
    main()
