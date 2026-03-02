import json, re, argparse, datetime

def nowz():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

def norm_ws(s): return re.sub(r"\s+"," ",(s or "").strip())
def norm_town(t): return norm_ws(t).upper()

def norm_street_name(sn):
    s=norm_ws(sn).upper()
    s=s.replace(".","").replace(",","")
    s=re.sub(r"\bSTREET\b","ST",s)
    s=re.sub(r"\bAVENUE\b","AVE",s)
    s=re.sub(r"\bROAD\b","RD",s)
    s=re.sub(r"\bDRIVE\b","DR",s)
    s=re.sub(r"\bBOULEVARD\b","BLVD",s)
    return norm_ws(s)

def norm_unit(u):
    u=norm_ws(u).upper()
    if not u: return ""
    u=u.replace("#","").replace(".","")
    u=re.sub(r"^(UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR|RM|ROOM)\s+","",u)
    return norm_ws(u)

def parse_from_match_key(match_key):
    """
    Expect:
      TOWN|<addr_part>|UNIT|<unit_part>
    where addr_part might be:
      "87 GAINSBOROUGH STREET"
      "125 PARK DRIVE 28"
      "370 380 HARRISON AV"
    """
    mk = norm_ws(match_key)
    parts = mk.split("|")
    if len(parts) < 4: return None
    town = parts[0]
    # find UNIT marker
    try:
        i = parts.index("UNIT")
    except ValueError:
        return None
    addr_part = norm_ws("|".join(parts[1:i]))  # in case addr had pipes (rare)
    unit_part = norm_ws("|".join(parts[i+1:]))
    if not (town and addr_part and unit_part): return None

    # addr_part often starts with street number(s)
    # Case A: "370 380 HARRISON AV" => street_no="370-380", street_name="HARRISON AV"
    m2 = re.match(r"^(\d{1,6})\s+(\d{1,6})\s+(.*)$", addr_part)
    if m2:
        a,b,rest = m2.group(1), m2.group(2), m2.group(3)
        return {"town": town, "street_no": f"{a}-{b}", "street_name": rest, "unit": unit_part}

    # Case B: "87 GAINSBOROUGH STREET" => street_no="87", street_name="GAINSBOROUGH STREET"
    m1 = re.match(r"^(\d{1,6}[A-Z]?)\s+(.*)$", addr_part)
    if m1:
        stno = m1.group(1)
        stnm = m1.group(2)
        return {"town": town, "street_no": stno, "street_name": stnm, "unit": unit_part}

    return None

def fix_swapped_unit(street_no, street_name, unit):
    # if street_name ends with digits and unit equals street_no, swap
    sn = norm_ws(street_name)
    m = re.match(r"^(.*\D)\s+(\d{1,6})$", sn)
    if street_no and unit and m:
        tail = m.group(2)
        if unit.strip() == street_no.strip() and tail != unit.strip():
            return street_no, m.group(1).strip(), tail
    return street_no, street_name, unit

def spine_unit_key(r):
    town = norm_town(r.get("town") or r.get("city") or "")
    stno = norm_ws(r.get("street_no") or "")
    stnm = norm_street_name(r.get("street_name") or "")
    unit = norm_unit(r.get("unit") or "")
    if not (town and stno and stnm and unit): return ""
    return f"{town}|{stno}|{stnm}|{unit}"

def make_event_unit_key(town, street_no, street_name, unit):
    town = norm_town(town)
    stno = norm_ws(street_no)
    stnm = norm_street_name(street_name)
    unit = norm_unit(unit)
    if not (town and stno and stnm and unit): return ""
    return f"{town}|{stno}|{stnm}|{unit}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", default="postfix.no_match_unit_then_base_unitkey_attach_v4")
    args = ap.parse_args()

    audit = {"engine_id": args.engine_id, "infile": args.infile, "spine": args.spine, "started_at": nowz()}

    # build spine unit index
    idx = {}
    spine_rows = 0
    with open(args.spine, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            spine_rows += 1
            r = json.loads(line)
            k = spine_unit_key(r)
            if not k: continue
            pid = r.get("property_id") or r.get("building_group_id") or r.get("parcel_id")
            if not pid: continue
            idx.setdefault(k, set()).add(pid)

    audit["spine_rows_scanned"] = spine_rows
    audit["spine_unit_keys"] = len(idx)

    rows_scanned = 0
    rows_key_parsed = 0
    rows_swapped_fixed = 0
    rows_attached = 0
    rows_still_unknown = 0
    no_match_key = 0
    no_parse = 0
    no_spine_match = 0
    multi_spine_match = 0

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            rows_scanned += 1
            ev = json.loads(line)
            a = ev.get("attach") or ev
            if a.get("attach_status") != "UNKNOWN" or a.get("match_method") != "no_match_unit_then_base":
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            mk = a.get("match_key") or ev.get("match_key") or ""
            if not mk:
                no_match_key += 1
                rows_still_unknown += 1
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            parsed = parse_from_match_key(mk)
            if not parsed:
                no_parse += 1
                rows_still_unknown += 1
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            rows_key_parsed += 1

            stno, stnm, unit = parsed["street_no"], parsed["street_name"], parsed["unit"]
            stno2, stnm2, unit2 = fix_swapped_unit(stno, stnm, unit)
            if (stno2, stnm2, unit2) != (stno, stnm, unit):
                rows_swapped_fixed += 1
            stno, stnm, unit = stno2, stnm2, unit2

            k = make_event_unit_key(parsed["town"], stno, stnm, unit)
            if not k:
                no_parse += 1
                rows_still_unknown += 1
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            hits = idx.get(k)
            if not hits:
                no_spine_match += 1
                rows_still_unknown += 1
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue
            if len(hits) != 1:
                multi_spine_match += 1
                rows_still_unknown += 1
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            pid = next(iter(hits))

            # write normalized components back for auditing
            ev["town"] = norm_town(parsed["town"])
            ev["street_no"] = norm_ws(stno)
            ev["street_name"] = norm_street_name(stnm)
            ev["unit"] = norm_unit(unit)

            ev["property_id"] = pid
            ev.setdefault("attach", {})
            ev["attach"]["attach_status"] = "ATTACHED_UNIT"
            ev["attach"]["match_method"] = "unitkey_from_match_key_v4"
            ev["attach"]["match_key_unit"] = k
            rows_attached += 1
            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit.update({
        "done": True,
        "rows_scanned": rows_scanned,
        "rows_key_parsed": rows_key_parsed,
        "rows_swapped_fixed": rows_swapped_fixed,
        "rows_attached": rows_attached,
        "rows_still_unknown_after_attempt": rows_still_unknown,
        "no_match_key": no_match_key,
        "no_parse": no_parse,
        "no_spine_match": no_spine_match,
        "multi_spine_match": multi_spine_match,
        "finished_at": nowz()
    })
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps({
        "done": True,
        "rows_scanned": rows_scanned,
        "rows_key_parsed": rows_key_parsed,
        "rows_swapped_fixed": rows_swapped_fixed,
        "rows_attached": rows_attached,
        "rows_still_unknown_after_attempt": rows_still_unknown,
        "no_match_key": no_match_key,
        "no_parse": no_parse,
        "no_spine_match": no_spine_match,
        "multi_spine_match": multi_spine_match,
        "out": args.out,
        "audit": args.audit
    }, indent=2))

if __name__ == "__main__":
    main()