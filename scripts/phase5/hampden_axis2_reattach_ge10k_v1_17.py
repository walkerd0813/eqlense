import json, re, argparse, time, os
from collections import defaultdict, Counter

UNIT_MARKERS = set(["UNIT","APT","APARTMENT","#","STE","SUITE","LOT","BLDG","BUILDING","FL","FLOOR"])

SUFFIX_MAP = {
  "ST":"ST","STREET":"ST",
  "RD":"RD","ROAD":"RD",
  "DR":"DR","DRIVE":"DR",
  "AVE":"AVE","AV":"AVE","AVENUE":"AVE",
  "BLVD":"BLVD","BOULEVARD":"BLVD",
  "CIR":"CIR","CIRCLE":"CIR",
  "CT":"CT","COURT":"CT",
  "TER":"TERR","TERR":"TERR","TERRACE":"TERR",
  "LN":"LA","LANE":"LA","LA":"LA",
  "WAY":"WAY",
  "PL":"PL","PLACE":"PL",
  "PKY":"PKY","PKWY":"PKY","PARKWAY":"PKY",
  "HWY":"HWY","HIGHWAY":"HWY",
  "EXTN":"EXTN","EXT":"EXTN","EXTENSION":"EXTN",
}

def it(p):
    with open(p,'r',encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def as_str(x):
    if x is None: return ""
    if isinstance(x,str): return x
    if isinstance(x,(int,float)): return str(x)
    if isinstance(x,dict):
        # common shapes we've seen in spine
        for k in ["norm","normalized","value","text","raw","full","display","name","address","town","city"]:
            v=x.get(k)
            if isinstance(v,str) and v.strip():
                return v
        return ""
    if isinstance(x,(list,tuple)):
        for v in x:
            s=as_str(v)
            if s: return s
        return ""
    return str(x)

def norm_town(t):
    t = (t or "").upper().strip()
    for pref in ("TOWN OF ","CITY OF "):
        if t.startswith(pref):
            t = t[len(pref):].strip()
    t = t.replace(",", " ").replace("  "," ").strip()
    if t.endswith(" MA"):
        t = t[:-3].strip()
    return t

def clean_tokens(s):
    s = (s or "").upper()
    s = s.replace(",", " ").replace(".", " ").replace("  "," ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.split(" ") if s else []

def extract_num(tok0):
    if not tok0: return None
    m = re.match(r"^(\d+)([A-Z]?)$", tok0)
    if not m: return None
    return (m.group(1), m.group(2) or "")

def parse_addr(addr_raw):
    """
    Returns: num(str), num_suffix(str), street(str), suffix(str), unit(str or "")
    Very conservative: we only use leading number; street tokens until unit marker.
    """
    toks = clean_tokens(addr_raw)
    if not toks: return (None,None,None,None,"")
    # handle "0 WOODLAND WAY" etc (0 allowed)
    n = extract_num(toks[0])
    if not n:
        return (None,None,None,None,"")
    num, num_suf = n
    rest = toks[1:]

    unit = ""
    # find first unit marker
    cut = len(rest)
    for i,t in enumerate(rest):
        if t in UNIT_MARKERS:
            cut = i
            # capture remainder as unit (marker + value)
            unit = " ".join(rest[i:])[:60]
            break

    street_toks = rest[:cut]
    if not street_toks:
        return (num,num_suf,None,None,unit)

    # suffix candidate = last token if it looks like suffix
    last = street_toks[-1]
    suf = SUFFIX_MAP.get(last, last if last in SUFFIX_MAP.values() else "")
    if suf:
        core = street_toks[:-1]
        if not core:
            # e.g. "ST GEORGE RD" would have core, but guard anyway
            core = street_toks
    else:
        core = street_toks

    street = " ".join(core).strip()
    suffix = suf or ""  # empty means unknown suffix; still usable but weaker
    return (num,num_suf,street,suffix,unit)

def make_full_key(town, num, street, suffix):
    if not (town and num and street): return None
    return f"{town}|{num}|{street}|{suffix or ''}"

def make_street_key(town, street, suffix):
    if not (town and street): return None
    return f"{town}|{street}|{suffix or ''}"

def make_street_unit_key(town, street, suffix, unit):
    if not (town and street and unit): return None
    return f"{town}|{street}|{suffix or ''}|{unit}"

def load_need_towns_from_events(events_path):
    need=set()
    for ev in it(events_path):
        a=ev.get("attach") or {}
        if a.get("attach_scope")=="SINGLE" and (a.get("attach_status") or "").upper()=="UNKNOWN" and a.get("match_method")=="no_match":
            pr=ev.get("property_ref") or {}
            town = norm_town(as_str(pr.get("town_norm") or pr.get("town_raw") or pr.get("town")))
            if town: need.add(town)
    return need

def build_spine_indexes(spine_path, need_towns, progress=250000):
    full_index = defaultdict(list)
    street_index = defaultdict(list)
    street_unit_index = defaultdict(list)

    seen=0
    kept=0
    dict_addr_norm=0

    t0=time.time()
    for r in it(spine_path):
        pr=r.get("property_ref") or r.get("ref") or {}
        town = norm_town(as_str(r.get("town_norm") or pr.get("town_norm") or r.get("town") or pr.get("town")))
        if not town or town not in need_towns:
            continue

        addr_norm = r.get("address_norm")
        if isinstance(addr_norm, dict):
            dict_addr_norm += 1

        addr = as_str(addr_norm or pr.get("address_norm") or r.get("address") or pr.get("address"))
        pid = as_str(r.get("property_id") or pr.get("property_id") or r.get("id") or pr.get("id"))
        if not pid or not addr:
            continue

        num, num_suf, street, suffix, unit = parse_addr(addr)
        if not (num and street):
            continue

        fk = make_full_key(town, num, street, suffix)
        sk = make_street_key(town, street, suffix)
        suk = make_street_unit_key(town, street, suffix, unit) if unit else None

        if fk: full_index[fk].append(pid)
        if sk: street_index[sk].append(pid)
        if suk: street_unit_index[suk].append(pid)

        kept += 1
        seen += 1
        if seen % progress == 0:
            dt=time.time()-t0
            print({"spine_rows_kept": kept, "dict_address_norm_seen": dict_addr_norm, "elapsed_s": round(dt,1)})

    # reduce to uniques
    full_unique = {k:v[0] for k,v in full_index.items() if len(set(v))==1}
    street_unique = {k:v[0] for k,v in street_index.items() if len(set(v))==1}
    street_unit_unique = {k:v[0] for k,v in street_unit_index.items() if len(set(v))==1}

    return {
      "full_unique": full_unique,
      "street_unique": street_unique,
      "street_unit_unique": street_unit_unique,
      "stats": {
        "need_towns_n": len(need_towns),
        "spine_rows_kept": kept,
        "dict_address_norm_seen_in_need_towns": dict_addr_norm,
        "full_keys_unique": len(full_unique),
        "street_keys_unique": len(street_unique),
        "street_unit_keys_unique": len(street_unit_unique),
      }
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args=ap.parse_args()

    print("=== AXIS2 REATTACH (>=10k) v1_17 (DICT-SAFE SPINE + SUFFIX CANON + PRESERVE MULTI) ===")
    print("[info] events:", args.events)
    print("[info] spine :", args.spine)

    need_towns = load_need_towns_from_events(args.events)
    print({"need_towns_n": len(need_towns), "need_towns": sorted(list(need_towns))})

    idx = build_spine_indexes(args.spine, need_towns, progress=250000)
    print("[ok] built town-filtered spine indexes")
    print(idx["stats"])

    fullU = idx["full_unique"]
    streetU = idx["street_unique"]
    streetUnitU = idx["street_unit_unique"]

    stats = Counter()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.out,'w',encoding='utf-8') as fo:
        for ev in it(args.events):
            a = ev.get("attach") or {}
            scope = a.get("attach_scope")
            status = (a.get("attach_status") or "").upper()
            mm = a.get("match_method")

            # Preserve MULTI/PARTIAL_MULTI and all non-target rows
            if not (scope=="SINGLE" and status=="UNKNOWN" and mm=="no_match"):
                stats["pass_through_other"] += 1
                fo.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            pr = ev.get("property_ref") or {}
            town = norm_town(as_str(pr.get("town_norm") or pr.get("town_raw") or pr.get("town")))
            addr  = as_str(pr.get("addr_norm") or pr.get("address_norm") or pr.get("addr_raw") or pr.get("address_raw") or pr.get("addr") or pr.get("address"))

            num, num_suf, street, suffix, unit = parse_addr(addr)

            if not (town and num and street):
                stats["single_still_unknown__no_num_or_bad_addr"] += 1
                fo.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            # 1) FULL strict
            fk = make_full_key(town, num, street, suffix)
            pid = fullU.get(fk)

            # 2) if not found, try STREET+UNIT if unit exists
            if not pid and unit:
                suk = make_street_unit_key(town, street, suffix, unit)
                pid = streetUnitU.get(suk)
                if pid:
                    a["match_method"] = "axis2_street+unit_unique_suffixcanon"
                    a["attach_status"] = "ATTACHED_A"
                    a["attach_confidence"] = "A"
                    a["attach_method"] = "town+street+unit_unique"
                    a["property_id"] = pid
                    ev["attach"] = a
                    stats["single_upgraded_to_attached"] += 1
                    fo.write(json.dumps(ev, ensure_ascii=False) + "\n")
                    continue

            # 3) if still not found, try STREET unique AND ensure number doesn't create ambiguity
            if not pid:
                sk = make_street_key(town, street, suffix)
                pid2 = streetU.get(sk)
                if pid2:
                    # still require FULL key uniqueness where possible; if street unique but full missing,
                    # this is a weaker match -> mark as B and only if suffix present
                    if suffix:
                        a["match_method"] = "axis2_street_unique_suffixcanon"
                        a["attach_status"] = "ATTACHED_B"
                        a["attach_confidence"] = "B"
                        a["attach_method"] = "town+street_unique"
                        a["property_id"] = pid2
                        ev["attach"] = a
                        stats["single_upgraded_to_attached"] += 1
                        fo.write(json.dumps(ev, ensure_ascii=False) + "\n")
                        continue

            # 4) if FULL match exists but not unique (collision)
            # detect collisions by checking raw lists if needed (cheap: check whether key exists but not in fullU)
            # We'll approximate: if key is None => no_num/bad, else no_match.
            stats["single_still_unknown__no_match"] += 1
            fo.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
      "out": args.out,
      "audit": args.audit,
      "index_stats": idx["stats"],
      "stats": dict(stats),
    }
    with open(args.audit,'w',encoding='utf-8') as fa:
        json.dump(audit, fa, indent=2)

    print(audit)

if __name__=="__main__":
    main()
