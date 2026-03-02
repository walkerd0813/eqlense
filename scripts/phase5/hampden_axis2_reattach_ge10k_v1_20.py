import argparse, json, os, time, re
from collections import defaultdict

def jread(path):
    with open(path,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if line:
                yield json.loads(line)

def jwrite(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,'w',encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def norm_space(s):
    return re.sub(r"\s+", " ", (s or "").strip()).upper()

def first_str(*vals):
    for v in vals:
        if isinstance(v,str) and v.strip():
            return v.strip()
    return ""

def dict_get_any(d, keys):
    for k in keys:
        v = d.get(k)
        if isinstance(v,str) and v.strip():
            return v.strip()
    return ""

def assemble_from_addr_dict(ad):
    if not isinstance(ad,dict):
        return ""

    num = dict_get_any(ad, ["house_number","street_number","street_no","number","addr_num","st_num"])
    predir = dict_get_any(ad, ["predir","pre_dir","street_predir","prefix_dir"])
    name = dict_get_any(ad, ["street_name","street","road","st_name","name"])
    suffix = dict_get_any(ad, ["suffix","street_suffix","st_suffix","type"])
    postdir = dict_get_any(ad, ["postdir","post_dir","street_postdir"])
    unit = dict_get_any(ad, ["unit","unit_no","apt","apt_no","suite","unit_number"])

    parts = []
    if num: parts.append(num)
    if predir: parts.append(predir)
    if name: parts.append(name)
    if suffix: parts.append(suffix)
    if postdir: parts.append(postdir)
    if unit:
        # normalize common unit marker
        if not re.search(r"^(UNIT|APT|#)\b", unit.upper()):
            parts.append("UNIT")
        parts.append(unit)

    return " ".join([p for p in parts if p])

def spine_extract_town(r):
    # try common town fields (top-level or nested)
    return norm_space(first_str(
        r.get("town_norm"), r.get("town_raw"), r.get("town"),
        (r.get("property_ref") or {}).get("town_norm"),
        (r.get("property_ref") or {}).get("town_raw"),
        (r.get("address") or {}).get("town"),
        (r.get("address") or {}).get("town_norm"),
        (r.get("location") or {}).get("town"),
    ))

def spine_extract_addr_str(r):
    # candidate top-level string fields
    candidates = [
        r.get("address_norm_str"), r.get("address_str"),
        r.get("address_full_norm"), r.get("address_full"),
        r.get("full_address"), r.get("site_address"),
        r.get("situs_address"), r.get("loc_address"),
        r.get("address_line1"), r.get("address1"),
        r.get("address_raw"), r.get("address"),
        r.get("addr"),
    ]
    s = first_str(*candidates)
    if s:
        return s

    # nested address dict
    ad = r.get("address")
    s = assemble_from_addr_dict(ad)
    if s:
        return s

    # nested in property_ref
    pr = r.get("property_ref") or {}
    s = first_str(pr.get("address_raw"), pr.get("address_norm"))
    if s:
        return s
    if isinstance(pr.get("address_norm"), dict):
        s = assemble_from_addr_dict(pr.get("address_norm"))
        if s:
            return s

    # nested in location
    loc = r.get("location") or {}
    s = first_str(loc.get("address"), loc.get("address_raw"), loc.get("address_line1"))
    if s:
        return s
    if isinstance(loc.get("address"), dict):
        s = assemble_from_addr_dict(loc.get("address"))
        if s:
            return s

    return ""

STREET_SUFFIXES = {"ST","STREET","AVE","AV","AVENUE","RD","ROAD","DR","DRIVE","LN","LANE","CT","COURT","PL","PLACE","BLVD","BOULEVARD","TER","TERR","TERRACE","WAY","PKY","PARKWAY","CIR","CIRCLE","HWY","HIGHWAY"}

def split_addr(addr):
    s = norm_space(addr)
    s = re.sub(r"[,\.;]+$", "", s).strip()
    s2 = re.sub(r"\b(APT|UNIT|#|SUITE)\b.*$", "", s).strip()
    return s, s2

def leading_num(addr):
    s = norm_space(addr)
    m = re.match(r"^(\d+)", s)
    return m.group(1) if m else ""

def tokenize_street(addr_no_unit):
    s = norm_space(addr_no_unit)
    s = re.sub(r"^\d+\s+", "", s).strip()
    if not s:
        return []
    return s.split(" ")

def street_key(addr_no_unit):
    toks = tokenize_street(addr_no_unit)
    return " ".join(toks) if toks else ""

def street_key_nosuf(addr_no_unit):
    toks = tokenize_street(addr_no_unit)
    if not toks:
        return ""
    if toks[-1] in STREET_SUFFIXES:
        toks = toks[:-1]
    return " ".join(toks)

def full_key(addr_no_unit):
    n = leading_num(addr_no_unit)
    st = street_key(addr_no_unit)
    if not n or not st:
        return ""
    return f"{n}|{st}"

def build_spine_index(spine_path):
    t0=time.time()
    idx_full=defaultdict(list)
    idx_street=defaultdict(list)
    idx_street_nosuf=defaultdict(list)

    debug={"indexed_rows":0,"no_town":0,"no_addr_str":0,"no_leading_num":0,"no_street_tokens":0}
    sample_addr_fields=[]

    for r in jread(spine_path):
        debug["indexed_rows"] += 1
        town = spine_extract_town(r)
        if not town:
            debug["no_town"] += 1
            continue

        addr = spine_extract_addr_str(r)
        if not addr:
            debug["no_addr_str"] += 1
            if len(sample_addr_fields) < 5:
                sample_addr_fields.append({"town": town, "keys": sorted(r.keys())[:80]})
            continue

        _, addr_no_unit = split_addr(addr)
        fk = full_key(addr_no_unit)
        sk = street_key(addr_no_unit)
        skn = street_key_nosuf(addr_no_unit)

        if not leading_num(addr_no_unit):
            debug["no_leading_num"] += 1
        if not sk:
            debug["no_street_tokens"] += 1
            continue

        if fk:
            idx_full[f"{town}|{fk}"].append(r)
        idx_street[f"{town}|{sk}"].append(r)
        if skn:
            idx_street_nosuf[f"{town}|{skn}"].append(r)

    return {
        "idx_full": idx_full,
        "idx_street": idx_street,
        "idx_street_nosuf": idx_street_nosuf,
        "debug": debug,
        "sample_addr_fields": sample_addr_fields,
        "elapsed_s": round(time.time()-t0,1)
    }

def extract_town_event(ev):
    pr = ev.get("property_ref") or {}
    return norm_space(pr.get("town_norm") or pr.get("town_raw") or "")

def extract_addr_event(ev):
    pr = ev.get("property_ref") or {}
    a = pr.get("address_raw")
    if isinstance(a, str) and a.strip():
        return a.strip()
    an = pr.get("address_norm")
    if isinstance(an, str) and an.strip():
        return an.strip()
    return ""

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args=ap.parse_args()

    print("=== AXIS2 REATTACH (>=10k) v1_20 (FIX spine address extraction + field fallbacks) ===")
    print("[info] events:", args.events)
    print("[info] spine :", args.spine)

    spine_ix = build_spine_index(args.spine)
    print("[ok] spine index built:", json.dumps({
        "full_keys": len(spine_ix["idx_full"]),
        "street_keys": len(spine_ix["idx_street"]),
        "street_nosuf_keys": len(spine_ix["idx_street_nosuf"]),
        "debug": spine_ix["debug"],
        "elapsed_s": spine_ix["elapsed_s"]
    }, indent=2))

    stats=defaultdict(int)
    debug_samples=[]
    out_rows=[]

    for ev in jread(args.events):
        town = extract_town_event(ev)
        addr_raw = extract_addr_event(ev)
        _, addr_no_unit = split_addr(addr_raw)
        num = leading_num(addr_no_unit)

        if len(debug_samples) < 12:
            debug_samples.append({
                "event_id": ev.get("event_id"),
                "town": town,
                "addr_raw": addr_raw,
                "addr_clean": addr_no_unit,
                "num": num
            })

        if not num:
            stats["single_unknown_no_num"] += 1
            out_rows.append(ev)
            continue

        out_rows.append(ev)

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    audit={
        "version":"v1_20",
        "events": args.events,
        "spine": args.spine,
        "out": args.out,
        "stats": dict(stats),
        "index_keys": {
            "full_keys": len(spine_ix["idx_full"]),
            "street_keys": len(spine_ix["idx_street"]),
            "street_nosuf_keys": len(spine_ix["idx_street_nosuf"]),
            "debug": spine_ix["debug"],
            "sample_addr_fields": spine_ix["sample_addr_fields"][:5]
        },
        "debug_samples_events": debug_samples
    }
    with open(args.audit,'w',encoding='utf-8') as f:
        json.dump(audit,f,ensure_ascii=False,indent=2)

    jwrite(args.out, out_rows)

    print(json.dumps({
        "out": args.out,
        "audit": args.audit,
        "stats": dict(stats)
    }, indent=2))

if __name__=="__main__":
    main()
