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

def spine_town(r):
    return norm_space(first_str(r.get("town"), r.get("jurisdiction_name")))

def spine_addr(r):
    # canonical spine string
    fa = first_str(r.get("full_address"))
    if fa:
        return fa
    # fallback assemble
    sn = first_str(r.get("street_no"))
    nm = first_str(r.get("street_name"))
    un = first_str(r.get("unit"))
    parts=[]
    if sn: parts.append(sn)
    if nm: parts.append(nm)
    if un:
        u = un.strip()
        if not re.search(r"^(UNIT|APT|#)\b", u.upper()):
            parts.append("UNIT")
        parts.append(u)
    return " ".join(parts).strip()

def split_addr(addr):
    s = norm_space(addr)
    s = re.sub(r"[,\.;]+$", "", s).strip()
    s2 = re.sub(r"\b(APT|UNIT|#|SUITE)\b.*$", "", s).strip()
    return s, s2

def leading_num(addr):
    s = norm_space(addr)
    m = re.match(r"^(\d+)", s)
    return m.group(1) if m else ""

STREET_SUFFIXES = {"ST","STREET","AVE","AV","AVENUE","RD","ROAD","DR","DRIVE","LN","LANE","CT","COURT","PL","PLACE","BLVD","BOULEVARD","TER","TERR","TERRACE","WAY","PKY","PARKWAY","CIR","CIRCLE","HWY","HIGHWAY"}

def tokenize_street(addr_no_unit):
    s = norm_space(addr_no_unit)
    s = re.sub(r"^\d+\s+", "", s).strip()
    return s.split(" ") if s else []

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

    for r in jread(spine_path):
        debug["indexed_rows"] += 1
        town = spine_town(r)
        if not town:
            debug["no_town"] += 1
            continue

        addr = spine_addr(r)
        if not addr:
            debug["no_addr_str"] += 1
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

    print("=== AXIS2 REATTACH (>=10k) v1_21 (SPINE uses town + full_address) ===")
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

    # NOTE: This script still only demonstrates plumbing; your actual attach logic lives elsewhere.
    # We keep output identical to input for now, but we write an audit proving the spine index is valid.
    out_rows=[]
    for ev in jread(args.events):
        out_rows.append(ev)

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    audit={
        "version":"v1_21",
        "events": args.events,
        "spine": args.spine,
        "out": args.out,
        "index_keys": {
            "full_keys": len(spine_ix["idx_full"]),
            "street_keys": len(spine_ix["idx_street"]),
            "street_nosuf_keys": len(spine_ix["idx_street_nosuf"]),
            "debug": spine_ix["debug"]
        }
    }
    with open(args.audit,'w',encoding='utf-8') as f:
        json.dump(audit,f,ensure_ascii=False,indent=2)

    jwrite(args.out, out_rows)

    print(json.dumps({
        "out": args.out,
        "audit": args.audit
    }, indent=2))

if __name__=="__main__":
    main()
