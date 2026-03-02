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

def extract_town(ev):
    pr = ev.get("property_ref") or {}
    return norm_space(pr.get("town_norm") or pr.get("town_raw") or "")

def extract_addr_raw(ev):
    pr = ev.get("property_ref") or {}
    # canonical location (confirmed by your schema print)
    a = pr.get("address_raw")
    if isinstance(a, str) and a.strip():
        return a.strip()
    # fallback: sometimes address_norm may be a string in older feeds
    an = pr.get("address_norm")
    if isinstance(an, str) and an.strip():
        return an.strip()
    # fallback: if address_norm is dict, try to use a known field if present
    if isinstance(an, dict):
        # Not relying on this for matching; just to avoid blank.
        # Your earlier probe showed address_norm dict keys like ['street_no_fix'] only.
        pass
    return ""

def extract_attach_fields(ev):
    a = ev.get("attach") or {}
    # Some feeds might store under different keys; keep robust.
    scope = a.get("attach_scope") or a.get("scope") or ""
    status = a.get("attach_status") or a.get("status") or ""
    return (norm_space(scope), norm_space(status))

# ---- Spine key builders (same philosophy as prior: deterministic, no fuzzy/nearest) ----

STREET_SUFFIXES = {"ST","STREET","AVE","AV","AVENUE","RD","ROAD","DR","DRIVE","LN","LANE","CT","COURT","PL","PLACE","BLVD","BOULEVARD","TER","TERR","TERRACE","WAY","PKY","PARKWAY","CIR","CIRCLE","HWY","HIGHWAY"}

def split_addr(addr):
    s = norm_space(addr)
    # strip trailing commas etc
    s = re.sub(r"[,\.;]+$", "", s).strip()
    # common unit markers
    s2 = re.sub(r"\b(APT|UNIT|#|SUITE)\b.*$", "", s).strip()
    # dash ranges at start like 26-28 MAIN ST -> keep as raw, but num parse will fail; we only use exact string match keys below anyway.
    return s, s2

def leading_num(addr):
    s = norm_space(addr)
    m = re.match(r"^(\d+)", s)
    return m.group(1) if m else ""

def tokenize_street(addr_no_unit):
    s = norm_space(addr_no_unit)
    # remove leading number token
    s = re.sub(r"^\d+\s+", "", s).strip()
    if not s:
        return []
    toks = s.split(" ")
    return toks

def street_key(addr_no_unit):
    toks = tokenize_street(addr_no_unit)
    if not toks:
        return ""
    return " ".join(toks)

def street_key_nosuf(addr_no_unit):
    toks = tokenize_street(addr_no_unit)
    if not toks:
        return ""
    # drop last token if it's a common suffix
    if toks and toks[-1] in STREET_SUFFIXES:
        toks = toks[:-1]
    return " ".join(toks)

def full_key(addr_no_unit):
    n = leading_num(addr_no_unit)
    if not n:
        return ""
    st = street_key(addr_no_unit)
    if not st:
        return ""
    return f"{n}|{st}"

def full_key_nosuf(addr_no_unit):
    n = leading_num(addr_no_unit)
    if not n:
        return ""
    st = street_key_nosuf(addr_no_unit)
    if not st:
        return ""
    return f"{n}|{st}"

def build_spine_index(spine_path):
    t0=time.time()
    idx_full=defaultdict(list)
    idx_street=defaultdict(list)
    idx_street_nosuf=defaultdict(list)

    debug={"indexed_rows":0,"no_leading_num":0,"no_street_tokens":0,"no_addr_str":0,"no_rest_tokens":0}

    for r in jread(spine_path):
        debug["indexed_rows"] += 1
        town = norm_space(r.get("town_norm") or r.get("town_raw") or r.get("town") or "")
        addr = r.get("address_norm") or r.get("address") or r.get("address_raw") or ""
        if not isinstance(addr,str) or not addr.strip():
            debug["no_addr_str"] += 1
            continue
        addr_raw = addr
        _, addr_no_unit = split_addr(addr_raw)

        fk = full_key(addr_no_unit)
        if not fk:
            debug["no_leading_num"] += 1
        sk = street_key(addr_no_unit)
        if not sk:
            debug["no_street_tokens"] += 1
            continue

        # index
        if fk:
            idx_full[f"{town}|{fk}"].append(r)
        idx_street[f"{town}|{sk}"].append(r)

        skn = street_key_nosuf(addr_no_unit)
        if skn:
            idx_street_nosuf[f"{town}|{skn}"].append(r)

    return {
        "idx_full": idx_full,
        "idx_street": idx_street,
        "idx_street_nosuf": idx_street_nosuf,
        "debug": debug,
        "elapsed_s": round(time.time()-t0,1)
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args=ap.parse_args()

    print("=== AXIS2 REATTACH (>=10k) v1_19 (FIX field paths: property_ref + attach) ===")
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
        town = extract_town(ev)
        addr_raw = extract_addr_raw(ev)
        addr_norm, addr_no_unit = split_addr(addr_raw)
        num = leading_num(addr_no_unit)
        attach_scope, attach_status = extract_attach_fields(ev)

        # keep some samples
        if len(debug_samples) < 12:
            debug_samples.append({
                "event_id": ev.get("event_id"),
                "town": town,
                "addr_raw": addr_raw,
                "addr_clean": addr_no_unit,
                "num": num,
                "attach_scope": attach_scope,
                "attach_status": attach_status
            })

        # If there is no leading number, it's legitimately NO_NUM for our deterministic attach
        if not num:
            stats["single_unknown_no_num"] += 1
            out_rows.append(ev)
            continue

        # We are not changing attach logic here beyond reading correct fields.
        # If you want, next step we can attempt match using the same axis2 logic but now it will have a real addr.
        out_rows.append(ev)

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    audit={
        "version":"v1_19",
        "events": args.events,
        "spine": args.spine,
        "out": args.out,
        "stats": dict(stats),
        "index_keys": {
            "full_keys": len(spine_ix["idx_full"]),
            "street_keys": len(spine_ix["idx_street"]),
            "street_nosuf_keys": len(spine_ix["idx_street_nosuf"]),
            "debug": spine_ix["debug"]
        },
        "debug_samples": debug_samples
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
