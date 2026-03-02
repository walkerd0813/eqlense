import argparse, json, os, re
from collections import defaultdict

def norm(s):
    return (s or "").strip().upper()

def parse_unit_from_match_key(mk):
    # mk like: TOWN|87 GAINSBOROUGH STREET|UNIT|406  (or ...|UNIT|6A)
    if not mk: return None
    parts=[p.strip() for p in mk.split("|") if p is not None]
    if len(parts) < 4: return None
    # last two should be UNIT, <val>
    if parts[-2].upper() != "UNIT": 
        return None
    return parts[-1].strip()

def base_from_match_key(mk):
    # remove trailing |UNIT|X if present
    if not mk: return mk
    parts=[p.strip() for p in mk.split("|") if p is not None]
    if len(parts) >= 4 and parts[-2].upper()=="UNIT":
        return "|".join(parts[:-2]).strip()
    return mk

def spine_unit_index(spine_path):
    idx=defaultdict(list)
    scanned=0
    with open(spine_path,"r",encoding="utf-8") as f:
        for line in f:
            if not line.strip(): 
                continue
            scanned += 1
            try:
                r=json.loads(line)
            except:
                continue
            town=norm(r.get("town"))
            sn=str(r.get("street_no") or "").strip()
            sname=norm(r.get("street_name"))
            unit=str(r.get("unit") or "").strip()
            pid=r.get("property_id")
            if not (town and sn and sname and unit and pid):
                continue
            k=f"{town}|{sn}|{sname}|{unit.upper()}"
            idx[k].append(pid)
    return idx, scanned

def event_unit_key(ev):
    # Best-effort: use match_key pattern, else address fields
    a=ev.get("attach") or {}
    mk=a.get("match_key_unit") or a.get("match_key") or ""
    unit=parse_unit_from_match_key(mk)
    if unit:
        base=base_from_match_key(mk)
        # base is "TOWN|87 GAINSBOROUGH STREET"
        bparts=[p.strip() for p in base.split("|") if p is not None]
        if len(bparts)==2:
            town=norm(bparts[0])
            addr=bparts[1].upper()
            # addr begins with house number; split first token as street_no
            toks=addr.split()
            if len(toks) >= 2 and toks[0].isdigit():
                sn=toks[0]
                sname=" ".join(toks[1:]).strip()
                return town, sn, sname, unit
    # fallback (rare): use normalized event fields if present
    town=norm(ev.get("town"))
    sn=str(ev.get("street_no") or "").strip()
    sname=norm(ev.get("street_name"))
    unit=str(ev.get("unit") or "").strip()
    if town and sn and sname and unit:
        return town, sn, sname, unit
    return None

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", default="postfix.no_match_unit_then_base_unitkey_attach_v1")
    args=ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    idx, spine_scanned = spine_unit_index(args.spine)

    rows_scanned=0
    rows_fixed_unitkey=0
    rows_attached=0
    rows_still_unknown=0
    no_unitkey=0
    multi=0
    miss=0

    with open(args.infile,"r",encoding="utf-8") as f, open(args.out,"w",encoding="utf-8") as fo:
        for line in f:
            if not line.strip():
                continue
            rows_scanned += 1
            ev=json.loads(line)
            a=ev.get("attach") or {}
            if a.get("attach_status")=="UNKNOWN" and (a.get("match_method")=="no_match_unit_then_base"):
                # synthesize match_key_unit if absent
                mk=a.get("match_key_unit") or a.get("match_key") or ""
                if (not a.get("match_key_unit")) and ("|UNIT|" in mk.upper()):
                    a["match_key_unit"]=mk
                    a["match_key"]=base_from_match_key(mk)
                    rows_fixed_unitkey += 1

                tup=event_unit_key(ev)
                if not tup:
                    no_unitkey += 1
                    ev["attach"]=a
                    fo.write(json.dumps(ev,ensure_ascii=False)+"\n")
                    continue

                town,sn,sname,unit=tup
                k=f"{town}|{sn}|{sname}|{unit.strip().upper()}"
                cands=idx.get(k,[])
                if not cands:
                    miss += 1
                    rows_still_unknown += 1
                elif len(cands)==1:
                    pid=cands[0]
                    a["attach_status"]="ATTACHED_A"
                    a["property_id"]=pid
                    a["match_method"]="unit_direct"
                    a["match_key_unit"]=a.get("match_key_unit") or f"{town}|{sn} {sname}|UNIT|{unit}"
                    rows_attached += 1
                else:
                    multi += 1
                    rows_still_unknown += 1

            ev["attach"]=a
            fo.write(json.dumps(ev,ensure_ascii=False)+"\n")

    audit={
        "engine_id": args.engine_id,
        "infile": args.infile,
        "spine": args.spine,
        "out": args.out,
        "rows_scanned": rows_scanned,
        "spine_rows_scanned": spine_scanned,
        "rows_fixed_match_key_unit": rows_fixed_unitkey,
        "rows_attached": rows_attached,
        "rows_still_unknown_after_attempt": rows_still_unknown,
        "no_unitkey_parse": no_unitkey,
        "no_spine_match": miss,
        "multi_spine_match": multi
    }
    with open(args.audit,"w",encoding="utf-8") as fa:
        json.dump(audit, fa, ensure_ascii=False, indent=2)

    print(json.dumps({"done": True, "rows_scanned": rows_scanned, "rows_fixed_match_key_unit": rows_fixed_unitkey, "rows_attached": rows_attached, "out": args.out, "audit": args.audit}, indent=2))

if __name__=="__main__":
    main()