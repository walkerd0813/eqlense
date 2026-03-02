import argparse, json, re, datetime
from collections import defaultdict

def nowz():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

def ws(s): return re.sub(r"\s+"," ",(s or "").strip())
def up(s): return ws(s).upper()

def norm_unit(u):
    u=up(u).replace("#","")
    u=re.sub(r"^(UNIT|APT|APARTMENT|UNITS)\s+","",u)
    u=u.replace(" ","")
    return u

def norm_street(s):
    s=up(s)
    s=re.sub(r"[.,;:]+","",s)
    return s

def parse_match_key(mk):
    # TOWN|ADDR  or  TOWN|ADDR|UNIT|U
    if not mk: return None,None,None
    parts=[p.strip() for p in mk.split("|")]
    if len(parts)<2: return None,None,None
    town=parts[0]; addr=parts[1]; unit=None
    if len(parts)>=4 and parts[2].strip().upper()=="UNIT":
        unit=parts[3].strip()
    return town, addr, unit

def parse_addr(addr):
    # returns (street_no_raw, street_name_raw, flags)
    addr=up(addr)
    if not addr or addr=="NULL NULL": return None,None,{"bad":True}
    toks=addr.split(" ")
    # HALF: 11 1/2 SPRING...
    if len(toks)>=3 and toks[0].isdigit() and toks[1].replace(",","")=="1/2":
        return f"{toks[0]} 1/2"," ".join(toks[2:]),{"bad":False,"is_half":True,"is_range":False}
    # RANGE: 108 114 CHESTNUT...
    if len(toks)>=3 and toks[0].isdigit() and toks[1].isdigit() and toks[2] not in ("&","AND"):
        return f"{toks[0]} {toks[1]}"," ".join(toks[2:]),{"bad":False,"is_half":False,"is_range":True}
    # PLAIN: 23 ROSEBERY...
    if toks[0].isdigit():
        return toks[0]," ".join(toks[1:]),{"bad":False,"is_half":False,"is_range":False}
    return None,None,{"bad":True}

def half_variants(street_no_raw):
    n=ws(street_no_raw)
    if "1/2" not in n: return [n]
    n0=n.replace(" 1/2","")
    out=[n, n.replace(" 1/2","-1/2")]
    try:
        out.append(str(float(n0)+0.5))
    except Exception:
        pass
    seen=set(); res=[]
    for x in out:
        if x and x not in seen:
            seen.add(x); res.append(x)
    return res

def pick_unique(rows):
    if not rows: return None,"no_match"
    if len(rows)==1: return rows[0],"unique"
    pids=set()
    for r in rows:
        pid=(r.get("property_id") or r.get("property_uid") or r.get("parcel_id") or "").strip()
        if pid: pids.add(pid)
    if len(pids)==1: return rows[0],"same_property_multi"
    return None,"multi_match"
def build_spine_indices(spine_path):
    base=defaultdict(list)
    unit=defaultdict(list)
    rows=0; used_ak=0; used_fb=0

    with open(spine_path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            rows+=1
            r=json.loads(line)

            town=up(r.get("town") or "")
            if not town: continue

            street_no=None; street_name=None

            ak=(r.get("address_key") or "").strip()
            # expected: A|no|street|town|zip
            if ak:
                parts=ak.split("|")
                if len(parts)>=5 and parts[1] and parts[2] and parts[3]:
                    street_no=parts[1].strip()
                    street_name=parts[2].strip()
                    town=up(parts[3].strip())
                    used_ak+=1

            if not street_no or not street_name:
                street_no=(r.get("street_no") or "").strip()
                street_name=(r.get("street_name") or "").strip()
                used_fb+=1

            if not street_no or not street_name: continue

            sk=f"{town}|{up(street_no)}|{norm_street(street_name)}"
            base[sk].append(r)

            u=r.get("unit")
            if u:
                uk=f"{sk}|U|{norm_unit(u)}"
                unit[uk].append(r)

    stats={
        "spine_rows_scanned": rows,
        "base_keys": len(base),
        "unit_keys": len(unit),
        "used_address_key": used_ak,
        "used_fallback_fields": used_fb
    }
    return base, unit, stats

def apply_attach(ev, sp, key_used):
    ev["attach_status"]="ATTACHED_D"
    ev["match_method"]="unknown_to_attachedD_range_half_unitkey_v1"
    for k in ("property_id","property_uid","parcel_id","building_group_id","site_key","address_key"):
        if sp.get(k) is not None:
            ev[k]=sp.get(k)
    ev.setdefault("attach",{})
    ev["attach"].update({"method":ev["match_method"],"key_used":key_used,"asof":nowz()})
    return ev

def attempt(base_index, unit_index, town, street_no, street_name_norm, unit_val=None):
    sn=up(street_no)
    bk=f"{town}|{sn}|{street_name_norm}"
    if unit_val is not None:
        uk=f"{bk}|U|{norm_unit(unit_val)}"
        got,why=pick_unique(unit_index.get(uk) or [])
        return got,why,uk
    got,why=pick_unique(base_index.get(bk) or [])
    return got,why,bk
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--infile",required=True)
    ap.add_argument("--spine",required=True)
    ap.add_argument("--out",required=True)
    ap.add_argument("--audit",required=True)
    ap.add_argument("--engine_id",required=True)
    args=ap.parse_args()

    base_index, unit_index, spine_stats = build_spine_indices(args.spine)

    audit={
        "engine_id": args.engine_id,
        "infile": args.infile,
        "spine": args.spine,
        "out": args.out,
        "audit": args.audit,
        "started_at": nowz(),
        **spine_stats,
        "rows_scanned": 0,
        "rows_unknown_in": 0,
        "rows_attached_d": 0,
        "rows_tried_range": 0,
        "rows_tried_half": 0,
        "rows_tried_unit": 0,
        "rows_no_match": 0,
        "rows_multi_match": 0,
        "rows_bad_key": 0,
        "detail_counts": {}
    }
    def bump(k): audit["detail_counts"][k]=audit["detail_counts"].get(k,0)+1

    with open(args.infile,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line: continue
            audit["rows_scanned"]+=1
            ev=json.loads(line)

            status = up(ev.get("attach_status") or (ev.get("attach") or {}).get("status") or "")
            mm     = up(ev.get("match_method")  or (ev.get("attach") or {}).get("match_method") or "")

            is_unknown = (status == "UNKNOWN") or (
                status in ("", "NULL") and (mm.startswith("NO_MATCH") or mm.startswith("COLLISION_BASE"))
            )

            if not is_unknown:
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            audit["rows_unknown_in"] += 1

            mk = (ev.get("match_key") or "").strip()

            # Prefer match_key if it parses; else fall back to event fields (deterministic)
            town = ""
            addr = ""
            unit = ""

            if mk:
                # expected: TOWN|ADDR  OR  TOWN|ADDR|UNIT|X
                parts = [p.strip() for p in mk.split("|") if p is not None]
                if len(parts) >= 2:
                    town = up(parts[0])
                    addr = parts[1]
                    if len(parts) >= 4 and up(parts[2]) in ("UNIT", "APT", "STE", "PH"):
                        unit = parts[3]

            # fallback town
            town = up(town or (ev.get("town") or ""))

            # fallback addr (use full_address first, else street_no + street_name)
            addr = (addr or (ev.get("full_address") or "")).strip()
            if not addr:
                sn = str(ev.get("street_no") or "").strip()
                st = str(ev.get("street_name") or "").strip()
                if sn and st:
                    addr = f"{sn} {st}"

            # fallback unit (event field)
            if not unit:
                unit = str(ev.get("unit") or "").strip()

            if unit.lower() in ("none", "null"):
                unit = ""

            if not town or not addr:
                audit["rows_bad_key"] += 1; bump("missing_town_or_addr")
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue


            street_no_raw, street_name_raw, flags = parse_addr(addr)
            if flags.get("bad") or not street_no_raw or not street_name_raw:
                audit["rows_bad_key"]+=1; bump("bad_addr_parse")
                fout.write(json.dumps(ev,ensure_ascii=False)+"\n")
                continue

            street_name_norm = norm_street(street_name_raw)
            attached=False

            # UNIT direct (if unit exists in match_key)
            if unit:
                audit["rows_tried_unit"]+=1
                sn = street_no_raw.split(" ")[0] if flags.get("is_range") else street_no_raw
                got,why,key=attempt(base_index,unit_index,town,sn,street_name_norm,unit)
                if got:
                    apply_attach(ev,got,key); audit["rows_attached_d"]+=1; bump("unit_direct"); attached=True
                else:
                    bump("unit_direct_"+why)

            # HALF variants
            if (not attached) and flags.get("is_half"):
                audit["rows_tried_half"]+=1
                for sn in half_variants(street_no_raw):
                    got,why,key=attempt(base_index,unit_index,town,sn,street_name_norm, unit if unit else None)
                    if got:
                        apply_attach(ev,got,key); audit["rows_attached_d"]+=1; bump("half_variant"); attached=True
                        break
                    else:
                        bump("half_"+why)

            # RANGE expansion + swapped-unit heuristic
            if (not attached) and flags.get("is_range"):
                audit["rows_tried_range"]+=1
                toks=street_no_raw.split(" ")
                a=toks[0] if len(toks)>0 else None
                b=toks[1] if len(toks)>1 else None

                # swapped-unit: if unit exists and b == unit => treat as street_no=a + unit
                if unit and b and norm_unit(b)==norm_unit(unit):
                    got,why,key=attempt(base_index,unit_index,town,a,street_name_norm,unit)
                    if got:
                        apply_attach(ev,got,key); audit["rows_attached_d"]+=1; bump("range_swapped_unit_fixed"); attached=True
                    else:
                        bump("range_swapped_unit_"+why)

                if not attached:
                    candidates=[]
                    for sn in (a,b):
                        if not sn: continue
                        got,why,key=attempt(base_index,unit_index,town,sn,street_name_norm, unit if unit else None)
                        if got: candidates.append((got,key))
                        else: bump("range_"+why)

                    if len(candidates)==1:
                        apply_attach(ev,candidates[0][0],candidates[0][1])
                        audit["rows_attached_d"]+=1; bump("range_single_endpoint"); attached=True
                    elif len(candidates)>=2:
                        pids=set()
                        for sp,_ in candidates:
                            pid=(sp.get("property_id") or sp.get("property_uid") or sp.get("parcel_id") or "").strip()
                            if pid: pids.add(pid)
                        if len(pids)==1 and pids:
                            apply_attach(ev,candidates[0][0],candidates[0][1])
                            audit["rows_attached_d"]+=1; bump("range_both_same_property"); attached=True
                        else:
                            audit["rows_multi_match"]+=1; bump("range_conflict_multi_property")

            if not attached:
                audit["rows_no_match"]+=1; bump("no_attach")

            fout.write(json.dumps(ev,ensure_ascii=False)+"\n")

    audit["finished_at"]=nowz()
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print(json.dumps({
        "done": True,
        "rows_scanned": audit["rows_scanned"],
        "rows_unknown_in": audit["rows_unknown_in"],
        "rows_attached_d": audit["rows_attached_d"],
        "rows_tried_range": audit["rows_tried_range"],
        "rows_tried_half": audit["rows_tried_half"],
        "rows_tried_unit": audit["rows_tried_unit"],
        "rows_no_match": audit["rows_no_match"],
        "rows_multi_match": audit["rows_multi_match"],
        "rows_bad_key": audit["rows_bad_key"],
        "out": args.out,
        "audit": args.audit
    }, indent=2))

if __name__=="__main__":
    main()
