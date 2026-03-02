import json, re, hashlib, argparse, datetime
def nowz(): return datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
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
    # strip common designators but keep PH/BSMT etc as value if that is the whole thing
    u=re.sub(r"^(UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR|RM|ROOM)\s+","",u)
    return norm_ws(u)
def parse_range_fix(street_no, street_name):
    # case: street_no="370" street_name="380 HARRISON AV" => street_no="370-380" street_name="HARRISON AV"
    sn=norm_ws(street_name)
    m=re.match(r"^(\d{1,6})\s+(.*)$", sn)
    if street_no and m:
        lead=m.group(1); rest=m.group(2)
        # only if lead is different and both look like plausible house numbers
        if lead.isdigit() and street_no.isdigit() and lead!=street_no and 1<=int(lead)<=999999 and 1<=int(street_no)<=999999:
            return f"{street_no}-{lead}", rest
    return street_no, street_name
def fix_swapped_unit(street_no, street_name, unit):
    # if street_name ends with digits and unit equals street_no, swap
    sn=norm_ws(street_name)
    m=re.match(r"^(.*\D)\s+(\d{1,6})$", sn)
    if street_no and unit and m:
        tail=m.group(2)
        if unit.strip()==street_no.strip() and tail!=unit.strip():
            return street_no, m.group(1).strip(), tail
    return street_no, street_name, unit
def spine_unit_key(r):
    town=norm_town(r.get("town") or r.get("city") or "")
    if not town: return ""
    stno=norm_ws(r.get("street_no") or "")
    stnm=norm_street_name(r.get("street_name") or "")
    unit=norm_unit(r.get("unit") or "")
    if not (stno and stnm and unit): return ""
    return f"{town}|{stno}|{stnm}|{unit}"
def event_unit_key(ev):
    town=norm_town(ev.get("town") or "")
    stno=norm_ws(ev.get("street_no") or "")
    stnm=ev.get("street_name") or ""
    unit=ev.get("unit") or ""
    # apply the two deterministic fixes
    stno, stnm = parse_range_fix(stno, stnm)
    stno, stnm, unit = fix_swapped_unit(stno, stnm, unit)
    stnm=norm_street_name(stnm)
    unit=norm_unit(unit)
    if not (town and stno and stnm and unit): return "", None
    return f"{town}|{stno}|{stnm}|{unit}", {"town":town,"street_no":stno,"street_name":stnm,"unit":unit}
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", default="postfix.no_match_unit_then_base_unitkey_attach_v3")
    args=ap.parse_args()

    audit={"engine_id":args.engine_id,"infile":args.infile,"spine":args.spine,"started_at":nowz()}
    # build spine unit index
    idx={}
    spine_rows=0
    with open(args.spine,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            spine_rows+=1
            r=json.loads(line)
            k=spine_unit_key(r)
            if not k: continue
            pid=r.get("property_id") or r.get("building_group_id") or r.get("parcel_id")
            if not pid: continue
            idx.setdefault(k,set()).add(pid)
    audit["spine_rows_scanned"]=spine_rows
    audit["spine_unit_keys"]=len(idx)

    rows_scanned=0
    rows_fixed_addr=0
    rows_attached=0
    rows_still_unknown=0
    no_unitkey_parse=0
    no_spine_match=0
    multi_spine_match=0

    with open(args.infile,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            rows_scanned+=1
            ev=json.loads(line)
            a=ev.get("attach") or ev
            # only operate on UNKNOWN with the unit-then-base method
            if a.get("attach_status")!="UNKNOWN" or a.get("match_method")!="no_match_unit_then_base":
                fout.write(json.dumps(ev,ensure_ascii=False)+"\n"); continue

            k, fixed = event_unit_key(ev)
            if fixed:
                rows_fixed_addr += 1
                # write back normalized pieces for downstream audits (does not break determinism)
                ev["town"]=fixed["town"]
                ev["street_no"]=fixed["street_no"]
                ev["street_name"]=fixed["street_name"]
                ev["unit"]=fixed["unit"]
            if not k:
                no_unitkey_parse += 1
                rows_still_unknown += 1
                fout.write(json.dumps(ev,ensure_ascii=False)+"\n"); continue

            hits=idx.get(k)
            if not hits:
                no_spine_match += 1
                rows_still_unknown += 1
                fout.write(json.dumps(ev,ensure_ascii=False)+"\n"); continue
            if len(hits)!=1:
                multi_spine_match += 1
                rows_still_unknown += 1
                fout.write(json.dumps(ev,ensure_ascii=False)+"\n"); continue

            pid=next(iter(hits))
            ev["property_id"]=pid
            if "attach" not in ev: ev["attach"]={}
            ev["attach"]["attach_status"]="ATTACHED_UNIT"
            ev["attach"]["match_method"]="unitkey_spine_fields_v3"
            ev["attach"]["match_key_unit"]=k
            rows_attached += 1
            fout.write(json.dumps(ev,ensure_ascii=False)+"\n")

    audit.update({
        "done": True,
        "rows_scanned": rows_scanned,
        "rows_fixed_addr_fields": rows_fixed_addr,
        "rows_attached": rows_attached,
        "rows_still_unknown_after_attempt": rows_still_unknown,
        "no_unitkey_parse": no_unitkey_parse,
        "no_spine_match": no_spine_match,
        "multi_spine_match": multi_spine_match,
        "finished_at": nowz()
    })
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)
    print(json.dumps({"done":True,"rows_scanned":rows_scanned,"rows_fixed_addr_fields":rows_fixed_addr,"rows_attached":rows_attached,"rows_still_unknown_after_attempt":rows_still_unknown,"no_unitkey_parse":no_unitkey_parse,"no_spine_match":no_spine_match,"multi_spine_match":multi_spine_match,"out":args.out,"audit":args.audit}, indent=2))
if __name__=="__main__":
    main()