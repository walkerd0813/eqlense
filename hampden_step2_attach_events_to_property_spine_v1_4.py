import argparse, json, os, re, sys, hashlib, datetime
from typing import Dict, Tuple, Optional, Any, Iterable

def utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def norm_ws(s:str)->str:
    return re.sub(r"\s+"," ",s or "").strip()

def norm_town(s:str)->str:
    s = norm_ws(s).upper()
    s = re.sub(r"[^A-Z0-9 \-]","",s)
    return s

def norm_addr(s:str)->str:
    s = norm_ws(s).upper()
    s = s.replace("#"," ")
    s = re.sub(r"[^A-Z0-9 \-\/]","",s)
    s = re.sub(r"\s+"," ",s).strip()
    return s

def read_ndjson(path:str)->Iterable[Dict[str,Any]]:
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def detect_spine_records_source(spine_path:str)->Tuple[str, Optional[str], Dict[str,Any]]:
    """
    Returns (mode, records_path, meta)
    mode:
      - "meta_json" : spine_path is a small JSON wrapper; records_path points to NDJSON file
      - "ndjson"    : spine_path itself is ndjson
      - "json_array": spine_path is JSON array of records
      - "unknown"
    """
    meta = {}
    # quick sniff first bytes
    with open(spine_path,"rb") as bf:
        head = bf.read(2048)
    txt = head.decode("utf-8","ignore").lstrip()
    if txt.startswith("{"):
        try:
            obj = json.loads(open(spine_path,"r",encoding="utf-8").read())
            meta = {"raw_keys": list(obj.keys())[:50]}
            # common wrapper keys we saw: properties_ndjson
            for k in ["properties_ndjson","properties_path","ndjson_path","records_ndjson","records_path"]:
                if k in obj and isinstance(obj[k], str) and obj[k].strip():
                    p = obj[k]
                    # allow relative paths from backend root
                    if not os.path.isabs(p):
                        p = os.path.normpath(os.path.join(os.path.dirname(spine_path), p))
                    return ("meta_json", p, {"wrapper_keys": list(obj.keys()), "records_key": k, "records_path": p})
            # maybe it's actually an array-ish object? fallthrough
            if isinstance(obj, list):
                return ("json_array", None, {"note":"top-level list"})
            return ("meta_json", None, {"wrapper_keys": list(obj.keys()), "note":"no records path key found"})
        except Exception as e:
            return ("unknown", None, {"note":"json parse failed", "error": str(e)})
    if txt.startswith("["):
        return ("json_array", None, {"note":"json array file (not supported for huge files)"})
    # assume ndjson
    return ("ndjson", spine_path, {"note":"treating as ndjson"})

def extract_locator_from_spine(rec:Dict[str,Any])->Tuple[str,str,Optional[str]]:
    """
    Try multiple field locations for town/address. Returns (town_norm,address_norm,property_id)
    """
    pid = rec.get("property_id") or rec.get("propertyId") or rec.get("id")
    # address containers
    town = ""
    addr = ""
    # common fields
    for k in ["town","municipality","city","city_town","town_name","muni"]:
        if rec.get(k):
            town = rec.get(k); break
    for k in ["address","site_address","siteAddress","address_full","full_address","street_address","address1","address_line1"]:
        if rec.get(k):
            addr = rec.get(k); break
    # nested
    if not addr and isinstance(rec.get("address"), dict):
        a = rec["address"]
        for k in ["full","line1","street","street_line","site"]:
            if a.get(k):
                addr = a.get(k); break
        if not town:
            for k in ["town","city","municipality"]:
                if a.get(k):
                    town = a.get(k); break
    # sometimes subjectDetails
    if (not addr or not town) and isinstance(rec.get("subjectDetails"), dict):
        sd = rec["subjectDetails"]
        if not town:
            for k in ["town","city","municipality"]:
                if sd.get(k): town = sd.get(k); break
        if not addr:
            for k in ["address","streetAddress","site_address","address_full"]:
                if sd.get(k): addr = sd.get(k); break
    return (norm_town(town), norm_addr(addr), pid)

def build_spine_index(spine_path:str)->Tuple[Dict[Tuple[str,str],str], Dict[str,Any]]:
    mode, rec_path, meta = detect_spine_records_source(spine_path)
    idx: Dict[Tuple[str,str],str] = {}
    samples=[]
    if mode=="meta_json":
        if not rec_path or not os.path.exists(rec_path):
            meta["error"]="wrapper did not contain a usable records path or file missing"
            return idx, {"mode": mode, **meta, "spine_index_keys": 0, "spine_key_examples":[]}
        # read NDJSON records from rec_path
        meta["records_path_used"]=rec_path
        count=0
        for rec in read_ndjson(rec_path):
            count += 1
            town, addr, pid = extract_locator_from_spine(rec)
            if town and addr and pid:
                idx[(town,addr)] = pid
                if len(samples) < 5:
                    samples.append({"property_id": pid, "town_norm": town, "address_norm": addr, "raw_keys": list(rec.keys())[:25]})
            elif len(samples) < 5 and (town or addr or pid):
                samples.append({"property_id": pid, "town_norm": town, "address_norm": addr, "raw_keys": list(rec.keys())[:25]})
        meta["records_seen"]=count
    elif mode=="ndjson":
        count=0
        for rec in read_ndjson(rec_path):
            count += 1
            town, addr, pid = extract_locator_from_spine(rec)
            if town and addr and pid:
                idx[(town,addr)] = pid
                if len(samples) < 5:
                    samples.append({"property_id": pid, "town_norm": town, "address_norm": addr, "raw_keys": list(rec.keys())[:25]})
        meta["records_seen"]=count
    else:
        meta["error"]="unsupported spine mode"
    return idx, {"mode": mode, **meta, "spine_index_keys": len(idx), "spine_key_examples": samples}

def extract_locator_from_event(ev:Dict[str,Any])->Tuple[str,str]:
    # places we expect from your events: property_ref, property_locator, document/town
    town=""
    addr=""
    pr = ev.get("property_ref") or {}
    if isinstance(pr, dict):
        for k in ["town_norm","town","city","municipality","town_raw"]:
            if pr.get(k): town = pr.get(k); break
        for k in ["address_norm","address","address_raw","street_address","site_address","address_full"]:
            if pr.get(k): addr = pr.get(k); break
    pl = ev.get("property_locator") or {}
    if (not town or not addr) and isinstance(pl, dict):
        if not town:
            for k in ["town_norm","town","city","town_raw"]:
                if pl.get(k): town = pl.get(k); break
        if not addr:
            for k in ["address_norm","address","address_raw","site_address","address_full"]:
                if pl.get(k): addr = pl.get(k); break
    # sometimes under document
    doc = ev.get("document") or {}
    if (not town or not addr) and isinstance(doc, dict):
        if not town:
            for k in ["town","city","municipality"]:
                if doc.get(k): town = doc.get(k); break
        if not addr:
            for k in ["address","address_raw","property_address"]:
                if doc.get(k): addr = doc.get(k); break
    return (norm_town(town), norm_addr(addr))

def attach_events(events_dir:str, spine_idx:Dict[Tuple[str,str],str], out_path:str)->Dict[str,Any]:
    counts={"total":0,"ATTACHED_A":0,"UNKNOWN":0,"MISSING_TOWN_OR_ADDRESS":0,"SPINE_INDEX_KEYS":len(spine_idx)}
    samples_missing=[]
    samples_unmatched=[]
    tables = ["deed_events.ndjson","mortgage_events.ndjson","assignment_events.ndjson","lien_events.ndjson","release_events.ndjson","lis_pendens_events.ndjson","foreclosure_events.ndjson"]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path,"w",encoding="utf-8") as out:
        for fn in tables:
            p=os.path.join(events_dir, fn)
            if not os.path.exists(p):
                continue
            for ev in read_ndjson(p):
                counts["total"] += 1
                town, addr = extract_locator_from_event(ev)
                attachment = {
                    "attach_status":"UNKNOWN",
                    "attach_method":"none",
                    "attach_confidence":"UNKNOWN",
                    "attach_score":0.0,
                    "property_id": None,
                    "parcel_id": None
                }
                if not town or not addr:
                    counts["UNKNOWN"] += 1
                    counts["MISSING_TOWN_OR_ADDRESS"] += 1
                    if len(samples_missing) < 5:
                        samples_missing.append({
                            "src": fn,
                            "event_id": ev.get("event_id"),
                            "event_type": ev.get("event_type"),
                            "town_raw": (ev.get("property_ref") or {}).get("town_raw","") if isinstance(ev.get("property_ref"),dict) else "",
                            "address_raw": (ev.get("property_ref") or {}).get("address_raw","") if isinstance(ev.get("property_ref"),dict) else "",
                            "available_keys": list(ev.keys())
                        })
                else:
                    pid = spine_idx.get((town,addr))
                    if pid:
                        attachment.update({
                            "attach_status":"ATTACHED_A",
                            "attach_method":"town_address_exact",
                            "attach_confidence":"A",
                            "attach_score":1.0,
                            "property_id": pid
                        })
                        counts["ATTACHED_A"] += 1
                    else:
                        counts["UNKNOWN"] += 1
                        if len(samples_unmatched) < 5:
                            samples_unmatched.append({
                                "src": fn,
                                "event_id": ev.get("event_id"),
                                "event_type": ev.get("event_type"),
                                "town_norm": town,
                                "address_norm": addr
                            })
                ev["attachment"]=attachment
                out.write(json.dumps(ev, ensure_ascii=False) + "\n")
    return {"counts":counts, "samples":{"missing_locator":samples_missing,"unmatched_locator":samples_unmatched}}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--eventsDir", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args=ap.parse_args()

    spine_idx, spine_meta = build_spine_index(args.spine)
    result = attach_events(args.eventsDir, spine_idx, args.out)

    audit = {
        "created_at": utc_now_iso(),
        "events_dir": os.path.abspath(args.eventsDir),
        "spine_path": os.path.abspath(args.spine),
        "spine_meta_detected": spine_meta,
        **result
    }
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)
    print("[info] spine_index_keys:", spine_meta.get("spine_index_keys",0))
    print("[done] out:", args.out)
    print("[done] audit:", args.audit)
    print("[done] attach_status_counts:", {k:v for k,v in audit["counts"].items() if k in ["ATTACHED_A","UNKNOWN","MISSING_TOWN_OR_ADDRESS"]})

if __name__=="__main__":
    main()
