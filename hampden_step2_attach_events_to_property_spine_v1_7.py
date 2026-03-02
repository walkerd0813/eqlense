# hampden_step2_attach_events_to_property_spine_v1_7.py
# FIX: safely extract town/address from dict or string (NO ZIP REQUIRED)

import json, argparse, os, hashlib
from datetime import datetime

def norm(s):
    return " ".join(s.upper().strip().split()) if isinstance(s,str) else ""

def extract_addr(v):
    if isinstance(v,str):
        return v
    if isinstance(v,dict):
        for k in ("norm","line1","raw","address","street"):
            if k in v and isinstance(v[k],str):
                return v[k]
    return ""

def extract_town(v):
    if isinstance(v,str):
        return v
    if isinstance(v,dict):
        for k in ("town","city","municipality","name"):
            if k in v and isinstance(v[k],str):
                return v[k]
    return ""

def build_spine_index(path):
    idx={}
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            p=json.loads(line)
            addr=extract_addr(
                p.get("address") or
                p.get("address_norm") or
                p.get("situs_address") or
                p.get("location",{}).get("address")
            )
            town=extract_town(
                p.get("town") or
                p.get("city") or
                p.get("municipality") or
                p.get("situs_city")
            )
            k=f"{norm(town)}|{norm(addr)}"
            if town and addr:
                idx[k]=p.get("property_id")
    return idx

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--eventsDir",required=True)
    ap.add_argument("--spine",required=True)
    ap.add_argument("--out",required=True)
    ap.add_argument("--audit",required=True)
    a=ap.parse_args()

    spine_idx=build_spine_index(a.spine)
    counts={"ATTACHED_A":0,"UNKNOWN":0}

    os.makedirs(os.path.dirname(a.out),exist_ok=True)

    with open(a.out,"w",encoding="utf-8") as out:
        for fn in os.listdir(a.eventsDir):
            if not fn.endswith(".ndjson"): continue
            with open(os.path.join(a.eventsDir,fn),"r",encoding="utf-8") as f:
                for line in f:
                    e=json.loads(line)
                    town=e.get("property_ref",{}).get("town") or ""
                    addr=e.get("property_ref",{}).get("address") or ""
                    key=f"{norm(town)}|{norm(addr)}"
                    pid=spine_idx.get(key)
                    if pid:
                        e["property_id"]=pid
                        e["attach_status"]="ATTACHED_A"
                        counts["ATTACHED_A"]+=1
                    else:
                        e["attach_status"]="UNKNOWN"
                        counts["UNKNOWN"]+=1
                    out.write(json.dumps(e)+"\n")

    with open(a.audit,"w") as af:
        json.dump({
            "created_at":datetime.utcnow().isoformat()+"Z",
            "spine_index_keys":len(spine_idx),
            "counts":counts
        },af,indent=2)

    print("[done] spine_index_keys:",len(spine_idx))
    print("[done] attach_status_counts:",counts)

if __name__=="__main__":
    main()
