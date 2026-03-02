#!/usr/bin/env python3
from __future__ import annotations
import argparse, datetime, json, os, shutil, hashlib
from typing import Any
ENGINE_VERSION="v0_1"
def utc_now_iso()->str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00","Z")
def sha256_file(p:str)->str:
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for c in iter(lambda:f.read(1024*1024), b""):
            h.update(c)
    return h.hexdigest()
def load_json_bom(p:str)->Any:
    if not os.path.exists(p): return None
    with open(p,"r",encoding="utf-8-sig") as f: return json.load(f)
def write_json(p:str,o:Any)->None:
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p,"w",encoding="utf-8") as f: json.dump(o,f,indent=2,sort_keys=False)
def main()->None:
    ap=argparse.ArgumentParser()
    ap.add_argument("--root",required=True); ap.add_argument("--state",required=True); ap.add_argument("--as_of",required=True)
    ap.add_argument("--indicators",required=True)
    a=ap.parse_args()
    root=a.root; state=a.state.upper()
    cur=os.path.join(root,"publicData","marketRadar","indicators","CURRENT")
    os.makedirs(cur, exist_ok=True)
    dst=os.path.join(cur,f"CURRENT_MARKET_RADAR_INDICATORS_P01_{state}.ndjson")
    shutil.copyfile(a.indicators,dst)
    sha=sha256_file(dst)
    sha_json=dst+".sha256.json"
    write_json(sha_json, {"path":dst,"sha256":sha,"built_at_utc":utc_now_iso()})
    ptr_path=os.path.join(cur,"CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")
    ptr=load_json_bom(ptr_path) or {"schema_version":"market_radar_indicators_pointers_v1","engine_version":ENGINE_VERSION,"states":{}}
    ptr["updated_at_utc"]=utc_now_iso()
    ptr["states"][state]={"as_of_date":a.as_of,"ndjson":dst,"sha256_json":sha_json,"updated_at_utc":utc_now_iso()}
    if os.path.exists(ptr_path):
        ts=datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")
        shutil.copyfile(ptr_path, ptr_path+f".bak_{ts}")
    write_json(ptr_path, ptr)
    print(json.dumps({"ok":True,"state":state,"indicators_current":dst,"sha256_json":sha_json,"pointers":ptr_path},indent=2))
if __name__=="__main__": main()
