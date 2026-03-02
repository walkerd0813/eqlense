import json, argparse
from collections import Counter

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
        for k in ["norm","normalized","value","text","raw","full","display","name","town","city"]:
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
    # common punctuation
    t = t.replace(",", " ").replace("  "," ").strip()
    # drop trailing state token
    if t.endswith(" MA"):
        t = t[:-3].strip()
    return t

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--spine", required=True)
    ap.add_argument("--events", required=True)
    args=ap.parse_args()

    # collect need_towns from events SINGLE UNKNOWN no_match
    need=set()
    for ev in it(args.events):
        a=ev.get("attach") or {}
        if a.get("attach_scope")=="SINGLE" and (a.get("attach_status") or "").upper()=="UNKNOWN" and a.get("match_method")=="no_match":
            pr=ev.get("property_ref") or {}
            town = norm_town(as_str(pr.get("town_norm") or pr.get("town_raw") or pr.get("town")))
            if town: need.add(town)

    print({"need_towns_n": len(need), "need_towns": sorted(list(need))})

    # scan spine and count towns that match
    seen=0
    match=0
    top=Counter()
    top_raw=Counter()
    for r in it(args.spine):
        pr=r.get("property_ref") or r.get("ref") or {}
        town_raw = as_str(r.get("town_norm") or pr.get("town_norm") or r.get("town") or pr.get("town"))
        t = norm_town(town_raw)
        if not t:
            continue
        seen += 1
        top[t] += 1
        top_raw[town_raw.upper().strip()] += 1
        if t in need:
            match += 1

    print({"spine_rows_with_town": seen, "spine_rows_matching_need_towns": match})
    print("TOP 20 spine town_norm:")
    for k,v in top.most_common(20):
        print(v, k)
    print("TOP 20 raw town strings (to spot format issues):")
    for k,v in top_raw.most_common(20):
        print(v, k)

if __name__=="__main__":
    main()
