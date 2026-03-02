import argparse,json,os,re,hashlib,datetime
from collections import defaultdict

def nowz():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

SUF={"ST":"STREET","ST.":"STREET","RD":"ROAD","RD.":"ROAD","AVE":"AVENUE","AVE.":"AVENUE","AV":"AVENUE","DR":"DRIVE","DR.":"DRIVE","LN":"LANE","LN.":"LANE","PL":"PLACE","PL.":"PLACE","CT":"COURT","CT.":"COURT","SQ":"SQUARE","SQ.":"SQUARE","PKWY":"PARKWAY","PARKWY":"PARKWAY","TER":"TERRACE","TER.":"TERRACE","CIR":"CIRCLE","CIR.":"CIRCLE","HWY":"HIGHWAY","HWY.":"HIGHWAY","RTE":"ROUTE","RT":"ROUTE","ROUTE":"ROUTE","BLVD":"BLVD","BLVD.":"BLVD","WY":"WAY"}

def ns(s): return re.sub(r"\s+"," ",(s or "").strip())
def tn(t): return ns(t).upper()
def sn(n): return str(n or "").strip().replace("#","").strip()

def sname(s):
    s=ns(s).upper()
    if not s: return s
    p=s.split(" ")
    if p[-1] in SUF: p[-1]=SUF[p[-1]]
    return " ".join(p)

def spine_key(r):
    t=tn(r.get("town")); no=sn(r.get("street_no")); st=sname(r.get("street_name") or "")
    if not t or not no or not st: return ""
    return f"{t}|{no} {st}"

def site_id(mk):
    return "ma:site:"+hashlib.sha1(mk.encode("utf-8")).hexdigest()

def load_needed(infile):
    need=set()
    st={"rows_scanned":0,"rows_collision_unknown":0}
    with open(infile,"r",encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            st["rows_scanned"]+=1
            try: ev=json.loads(line)
            except: continue
            a=ev.get("attach") or {}
            if a.get("attach_status")=="UNKNOWN" and a.get("match_method")=="collision_base":
                mk=(a.get("match_key") or "").strip()
                if mk:
                    need.add(mk)
                    st["rows_collision_unknown"]+=1
    return need,st

def build_map(spine,need):
    m=defaultdict(set)
    st={"spine_rows_scanned":0,"keys_needed":len(need),"keys_touched":0}
    touched=set()
    with open(spine,"r",encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            st["spine_rows_scanned"]+=1
            try: r=json.loads(line)
            except: continue
            k=spine_key(r)
            if not k or k not in need: continue
            pid=r.get("property_id") or r.get("parcel_id")
            if not pid: continue
            m[k].add(pid)
            touched.add(k)
    st["keys_touched"]=len(touched)
    return m,st

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--infile",required=True)
    ap.add_argument("--spine",required=True)
    ap.add_argument("--out",required=True)
    ap.add_argument("--audit",required=True)
    ap.add_argument("--engine_id",default="postfix.collision_base_to_sitefix_v1")
    args=ap.parse_args()

    os.makedirs(os.path.dirname(args.out),exist_ok=True)
    os.makedirs(os.path.dirname(args.audit),exist_ok=True)

    need,s1=load_needed(args.infile)
    mp,s2=build_map(args.spine,need)

    counts=defaultdict(int)
    ex={}
    with open(args.infile,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            if not line.strip(): continue
            try: ev=json.loads(line)
            except: continue
            a=ev.get("attach") or {}
            if a.get("attach_status")=="UNKNOWN" and a.get("match_method")=="collision_base":
                mk=(a.get("match_key") or "").strip()
                c=sorted(mp.get(mk,[]))
                if len(c)>=2:
                    a2=dict(a)
                    a2["attach_status"]="ATTACHED_SITE"
                    a2["match_method"]="site|collision_multi_anchor"
                    a2["site_id"]=site_id(mk)
                    a2["candidate_property_ids"]=c[:20]
                    a2["candidate_count"]=len(c)
                    ev["attach"]=a2
                    counts["resolved_to_site"]+=1
                    if "resolved_to_site" not in ex:
                        ex["resolved_to_site"]={"match_key":mk,"candidate_count":len(c),"candidates":c[:6]}
                else:
                    counts["collision_no_spine_or_single"]+=1
                    if "collision_no_spine_or_single" not in ex:
                        ex["collision_no_spine_or_single"]={"match_key":mk,"candidate_count":len(c),"candidates":c[:6]}
            fout.write(json.dumps(ev,ensure_ascii=False)+"\n")

    audit={
        "engine_id":args.engine_id,
        "ran_at":nowz(),
        "inputs":{"infile":args.infile,"spine":args.spine},
        "outputs":{"out":args.out,"audit":args.audit},
        "stats_infile":s1,
        "stats_spine":s2,
        "counts":dict(counts),
        "examples":ex
    }
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,ensure_ascii=False,indent=2)

    print(json.dumps({"done":True,"rows_scanned":s1.get("rows_scanned",0),"rows_resolved_to_site":counts.get("resolved_to_site",0)},indent=2))

if __name__=="__main__":
    main()