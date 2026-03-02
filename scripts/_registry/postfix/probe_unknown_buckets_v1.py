import argparse, json, os
from collections import Counter, defaultdict

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--top", type=int, default=20)
    args=ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    total=0
    unknown=0

    # bucket key -> count
    c_all=Counter()
    c_by_method=defaultdict(Counter)

    # bucket key -> sample dict
    samples_all={}
    samples_by_method=defaultdict(dict)

    def bucket(a):
        mm=a.get("match_method") or "(none)"
        mk=a.get("match_key") or "(no_match_key)"
        return (mm, mk)

    def add_sample(store, key, ev, a):
        if key in store: return
        store[key]={
            "event_id": ev.get("event_id"),
            "match_method": a.get("match_method"),
            "match_key": a.get("match_key"),
            "match_key_unit": a.get("match_key_unit"),
            "unit_present": bool(a.get("match_key_unit")),
        }

    with open(args.infile,"r",encoding="utf-8") as f:
        for line in f:
            if not line.strip(): 
                continue
            total += 1
            try:
                ev=json.loads(line)
            except:
                continue
            a=ev.get("attach") or {}
            if a.get("attach_status")!="UNKNOWN":
                continue
            unknown += 1
            b=bucket(a)
            c_all[b]+=1
            c_by_method[a.get("match_method") or "(none)"][b]+=1
            add_sample(samples_all, b, ev, a)
            mm=a.get("match_method") or "(none)"
            add_sample(samples_by_method[mm], b, ev, a)

    def top_list(counter, store, n):
        out=[]
        for (mm,mk),cnt in counter.most_common(n):
            s=store.get((mm,mk),{})
            out.append({
                "count": cnt,
                "match_method": mm,
                "match_key": mk,
                "sample": s
            })
        return out

    out={
        "infile": args.infile,
        "rows_total": total,
        "rows_unknown": unknown,
        "top_unknown_overall": top_list(c_all, samples_all, args.top),
        "top_unknown_collision_base": top_list(c_by_method.get("collision_base",Counter()), samples_by_method.get("collision_base",{}), args.top),
        "top_unknown_no_match_unit_then_base": top_list(c_by_method.get("no_match_unit_then_base",Counter()), samples_by_method.get("no_match_unit_then_base",{}), args.top),
        "top_unknown_no_match_base": top_list(c_by_method.get("no_match_base",Counter()), samples_by_method.get("no_match_base",{}), args.top),
    }

    with open(args.out,"w",encoding="utf-8") as fo:
        json.dump(out, fo, ensure_ascii=False, indent=2)

    print(json.dumps({"done": True, "rows_total": total, "rows_unknown": unknown, "out": args.out}, indent=2))

if __name__=="__main__":
    main()