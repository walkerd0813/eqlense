#!/usr/bin/env python3
# hampden_axis2_rescue_diagnostics_v1_34_1.py
import argparse, json, re
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

RANGE_RE = re.compile(r"^(\d+)\s*-\s*(\d+)$")
LEADNUM_RE = re.compile(r"^(\d+)\b")

def get_addr_str(row):
    a = row.get("addr")
    if isinstance(a, str):
        return a
    if isinstance(a, dict):
        for k in ("full","addr","address","raw","line1","display"):
            v=a.get(k)
            if isinstance(v,str) and v.strip():
                return v
    return ""

def norm_ws(s):
    return re.sub(r"\s+", " ", s.strip().upper())

def extract_house_tokens(addr):
    addr = norm_ws(addr)
    if not addr:
        return None, None, addr
    first = addr.split(" ",1)[0]
    m=RANGE_RE.match(first)
    if m:
        return int(m.group(1)), int(m.group(2)), addr
    m=LEADNUM_RE.match(addr)
    if m:
        return int(m.group(1)), None, addr
    return None, None, addr

def load_spine_index(spine_path):
    idx=defaultdict(list)
    bad=0; n=0
    for r in iter_ndjson(spine_path):
        n+=1
        town = r.get("town") or r.get("city") or r.get("municipality")
        addr = r.get("address") or r.get("addr") or r.get("address_full") or r.get("address_line1")
        if not isinstance(town,str) or not isinstance(addr,str):
            bad+=1; continue
        town_u = town.strip().upper()
        hn, _, _ = extract_house_tokens(addr)
        if hn is None:
            bad+=1; continue
        rest = norm_ws(addr)
        rest = re.sub(r"^\d+\s*", "", rest)
        rest = re.sub(r"\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b.*$", "", rest).strip()
        if rest:
            idx[(town_u, hn)].append(rest)
    return idx, {"rows": n, "bad": bad, "keys": len(idx)}

def street_close(a,b):
    def norm_street(s):
        s=norm_ws(s)
        s=s.replace("STREET","ST").replace("AVENUE","AVE").replace("ROAD","RD").replace("DRIVE","DR").replace("BOULEVARD","BLVD").replace("LANE","LN").replace("COURT","CT").replace("PLACE","PL").replace("TERRACE","TER")
        s=re.sub(r"\bNORTH\b","N",s); s=re.sub(r"\bSOUTH\b","S",s); s=re.sub(r"\bEAST\b","E",s); s=re.sub(r"\bWEST\b","W",s)
        s=re.sub(r"\s+"," ",s).strip()
        return s
    return norm_street(a)==norm_street(b)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out_json", required=True)
    ap.add_argument("--max_samples", type=int, default=25)
    args=ap.parse_args()

    spine_idx, spine_meta = load_spine_index(args.spine)

    ctr=Counter()
    samples=defaultdict(list)

    for row in iter_ndjson(args.inp):
        if row.get("attach_status") != "UNKNOWN":
            continue
        town = (row.get("town") or "").strip().upper()
        addr = get_addr_str(row)
        hn1, hn2, addr_u = extract_house_tokens(addr)

        if hn1 is None:
            k=("NO_NUM",)
            ctr[k]+=1
            if len(samples[k])<args.max_samples: samples[k].append({"event_id":row.get("event_id"),"town":town,"addr":addr})
            continue

        cands = spine_idx.get((town, hn1), [])
        if not cands:
            k=("NO_SPINE_SAME_NO",)
            ctr[k]+=1
            if len(samples[k])<args.max_samples: samples[k].append({"event_id":row.get("event_id"),"town":town,"addr":addr,"house_no":hn1})
            continue

        street = norm_ws(addr_u)
        street = re.sub(r"^\d+\s*-\s*\d+\s*","",street)
        street = re.sub(r"^\d+\s*","",street)
        street = re.sub(r"\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b.*$","",street).strip()

        close = any(street_close(street, s) for s in cands)
        if not close:
            k=("SPINE_HAS_SAME_NO_BUT_STREET_NOT_CLOSE",)
            ctr[k]+=1
            if len(samples[k])<args.max_samples:
                samples[k].append({"event_id":row.get("event_id"),"town":town,"addr":addr,"house_no":hn1,"street":street,"spine_streets_sample":cands[:5]})
            continue

        if hn2 is None:
            k=("HAS_CLOSE_STREET_SINGLE_NO_STILL_UNKNOWN",)
            ctr[k]+=1
            if len(samples[k])<args.max_samples:
                samples[k].append({"event_id":row.get("event_id"),"town":town,"addr":addr,"house_no":hn1,"street":street,"spine_streets_sample":cands[:5]})
        else:
            width = abs(hn2-hn1)
            k=("HAS_CLOSE_STREET_RANGE", "W"+str(width))
            ctr[k]+=1
            if len(samples[k])<args.max_samples:
                samples[k].append({"event_id":row.get("event_id"),"town":town,"addr":addr,"range":[hn1,hn2],"street":street,"spine_streets_sample":cands[:5]})

    out = {
        "meta": {"spine": args.spine, "in": args.inp, "spine_index": spine_meta},
        "counts": { "|".join(k): v for k,v in ctr.most_common() },
        "samples": { "|".join(k): v for k,v in samples.items() },
    }
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print("[in]", args.inp)
    print("[spine]", args.spine)
    print("[out_json]", args.out_json)
    print("\nTOP WHY buckets:")
    for k,v in ctr.most_common(12):
        print(f"{v:5d}  {'|'.join(k)}")
    print("\nNote: conservative street closeness (suffix-normalized exact).")

if __name__=="__main__":
    main()
