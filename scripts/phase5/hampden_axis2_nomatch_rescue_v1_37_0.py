import argparse, json, re
from difflib import SequenceMatcher
from collections import defaultdict, Counter

def town_norm(t):
    if not t: return None
    return re.sub(r'[^A-Z0-9 ]+', ' ', str(t).upper()).strip()

SUFFIX_MAP = {
    "ST":"ST","STREET":"ST",
    "RD":"RD","ROAD":"RD",
    "AVE":"AVE","AV":"AVE","AVENUE":"AVE",
    "DR":"DR","DRIVE":"DR",
    "LN":"LN","LANE":"LN","LA":"LN",
    "BLVD":"BLVD","BOULEVARD":"BLVD",
    "PKWY":"PKWY","PKY":"PKWY","PARKWAY":"PKWY",
    "CT":"CT","COURT":"CT",
    "PL":"PL","PLACE":"PL",
    "TER":"TER","TERR":"TER","TERRACE":"TER",
    "HWY":"HWY","HGY":"HWY","HIGHWAY":"HWY",
    "CIR":"CIR","CIRCLE":"CIR",
    "WAY":"WAY",
}

UNIT_WORDS = {"UNIT","APT","APARTMENT","#","STE","SUITE"}

def basic_tokens(s: str):
    s = re.sub(r'[^A-Z0-9 \-#]+',' ', str(s).upper()).strip()
    s = re.sub(r'\s+',' ', s)
    return s.split()

def parse_addr(addr):
    if not addr:
        return None
    toks = basic_tokens(addr)
    if not toks:
        return None
    first = toks[0]

    # range like 19-21
    m2 = re.match(r'^(\d+)\-(\d+)$', first)
    if m2:
        lo = int(m2.group(1)); hi = int(m2.group(2))
        if lo == 0 or hi == 0 or hi < lo:
            return None
        street_toks = toks[1:]
        return ("range", lo, hi, " ".join(street_toks).strip())

    m = re.match(r'^(\d+)([A-Z]?)$', first)
    if not m:
        return None
    num = int(m.group(1))
    if num == 0:
        return None
    street_toks = toks[1:]
    if not street_toks:
        return None

    street_clean=[]
    for tk in street_toks:
        if tk in UNIT_WORDS:
            break
        street_clean.append(tk)
    if not street_clean:
        street_clean = street_toks
    return ("single", num, None, " ".join(street_clean).strip())

def norm_street(street: str):
    if not street:
        return None
    toks = basic_tokens(street)
    if not toks:
        return None
    if len(toks) >= 2:
        last = toks[-1]
        if last in SUFFIX_MAP:
            toks[-1] = SUFFIX_MAP[last]
    return " ".join(toks)

def edit_distance_limited(a: str, b: str, limit: int=2) -> int:
    if a == b:
        return 0
    if abs(len(a)-len(b)) > limit:
        return limit+1
    prev = list(range(len(b)+1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        row_min = cur[0]
        for j, cb in enumerate(b, 1):
            ins = cur[j-1] + 1
            dele = prev[j] + 1
            sub = prev[j-1] + (0 if ca==cb else 1)
            v = ins if ins < dele else dele
            if sub < v:
                v = sub
            cur.append(v)
            if v < row_min:
                row_min = v
        if row_min > limit:
            return limit+1
        prev = cur
    return prev[-1]

def score(a: str, b: str):
    r = SequenceMatcher(None, a, b).ratio()
    d = edit_distance_limited(a, b, limit=2)
    return r, d

def load_needed_keys(in_path):
    keys=set()
    eligible=0
    with open(in_path,'r',encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            r=json.loads(line)
            if r.get("attach_status")!="UNKNOWN":
                continue
            if str(r.get("why","")).lower() != "no_match":
                continue
            t=town_norm(r.get("town"))
            parsed=parse_addr(r.get("addr"))
            if not t or not parsed:
                continue
            if parsed[0]=="single":
                keys.add((t, parsed[1]))
                eligible += 1
            elif parsed[0]=="range":
                lo,hi=parsed[1],parsed[2]
                if hi-lo <= 4:
                    for n in range(lo,hi+1):
                        keys.add((t,n))
                    eligible += 1
    return keys, eligible

def build_spine_index(spine_path, needed_keys):
    idx=defaultdict(list)
    with open(spine_path,'r',encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            r=json.loads(line)
            t = town_norm(r.get("town") or r.get("municipality") or r.get("city"))
            if not t:
                continue
            hn = r.get("house_number") or r.get("addr_number") or r.get("street_number") or r.get("number")
            hn_int=None
            if hn is not None:
                try:
                    hn_int=int(str(hn).strip())
                except:
                    hn_int=None
            if hn_int is None:
                addr = r.get("address") or r.get("addr") or r.get("site_address")
                p=parse_addr(addr)
                if p and p[0]=="single":
                    hn_int=p[1]
            if hn_int is None:
                continue
            key=(t,hn_int)
            if key not in needed_keys:
                continue
            street = r.get("street_name") or r.get("street") or r.get("street_full") or r.get("street_nm")
            if not street:
                addr = r.get("address") or r.get("addr") or r.get("site_address")
                p=parse_addr(addr)
                if p:
                    street=p[3]
            st_norm = norm_street(street) if street else None
            if not st_norm:
                continue
            idx[key].append({
                "property_id": r.get("property_id") or r.get("id") or r.get("propertyId"),
                "street_norm": st_norm
            })
    return idx

def choose_unique_match(street_norm_in, candidates, ratio_min=0.92, dist_max=2):
    exact=[c for c in candidates if c["street_norm"]==street_norm_in]
    if len(exact)==1:
        return exact[0], "EXACT"

    scored=[]
    for c in candidates:
        r,d=score(street_norm_in, c["street_norm"])
        if r >= ratio_min and d <= dist_max:
            scored.append((r,d,c))
    if not scored:
        return None, None
    scored.sort(key=lambda x:(-x[0], x[1]))
    best=scored[0]
    if len(scored)==1:
        return best[2], "FUZZY"
    second=scored[1]
    if best[0] - second[0] >= 0.03:
        return best[2], "FUZZY"
    return None, None

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    args=ap.parse_args()

    needed_keys, eligible = load_needed_keys(args.inp)
    print("[info] eligible UNKNOWN/no_match rows:", eligible)
    print("[info] needed (town,house#) keys:", len(needed_keys))

    idx = build_spine_index(args.spine, needed_keys)
    print("[info] spine index keys built:", len(idx))

    counts=Counter()
    rescued=0

    with open(args.inp,'r',encoding='utf-8') as fin, open(args.out,'w',encoding='utf-8') as fout:
        for line in fin:
            if not line.strip():
                continue
            r=json.loads(line)

            if r.get("attach_status")!="UNKNOWN" or str(r.get("why","")).lower()!="no_match":
                fout.write(json.dumps(r, ensure_ascii=False)+"\n")
                counts["pass_through"] += 1
                continue

            t=town_norm(r.get("town"))
            parsed=parse_addr(r.get("addr"))
            if not t or not parsed:
                fout.write(json.dumps(r, ensure_ascii=False)+"\n")
                counts["still_unknown_unparsed"] += 1
                continue

            street_in = norm_street(parsed[3])
            if not street_in:
                fout.write(json.dumps(r, ensure_ascii=False)+"\n")
                counts["still_unknown_no_street"] += 1
                continue

            match=None
            method=None

            if parsed[0]=="single":
                key=(t, parsed[1])
                cands=idx.get(key, [])
                if cands:
                    match, kind = choose_unique_match(street_in, cands)
                    if match:
                        method = "axis2_nomatch_rescue_strong_unique" if kind=="FUZZY" else "axis2_nomatch_rescue_exact_unique"
            else:
                lo,hi = parsed[1], parsed[2]
                if hi-lo <= 4:
                    found=[]
                    for n in range(lo,hi+1):
                        cands=idx.get((t,n), [])
                        if not cands:
                            continue
                        m, kind = choose_unique_match(street_in, cands, ratio_min=0.96, dist_max=1)
                        if m:
                            found.append((n,m,kind))
                    if len(found)==1:
                        match=found[0][1]
                        method="axis2_range_small_unique"

            if not match or not match.get("property_id"):
                fout.write(json.dumps(r, ensure_ascii=False)+"\n")
                counts["still_unknown_no_unique"] += 1
                continue

            r["attach_status"]="ATTACHED_B"
            r["match_method"]=method
            r["why"]="NONE"
            r["property_id"]=match["property_id"]
            r["attachments_n"]=1

            rescued += 1
            counts["rescued"] += 1
            fout.write(json.dumps(r, ensure_ascii=False)+"\n")

    print("[done] v1_37_0 NO_MATCH rescue")
    for k,v in counts.most_common():
        print(" ", k, ":", v)
    print("[done] rescued:", rescued)

if __name__=="__main__":
    main()
