import argparse, json, re, sys
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            yield json.loads(line)

def dump_ndjson(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

_SUFFIXES = {
    "STREET":"ST","ST":"ST","ROAD":"RD","RD":"RD","AVENUE":"AVE","AVE":"AVE","BOULEVARD":"BLVD","BLVD":"BLVD",
    "LANE":"LN","LN":"LN","DRIVE":"DR","DR":"DR","COURT":"CT","CT":"CT","CIRCLE":"CIR","CIR":"CIR","PLACE":"PL","PL":"PL",
    "TERRACE":"TER","TER":"TER","HIGHWAY":"HWY","HWY":"HWY","PARKWAY":"PKY","PKY":"PKY","WAY":"WAY"
}
def norm_street_tokens(s: str):
    s = re.sub(r"\s+", " ", s.strip().upper())
    s = re.sub(r"[.,#]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = s.split()
    if not toks:
        return []
    # normalize last token if suffix-like
    last = toks[-1]
    toks[-1] = _SUFFIXES.get(last, last)
    return toks

def addr_text_from_row(row):
    # try common fields; be defensive about types
    for key in ("addr","address","addr_raw","address_raw"):
        if key in row:
            v=row.get(key)
            if isinstance(v,str) and v.strip():
                return v
            if isinstance(v,dict):
                for kk in ("raw","text","addr","address","value"):
                    vv=v.get(kk)
                    if isinstance(vv,str) and vv.strip():
                        return vv
    # nested places seen sometimes
    pr=row.get("property_ref")
    if isinstance(pr,dict):
        for key in ("addr","address","addr_raw","address_raw"):
            v=pr.get(key)
            if isinstance(v,str) and v.strip():
                return v
    ac=row.get("address_candidates")
    if isinstance(ac,list) and ac:
        v=ac[0]
        if isinstance(v,dict):
            for key in ("addr","address","addr_raw","address_raw","context","raw"):
                vv=v.get(key)
                if isinstance(vv,str) and vv.strip():
                    return vv
    return ""

_house_re = re.compile(r"^\s*(\d+)\s*([A-Z])?(?:\s*[-/]\s*(\d+))?\b")
def extract_house_numbers(addr: str):
    """
    Returns tuple(list_of_house_numbers, unit_letter_or_None, why)
    """
    if not isinstance(addr,str):
        return [], None, "ADDR_NOT_STRING"
    m=_house_re.search(addr.strip().upper())
    if not m:
        return [], None, "NO_NUM"
    a=int(m.group(1))
    unit = m.group(2)
    b=m.group(3)
    if b:
        b=int(b)
        width=abs(b-a)
        # conservative: only try endpoints when width <= 2
        if width<=2 and a!=b:
            return sorted({a,b}), unit, f"RANGE_ENDPOINTS_W{width}"
        return [a], unit, f"RANGE_TOO_WIDE_W{width}"
    return [a], unit, "OK"

def build_spine_index(spine_path):
    # index by (town_upper, house_no) -> list of candidate street token lists + property_id
    idx=defaultdict(list)
    total=0
    for r in iter_ndjson(spine_path):
        total += 1
        town = (r.get("town") or r.get("city") or "").strip().upper()
        pid = r.get("property_id") or r.get("propertyId") or r.get("id")
        # try common house number fields
        hn = r.get("house_no") or r.get("house_number") or r.get("st_num") or r.get("street_number")
        if isinstance(hn,str) and hn.isdigit():
            hn=int(hn)
        if not isinstance(hn,int):
            continue
        street = r.get("street") or r.get("street_name") or r.get("st_name") or r.get("street_full")
        if not isinstance(street,str) or not street.strip():
            # try address field
            street = r.get("address") or r.get("addr") or ""
        street_tokens = norm_street_tokens(str(street))
        if not town or not pid or not street_tokens:
            continue
        idx[(town, hn)].append((pid, street_tokens))
    return idx, total

def tokens_close(a_tokens, b_tokens):
    # conservative: exact match after suffix normalization OR exact match without suffix token
    if a_tokens == b_tokens:
        return True
    if len(a_tokens)>1 and len(b_tokens)>1 and a_tokens[:-1]==b_tokens[:-1]:
        return True
    return False

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    args=ap.parse_args()

    spine_idx, spine_total = build_spine_index(args.spine)

    stats=Counter()
    why_counter=Counter()

    out_rows=[]
    for row in iter_ndjson(args.inp):
        st = row.get("attach_status")
        # pass through everything that is not UNKNOWN
        if st and st != "UNKNOWN":
            stats["pass_through"] += 1
            out_rows.append(row)
            continue

        addr = addr_text_from_row(row)
        houses, unit, why = extract_house_numbers(addr)
        why_counter[why] += 1
        if not houses:
            stats["still_unknown_no_num"] += 1
            out_rows.append(row)
            continue

        town = (row.get("town") or row.get("city") or "").strip().upper()
        street_tokens = norm_street_tokens(addr)
        rescued=False

        for hn in houses:
            cands = spine_idx.get((town, hn), [])
            if not cands:
                continue
            # filter by conservative closeness
            close = [(pid,toks) for (pid,toks) in cands if tokens_close(street_tokens, toks)]
            if len(close)==1:
                pid,_ = close[0]
                # attach safely
                row["attach_status"] = "ATTACHED_B"
                row["match_method"] = "axis2_nonum_rescue_unique_close"
                row["why"] = why
                row["attachments_n"] = 1
                row.setdefault("attach", {})
                row["attach"].update({
                    "property_id": pid,
                    "attach_method": "axis2_nonum_rescue_unique_close",
                    "attach_confidence": "B",
                })
                stats["rescued_attached_b"] += 1
                rescued=True
                break
            elif len(close)>1:
                stats["collision_close_multi"] += 1
                break
        if not rescued:
            stats["still_unknown"] += 1
        out_rows.append(row)

    dump_ndjson(args.out, out_rows)

    audit_path = re.sub(r"\.ndjson$", "", args.out) + "__audit_v1_35_0.json"
    audit = {
        "in": args.inp,
        "spine": args.spine,
        "out": args.out,
        "spine_index_total_rows_seen": spine_total,
        "stats": dict(stats),
        "top_why": why_counter.most_common(20),
    }
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] v1_35_0 NONUM rescue")
    for k,v in stats.most_common():
        print(f"  {k}: {v}")
    print(f"[ok] OUT   {args.out}")
    print(f"[ok] AUDIT {audit_path}")

if __name__=="__main__":
    main()
