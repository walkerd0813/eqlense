import argparse, json, os, re, datetime
from collections import defaultdict, Counter

def nowz():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def ns(s): return re.sub(r"\s+"," ",(s or "").strip())
def up(s): return ns(s).upper()

# very small + safe suffix swaps (deterministic)
SUF = {
  "STREET": "ST",
  "ST": "STREET",
  "ROAD": "RD",
  "RD": "ROAD",
  "AVENUE": "AVE",
  "AVE": "AVENUE",
  "BOULEVARD": "BLVD",
  "BLVD": "BOULEVARD",
  "LANE": "LN",
  "LN": "LANE",
  "DRIVE": "DR",
  "DR": "DRIVE",
  "COURT": "CT",
  "CT": "COURT",
  "PLACE": "PL",
  "PL": "PLACE",
  "TERRACE":"TER",
  "TER":"TERRACE",
}

SUF_ENDINGS=set(SUF.keys())

def parse_match_key(mk):
    # mk like "BOSTON|113 ENDICOTT STREET"
    mk = (mk or "").strip()
    if "|" not in mk: return ("","")
    town, rest = mk.split("|",1)
    return (up(town), up(rest))

def split_number_and_street(rest):
    # rest like "113 ENDICOTT STREET" or "108 114 CHESTNUT STREET" or "ESSEX STREET"
    r = up(rest)
    m = re.match(r"^(\d+)\s+(.*)$", r)
    if not m:
        return ("", r)
    return (m.group(1), up(m.group(2)))

def fix_leading_street_token(street_name):
    # "STREET ANDREWS ROAD" => "ST ANDREWS ROAD" (OCR artifact)
    toks = up(street_name).split()
    if len(toks) >= 2 and toks[0] == "STREET" and toks[-1] in SUF_ENDINGS:
        toks[0] = "ST"
        return " ".join(toks)
    return up(street_name)

def suffix_variants(street_name):
    # only tail token swap
    toks = up(street_name).split()
    if not toks: return [up(street_name)]
    last = toks[-1]
    out=set([up(street_name)])
    if last in SUF:
        toks2 = toks[:-1] + [SUF[last]]
        out.add(" ".join(toks2))
    return list(out)

def range_variants(street_no, street_name):
    # if street_no is actually "108 114" style, handle from the raw rest instead
    # (this function expects single street_no, so we do range detection earlier)
    return [(street_no, street_name)]

def mk_from_parts(town, no, street_name):
    return f"{town}|{no} {street_name}".strip()

def build_spine_index(spine_path, needed_keys=None, cap=10):
    idx = defaultdict(list)
    scanned=0
    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): 
                continue
            scanned += 1
            try:
                r = json.loads(line)
            except:
                continue
            town = up(r.get("town"))
            no = ns(r.get("street_no"))
            sn = up(r.get("street_name"))
            if not town or not no or not sn:
                continue
            k = f"{town}|{no} {sn}"
            if needed_keys is not None and k not in needed_keys:
                continue
            if len(idx[k]) >= cap:
                continue
            idx[k].append({
                "property_id": r.get("property_id"),
                "parcel_id": r.get("parcel_id"),
                "building_group_id": r.get("building_group_id"),
                "unit": r.get("unit"),
                "full_address": r.get("full_address"),
                "address_key": r.get("address_key"),
                "address_tier": r.get("address_tier"),
            })
    return idx, scanned

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", default="postfix.no_match_base_relaxed_v1")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    # first pass: collect needed relaxed keys (keeps spine index fast)
    needed=set()
    rows=0
    candidates=0

    with open(args.infile, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): 
                continue
            rows += 1
            try:
                ev = json.loads(line)
            except:
                continue
            a = ev.get("attach") or {}
            if a.get("attach_status") != "UNKNOWN": 
                continue
            if a.get("match_method") != "no_match_base":
                continue

            town, rest = parse_match_key(a.get("match_key"))
            if not town or not rest:
                continue

            # detect "108 114 CHESTNUT STREET"
            m = re.match(r"^(\d+)\s+(\d+)\s+(.*)$", rest)
            if m:
                n1, n2, sn_raw = m.group(1), m.group(2), m.group(3)
                sn_raw = fix_leading_street_token(sn_raw)
                for sn in suffix_variants(sn_raw):
                    needed.add(mk_from_parts(town, n1, sn))
                    needed.add(mk_from_parts(town, n2, sn))
                candidates += 1
                continue

            no, sn_raw = split_number_and_street(rest)
            if not no:
                continue  # street-only -> can't resolve deterministically here
            sn_raw = fix_leading_street_token(sn_raw)
            for sn in suffix_variants(sn_raw):
                needed.add(mk_from_parts(town, no, sn))
            candidates += 1

    spine_idx, spine_scanned = build_spine_index(args.spine, needed_keys=needed, cap=12)

    rows_resolved=0
    reasons=Counter()

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            try:
                ev = json.loads(line)
            except:
                continue
            a = ev.get("attach") or {}
            if a.get("attach_status") != "UNKNOWN" or a.get("match_method") != "no_match_base":
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            town, rest = parse_match_key(a.get("match_key"))
            resolved=False
            used_key=None
            anchors=set()

            if town and rest:
                m = re.match(r"^(\d+)\s+(\d+)\s+(.*)$", rest)
                if m:
                    n1, n2, sn_raw = m.group(1), m.group(2), m.group(3)
                    sn_raw = fix_leading_street_token(sn_raw)
                    tries=[]
                    for sn in suffix_variants(sn_raw):
                        tries.append(mk_from_parts(town, n1, sn))
                        tries.append(mk_from_parts(town, n2, sn))
                else:
                    no, sn_raw = split_number_and_street(rest)
                    if no:
                        sn_raw = fix_leading_street_token(sn_raw)
                        tries=[]
                        for sn in suffix_variants(sn_raw):
                            tries.append(mk_from_parts(town, no, sn))
                    else:
                        tries=[]

                for k in tries:
                    rows2 = spine_idx.get(k, [])
                    if not rows2:
                        continue
                    anchors = set([r.get("property_id") for r in rows2 if r.get("property_id")])
                    if len(anchors)==1:
                        used_key=k
                        pid=list(anchors)[0]
                        a2 = dict(a)
                        a2["attach_status"]="ATTACHED_SITE_RELAXED"
                        a2["property_id"]=pid
                        a2["match_method"]="postfix|no_match_base_relaxed"
                        a2["match_key_used"]=used_key
                        ev["attach"]=a2
                        rows_resolved += 1
                        resolved=True
                        break

            if not resolved:
                if not town or not rest:
                    reasons["missing_match_key"] += 1
                elif not re.match(r"^\d+\s+", rest):
                    reasons["street_only_or_no_number"] += 1
                else:
                    reasons["no_unique_anchor"] += 1

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
        "engine_id": args.engine_id,
        "ran_at": nowz(),
        "infile": args.infile,
        "spine": args.spine,
        "out": args.out,
        "rows_scanned": rows,
        "candidates_considered": candidates,
        "unique_relaxed_keys": len(needed),
        "spine_rows_scanned_for_index": spine_scanned,
        "rows_resolved": rows_resolved,
        "reasons": dict(reasons),
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print(json.dumps({"done": True, "rows_scanned": rows, "rows_resolved": rows_resolved}, indent=2))

if __name__=="__main__":
    main()