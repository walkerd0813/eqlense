import argparse, json, re, os, hashlib, datetime
from collections import Counter

def nowz():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def up(s): return (s or "").strip().upper()

def norm_ws(s): return re.sub(r"\s+"," ",(s or "").strip())

def sha256_file(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda: f.read(1024*1024), b""):
            h.update(b)
    return h.hexdigest()

def parse_address_key(ak):
    # A|350|REVERE BEACH BLVD|REVERE|02151  (unit not in here)
    # returns (town, "350 REVERE BEACH BLVD")
    if not ak: return (None, None, None)
    parts = ak.split("|")
    if len(parts) < 5: return (None, None, None)
    if up(parts[0]) != "A": return (None, None, None)
    no = norm_ws(parts[1])
    st = norm_ws(parts[2])
    town = up(parts[3])
    z = norm_ws(parts[4])
    addr = norm_ws(f"{no} {st}") if no and st else None
    return (town, addr, z)

UNIT_TOKENS = ["UNIT","APT","#","PH","PENTHOUSE","STE","SUITE","RM","ROOM"]

def extract_unit(addr_norm):
    if not addr_norm: return None
    s = up(addr_norm)
    # try patterns: "... UNIT 3", "... APT 2D", "... # 7", "... STE 120"
    m = re.search(r"\b(UNIT|APT|STE|SUITE|RM|ROOM|PH)\b\s*([A-Z0-9\-]+)\b", s)
    if m:
        return m.group(2)
    m2 = re.search(r"\#\s*([A-Z0-9\-]+)\b", s)
    if m2:
        return m2.group(1)
    return None

def normalize_apostrophes(street):
    # ODONNELL -> O'DONNELL (common in your bucket)
    s = up(street)
    s = re.sub(r"\bODONNELL\b", "O'DONNELL", s)
    return s

def normalize_st_saint(street):
    # Only apply to known SAINT names to avoid converting ST (street type) incorrectly.
    # This is conservative on purpose.
    s = up(street)
    s = re.sub(r"\bST\s+JOSEPH\b", "SAINT JOSEPH", s)
    s = re.sub(r"\bST\s+JAMES\b", "SAINT JAMES", s)
    s = re.sub(r"\bST\s+JOHN\b", "SAINT JOHN", s)
    s = re.sub(r"\bST\s+MARY\b", "SAINT MARY", s)
    return s

def parse_match_key(mk):
    # "BOSTON|366 W SECOND STREET|UNIT|3" OR "BOSTON|23 ROSEBERY ROAD"
    if not mk: return (None,None,None)
    parts = mk.split("|")
    if len(parts) >= 4 and up(parts[2]) == "UNIT":
        town = up(parts[0])
        addr = norm_ws(parts[1])
        unit = up(parts[3])
        return (town, addr, unit)
    if len(parts) >= 2:
        return (up(parts[0]), norm_ws(parts[1]), None)
    return (None,None,None)

def split_no_street(addr):
    # expects "<no> <street...>"
    if not addr: return (None,None)
    s = norm_ws(addr)
    m = re.match(r"^([0-9]+(?:\s*[\-]\s*[0-9]+)?)\s+(.*)$", s)
    if not m: return (None, None)
    return (norm_ws(m.group(1)), norm_ws(m.group(2)))

def range_variants(no_raw):
    # "35 37" (already split might not happen) / "35-37"
    s = norm_ws(no_raw)
    if re.match(r"^[0-9]+\s+[0-9]+$", s):
        a,b = s.split()
        return [f"{a}-{b}", f"{a} {b}"]
    if re.match(r"^[0-9]+\-[0-9]+$", s):
        a,b = s.split("-")
        return [f"{a} {b}", f"{a}-{b}"]
    return [s]

def build_spine_indexes(spine_path):
    base = {}   # base_key -> property_id or "__MULTI__"
    unit = {}   # unit_key -> property_id or "__MULTI__"
    stats = Counter()
    with open(spine_path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            r = json.loads(line)
            ak = (r.get("address_key") or "").strip()
            town, addr, _zip = parse_address_key(ak)
            pid = (r.get("property_id") or "").strip()
            if not town or not addr or not pid:
                stats["spine_skip_missing_fields"] += 1
                continue
            bkey = f"{town}|{up(addr)}"
            prev = base.get(bkey)
            if prev is None:
                base[bkey] = pid
            elif prev != pid:
                base[bkey] = "__MULTI__"
            # unit if present
            u = (r.get("unit") or "").strip()
            if u:
                ukey = f"{town}|{up(addr)}|UNIT|{up(u)}"
                prevu = unit.get(ukey)
                if prevu is None:
                    unit[ukey] = pid
                elif prevu != pid:
                    unit[ukey] = "__MULTI__"
            stats["spine_rows_used"] += 1
    stats["base_keys"] = len(base)
    stats["unit_keys"] = len(unit)
    return base, unit, stats

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--engine_id", default="postfix.classify_unknown_buckets_v1")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    base_index, unit_index, spine_stats = build_spine_indexes(args.spine)

    bucket_paths = {}
    def w(bucket, ev):
        if bucket not in bucket_paths:
            bucket_paths[bucket] = os.path.join(args.outdir, f"bucket__{bucket}.ndjson")
        with open(bucket_paths[bucket], "a", encoding="utf-8") as fo:
            fo.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
        "engine_id": args.engine_id,
        "infile": args.infile,
        "spine": args.spine,
        "outdir": args.outdir,
        "started_at": nowz(),
        "sha256_infile": sha256_file(args.infile),
        "sha256_spine": sha256_file(args.spine),
        "spine_stats": dict(spine_stats),
        "rows_scanned": 0,
        "rows_unknown": 0,
        "bucket_counts": {},
        "detail_counts": {},
    }

    def bump(d, k):
        d[k] = d.get(k, 0) + 1

    def classify(ev):
        mk = (ev.get("match_key") or (ev.get("attach") or {}).get("match_key") or "").strip()
        town, addr, unit_from_mk = parse_match_key(mk)

        # fall back to property_ref
        pr = ev.get("property_ref") or {}
        if not town:
            town = up(pr.get("town_code") or pr.get("town") or ev.get("town"))
        addr_norm = pr.get("address_norm") or pr.get("address_raw") or ev.get("full_address") or ""
        addr_norm = norm_ws(addr_norm)

        # if mk gave addr, prefer it (it is already normalized upstream)
        if addr:
            addr_norm = addr

        if not town or not addr_norm:
            return ("UNSALVAGEABLE_MISSING_TOWN_OR_ADDR", {"reason":"missing_town_or_addr"})

        # split address into no + street
        no_raw, street_raw = split_no_street(addr_norm)
        if not no_raw or not street_raw:
            return ("UNSALVAGEABLE_BAD_ADDR_PARSE", {"reason":"addr_parse_fail", "addr_norm":addr_norm})

        unit_val = unit_from_mk or extract_unit(addr_norm)

        # normalize street for controlled heuristics
        street_u = up(street_raw)
        street_ap = normalize_apostrophes(street_u)
        street_sa = normalize_st_saint(street_u)

        # base key candidates
        base_candidates = []
        for nvar in range_variants(no_raw):
            base_candidates.append((f"{town}|{up(norm_ws(nvar+' '+street_u))}", "base_raw"))
            if street_ap != street_u:
                base_candidates.append((f"{town}|{up(norm_ws(nvar+' '+street_ap))}", "apostrophe_fix"))
            if street_sa != street_u:
                base_candidates.append((f"{town}|{up(norm_ws(nvar+' '+street_sa))}", "saint_fix"))

        # 1) unit exact
        if unit_val:
            for bkey, tag in base_candidates:
                ukey = f"{bkey}|UNIT|{up(unit_val)}"
                pid = unit_index.get(ukey)
                if pid and pid != "__MULTI__":
                    return ("SALVAGE_UNIT_EXACT", {"candidate_key":ukey, "variant":tag, "property_id":pid})

        # 2) base ignore unit (building match)
        for bkey, tag in base_candidates:
            pid = base_index.get(bkey)
            if pid and pid != "__MULTI__":
                # decide which bucket based on tag / range transform
                if tag == "base_raw":
                    # if no_raw was a range-like and we used a variant, bucket range
                    if re.search(r"\s", no_raw) or "-" in no_raw:
                        return ("SALVAGE_RANGE_NORMALIZE", {"candidate_key":bkey, "variant":tag, "property_id":pid})
                    return ("SALVAGE_BASE_IGNORE_UNIT", {"candidate_key":bkey, "variant":tag, "property_id":pid})
                if tag == "apostrophe_fix":
                    return ("SALVAGE_APOSTROPHE_FIX", {"candidate_key":bkey, "variant":tag, "property_id":pid})
                if tag == "saint_fix":
                    return ("SALVAGE_ST_SAINT_FIX", {"candidate_key":bkey, "variant":tag, "property_id":pid})

        # 3) collision?
        for bkey, tag in base_candidates:
            if base_index.get(bkey) == "__MULTI__":
                return ("UNSALVAGEABLE_COLLISION", {"candidate_key":bkey, "variant":tag, "reason":"multi_property_match"})

        return ("UNSALVAGEABLE_NOT_IN_SPINE", {"town":town, "addr_norm":addr_norm})

    with open(args.infile, "r", encoding="utf-8") as fin:
        for line in fin:
            line=line.strip()
            if not line: continue
            audit["rows_scanned"] += 1
            ev = json.loads(line)

            st = ((ev.get("attach") or {}).get("attach_status") or ev.get("attach_status") or "")
            if up(st) != "UNKNOWN":
                continue

            audit["rows_unknown"] += 1
            bucket, meta = classify(ev)
            bump(audit["bucket_counts"], bucket)
            bump(audit["detail_counts"], meta.get("reason") or bucket)

            ev2 = dict(ev)
            ev2["_unknown_bucket"] = bucket
            ev2["_unknown_bucket_meta"] = meta
            w(bucket, ev2)

    audit["finished_at"] = nowz()
    audit_path = os.path.join(args.outdir, "audit__unknown_bucket_classification_v1.json")
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2, ensure_ascii=False)

    print(json.dumps({
        "done": True,
        "rows_scanned": audit["rows_scanned"],
        "rows_unknown": audit["rows_unknown"],
        "outdir": args.outdir,
        "audit": audit_path,
        "top_buckets": sorted(audit["bucket_counts"].items(), key=lambda x:-x[1])[:12]
    }, indent=2))

if __name__ == "__main__":
    main()
