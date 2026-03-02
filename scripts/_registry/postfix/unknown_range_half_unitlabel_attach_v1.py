import argparse, json, os, re, datetime

def nowz():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def norm_ws(s): return re.sub(r"\s+", " ", (s or "").strip())

def norm_addr_token(s):
    s = (s or "").upper().strip()
    s = s.replace(".", " ").replace(",", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_half(addr):
    # normalize common "half" forms to "1/2"
    a = (addr or "")
    a = a.replace("1/2", " 1/2 ")
    a = re.sub(r"\b(\d+)\.5\b", r"\1 1/2", a)
    a = re.sub(r"\b(\d+)\s+1\s*/\s*2\b", r"\1 1/2", a)
    a = re.sub(r"\s+", " ", a).strip()
    return a

RANGE_RE = re.compile(r"^\s*(\d+)\s+(\d+)\s+(.*)$")

def split_range(addr_body):
    # returns list of candidate bodies (street_no + rest) if looks like "108 114 CHESTNUT STREET"
    m = RANGE_RE.match(addr_body or "")
    if not m: return None
    a=int(m.group(1)); b=int(m.group(2)); rest=m.group(3).strip()
    if a<=0 or b<=0: return None
    # Only handle tight ranges (avoid huge spans)
    if abs(b-a) > 12: return None
    lo=min(a,b); hi=max(a,b)
    return [f"{n} {rest}" for n in range(lo,hi+1)]

def parse_unit_from_match_key(mk):
    # expects "...|UNIT|<U>"
    if not mk: return None
    parts = mk.split("|")
    for i,p in enumerate(parts):
        if p == "UNIT" and i+1 < len(parts):
            u = parts[i+1].strip()
            return u if u else None
    return None

def addr_body_from_match_key(mk):
    # mk like "BOSTON|366 W SECOND STREET|UNIT|3" OR "BOSTON|108 114 CHESTNUT STREET"
    if not mk: return None
    parts = mk.split("|")
    if len(parts) < 2: return None
    return parts[1].strip()

def build_base_index(spine_path):
    # index: TOWN|ADDR_BODY_NORM -> [spine_row,...]
    idx = {}
    rows = 0
    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            r=json.loads(line)
            rows += 1
            town = (r.get("town") or "").strip().upper()
            al = (r.get("address_label") or "")
            # Use address_key when possible, else rebuild from address_label
            ak = (r.get("address_key") or "").strip()
            body = None
            if ak:
                # A|68|KINGSBURY ST|NEEDHAM|02492 -> "68 KINGSBURY ST"
                parts = ak.split("|")
                if len(parts) >= 4:
                    body = f"{parts[1]} {parts[2]}".strip()
            if not body:
                # fallback: take everything before first comma
                body = (al.split(",")[0] if al else "").strip()
            body = norm_addr_token(normalize_half(body))
            if not town or not body: 
                continue
            key = f"{town}|{body}"
            idx.setdefault(key, []).append(r)
    return idx, rows

def spine_row_has_unit(sp_row, unit_token):
    if not unit_token: 
        return True
    u = norm_addr_token(unit_token)
    if not u: 
        return True
    al = norm_addr_token(sp_row.get("address_label") or "")
    # Accept common renderings
    # ... " 6", "#6", "UNIT 6", "APT 6", "PH", "6A", etc.
    patterns = [
        rf"\bUNIT\s*{re.escape(u)}\b",
        rf"\bAPT\s*{re.escape(u)}\b",
        rf"\b#{re.escape(u)}\b",
        rf"\b{re.escape(u)}\b$",
    ]
    for pat in patterns:
        if re.search(pat, al):
            return True
    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", default="postfix.unknown_range_half_unitlabel_attach_v1")
    args = ap.parse_args()

    base_idx, spine_rows = build_base_index(args.spine)

    stats = {
        "engine_id": args.engine_id,
        "infile": args.infile,
        "spine": args.spine,
        "out": args.out,
        "audit": args.audit,
        "started_at": nowz(),
        "spine_rows_scanned": spine_rows,
        "rows_scanned": 0,
        "rows_attached": 0,
        "rows_range_candidates": 0,
        "rows_half_normalized": 0,
        "rows_unit_filtered": 0,
        "rows_skipped_non_unknown": 0,
        "rows_no_match": 0,
        "rows_multi_match": 0,
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line: 
                continue
            r=json.loads(line)
            stats["rows_scanned"] += 1

            attach = r.get("attach") or {}
            if attach.get("attach_status") != "UNKNOWN":
                stats["rows_skipped_non_unknown"] += 1
                fout.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            mk = attach.get("match_key") or ""
            mm = attach.get("match_method") or ""
            town = (mk.split("|")[0] if "|" in mk else (r.get("town") or "")).strip().upper()
            body = addr_body_from_match_key(mk)

            unit = None
            if mm == "no_match_unit_then_base":
                unit = parse_unit_from_match_key(mk)

            if not town or not body:
                stats["rows_no_match"] += 1
                fout.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            body0 = body
            body = normalize_half(body)
            if body != body0:
                stats["rows_half_normalized"] += 1

            candidates = split_range(body)
            if candidates:
                stats["rows_range_candidates"] += 1
            else:
                candidates = [body]

            hits = []
            for cand in candidates:
                key = f"{town}|{norm_addr_token(cand)}"
                rows = base_idx.get(key) or []
                if not rows:
                    continue
                if unit:
                    # filter by address_label containing unit token
                    rows2 = [sr for sr in rows if spine_row_has_unit(sr, unit)]
                    if rows2:
                        stats["rows_unit_filtered"] += 1
                    rows = rows2
                # for deterministic attach: we only accept if exactly one spine row matches
                if len(rows) == 1:
                    hits.append(rows[0])

            # Dedup hits by property_id if multiple candidates hit same row
            if hits:
                uniq = {}
                for h in hits:
                    pid = h.get("property_id") or h.get("property_uid") or h.get("row_uid") or id(h)
                    uniq[pid] = h
                hits = list(uniq.values())

            if len(hits) == 1:
                sp = hits[0]
                r["property_id"] = sp.get("property_id") or sp.get("property_uid") or r.get("property_id")
                r["property_uid"] = sp.get("property_uid") or r.get("property_uid")
                r["attach"] = dict(attach)
                r["attach"]["attach_status"] = "ATTACHED_C"
                r["attach"]["attach_engine"] = args.engine_id
                r["attach"]["attach_at"] = nowz()
                r["attach"]["attach_method"] = "town|addr_body(+range,+half,+unitlabel)"
                r["attach"]["attach_key_used"] = f"{town}|{norm_addr_token(body)}"
                stats["rows_attached"] += 1
                fout.write(json.dumps(r, ensure_ascii=False) + "\n")
            elif len(hits) == 0:
                stats["rows_no_match"] += 1
                fout.write(json.dumps(r, ensure_ascii=False) + "\n")
            else:
                stats["rows_multi_match"] += 1
                fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    stats["finished_at"] = nowz()
    with open(args.audit, "w", encoding="utf-8") as fa:
        json.dump(stats, fa, indent=2)

    print(json.dumps({"done": True, **{k:stats[k] for k in [
        "rows_scanned","rows_attached","rows_range_candidates","rows_half_normalized",
        "rows_unit_filtered","rows_no_match","rows_multi_match"
    ]}}, indent=2))

if __name__=="__main__":
    main()