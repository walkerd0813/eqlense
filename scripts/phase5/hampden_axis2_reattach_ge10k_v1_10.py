import json, re, os
from collections import Counter, defaultdict

IN_PATH  = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_7.ndjson"
SPINE    = r"publicData/properties/_attached/phase4_assessor_unknown_classify_v1/properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"
OUT_PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_8.ndjson"
AUDIT    = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_8.json"

TOWN_ADDR = re.compile(r"Town:\s*([A-Z][A-Z\s\.\-']+?)\s+Addr:\s*([^\n\r]+)", re.IGNORECASE)
DASH_ANY = re.compile(r"-{40,}")

SUFFIX_CANON = {
  "BL":"BLVD","BLVD":"BLVD","BOULEVARD":"BLVD",
  "LA":"LN","LN":"LN","LANE":"LN",
  "AV":"AVE","AVE":"AVE","AVENUE":"AVE",
  "RD":"RD","ROAD":"RD",
  "ST":"ST","STREET":"ST",
  "CI":"CIR","CIR":"CIR","CIRCLE":"CIR",
  "TER":"TERR","TERR":"TERR","TERRACE":"TERR",
  "DR":"DR","DRIVE":"DR",
  "PKY":"PKY","PKWY":"PKY","PARKWAY":"PKY",
  "CT":"CT","COURT":"CT",
  "PL":"PL","PLACE":"PL",
  "WAY":"WAY",
  "HWY":"HWY",
  "EXTN":"EXTN","EXT":"EXTN",
}
SUFFIX_SET = set(SUFFIX_CANON.keys())
UNIT_WORDS = {"UNIT","APT","APARTMENT","#","STE","SUITE","FL","FLOOR"}

HAMPDEN_TOWNS = {
 "AGAWAM","WILBRAHAM","CHICOPEE","HAMPDEN","EAST LONGMEADOW","LUDLOW","WESTFIELD",
 "HOLYOKE","SPRINGFIELD","PALMER","RUSSELL","WEST SPRINGFIELD","LONGMEADOW"
}

def it_ndjson(p):
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if line:
                yield json.loads(line)

def clean(s: str) -> str:
    s = (s or "").upper()
    s = s.replace("\t"," ").replace("\r"," ").replace("\n"," ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s+Y$", "", s).strip()
    return s

def split_unit(addr: str):
    toks = addr.split()
    for i,t in enumerate(toks):
        if t in UNIT_WORDS:
            return " ".join(toks[:i]).strip(), " ".join(toks[i:]).strip() or None
    return addr, None

def parse_street(addr: str):
    addr = clean(addr)
    base, unit = split_unit(addr)
    m = re.match(r"^(\d+)\s+(.*)$", base)
    if not m:
        return None
    return {"street_no": m.group(1), "street_name": re.sub(r"\s+"," ",m.group(2)).strip(), "unit": unit}

def suffix_alias(street_name: str) -> str:
    toks = street_name.split()
    if not toks: return street_name
    last = toks[-1]
    toks[-1] = SUFFIX_CANON.get(last, last)
    return " ".join(toks)

def drop_suffix(street_name: str) -> str:
    toks = street_name.split()
    if len(toks)>=2 and toks[-1] in SUFFIX_SET:
        return " ".join(toks[:-1])
    return street_name

def norm_full(no: str, st: str):
    return clean(f"{no} {st}")

def extract_pairs(raw_block: str):
    pairs=[]
    for m in TOWN_ADDR.finditer(raw_block or ""):
        town = clean(m.group(1))
        addr = clean(m.group(2))
        addr = re.split(r"\s{2,}", addr)[0].strip()
        addr = re.sub(r"\s+Y$", "", addr).strip()
        if town and addr:
            pairs.append((town, addr))
    out=[]
    seen=set()
    for t,a in pairs:
        k=(t,a)
        if k in seen: continue
        seen.add(k)
        out.append(k)
    return out

def segment_by_dash(raw_block: str, town0: str, addr0: str, docno_raw: str|None):
    rb = raw_block or ""
    if not rb.strip():
        return rb, {"segmented": False, "why":"empty_raw_block", "chunks":0, "match_chunks":0}
    # If no delimiter, don't pretend.
    if not DASH_ANY.search(rb):
        return rb, {"segmented": False, "why":"no_dash_delim", "chunks":1, "match_chunks":0}

    chunks = [c for c in DASH_ANY.split(rb) if c.strip()]
    match = []

    # strongest: Town+Addr evidence    town_re = re.compile(rf"Town:\s*{re.escape(town0)}\b", re.IGNORECASE) if town0 else None
    addr_re  = re.compile(rf"Addr:\s*{re.escape(addr0)}\b", re.IGNORECASE) if addr0 else None
    town_re = re.compile(rf"Town:\s*{re.escape(town0)}\b", re.IGNORECASE) if town0 else None
    addr_re = re.compile(rf"Addr:\s*{re.escape(addr0)}\b", re.IGNORECASE) if addr0 else None
    docno_re = re.compile(rf"\b{re.escape(docno_raw)}\b") if docno_raw else None

    for c in chunks:
        if town_re and addr_re and town_re.search(c) and addr_re.search(c):
            match.append(("town+addr", c))

    # fallback: docno token evidence
    if not match and docno_raw:
        d = str(docno_raw).strip()
        if d:
            doc_pat = re.compile(rf"(?<!\d){re.escape(d)}(?!\d)")
    town_re = re.compile(rf"Town:\s*{re.escape(town0)}\b", re.IGNORECASE) if town0 else None
    addr_re = re.compile(rf"Addr:\s*{re.escape(addr0)}\b", re.IGNORECASE) if addr0 else None
    docno_re = re.compile(rf"\b{re.escape(docno_raw)}\b") if docno_raw else None

    for c in chunks:
                if doc_pat.search(c):
                    match.append(("docno", c))

    if len(match) == 1:
        return match[0][1], {"segmented": True, "why":match[0][0], "chunks":len(chunks), "match_chunks":1}
    if len(match) > 1:
        return rb, {"segmented": False, "why":"ambiguous_multi_match", "chunks":len(chunks), "match_chunks":len(match)}
    return rb, {"segmented": False, "why":"no_chunk_match", "chunks":len(chunks), "match_chunks":0}

# ---- build spine indexes ----
full_idx = defaultdict(list)
street_idx = defaultdict(list)
street_idx_alias = defaultdict(list)
street_idx_nosuf = defaultdict(list)

rows = 0
for pr in it_ndjson(SPINE):
    town = clean(pr.get("town"))
    if town not in HAMPDEN_TOWNS:
        continue
    no = clean(pr.get("street_no"))
    st = clean(pr.get("street_name"))
    pid = pr.get("property_id")
    if not pid or not no or not st:
        continue
    rows += 1
    full_idx[f"{town}|{norm_full(no, st)}"].append(pid)
    st2 = suffix_alias(st)
    st3 = drop_suffix(st2)
    street_idx[f"{town}|{no}|{st}"].append(pid)
    street_idx_alias[f"{town}|{no}|{st2}"].append(pid)
    street_idx_nosuf[f"{town}|{no}|{st3}"].append(pid)

def unique_pick(arr):
    if not arr: return None, "no_match"
    u = list(dict.fromkeys(arr))
    if len(u)==1: return u[0], "unique"
    return None, "collision"

def match_one(town, addr_norm):
    parsed = parse_street(addr_norm)
    if not parsed:
        return None, "no_num"
    no = parsed["street_no"]
    st = clean(parsed["street_name"])
    full = norm_full(no, st)
    pid, why = unique_pick(full_idx.get(f"{town}|{full}", []))
    if pid: return pid, "axis2_full_address_exact"
    if why=="collision": return None, "collision_full"

    st2 = suffix_alias(st)
    full2 = norm_full(no, st2)
    pid, why = unique_pick(full_idx.get(f"{town}|{full2}", []))
    if pid: return pid, "axis2_full_address_suffix_alias"
    if why=="collision": return None, "collision_full"

    pid, why = unique_pick(street_idx.get(f"{town}|{no}|{st}", []))
    if pid: return pid, "axis2_street_unique_exact"
    if why=="collision": return None, "collision_street"

    pid, why = unique_pick(street_idx_alias.get(f"{town}|{no}|{st2}", []))
    if pid: return pid, "axis2_street_unique_suffix_alias"
    if why=="collision": return None, "collision_street"

    st3 = drop_suffix(st2)
    pid, why = unique_pick(street_idx_nosuf.get(f"{town}|{no}|{st3}", []))
    if pid: return pid, "axis2_street_unique_nosuf"
    if why=="collision": return None, "collision_street"

    return None, "no_match"

stats = Counter()
examples = []

os.makedirs(os.path.dirname(AUDIT), exist_ok=True)

with open(OUT_PATH, "w", encoding="utf-8") as out:
    for ev in it_ndjson(IN_PATH):
        a = ev.get("attach") or {}
        if (a.get("attach_status") or "").upper() != "UNKNOWN":
            stats["already_attached_or_not_unknown"] += 1
            out.write(json.dumps(ev, ensure_ascii=False) + "\n")
            continue

        pr = ev.get("property_ref") or {}
        town0 = clean(pr.get("town_norm") or pr.get("town_raw"))
        addr0 = clean(pr.get("address_norm") or pr.get("address_raw"))
        docno_raw = (ev.get("recording") or {}).get("document_number_raw")
        raw_block = ((ev.get("document") or {}).get("raw_block")) or ""

        seg, seg_meta = segment_by_dash(raw_block, town0, addr0, docno_raw)
        if seg_meta.get("segmented"):
            stats["raw_block_segmented"] += 1
        stats[f"seg_reason__{seg_meta.get('why')}"] += 1

        pairs = extract_pairs(seg)

        primary_is_multi = (len(pairs) > 1)
        multi_list = []
        if pairs:
            for (t, addr) in pairs:
                if t == town0 and addr == addr0:
                    continue
                multi_list.append({"town_raw": t, "address_raw": addr, "town_norm": t, "address_norm": addr})

        pr["primary_is_multi"] = bool(primary_is_multi)
        pr["multi_address"] = multi_list
        ev["property_ref"] = pr

        attach_scope = "MULTI" if primary_is_multi else "SINGLE"
        attachments = []

        if town0 and addr0:
            pid, method = match_one(town0, addr0)
            if pid:
                attachments.append({"town_norm":town0,"address_norm":addr0,"attach_status":"ATTACHED_A","property_id":pid,"match_method":method,"match_key":f"{town0}|{addr0}"})
            else:
                attachments.append({"town_norm":town0,"address_norm":addr0,"attach_status":"UNKNOWN","property_id":None,"match_method":method,"match_key":f"{town0}|{addr0}","why":method})

        for m in pr.get("multi_address") or []:
            t = clean(m.get("town_norm") or m.get("town_raw"))
            ad = clean(m.get("address_norm") or m.get("address_raw"))
            if not t or not ad:
                continue
            pid, method = match_one(t, ad)
            if pid:
                attachments.append({"town_norm":t,"address_norm":ad,"attach_status":"ATTACHED_A","property_id":pid,"match_method":method,"match_key":f"{t}|{ad}"})
            else:
                attachments.append({"town_norm":t,"address_norm":ad,"attach_status":"UNKNOWN","property_id":None,"match_method":method,"match_key":f"{t}|{ad}","why":method})

        attached_any = any(x.get("attach_status")=="ATTACHED_A" for x in attachments)
        unknown_any  = any(x.get("attach_status")=="UNKNOWN" for x in attachments)

        if attach_scope == "SINGLE":
            if attached_any and not unknown_any:
                a["attach_status"] = "ATTACHED_A"
                a["property_id"] = next(x["property_id"] for x in attachments if x["attach_status"]=="ATTACHED_A")
                a["match_method"] = next(x["match_method"] for x in attachments if x["attach_status"]=="ATTACHED_A")
                a["match_key"] = f"{town0}|{addr0}"
                a["attachments"] = []
                stats["single_upgraded_to_attached"] += 1
            else:
                a["attach_status"] = "UNKNOWN"
                a["property_id"] = None
                a["match_method"] = attachments[0].get("match_method") if attachments else "no_match"
                a["match_key"] = f"{town0}|{addr0}"
                a["attachments"] = []
                reason = a["match_method"]
                if reason in ("collision_full","collision_street"): stats["single_still_unknown__collision"] += 1
                elif reason == "no_num": stats["single_still_unknown__no_num"] += 1
                else: stats["single_still_unknown__no_match"] += 1
        else:
            if attached_any and unknown_any:
                a["attach_status"] = "PARTIAL_MULTI"; stats["multi_partial"] += 1
            elif attached_any and not unknown_any:
                a["attach_status"] = "ATTACHED_A"; stats["multi_all_attached"] += 1
            else:
                a["attach_status"] = "UNKNOWN"; stats["multi_all_unknown"] += 1
            a["property_id"] = None
            a["match_method"] = None
            a["match_key"] = None
            a["attachments"] = attachments

        a["attach_scope"] = attach_scope
        a["evidence"] = {
            "join_basis":"axis2_dash_segment_v1_8",
            "raw_block_segmented": bool(seg_meta.get("segmented")),
            "raw_block_segment_why": seg_meta.get("why"),
            "raw_block_chunks": seg_meta.get("chunks"),
            "raw_block_match_chunks": seg_meta.get("match_chunks"),
        }
        ev["attach"] = a

        if len(examples) < 8 and seg_meta.get("segmented"):
            examples.append({"event_id": ev.get("event_id"), "why": seg_meta.get("why"), "primary": (town0, addr0), "pairs": pairs[:6], "status": a.get("attach_status")})

        out.write(json.dumps(ev, ensure_ascii=False) + "\n")

audit = {"in": IN_PATH, "out": OUT_PATH, "spine": SPINE, "stats": dict(stats), "spine_rows_indexed_hampden_towns": rows, "examples": examples}
with open(AUDIT, "w", encoding="utf-8") as f:
    json.dump(audit, f, ensure_ascii=False, indent=2)

print("=== AXIS2 REATTACH (>=10k) v1_8 ===")
print(json.dumps({"out": OUT_PATH, "audit": AUDIT, "stats": dict(stats)}, ensure_ascii=False))


