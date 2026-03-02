import json, re, os
from collections import Counter, defaultdict

IN_PATH  = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_6.ndjson"
SPINE    = r"publicData/properties/_attached/phase4_assessor_unknown_classify_v1/properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"
OUT_PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_7.ndjson"
AUDIT    = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_7.json"

DATE = re.compile(r"\b\d{2}-\d{2}-\d{4}\b")
DOCNO_TOKEN = lambda d: re.compile(rf"(?<!\d){re.escape(str(d))}(?!\d)")

TOWN_ADDR = re.compile(r"Town:\s*([A-Z][A-Z\s\.\-']+?)\s+Addr:\s*([^\n\r]+)", re.IGNORECASE)

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
    # dedupe
    out=[]
    seen=set()
    for t,a in pairs:
        k=(t,a)
        if k in seen: continue
        seen.add(k)
        out.append(k)
    return out

def segment_by_docno(raw_block: str, docno_raw: str|None):
    rb = raw_block or ""
    if not rb.strip():
        return rb, {"segmented": False, "why":"empty_raw_block", "docnos_in_rb":0, "dates_in_rb":0}
    if not docno_raw:
        return rb, {"segmented": False, "why":"no_docno_raw", "docnos_in_rb":0, "dates_in_rb":len(DATE.findall(rb))}
    docno_raw = str(docno_raw).strip()
    doc_pat = DOCNO_TOKEN(docno_raw)
    doc_hits = [m.start() for m in doc_pat.finditer(rb)]
    date_hits = [m.start() for m in DATE.finditer(rb)]
    if len(doc_hits) == 0:
        return rb, {"segmented": False, "why":"docno_not_found", "docnos_in_rb":0, "dates_in_rb":len(date_hits)}
    # if there's only 1 doc hit AND only 0/1 date, segmentation isn't needed
    if len(doc_hits)==1 and len(date_hits)<=1:
        return rb, {"segmented": False, "why":"single_docno_no_multi_date", "docnos_in_rb":1, "dates_in_rb":len(date_hits)}

    # choose first doc hit as anchor
    anchor = doc_hits[0]

    # start = nearest date before anchor, else line boundary before anchor
    start = 0
    for p in date_hits:
        if p < anchor: start = p
        else: break
    if start == 0:
        # try previous line break-ish (in these dumps we still have \n sometimes)
        # but rb was created with original newlines in document.raw_block (kept), so we can use them
        lb = rb.rfind("\n", 0, anchor)
        if lb != -1 and (anchor - lb) < 4000:
            start = lb+1

    # end = next date after anchor, else end of rb
    end = len(rb)
    for p in date_hits:
        if p > anchor:
            end = p
            break

    seg = rb[start:end]
    # safety: require that the segment contains the docno token AND at least 1 Town: Addr:
    if not doc_pat.search(seg):
        return rb, {"segmented": False, "why":"window_missing_docno", "docnos_in_rb":len(doc_hits), "dates_in_rb":len(date_hits)}
    if not TOWN_ADDR.search(seg):
        return rb, {"segmented": False, "why":"window_missing_town_addr", "docnos_in_rb":len(doc_hits), "dates_in_rb":len(date_hits)}
    return seg, {"segmented": True, "why":"docno_window_by_dates", "docnos_in_rb":len(doc_hits), "dates_in_rb":len(date_hits), "start":start, "end":end}

# ---- build spine indexes (Hampden towns only) ----
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

        docno_raw = (ev.get("recording") or {}).get("document_number_raw")
        raw_block = ((ev.get("document") or {}).get("raw_block")) or ""
        seg, seg_meta = segment_by_docno(raw_block, docno_raw)

        pairs = extract_pairs(seg)
        pr = ev.get("property_ref") or {}
        town0 = clean(pr.get("town_norm") or pr.get("town_raw"))
        addr0 = clean(pr.get("address_norm") or pr.get("address_raw"))

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

        if seg_meta.get("segmented"):
            stats["raw_block_segmented"] += 1

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
            "join_method":"unique-only deterministic (full -> suffix-alias full -> street -> suffix-alias street -> nosuf street)",
            "join_basis":"axis2_docno_segment_v1_7",
            "raw_block_segmented": bool(seg_meta.get("segmented")),
            "raw_block_segment_why": seg_meta.get("why"),
            "docnos_in_raw_block": seg_meta.get("docnos_in_rb"),
            "dates_in_raw_block": seg_meta.get("dates_in_rb"),
        }
        ev["attach"] = a

        if len(examples) < 8 and seg_meta.get("segmented"):
            examples.append({
                "event_id": ev.get("event_id"),
                "docno_raw": docno_raw,
                "seg_meta": seg_meta,
                "pairs": pairs[:6],
                "primary": (town0, addr0),
                "status": a.get("attach_status"),
            })

        out.write(json.dumps(ev, ensure_ascii=False) + "\n")

audit = {
  "in": IN_PATH, "out": OUT_PATH, "spine": SPINE,
  "stats": dict(stats),
  "spine_rows_indexed_hampden_towns": rows,
  "examples": examples
}
with open(AUDIT, "w", encoding="utf-8") as f:
    json.dump(audit, f, ensure_ascii=False, indent=2)

print("=== AXIS2 REATTACH (>=10k) v1_7 ===")
print(json.dumps({"out": OUT_PATH, "audit": AUDIT, "stats": dict(stats)}, ensure_ascii=False))
