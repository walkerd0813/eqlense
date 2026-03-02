import json, re, os
from collections import Counter, defaultdict

IN_PATH  = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_5.ndjson"
SPINE    = r"publicData/properties/_attached/phase4_assessor_unknown_classify_v1/properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"
OUT_PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_6.ndjson"
AUDIT    = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_6.json"

# Hampden header pattern: "MM-DD-YYYY  9:57:38a  26120  359  56029"
HDR = re.compile(r"\b(\d{2}-\d{2}-\d{4})\s+\d{1,2}:\d{2}:\d{2}[ap]\s+\d+\s+\d+\s+(\d+)\b")

TOWN_ADDR = re.compile(r"Town:\s*([A-Z][A-Z\s\.\-']+?)\s+Addr:\s*([^\n\r]+)", re.IGNORECASE)

# ---- address helpers ----
SUFFIX_CANON = {
  # boulevard
  "BL":"BLVD","BLVD":"BLVD","BOULEVARD":"BLVD",
  # lane
  "LA":"LN","LN":"LN","LANE":"LN",
  # avenue
  "AV":"AVE","AVE":"AVE","AVENUE":"AVE",
  # road
  "RD":"RD","ROAD":"RD",
  # street
  "ST":"ST","STREET":"ST",
  # circle
  "CI":"CIR","CIR":"CIR","CIRCLE":"CIR",
  # terrace
  "TER":"TERR","TERR":"TERR","TERRACE":"TERR",
  # drive
  "DR":"DR","DRIVE":"DR",
  # parkway
  "PKY":"PKY","PKWY":"PKY","PARKWAY":"PKY",
  # court
  "CT":"CT","COURT":"CT",
  # place
  "PL":"PL","PLACE":"PL",
  # way
  "WAY":"WAY",
  # highway
  "HWY":"HWY",
  # extension (keep as-is)
  "EXTN":"EXTN","EXT":"EXTN",
}

SUFFIX_SET = set(SUFFIX_CANON.keys())

UNIT_WORDS = {"UNIT","APT","APARTMENT","#","STE","SUITE","FL","FLOOR"}

def clean_addr_text(s: str) -> str:
    s = (s or "").upper()
    s = s.replace("\t"," ").replace("\r"," ").replace("\n"," ")
    s = re.sub(r"\s+", " ", s).strip()
    # strip trailing Y tokens used in PDF table dumps
    s = re.sub(r"\s+Y$", "", s).strip()
    return s

def split_unit(addr: str):
    # returns (base, unit_str_or_None)
    toks = addr.split()
    if not toks:
        return addr, None
    for i, t in enumerate(toks):
        if t in UNIT_WORDS:
            base = " ".join(toks[:i]).strip()
            unit = " ".join(toks[i:]).strip()
            return base, unit or None
    return addr, None

def parse_street(addr: str):
    addr = clean_addr_text(addr)
    base, unit = split_unit(addr)
    m = re.match(r"^(\d+)\s+(.*)$", base)
    if not m:
        return None
    no = m.group(1)
    name = m.group(2).strip()
    name = re.sub(r"\s+", " ", name)
    return {"street_no": no, "street_name": name, "unit": unit}

def suffix_alias(street_name: str) -> str:
    toks = street_name.split()
    if not toks:
        return street_name
    last = toks[-1]
    canon = SUFFIX_CANON.get(last, last)
    if canon != last:
        toks[-1] = canon
    return " ".join(toks)

def drop_suffix(street_name: str) -> str:
    toks = street_name.split()
    if len(toks) >= 2 and toks[-1] in SUFFIX_SET:
        return " ".join(toks[:-1])
    return street_name

def norm_full(no: str, street_name: str, unit: str|None):
    s = f"{no} {street_name}".strip()
    if unit:
        # do NOT include unit in matching for now; keep deterministic and conservative
        pass
    return clean_addr_text(s)

# ---- raw_block segmentation ----
def segment_raw_block(raw_block: str, docno_raw: str|None):
    rb = raw_block or ""
    hits = [(m.start(), m.group(2)) for m in HDR.finditer(rb)]
    if not hits:
        return rb, {"segmented": False, "why": "no_headers"}
    if not docno_raw:
        return rb, {"segmented": False, "why": "no_docno_raw"}

    docno_raw = str(docno_raw).strip()
    idxs = [i for i,(pos,doc) in enumerate(hits) if doc == docno_raw]
    if not idxs:
        return rb, {"segmented": False, "why": "docno_not_found_in_raw_block"}

    i = idxs[0]  # choose first occurrence
    start = hits[i][0]
    end = hits[i+1][0] if i+1 < len(hits) else len(rb)
    seg = rb[start:end]
    # sanity: keep it non-empty
    if seg.strip():
        return seg, {"segmented": True, "why": "docno_window", "headers_in_rb": len(hits), "docno_matches": len(idxs)}
    return rb, {"segmented": False, "why": "empty_segment"}

def extract_town_addr_pairs(raw_block_segment: str):
    pairs = []
    for m in TOWN_ADDR.finditer(raw_block_segment or ""):
        town = clean_addr_text(m.group(1))
        addr = clean_addr_text(m.group(2))
        # remove trailing table junk after address (common huge spacing); keep left side
        addr = re.split(r"\s{2,}", addr)[0].strip()
        addr = re.sub(r"\s+Y$", "", addr).strip()
        if town and addr:
            pairs.append((town, addr))
    return pairs

# ---- build spine indexes (Hampden towns only) ----
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

# indexes: { town|full : [pid...] }, { town|no|street : [pid...] }
full_idx = defaultdict(list)
street_idx = defaultdict(list)
street_idx_alias = defaultdict(list)
street_idx_nosuf = defaultdict(list)

rows = 0
for pr in it_ndjson(SPINE):
    town = clean_addr_text(pr.get("town"))
    if town not in HAMPDEN_TOWNS:
        continue
    no = clean_addr_text(pr.get("street_no"))
    st = clean_addr_text(pr.get("street_name"))
    pid = pr.get("property_id")
    if not pid or not no or not st:
        continue

    rows += 1
    full = norm_full(no, st, None)
    full_idx[f"{town}|{full}"].append(pid)

    st_alias = suffix_alias(st)
    st_nosuf = drop_suffix(st)

    street_idx[f"{town}|{no}|{st}"].append(pid)
    street_idx_alias[f"{town}|{no}|{st_alias}"].append(pid)
    street_idx_nosuf[f"{town}|{no}|{st_nosuf}"].append(pid)

# ---- deterministic unique-only matcher ----
def unique_pick(arr):
    if not arr:
        return None, "no_match"
    u = list(dict.fromkeys(arr))  # stable unique
    if len(u) == 1:
        return u[0], "unique"
    return None, "collision"

def match_one(town, addr_norm):
    parsed = parse_street(addr_norm)
    if not parsed:
        return None, "no_num"

    no = parsed["street_no"]
    st = clean_addr_text(parsed["street_name"])
    full = norm_full(no, st, None)

    # 1) full exact
    pid, why = unique_pick(full_idx.get(f"{town}|{full}", []))
    if pid:
        return pid, "axis2_full_address_exact"
    if why == "collision":
        return None, "collision_full"

    # 2) full suffix-alias
    st2 = suffix_alias(st)
    full2 = norm_full(no, st2, None)
    pid, why = unique_pick(full_idx.get(f"{town}|{full2}", []))
    if pid:
        return pid, "axis2_full_address_suffix_alias"
    if why == "collision":
        return None, "collision_full"

    # 3) street exact unique
    pid, why = unique_pick(street_idx.get(f"{town}|{no}|{st}", []))
    if pid:
        return pid, "axis2_street_unique_exact"
    if why == "collision":
        return None, "collision_street"

    # 4) street suffix-alias unique
    pid, why = unique_pick(street_idx_alias.get(f"{town}|{no}|{st2}", []))
    if pid:
        return pid, "axis2_street_unique_suffix_alias"
    if why == "collision":
        return None, "collision_street"

    # 5) street no-suffix unique (only if suffix exists)
    st3 = drop_suffix(st2)
    pid, why = unique_pick(street_idx_nosuf.get(f"{town}|{no}|{st3}", []))
    if pid:
        return pid, "axis2_street_unique_nosuf"
    if why == "collision":
        return None, "collision_street"

    return None, "no_match"

# ---- main pass ----
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

        # segment raw_block to stop cross-row contamination
        docno_raw = (ev.get("recording") or {}).get("document_number_raw")
        raw_block = ((ev.get("document") or {}).get("raw_block")) or ""
        seg, seg_meta = segment_raw_block(raw_block, docno_raw)

        # rebuild multi_address ONLY from segmented block
        pairs = extract_town_addr_pairs(seg)
        pr = ev.get("property_ref") or {}
        town0 = clean_addr_text(pr.get("town_norm") or pr.get("town_raw"))
        addr0 = clean_addr_text(pr.get("address_norm") or pr.get("address_raw"))

        # dedupe & clean pairs
        seen = set()
        cleaned_pairs = []
        for t, addr in pairs:
            addr = clean_addr_text(addr)
            # keep "Addr:" left side only (strip double-space columns)
            addr = re.split(r"\s{2,}", addr)[0].strip()
            if not t or not addr:
                continue
            k = (t, addr)
            if k in seen: 
                continue
            seen.add(k)
            cleaned_pairs.append(k)

        # set primary_is_multi + multi_address list (excluding primary if present)
        primary_is_multi = False
        multi_list = []
        if cleaned_pairs:
            primary_is_multi = (len(cleaned_pairs) > 1)
            for (t, addr) in cleaned_pairs:
                if t == town0 and addr == addr0:
                    continue
                multi_list.append({
                    "town_raw": t,
                    "address_raw": addr,
                    "town_norm": t,
                    "address_norm": addr
                })

        # overwrite property_ref.multi_address safely
        pr["primary_is_multi"] = bool(primary_is_multi)
        pr["multi_address"] = multi_list
        ev["property_ref"] = pr

        # now attach deterministic unique-only
        attach_scope = "MULTI" if pr.get("primary_is_multi") else "SINGLE"
        attachments = []

        # attach primary
        if town0 and addr0:
            pid, method = match_one(town0, addr0)
            if pid:
                attachments.append({
                    "town_norm": town0, "address_norm": addr0,
                    "attach_status": "ATTACHED_A",
                    "property_id": pid,
                    "match_method": method,
                    "match_key": f"{town0}|{addr0}"
                })
            else:
                attachments.append({
                    "town_norm": town0, "address_norm": addr0,
                    "attach_status": "UNKNOWN",
                    "property_id": None,
                    "match_method": method,
                    "match_key": f"{town0}|{addr0}",
                    "why": "no_match" if method=="no_match" else method
                })

        # attach multi addresses
        for m in pr.get("multi_address") or []:
            t = clean_addr_text(m.get("town_norm") or m.get("town_raw"))
            ad = clean_addr_text(m.get("address_norm") or m.get("address_raw"))
            if not t or not ad:
                continue
            pid, method = match_one(t, ad)
            if pid:
                attachments.append({
                    "town_norm": t, "address_norm": ad,
                    "attach_status": "ATTACHED_A",
                    "property_id": pid,
                    "match_method": method,
                    "match_key": f"{t}|{ad}"
                })
            else:
                attachments.append({
                    "town_norm": t, "address_norm": ad,
                    "attach_status": "UNKNOWN",
                    "property_id": None,
                    "match_method": method,
                    "match_key": f"{t}|{ad}",
                    "why": "no_match" if method=="no_match" else method
                })

        # compute event attach status
        attached_any = any(x.get("attach_status") == "ATTACHED_A" for x in attachments)
        unknown_any  = any(x.get("attach_status") == "UNKNOWN" for x in attachments)

        if attach_scope == "SINGLE":
            if attached_any and not unknown_any:
                ev_status = "ATTACHED_A"
                # for SINGLE we store property_id directly if attached
                pid = next(x["property_id"] for x in attachments if x.get("attach_status")=="ATTACHED_A")
                a["property_id"] = pid
                a["match_method"] = next(x["match_method"] for x in attachments if x.get("attach_status")=="ATTACHED_A")
                a["match_key"] = f"{town0}|{addr0}"
                a["attachments"] = []
                stats["single_upgraded_to_attached"] += 1
            else:
                ev_status = "UNKNOWN"
                a["property_id"] = None
                a["match_method"] = "no_match"
                a["match_key"] = f"{town0}|{addr0}"
                a["attachments"] = []
                # categorize why primary failed
                primary_reason = attachments[0].get("match_method") if attachments else "no_match"
                if primary_reason in ("collision_full","collision_street"):
                    stats["single_still_unknown__collision"] += 1
                elif primary_reason == "no_num":
                    stats["single_still_unknown__no_num"] += 1
                else:
                    stats["single_still_unknown__no_match"] += 1

                if seg_meta.get("segmented"):
                    stats["segmented_primary_unknown"] += 1

        else:
            # MULTI: preserve attachments array
            if attached_any and unknown_any:
                ev_status = "PARTIAL_MULTI"
                stats["multi_partial"] += 1
            elif attached_any and not unknown_any:
                ev_status = "ATTACHED_A"
                stats["multi_all_attached"] += 1
            else:
                ev_status = "UNKNOWN"
                stats["multi_all_unknown"] += 1

            a["property_id"] = None
            a["match_method"] = None
            a["match_key"] = None
            a["attachments"] = attachments

        a["attach_scope"] = attach_scope
        a["attach_status"] = ev_status
        a["evidence"] = {
            "join_method": "unique-only: town+full_address -> town+street_no+street_name (suffix-alias, nosuf fallbacks)",
            "join_basis": "axis2_rawblock_segment_v1_6 + deterministic_components",
            "spine_version": None,
            "spine_dataset_hash": None,
            "events_version": "DEED_ONLY_v1",
            "events_dataset_hash": None,
            "raw_block_segmented": bool(seg_meta.get("segmented")),
            "raw_block_segment_why": seg_meta.get("why"),
            "raw_block_headers_in_rb": seg_meta.get("headers_in_rb"),
        }
        ev["attach"] = a

        # count multi-doc rows (for sanity)
        if seg_meta.get("segmented") and seg_meta.get("headers_in_rb",0) and seg_meta.get("headers_in_rb") >= 2:
            stats["rows_with_multi_doc_headers_in_rb_seen"] += 1

        # keep a few examples
        if len(examples) < 8 and seg_meta.get("segmented"):
            examples.append({
                "event_id": ev.get("event_id"),
                "docno_raw": docno_raw,
                "attach_scope": attach_scope,
                "attach_status": ev_status,
                "raw_block_segmented": True,
                "extracted_pairs": cleaned_pairs[:6],
                "primary": (town0, addr0),
            })

        out.write(json.dumps(ev, ensure_ascii=False) + "\n")

audit = {
    "in": IN_PATH,
    "out": OUT_PATH,
    "spine": SPINE,
    "stats": dict(stats),
    "spine_rows_indexed_hampden_towns": rows,
    "examples": examples,
}

with open(AUDIT, "w", encoding="utf-8") as f:
    json.dump(audit, f, ensure_ascii=False, indent=2)

print("=== AXIS2 REATTACH (>=10k) v1_6 ===")
print(json.dumps({"out": OUT_PATH, "audit": AUDIT, "stats": dict(stats)}, ensure_ascii=False))
