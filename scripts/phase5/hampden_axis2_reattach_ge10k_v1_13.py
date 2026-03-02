import json, re, os
from collections import defaultdict, Counter

# -----------------------------
# CONFIG
# -----------------------------
IN_PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_12.ndjson"
OUT_PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_13.ndjson"
AUDIT_PATH = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_13.json"

# Property spine CURRENT pointer (Phase 4 canonical)
SPINE_CURRENT_JSON = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"

# -----------------------------
# HELPERS
# -----------------------------
def it_ndjson(p):
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def write_ndjson(p, rows):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def read_current_spine_path():
    with open(SPINE_CURRENT_JSON, "r", encoding="utf-8") as f:
        obj = json.load(f)
    # expected {"path": "..."} or {"ndjson": "..."} style; be tolerant
    for k in ("path","ndjson","file","in","src"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v
    raise RuntimeError(f"Could not find spine path inside {SPINE_CURRENT_JSON}")

_ws = re.compile(r"\s+")
def norm_ws(s: str) -> str:
    return _ws.sub(" ", (s or "").strip())

def clean_addr_noise(s: str) -> str:
    s = (s or "")
    s = s.replace("\u00a0", " ")
    s = norm_ws(s)
    # strip the stray " Y" artifacts we keep seeing
    if s.endswith(" Y"):
        s = s[:-2].rstrip()
    return s

# Canonicalize suffix tokens in a *defensible* way (no fuzzy / no nearest)
SUFFIX_ALIAS = {
    "STREET":"ST", "ST":"ST",
    "ROAD":"RD", "RD":"RD",
    "AVENUE":"AVE", "AV":"AVE", "AVE":"AVE",
    "BOULEVARD":"BLVD", "BLVD":"BLVD", "BL":"BLVD",
    "CIRCLE":"CIR", "CIR":"CIR", "CI":"CIR",
    "TERRACE":"TERR", "TERR":"TERR", "TER":"TERR",
    "DRIVE":"DR", "DR":"DR",
    "WAY":"WAY",
    "LANE":"LN", "LN":"LN",
    "COURT":"CT", "CT":"CT",
    "PLACE":"PL", "PL":"PL",
    "PARKWAY":"PKWY", "PKWY":"PKWY",
    "HIGHWAY":"HWY", "HWY":"HWY",
    "EXT":"EXTN", "EXTN":"EXTN",
}

def canon_street_name(street_name: str) -> str:
    s = clean_addr_noise(street_name).upper()
    s = re.sub(r"[^\w\s\-]", "", s)
    toks = [t for t in s.split() if t]
    if not toks:
        return ""
    # normalize last token if it's a known suffix
    last = toks[-1]
    if last in SUFFIX_ALIAS:
        toks[-1] = SUFFIX_ALIAS[last]
    return " ".join(toks)

def parse_basic_address(addr: str):
    """
    Parses ONLY: street_no + remainder as street_name.
    No unit heuristics beyond obvious 'UNIT|APT|#' tokens (kept separately).
    """
    a = clean_addr_noise(addr).upper()
    a = re.sub(r"[^\w\s#\-]", "", a)
    a = norm_ws(a)

    m = re.match(r"^(\d+)\s+(.*)$", a)
    if not m:
        return {"street_no": None, "street_name": None, "unit": None}
    street_no = m.group(1)
    rest = m.group(2).strip()

    unit = None
    # minimal unit capture (defensible): "UNIT 315", "APT 2", "#2A"
    um = re.search(r"(?:\bUNIT\b|\bAPT\b|\bAPARTMENT\b|\bSTE\b|\bSUITE\b|#)\s*([A-Z0-9\-]+)\b", rest)
    if um:
        unit = um.group(1)
        # remove the unit fragment from street name
        rest = re.sub(r"(?:\bUNIT\b|\bAPT\b|\bAPARTMENT\b|\bSTE\b|\bSUITE\b|#)\s*[A-Z0-9\-]+\b", "", rest).strip()
        rest = norm_ws(rest)

    street_name = canon_street_name(rest)
    return {"street_no": street_no, "street_name": street_name, "unit": unit}

def canon_full_address(addr: str) -> str:
    p = parse_basic_address(addr)
    if not p["street_no"] or not p["street_name"]:
        return ""
    # NOTE: we do NOT append unit into the key for now (your axis2 matching was unit-optional)
    return f'{p["street_no"]} {p["street_name"]}'

# -----------------------------
# DOCNO-HEADER SEGMENTATION
# -----------------------------
# header lines like: 01-06-2025  4:00:34p  25717  135    578
HDR = re.compile(r"(?m)^\s*(\d{2}-\d{2}-\d{4})\s+\d{1,2}:\d{2}:\d{2}[ap]\s+\d+\s+\d+\s+(\d+)\s+", re.I)

TOWN_ADDR = re.compile(r"Town:\s*([A-Z][A-Z\s]+?)\s+Addr:\s*([^\n\r]+)", re.I)

def segment_by_docno(raw_block: str, docno_raw: str):
    """
    Returns (segment_text, meta_dict). If raw_block has multiple headers,
    pick the segment whose header docno matches docno_raw.
    If no segmentation applies, returns (None, meta) and caller uses original fields.
    """
    rb = raw_block or ""
    docno = (docno_raw or "").strip()
    if not rb or not docno:
        return None, {"seg_applied": False, "why": "missing_rb_or_docno"}

    hits = list(HDR.finditer(rb))
    if len(hits) <= 1:
        return None, {"seg_applied": False, "why": "single_or_no_header", "header_count": len(hits)}

    # build chunks
    starts = [m.start() for m in hits]
    ends = starts[1:] + [len(rb)]
    chunks = []
    docnos = []
    for m, s, e in zip(hits, starts, ends):
        chunks.append(rb[s:e])
        docnos.append(m.group(2))

    if docno not in docnos:
        return None, {"seg_applied": False, "why": "docno_not_found_in_headers", "docno_raw": docno, "docnos": docnos[:12]}

    # pick first matching docno (should be unique within the row set)
    idx = docnos.index(docno)
    return chunks[idx], {"seg_applied": True, "why": "docno_header_match", "header_count": len(docnos), "picked_index": idx, "docnos": docnos[:12]}

def extract_town_addr_from_segment(seg_text: str):
    """
    Returns (town_norm, addr_norm) from the selected segment if present.
    """
    if not seg_text:
        return None, None
    m = TOWN_ADDR.search(seg_text)
    if not m:
        return None, None
    town = norm_ws(m.group(1)).upper()
    addr = clean_addr_noise(m.group(2)).upper()
    # remove trailing artifacts
    addr = addr.replace("  Y", " ").strip()
    addr = norm_ws(addr)
    return town, addr

# -----------------------------
# BUILD TARGETED AXIS2 INDEX (unique-only)
# -----------------------------
def build_target_keys(events):
    need_full = set()
    need_street = set()
    need_towns = set()

    for ev in events:
        pr = ev.get("property_ref") or {}
        town = (pr.get("town_norm") or pr.get("town_raw") or "").upper().strip()
        addr = (pr.get("address_norm") or pr.get("address_raw") or "").upper().strip()
        addr = clean_addr_noise(addr)

        if town:
            need_towns.add(town)

        fa = canon_full_address(addr)
        if town and fa:
            need_full.add(f"{town}|{fa}")

        p = parse_basic_address(addr)
        if town and p["street_no"] and p["street_name"]:
            need_street.add(f"{town}|{p['street_no']}|{p['street_name']}")

        # if multi_address exists, include those too (for PARTIAL_MULTI upgrades)
        if pr.get("primary_is_multi") and isinstance(pr.get("multi_address"), list):
            for ma in pr["multi_address"]:
                t2 = (ma.get("town_norm") or ma.get("town_raw") or "").upper().strip()
                a2 = clean_addr_noise((ma.get("address_norm") or ma.get("address_raw") or "").upper().strip())
                if t2:
                    need_towns.add(t2)
                fa2 = canon_full_address(a2)
                if t2 and fa2:
                    need_full.add(f"{t2}|{fa2}")
                p2 = parse_basic_address(a2)
                if t2 and p2["street_no"] and p2["street_name"]:
                    need_street.add(f"{t2}|{p2['street_no']}|{p2['street_name']}")

    return need_towns, need_full, need_street

def load_spine_index(need_towns, need_full, need_street):
    spine_path = read_current_spine_path()
    full_index = defaultdict(list)
    street_index = defaultdict(list)

    for row in it_ndjson(spine_path):
        # tolerate schema differences
        pid = row.get("property_id") or row.get("id")
        if not pid:
            continue

        town = (row.get("town_norm") or row.get("town") or row.get("municipality") or "").upper().strip()
        if town and need_towns and town not in need_towns:
            continue

        addr = (row.get("full_address") or row.get("address_norm") or row.get("address") or "").upper().strip()
        addr = clean_addr_noise(addr)

        fa = canon_full_address(addr)
        if town and fa:
            k_full = f"{town}|{fa}"
            if k_full in need_full:
                full_index[k_full].append(pid)

            p = parse_basic_address(addr)
            if p["street_no"] and p["street_name"]:
                k_st = f"{town}|{p['street_no']}|{p['street_name']}"
                if k_st in need_street:
                    street_index[k_st].append(pid)

    return spine_path, full_index, street_index

def unique_or_none(lst):
    if not lst:
        return None, "no_match"
    if len(lst) == 1:
        return lst[0], None
    return None, "collision"

# -----------------------------
# MAIN REATTACH
# -----------------------------
def main():
    events = list(it_ndjson(IN_PATH))

    stats = Counter()
    seg_stats = Counter()

    # Build targeted index for speed/defensibility
    need_towns, need_full, need_street = build_target_keys(events)
    spine_path, full_index, street_index = load_spine_index(need_towns, need_full, need_street)

    out_rows = []

    for ev in events:
        a = ev.get("attach") or {}
        status = (a.get("attach_status") or "").upper()

        if status and status != "UNKNOWN" and status != "PARTIAL_MULTI":
            stats["already_attached_or_not_unknown"] += 1
            out_rows.append(ev)
            continue

        # If raw_block contains multiple headers, isolate segment by docno and override town/addr from segment.
        rb = ((ev.get("document") or {}).get("raw_block")) or ""
        docno_raw = ((ev.get("recording") or {}).get("document_number_raw")) or ""
        seg, meta = segment_by_docno(rb, str(docno_raw))

        if meta.get("seg_applied"):
            seg_stats["seg_applied"] += 1
            town_seg, addr_seg = extract_town_addr_from_segment(seg)
            if town_seg and addr_seg:
                seg_stats["seg_extracted_town_addr"] += 1
                pr = ev.get("property_ref") or {}
                pr["town_norm"] = town_seg
                pr["address_norm"] = addr_seg
                pr["town_raw"] = pr.get("town_raw") or town_seg
                pr["address_raw"] = pr.get("address_raw") or addr_seg
                # IMPORTANT: clear contaminated multi_address if present (rebuild only if needed later)
                if pr.get("primary_is_multi"):
                    pr["multi_address"] = []
                ev["property_ref"] = pr
            else:
                seg_stats["seg_no_town_addr_in_segment"] += 1

            # add evidence about segmentation
            ev.setdefault("meta", {})
            ev["meta"]["raw_block_segmented_docno"] = meta
        else:
            seg_stats[f"seg_skip__{meta.get('why','unknown')}"] += 1

        # Re-read normalized town/address after potential override
        pr = ev.get("property_ref") or {}
        town = (pr.get("town_norm") or pr.get("town_raw") or "").upper().strip()
        addr = clean_addr_noise((pr.get("address_norm") or pr.get("address_raw") or "").upper().strip())

        # If MULTI handling: attach each candidate independently, keep evidence
        if pr.get("primary_is_multi"):
            stats["multi_seen"] += 1
            attachments = []

            # primary
            pid_primary = None
            why_primary = None
            fa = canon_full_address(addr)
            if town and fa:
                pid_primary, why_primary = unique_or_none(full_index.get(f"{town}|{fa}", []))
                if not pid_primary:
                    # fall back to street unique
                    p = parse_basic_address(addr)
                    if p["street_no"] and p["street_name"]:
                        pid_primary, why_primary = unique_or_none(street_index.get(f"{town}|{p['street_no']}|{p['street_name']}", []))

            attachments.append({
                "town_norm": town,
                "address_norm": fa if fa else addr,
                "attach_status": "ATTACHED_A" if pid_primary else "UNKNOWN",
                "property_id": pid_primary,
                "match_method": "docno_seg+unique" if meta.get("seg_applied") else "unique",
                "match_key": f"{town}|{fa}" if town and fa else f"{town}|{addr}",
                **({"why":"collision"} if why_primary=="collision" else ({"why":"no_match"} if why_primary=="no_match" else {}))
            })

            # We are NOT rebuilding secondary multi_address lines here unless the event explicitly has them
            # (and v1_12 frequently had contaminated ones). Keeping strict.

            # finalize multi attach status
            attached_any = any(x["attach_status"]=="ATTACHED_A" for x in attachments)
            attached_all = all(x["attach_status"]=="ATTACHED_A" for x in attachments)

            ev.setdefault("attach", {})
            ev["attach"]["attach_scope"] = "MULTI"
            ev["attach"]["attachments"] = attachments
            ev["attach"]["attach_status"] = "ATTACHED_A" if attached_all else ("PARTIAL_MULTI" if attached_any else "UNKNOWN")
            ev["attach"]["property_id"] = None
            ev["attach"]["evidence"] = {
                "join_method": "docno-header segmentation (if needed) + town+full_address unique_only, fallback town+street_no+street_name unique_only (suffix-alias canonical)",
                "join_basis": "axis2_docno_segment_v1_13",
                "spine_current": SPINE_CURRENT_JSON,
                "spine_path": spine_path,
                "events_in": IN_PATH,
            }

            if attached_all:
                stats["multi_upgraded_to_attached_all"] += 1
            elif attached_any:
                stats["multi_upgraded_some_attachments"] += 1
            else:
                stats["multi_still_unknown"] += 1

            out_rows.append(ev)
            continue

        # SINGLE attach
        if not town:
            stats["single_still_unknown__no_town"] += 1
            out_rows.append(ev)
            continue

        fa = canon_full_address(addr)
        if not fa:
            stats["single_still_unknown__no_num_or_bad_addr"] += 1
            out_rows.append(ev)
            continue

        k_full = f"{town}|{fa}"
        pid, why = unique_or_none(full_index.get(k_full, []))

        match_method = None
        if pid:
            match_method = "axis2_full_unique"
        else:
            # fallback street unique
            p = parse_basic_address(addr)
            if p["street_no"] and p["street_name"]:
                k_st = f"{town}|{p['street_no']}|{p['street_name']}"
                pid2, why2 = unique_or_none(street_index.get(k_st, []))
                if pid2:
                    pid = pid2
                    why = None
                    match_method = "axis2_street_unique_suffix_alias"
                else:
                    why = why2 or why

        ev.setdefault("attach", {})
        if pid:
            ev["attach"]["attach_scope"] = "SINGLE"
            ev["attach"]["attach_status"] = "ATTACHED_A"
            ev["attach"]["property_id"] = pid
            ev["attach"]["match_method"] = match_method
            ev["attach"]["match_key"] = k_full
            ev["attach"]["attachments"] = []
            ev["attach"]["evidence"] = {
                "join_method": "docno-header segmentation (if needed) + town+full_address unique_only, fallback town+street_no+street_name unique_only (suffix-alias canonical)",
                "join_basis": "axis2_docno_segment_v1_13",
                "spine_current": SPINE_CURRENT_JSON,
                "spine_path": spine_path,
                "events_in": IN_PATH,
            }
            stats["single_upgraded_to_attached"] += 1
        else:
            ev["attach"]["attach_scope"] = "SINGLE"
            ev["attach"]["attach_status"] = "UNKNOWN"
            ev["attach"]["property_id"] = None
            ev["attach"]["match_method"] = "collision" if why=="collision" else "no_match"
            ev["attach"]["match_key"] = k_full
            ev["attach"]["attachments"] = []
            ev["attach"]["evidence"] = {
                "join_method": "docno-header segmentation (if needed) + town+full_address unique_only, fallback town+street_no+street_name unique_only (suffix-alias canonical)",
                "join_basis": "axis2_docno_segment_v1_13",
                "spine_current": SPINE_CURRENT_JSON,
                "spine_path": spine_path,
                "events_in": IN_PATH,
            }
            if why == "collision":
                stats["single_still_unknown__collision"] += 1
            else:
                stats["single_still_unknown__no_match"] += 1

        out_rows.append(ev)

    write_ndjson(OUT_PATH, out_rows)

    os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
    audit = {
        "in": IN_PATH,
        "out": OUT_PATH,
        "spine_current": SPINE_CURRENT_JSON,
        "audit_path": AUDIT_PATH,
        "stats": dict(stats),
        "seg_stats": dict(seg_stats),
    }
    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("=== AXIS2 REATTACH (>=10k) v1_13 (DOCNO SEG) ===")
    print(json.dumps({"out": OUT_PATH, "audit": AUDIT_PATH, "stats": dict(stats), "seg_stats": dict(seg_stats)}, indent=2))

if __name__ == "__main__":
    main()
