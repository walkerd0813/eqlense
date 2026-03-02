import json, re, os, argparse
from collections import defaultdict, Counter

IN_PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_12.ndjson"
OUT_PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_14.ndjson"
AUDIT_PATH = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_14.json"

SPINE_CURRENT_JSON = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"

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

def find_first_ndjson_in_text(txt: str):
    if not txt:
        return None
    # Prefer publicData-relative paths if present
    m = re.search(r'(?i)(publicData[\\/][^"\r\n]+?\.ndjson)', txt)
    if m:
        return m.group(1)
    # otherwise any .ndjson
    m = re.search(r'(?i)([^"\r\n]+?\.ndjson)', txt)
    if m:
        return m.group(1)
    return None

def read_current_spine_path_or_die():
    """
    Robust CURRENT pointer resolver:
    - tries common keys
    - if that fails, regex the raw json text for first *.ndjson
    """
    with open(SPINE_CURRENT_JSON, "r", encoding="utf-8") as f:
        raw = f.read()
    try:
        obj = json.loads(raw)
    except Exception:
        obj = None

    # common direct keys
    if isinstance(obj, dict):
        for k in ("path","ndjson","file","in","src","target","resolved","resolved_path","current","current_path","value"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip().lower().endswith(".ndjson"):
                return v.strip()
        # sometimes nested under {"current": {"path": "...ndjson"}}
        for k in ("current","pointer","ref","artifact","spine"):
            v = obj.get(k)
            if isinstance(v, dict):
                for kk, vv in v.items():
                    if isinstance(vv, str) and vv.strip().lower().endswith(".ndjson"):
                        return vv.strip()

    # fallback: regex scan
    nd = find_first_ndjson_in_text(raw)
    if nd:
        return nd.strip()

    raise RuntimeError(f"Could not find any .ndjson path inside {SPINE_CURRENT_JSON}")

_ws = re.compile(r"\s+")
def norm_ws(s: str) -> str:
    return _ws.sub(" ", (s or "").strip())

def clean_addr_noise(s: str) -> str:
    s = (s or "")
    s = s.replace("\u00a0", " ")
    s = norm_ws(s)
    if s.endswith(" Y"):
        s = s[:-2].rstrip()
    return s

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
    last = toks[-1]
    if last in SUFFIX_ALIAS:
        toks[-1] = SUFFIX_ALIAS[last]
    return " ".join(toks)

def parse_basic_address(addr: str):
    a = clean_addr_noise(addr).upper()
    a = re.sub(r"[^\w\s#\-]", "", a)
    a = norm_ws(a)
    m = re.match(r"^(\d+)\s+(.*)$", a)
    if not m:
        return {"street_no": None, "street_name": None, "unit": None}
    street_no = m.group(1)
    rest = m.group(2).strip()

    unit = None
    um = re.search(r"(?:\bUNIT\b|\bAPT\b|\bAPARTMENT\b|\bSTE\b|\bSUITE\b|#)\s*([A-Z0-9\-]+)\b", rest)
    if um:
        unit = um.group(1)
        rest = re.sub(r"(?:\bUNIT\b|\bAPT\b|\bAPARTMENT\b|\bSTE\b|\bSUITE\b|#)\s*[A-Z0-9\-]+\b", "", rest).strip()
        rest = norm_ws(rest)

    street_name = canon_street_name(rest)
    return {"street_no": street_no, "street_name": street_name, "unit": unit}

def canon_full_address(addr: str) -> str:
    p = parse_basic_address(addr)
    if not p["street_no"] or not p["street_name"]:
        return ""
    return f'{p["street_no"]} {p["street_name"]}'

HDR = re.compile(r"(?m)^\s*(\d{2}-\d{2}-\d{4})\s+\d{1,2}:\d{2}:\d{2}[ap]\s+\d+\s+\d+\s+(\d+)\s+", re.I)
TOWN_ADDR = re.compile(r"Town:\s*([A-Z][A-Z\s]+?)\s+Addr:\s*([^\n\r]+)", re.I)

def segment_by_docno(raw_block: str, docno_raw: str):
    rb = raw_block or ""
    docno = (docno_raw or "").strip()
    if not rb or not docno:
        return None, {"seg_applied": False, "why": "missing_rb_or_docno"}

    hits = list(HDR.finditer(rb))
    if len(hits) <= 1:
        return None, {"seg_applied": False, "why": "single_or_no_header", "header_count": len(hits)}

    starts = [m.start() for m in hits]
    ends = starts[1:] + [len(rb)]
    chunks, docnos = [], []
    for m, s, e in zip(hits, starts, ends):
        chunks.append(rb[s:e])
        docnos.append(m.group(2))

    if docno not in docnos:
        return None, {"seg_applied": False, "why": "docno_not_found_in_headers", "docno_raw": docno, "docnos": docnos[:12]}

    idx = docnos.index(docno)
    return chunks[idx], {"seg_applied": True, "why": "docno_header_match", "header_count": len(docnos), "picked_index": idx, "docnos": docnos[:12]}

def extract_town_addr_from_segment(seg_text: str):
    if not seg_text:
        return None, None
    m = TOWN_ADDR.search(seg_text)
    if not m:
        return None, None
    town = norm_ws(m.group(1)).upper()
    addr = clean_addr_noise(m.group(2)).upper()
    addr = addr.replace("  Y", " ").strip()
    addr = norm_ws(addr)
    return town, addr

def build_target_keys(events):
    need_full, need_street, need_towns = set(), set(), set()
    for ev in events:
        pr = ev.get("property_ref") or {}
        town = (pr.get("town_norm") or pr.get("town_raw") or "").upper().strip()
        addr = clean_addr_noise((pr.get("address_norm") or pr.get("address_raw") or "").upper().strip())
        if town:
            need_towns.add(town)
        fa = canon_full_address(addr)
        if town and fa:
            need_full.add(f"{town}|{fa}")
        p = parse_basic_address(addr)
        if town and p["street_no"] and p["street_name"]:
            need_street.add(f"{town}|{p['street_no']}|{p['street_name']}")
    return need_towns, need_full, need_street

def load_spine_index(spine_path, need_towns, need_full, need_street):
    full_index = defaultdict(list)
    street_index = defaultdict(list)

    for row in it_ndjson(spine_path):
        pid = row.get("property_id") or row.get("id")
        if not pid:
            continue
        town = (row.get("town_norm") or row.get("town") or row.get("municipality") or "").upper().strip()
        if town and need_towns and town not in need_towns:
            continue
        addr = clean_addr_noise((row.get("full_address") or row.get("address_norm") or row.get("address") or "").upper().strip())
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
    return full_index, street_index

def unique_or_none(lst):
    if not lst:
        return None, "no_match"
    if len(lst) == 1:
        return lst[0], None
    return None, "collision"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spine", default=None, help="Optional override: path to canonical spine ndjson")
    args = ap.parse_args()

    events = list(it_ndjson(IN_PATH))
    stats = Counter()
    seg_stats = Counter()

    spine_path = args.spine.strip() if args.spine else read_current_spine_path_or_die()
    if not os.path.exists(spine_path):
        raise RuntimeError(f"Resolved spine ndjson does not exist: {spine_path}")

    need_towns, need_full, need_street = build_target_keys(events)
    full_index, street_index = load_spine_index(spine_path, need_towns, need_full, need_street)

    out_rows = []
    for ev in events:
        a = ev.get("attach") or {}
        status = (a.get("attach_status") or "").upper()

        if status and status != "UNKNOWN" and status != "PARTIAL_MULTI":
            stats["already_attached_or_not_unknown"] += 1
            out_rows.append(ev)
            continue

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
                ev["property_ref"] = pr
            else:
                seg_stats["seg_no_town_addr_in_segment"] += 1
            ev.setdefault("meta", {})
            ev["meta"]["raw_block_segmented_docno"] = meta

        pr = ev.get("property_ref") or {}
        town = (pr.get("town_norm") or pr.get("town_raw") or "").upper().strip()
        addr = clean_addr_noise((pr.get("address_norm") or pr.get("address_raw") or "").upper().strip())
        fa = canon_full_address(addr)

        if not town:
            stats["single_still_unknown__no_town"] += 1
            out_rows.append(ev)
            continue
        if not fa:
            stats["single_still_unknown__no_num_or_bad_addr"] += 1
            out_rows.append(ev)
            continue

        k_full = f"{town}|{fa}"
        pid, why = unique_or_none(full_index.get(k_full, []))
        match_method = "axis2_full_unique" if pid else None

        if not pid:
            p = parse_basic_address(addr)
            if p["street_no"] and p["street_name"]:
                k_st = f"{town}|{p['street_no']}|{p['street_name']}"
                pid2, why2 = unique_or_none(street_index.get(k_st, []))
                if pid2:
                    pid, why = pid2, None
                    match_method = "axis2_street_unique_suffix_alias"
                else:
                    why = why2 or why

        ev.setdefault("attach", {})
        if pid:
            ev["attach"].update({
                "attach_scope":"SINGLE",
                "attach_status":"ATTACHED_A",
                "property_id":pid,
                "match_method":match_method,
                "match_key":k_full,
                "attachments":[],
                "evidence":{
                    "join_method":"docno-header segmentation (if needed) + unique-only axis2",
                    "join_basis":"axis2_docno_segment_v1_14",
                    "spine_current":SPINE_CURRENT_JSON,
                    "spine_path":spine_path,
                    "events_in":IN_PATH
                }
            })
            stats["single_upgraded_to_attached"] += 1
        else:
            ev["attach"].update({
                "attach_scope":"SINGLE",
                "attach_status":"UNKNOWN",
                "property_id":None,
                "match_method":"collision" if why=="collision" else "no_match",
                "match_key":k_full,
                "attachments":[],
                "evidence":{
                    "join_method":"docno-header segmentation (if needed) + unique-only axis2",
                    "join_basis":"axis2_docno_segment_v1_14",
                    "spine_current":SPINE_CURRENT_JSON,
                    "spine_path":spine_path,
                    "events_in":IN_PATH
                }
            })
            if why=="collision":
                stats["single_still_unknown__collision"] += 1
            else:
                stats["single_still_unknown__no_match"] += 1

        out_rows.append(ev)

    write_ndjson(OUT_PATH, out_rows)
    os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
    audit = {"in":IN_PATH,"out":OUT_PATH,"spine_current":SPINE_CURRENT_JSON,"spine_path":spine_path,"stats":dict(stats),"seg_stats":dict(seg_stats)}
    with open(AUDIT_PATH,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("=== AXIS2 REATTACH (>=10k) v1_14 (DOCNO SEG + ROBUST SPINE) ===")
    print(json.dumps({"out":OUT_PATH,"audit":AUDIT_PATH,"spine_path":spine_path,"stats":dict(stats),"seg_stats":dict(seg_stats)}, indent=2))

if __name__=="__main__":
    main()
