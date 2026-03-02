import os, json, re, hashlib
from datetime import datetime

CANDIDATES = r"publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k.ndjson"
CURRENT_PTR = r"publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"

OUT_DIR = r"publicData\registry\hampden\_attached_DEED_ONLY_v1_8_2_AXIS2"
OUT_NDJSON = os.path.join(OUT_DIR, "events_axis2_upgrades_ge10k.ndjson")
OUT_REPORT = os.path.join(OUT_DIR, "axis2_report_ge10k.json")

def load_current_spine_path():
    with open(CURRENT_PTR, "r", encoding="utf-8") as f:
        ptr = json.load(f)
    p = ptr.get("properties_ndjson")
    if not p:
        raise RuntimeError("CURRENT pointer missing properties_ndjson")
    if not os.path.exists(p):
        raise RuntimeError(f"Spine ndjson not found: {p}")
    return p

def norm_space(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    return s

def strip_trailing_artifacts(addr: str) -> str:
    # common artifact: trailing single-letter token like "Y"
    addr = norm_space(addr)
    addr = re.sub(r"\s+[A-Z]$", "", addr)  # "... Y"
    return addr

SUFFIX_SWAPS = [
    (r"\bLA\b", "LN"),
    (r"\bLN\b", "LA"),
    (r"\bRD\b", "ROAD"),
    (r"\bROAD\b", "RD"),
    (r"\bST\b", "STREET"),
    (r"\bSTREET\b", "ST"),
    (r"\bAVE\b", "AVENUE"),
    (r"\bAVENUE\b", "AVE"),
    (r"\bDR\b", "DRIVE"),
    (r"\bDRIVE\b", "DR"),
]

def variant_suffix_swaps(addr: str):
    out = set()
    base = addr
    out.add(base)
    for pat, rep in SUFFIX_SWAPS:
        swapped = re.sub(pat, rep, base)
        if swapped != base:
            out.add(swapped)
    return out

def variant_unit_forms(addr: str):
    out = set()
    a = addr
    out.add(a)

    # "#47" <-> "UNIT 47"
    m = re.search(r"#\s*([0-9A-Z\-]+)\b", a)
    if m:
        out.add(re.sub(r"#\s*([0-9A-Z\-]+)\b", r"UNIT \1", a))

    m2 = re.search(r"\bUNIT\s+([0-9A-Z\-]+)\b", a)
    if m2:
        out.add(re.sub(r"\bUNIT\s+([0-9A-Z\-]+)\b", r"#\1", a))

    # also try plain unit token removal (lower confidence path)
    out.add(re.sub(r"\s+\b(UNIT|APT|APARTMENT|#)\s*([0-9A-Z\-]+)\b", "", a).strip())
    out = {norm_space(x) for x in out if x and x.strip()}
    return out

def variant_ranges(addr: str):
    out = set([addr])
    # for "19-21 THOMAS AVE": try 19, 21, and keep the range
    m = re.match(r"^(\d+)\s*-\s*(\d+)\s+(.*)$", addr)
    if m:
        a1, a2, rest = m.group(1), m.group(2), m.group(3)
        out.add(f"{a1} {rest}")
        out.add(f"{a2} {rest}")
        out.add(f"{a1}-{a2} {rest}")  # normalized spacing
    return {norm_space(x) for x in out}

def gen_addr_variants(addr_raw: str):
    addr = (addr_raw or "").upper()
    addr = addr.replace(",", " ")
    addr = re.sub(r"[\.]", "", addr)
    addr = norm_space(addr)
    addr = strip_trailing_artifacts(addr)

    variants = set([addr])

    # range variants
    tmp = set()
    for v in variants:
        tmp |= variant_ranges(v)
    variants = tmp

    # unit variants
    tmp = set()
    for v in variants:
        tmp |= variant_unit_forms(v)
    variants = tmp

    # suffix swaps
    tmp = set()
    for v in variants:
        tmp |= variant_suffix_swaps(v)
    variants = tmp

    # final cleanup
    variants = {strip_trailing_artifacts(norm_space(v)) for v in variants if v and v.strip()}
    return variants

def key(town: str, addr: str) -> str:
    return f"{town}|{addr}"

def _as_text(v):
    """Coerce string/dict/list address fields into a stable string."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        # try common fields first
        for k in ("address_norm", "text", "full", "full_text", "value", "raw", "raw_text", "formatted", "label"):
            if k in v and isinstance(v.get(k), str):
                return v.get(k)
        # if it looks like components, join them
        parts = []
        for k in ("street_number", "number", "street", "street_name", "street_suffix", "unit", "unit_type"):
            if k in v and v.get(k):
                parts.append(str(v.get(k)))
        if parts:
            return " ".join(parts)
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        # join any stringish components
        return " ".join([_as_text(x) for x in v if _as_text(x)])
    return str(v)

def load_spine_index(spine_path: str):
    idx = {}
    rows = 0
    for line in open(spine_path, "r", encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        rows += 1
        try:
            rec = json.loads(line)
        except:
            continue

        town_v = rec.get("town_norm") or rec.get("town") or rec.get("municipality") or rec.get("city") or ""
        town = _as_text(town_v).upper().strip()

        addr_v = rec.get("address_norm") or rec.get("address") or rec.get("address_full") or rec.get("site_address") or ""
        addr = _as_text(addr_v).upper().strip()

        pid = rec.get("property_id") or rec.get("id") or rec.get("propertyId")
        if not pid or not town or not addr:
            continue

        # build multiple keys per spine record
        for av in gen_addr_variants(addr):
            k = key(town, av)
            idx.setdefault(k, set()).add(pid)

    # collapse sets to either single id or collision marker
    final = {}
    collisions = 0
    for k, s in idx.items():
        if len(s) == 1:
            final[k] = list(s)[0]
        else:
            collisions += 1
            final[k] = None

    return final, {
        "spine_rows_scanned": rows,
        "spine_index_keys": len(final),
        "spine_collisions": collisions,
        "spine_path": spine_path
    }

def choose_unique(spine_idx, town, addr_variants):
    # return (property_id, matched_key, rule_tag)
    for a in addr_variants:
        k = key(town, a)
        pid = spine_idx.get(k)
        if pid:
            return pid, k, "variant_match"
    return None, None, None

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    spine_path = load_current_spine_path()
    spine_idx, spine_stats = load_spine_index(spine_path)

    stats = {
        "run_utc": datetime.utcnow().isoformat() + "Z",
        "min_consideration": 10000,
        "candidates_in": 0,
        "upgrades_written": 0,
        "no_hits": 0,
        "hit_examples": [],
        "spine": spine_stats,
        "rules": {}
    }

    with open(CANDIDATES, "r", encoding="utf-8") as fin, open(OUT_NDJSON, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            stats["candidates_in"] += 1
            ev = json.loads(line)

            attach = ev.get("attach", {}) or {}
            scope = attach.get("attach_scope")
            town = (ev.get("property_ref", {}) or {}).get("town_norm") or (ev.get("property_ref", {}) or {}).get("town_raw")
            town = (town or "").upper().strip()

            # primary + multi addresses
            pr = ev.get("property_ref", {}) or {}
            primary_addr = pr.get("address_norm") or pr.get("address_raw") or ""
            addrs = [primary_addr]
            for ma in pr.get("multi_address") or []:
                addrs.append(ma.get("address_norm") or ma.get("address_raw") or "")

            matched = []
            for addr_raw in addrs:
                addr_vars = gen_addr_variants(addr_raw)
                pid, mk, rule = choose_unique(spine_idx, town, addr_vars)
                if pid:
                    matched.append((addr_raw, pid, mk, rule))

            if not matched:
                stats["no_hits"] += 1
                continue

            # write upgrade record (do NOT mutate original v1_8_1 file)
            # attach evidence per match
            up = ev
            up.setdefault("axis2", {})
            up["axis2"]["matched"] = []
            for addr_raw, pid, mk, rule in matched:
                up["axis2"]["matched"].append({
                    "town_norm": town,
                    "address_raw": addr_raw,
                    "property_id": pid,
                    "match_key": mk,
                    "rule": rule
                })
                stats["rules"][rule] = stats["rules"].get(rule, 0) + 1

            fout.write(json.dumps(up, ensure_ascii=False) + "\n")
            stats["upgrades_written"] += 1
            if len(stats["hit_examples"]) < 5:
                stats["hit_examples"].append({"event_id": ev.get("event_id"), "matches": up["axis2"]["matched"]})

    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print(stats)

if __name__ == "__main__":
    main()

