import os, re, json
from datetime import datetime, timezone
from collections import defaultdict, Counter

ROOT = r"C:\seller-app\backend"

PTR = os.path.join(ROOT, r"publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json")

MIN_CONSIDERATION = 10000

WS_RE = re.compile(r"\s+")
TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.I)
PUNCT_RE = re.compile(r"[,\.;]")
UNIT_TOKEN_RE = re.compile(r"\b(?:UNIT|APT|APARTMENT|#)\s*([A-Z0-9\-]+)\b", re.I)

TYPE_MAP = {
    "STREET": "ST", "ST": "ST",
    "ROAD": "RD", "RD": "RD",
    "AVENUE": "AVE", "AVE": "AVE", "AV": "AVE",
    "DRIVE": "DR", "DR": "DR",
    "LANE": "LN", "LN": "LN", "LA": "LN",
    "COURT": "CT", "CT": "CT",
    "PLACE": "PL", "PL": "PL",
    "WAY": "WAY",
    "BOULEVARD": "BLVD", "BLVD": "BLVD",
    "TERRACE": "TER", "TER": "TER",
    "CIRCLE": "CIR", "CIR": "CIR",
    "PARKWAY": "PKWY", "PKWY": "PKWY",
}

def norm_text(x: str) -> str:
    if x is None:
        return ""
    s = str(x)
    s = TRAILING_Y_RE.sub("", s)
    s = PUNCT_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip().upper()
    return s

def norm_street_line(street_line: str) -> str:
    s = norm_text(street_line)
    if not s:
        return s
    toks = s.split(" ")
    if toks:
        last = toks[-1]
        if last in TYPE_MAP:
            toks[-1] = TYPE_MAP[last]
    return " ".join(toks)

def unit_variants(unit_raw: str):
    u = norm_text(unit_raw)
    if not u:
        return []
    u = u.replace("UNIT ", "").replace("APT ", "").replace("APARTMENT ", "").strip()
    if not u:
        return []
    return [f"UNIT {u}", f"#{u}"]

def build_address_variants(street_no: str, street_name: str, unit: str, full_address: str):
    variants = []
    no = norm_text(street_no)
    name = norm_street_line(street_name)
    base = norm_street_line(f"{no} {name}".strip())
    if base:
        variants.append(base)
    for uv in unit_variants(unit):
        if base:
            variants.append(norm_street_line(f"{base} {uv}"))
    fa = norm_street_line(full_address)
    if fa and fa not in variants:
        variants.append(fa)
    if fa:
        m = UNIT_TOKEN_RE.search(fa)
        if m:
            stripped = UNIT_TOKEN_RE.sub("", fa)
            stripped = norm_street_line(stripped)
            if stripped and stripped not in variants:
                variants.append(stripped)
    out, seen = [], set()
    for v in variants:
        if v and v not in seen:
            seen.add(v); out.append(v)
    return out

def extract_unit_from_raw_block(raw_block: str):
    if not raw_block:
        return None
    m = re.search(r"\bUNIT\s+([A-Z0-9\-]+)\b", raw_block.upper())
    if m:
        return m.group(1).strip()
    return None

def candidate_address_variants(event):
    pr = (event.get("property_ref") or {})
    town = norm_text(pr.get("town_norm") or pr.get("town_raw") or "")
    addr = pr.get("address_norm") or pr.get("address_raw") or ""
    addr = norm_street_line(addr)

    variants = []
    if addr:
        variants.append(addr)

    if addr:
        stripped = UNIT_TOKEN_RE.sub("", addr)
        stripped = norm_street_line(stripped)
        if stripped and stripped not in variants:
            variants.append(stripped)

    raw_block = (((event.get("document") or {}).get("raw_block")) or "")
    unit_hint = extract_unit_from_raw_block(raw_block)
    if unit_hint and addr and ("UNIT" not in addr and "#" not in addr):
        for uv in unit_variants(unit_hint):
            variants.append(norm_street_line(f"{addr} {uv}"))

    out, seen = [], set()
    for v in variants:
        if v and v not in seen:
            seen.add(v); out.append(v)
    return town, out

def resolve_spine_path():
    ptr = json.load(open(PTR, "r", encoding="utf-8"))
    spine = ptr.get("properties_ndjson")
    if not spine or not os.path.exists(spine):
        raise RuntimeError(f"Could not resolve spine path. properties_ndjson={spine}")
    return spine

def load_spine_index(spine_path):
    idx = defaultdict(list)
    rows = 0
    collisions = 0
    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            rows += 1
            rec = json.loads(line)
            town = norm_text(rec.get("town") or "")
            if not town:
                continue
            variants = build_address_variants(
                rec.get("street_no") or "",
                rec.get("street_name") or "",
                rec.get("unit") or "",
                rec.get("full_address") or "",
            )
            if not variants:
                continue
            payload = {
                "property_id": rec.get("property_id"),
                "parcel_id": rec.get("parcel_id"),
                "building_group_id": rec.get("building_group_id"),
            }
            for a in variants:
                key = f"{town}|{a}"
                if idx.get(key):
                    collisions += 1
                idx[key].append(payload)
    stats = {
        "spine_rows_scanned": rows,
        "spine_index_keys": len(idx),
        "spine_collisions": collisions,
        "spine_path": spine_path
    }
    return idx, stats

def get_consideration_amount(event):
    cons = event.get("consideration") or {}
    amt = cons.get("amount")
    try:
        return float(amt) if amt is not None else None
    except:
        return None

def is_unknownish_attach(event):
    att = event.get("attach")
    if not att:
        return True  # missing attach counts as unknownish for Axis2 reattach attempts
    s = (att.get("attach_status") or "").upper().strip()
    return (s == "UNKNOWN") or s.startswith("UNKNOWN")

def attach_event(event, spine_idx):
    town, addr_vars = candidate_address_variants(event)
    if not town or not addr_vars:
        return None, "NO_TOWN_OR_ADDR", {"town": town, "addr_vars": addr_vars}

    for addr in addr_vars:
        key = f"{town}|{addr}"
        hits = spine_idx.get(key, [])
        if len(hits) == 1:
            h = hits[0]
            out = json.loads(json.dumps(event))
            out.setdefault("attach", {})
            out["attach"].update({
                "attach_scope": "SINGLE",
                "attach_status": "ATTACHED_A",
                "property_id": h.get("property_id"),
                "match_method": "axis2_spine_town|addr",
                "match_key": key,
                "attachments": [{
                    "town_norm": town,
                    "address_norm": addr,
                    "attach_status": "ATTACHED_A",
                    "property_id": h.get("property_id"),
                    "match_method": "direct",
                    "match_key": key
                }],
            })
            return out, "HIT_SINGLE", {"key": key, "hit": h}

        if len(hits) > 1:
            bgs = set([x.get("building_group_id") for x in hits if x.get("building_group_id")])
            out = json.loads(json.dumps(event))
            out.setdefault("attach", {})
            out["attach"].update({
                "attach_scope": "BUILDING_GROUP" if len(bgs) == 1 else "MULTI",
                "attach_status": "GROUP_CANDIDATES",
                "property_id": None,
                "match_method": "axis2_spine_town|addr_multi",
                "match_key": key,
                "building_group_id": list(bgs)[0] if len(bgs) == 1 else None,
                "attachments": [{
                    "town_norm": town,
                    "address_norm": addr,
                    "attach_status": "CANDIDATE",
                    "property_id": x.get("property_id"),
                    "match_method": "candidate",
                    "match_key": key
                } for x in hits[:25]],
            })
            return out, "HIT_GROUP_CANDIDATES", {"key": key, "hit_count": len(hits), "building_group_ids": list(bgs)[:5]}

    return None, "NO_HIT", {"town": town, "addr_vars": addr_vars[:8]}

def find_latest_attached_events():
    base = os.path.join(ROOT, r"publicData\registry\hampden")
    if not os.path.isdir(base):
        raise RuntimeError(f"Missing hampden registry dir: {base}")

    candidates = []
    for name in os.listdir(base):
        if not name.startswith("_attached_DEED_ONLY"):
            continue
        d = os.path.join(base, name)
        if not os.path.isdir(d):
            continue
        for fn in os.listdir(d):
            if fn.lower().endswith(".ndjson") and ("attached" in fn.lower() or "deed_events" in fn.lower()):
                p = os.path.join(d, fn)
                try:
                    candidates.append((os.path.getmtime(p), p))
                except:
                    pass

    if not candidates:
        raise RuntimeError("Could not find any attached ndjson under publicData/registry/hampden/_attached_DEED_ONLY*")

    candidates.sort(reverse=True)
    return candidates[0][1]

def main():
    out_dir = os.path.join(ROOT, r"publicData\registry\hampden\_attached_DEED_ONLY_v1_8_4_AXIS2")
    os.makedirs(out_dir, exist_ok=True)
    out_upgrades = os.path.join(out_dir, "events_axis2_upgrades_ge10k.ndjson")
    out_report = os.path.join(out_dir, "axis2_report_ge10k.json")

    in_path = find_latest_attached_events()
    print("[info] candidates_in (auto):", in_path)

    spine_path = resolve_spine_path()
    print("[info] spine:", spine_path)

    spine_idx, spine_stats = load_spine_index(spine_path)
    print("[info] spine index keys:", spine_stats["spine_index_keys"], "collisions:", spine_stats["spine_collisions"])

    candidates = []
    with open(in_path, "r", encoding="utf-8") as f:
        for line in f:
            ev = json.loads(line)
            amt = get_consideration_amount(ev)
            if amt is None or amt < MIN_CONSIDERATION:
                continue
            if not is_unknownish_attach(ev):
                continue
            candidates.append(ev)

    outcomes = Counter()
    upgrades = 0
    hit_examples = []

    with open(out_upgrades, "w", encoding="utf-8") as out:
        for ev in candidates:
            upgraded, outcome, dbg = attach_event(ev, spine_idx)
            outcomes[outcome] += 1
            if upgraded is not None:
                out.write(json.dumps(upgraded, ensure_ascii=False) + "\n")
                upgrades += 1
                if len(hit_examples) < 10:
                    hit_examples.append({"event_id": ev.get("event_id"), "outcome": outcome, "dbg": dbg})

    report = {
        "run_utc": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "min_consideration": MIN_CONSIDERATION,
        "candidates_source": in_path,
        "candidates_in": len(candidates),
        "upgrades_written": upgrades,
        "outcomes": dict(outcomes),
        "hit_examples": hit_examples,
        "spine": spine_stats,
        "paths": {"out_upgrades": out_upgrades, "out_report": out_report}
    }

    with open(out_report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(report)
    print("[ok] wrote:", out_upgrades)
    print("[ok] report:", out_report)

if __name__ == "__main__":
    main()
