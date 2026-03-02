import os, re, json
from datetime import datetime, timezone
from collections import defaultdict, Counter

ROOT = r"C:\seller-app\backend"

PTR = os.path.join(ROOT, r"publicData\properties\_attached\CURRENT\CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json")

# Candidate source (same idea as v2: scan Hampden deeds-only attached feed and find >= 10k UNKNOWN)
CANDIDATES_IN = os.path.join(ROOT, r"publicData\registry\hampden\_events_DEED_ONLY_v1\deed_events.ndjson")

OUT_DIR = os.path.join(ROOT, r"publicData\registry\hampden\_attached_DEED_ONLY_v1_8_3_AXIS2")
OUT_UPGRADES = os.path.join(OUT_DIR, "events_axis2_upgrades_ge10k.ndjson")
OUT_REPORT = os.path.join(OUT_DIR, "axis2_report_ge10k.json")

MIN_CONSIDERATION = 10000


# -------------------------
# Normalization helpers
# -------------------------
WS_RE = re.compile(r"\s+")
TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.I)
PUNCT_RE = re.compile(r"[,\.;]")
UNIT_TOKEN_RE = re.compile(r"\b(?:UNIT|APT|APARTMENT|#)\s*([A-Z0-9\-]+)\b", re.I)

# Very light street-type normalization (don’t get fancy; stay deterministic)
TYPE_MAP = {
    "STREET": "ST", "ST": "ST",
    "ROAD": "RD", "RD": "RD",
    "AVENUE": "AVE", "AVE": "AVE", "AV": "AVE",
    "DRIVE": "DR", "DR": "DR",
    "LANE": "LN", "LN": "LN", "LA": "LN",  # important for PARTRIDGE LA
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
    """
    Normalize street line but keep it conservative:
    - uppercase
    - remove trailing ' Y'
    - collapse spaces
    - normalize final street type token if known (LA -> LN etc.)
    """
    s = norm_text(street_line)

    if not s:
        return s

    toks = s.split(" ")
    if toks:
        last = toks[-1]
        if last in TYPE_MAP:
            toks[-1] = TYPE_MAP[last]
        elif last in TYPE_MAP.keys():
            toks[-1] = TYPE_MAP[last]
    return " ".join(toks)

def unit_variants(unit_raw: str):
    """
    Generate safe unit variants.
    """
    u = norm_text(unit_raw)
    if not u:
        return []
    u = u.replace("UNIT ", "").replace("APT ", "").replace("APARTMENT ", "").strip()
    if not u:
        return []
    return [f"UNIT {u}", f"#{u}"]

def build_address_variants(street_no: str, street_name: str, unit: str, full_address: str):
    """
    Return list of normalized address strings to index/match.
    Includes:
      - base: "<no> <name>"
      - with unit: "<no> <name> UNIT X" and "<no> <name> #X"
      - fallback from full_address if needed
    """
    variants = []
    no = norm_text(street_no)
    name = norm_street_line(street_name)

    base = norm_street_line(f"{no} {name}".strip())
    if base:
        variants.append(base)

    # unit variants
    for uv in unit_variants(unit):
        if base:
            variants.append(norm_street_line(f"{base} {uv}"))

    # fallback from full_address
    fa = norm_street_line(full_address)
    if fa and fa not in variants:
        variants.append(fa)

    # Also index base from full_address with unit stripped (helps if full_address embeds unit)
    if fa:
        m = UNIT_TOKEN_RE.search(fa)
        if m:
            stripped = UNIT_TOKEN_RE.sub("", fa)
            stripped = norm_street_line(stripped)
            if stripped and stripped not in variants:
                variants.append(stripped)

    # Dedup preserve order
    out = []
    seen = set()
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

def extract_unit_from_raw_block(raw_block: str):
    """
    Pull 'UNIT X' from raw block if present. Conservative.
    """
    if not raw_block:
        return None
    m = re.search(r"\bUNIT\s+([A-Z0-9\-]+)\b", raw_block.upper())
    if m:
        return m.group(1).strip()
    return None

def candidate_address_variants(event):
    """
    Build candidate variants from event property_ref + raw_block unit hints.
    """
    pr = (event.get("property_ref") or {})
    town = norm_text(pr.get("town_norm") or pr.get("town_raw") or "")
    addr = pr.get("address_norm") or pr.get("address_raw") or ""
    addr = norm_street_line(addr)

    variants = []
    if addr:
        variants.append(addr)

    # Strip unit tokens for base match fallback
    if addr:
        stripped = UNIT_TOKEN_RE.sub("", addr)
        stripped = norm_street_line(stripped)
        if stripped and stripped not in variants:
            variants.append(stripped)

    # LA -> LN is handled by norm_street_line’s final-token map, but also catch " PARTRIDGE LA " mid-case
    # (kept conservative: only final token changes)

    # If raw_block has UNIT but address line doesn't contain UNIT/#, add unit-appended variants
    raw_block = (((event.get("document") or {}).get("raw_block")) or "")
    unit_hint = extract_unit_from_raw_block(raw_block)
    if unit_hint and addr and ("UNIT" not in addr and "#" not in addr):
        for uv in unit_variants(unit_hint):
            variants.append(norm_street_line(f"{addr} {uv}"))

    # Dedup
    out = []
    seen = set()
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return town, out


# -------------------------
# Spine loading / indexing
# -------------------------
def resolve_spine_path():
    ptr = json.load(open(PTR, "r", encoding="utf-8"))
    spine = ptr.get("properties_ndjson")
    if not spine or not os.path.exists(spine):
        raise RuntimeError(f"Could not resolve spine path. properties_ndjson={spine}")
    return spine

def load_spine_index(spine_path):
    """
    Index: TOWN|ADDRESS_VARIANT -> list of {property_id, parcel_id, building_group_id}
    """
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

            street_no = rec.get("street_no") or ""
            street_name = rec.get("street_name") or ""
            unit = rec.get("unit") or ""
            full_address = rec.get("full_address") or ""

            variants = build_address_variants(street_no, street_name, unit, full_address)
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


# -------------------------
# Candidate selection
# -------------------------
def get_consideration_amount(event):
    cons = event.get("consideration") or {}
    amt = cons.get("amount")
    try:
        return float(amt) if amt is not None else None
    except:
        return None

def is_unknown_attach(event):
    att = event.get("attach") or {}
    return (att.get("attach_status") or "").upper() == "UNKNOWN"

def attach_event(event, spine_idx):
    """
    Return (upgraded_event_or_none, outcome_str, debug_info)
    """
    town, addr_vars = candidate_address_variants(event)
    if not town or not addr_vars:
        return None, "NO_TOWN_OR_ADDR", {"town": town, "addr_vars": addr_vars}

    # Try variants in order; accept only deterministic single-hit
    for addr in addr_vars:
        key = f"{town}|{addr}"
        hits = spine_idx.get(key, [])
        if len(hits) == 1:
            h = hits[0]
            out = json.loads(json.dumps(event))  # deep-ish copy
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
            # If multiple hits but all share same building_group_id, we can return GROUP_CANDIDATES safely
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


# -------------------------
# Main
# -------------------------
def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    spine_path = resolve_spine_path()
    print("[info] spine:", spine_path)

    spine_idx, spine_stats = load_spine_index(spine_path)
    print("[info] spine index keys:", spine_stats["spine_index_keys"], "collisions:", spine_stats["spine_collisions"])

    candidates = []
    with open(CANDIDATES_IN, "r", encoding="utf-8") as f:
        for line in f:
            ev = json.loads(line)
            amt = get_consideration_amount(ev)
            if amt is None or amt < MIN_CONSIDERATION:
                continue
            if not is_unknown_attach(ev):
                continue
            candidates.append(ev)

    outcomes = Counter()
    upgrades = 0
    hit_examples = []

    with open(OUT_UPGRADES, "w", encoding="utf-8") as out:
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
        "candidates_in": len(candidates),
        "upgrades_written": upgrades,
        "outcomes": dict(outcomes),
        "hit_examples": hit_examples,
        "spine": spine_stats,
        "paths": {
            "candidates_in": CANDIDATES_IN,
            "out_upgrades": OUT_UPGRADES,
            "out_report": OUT_REPORT,
        }
    }

    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(report)
    print("[ok] wrote:", OUT_UPGRADES)
    print("[ok] report:", OUT_REPORT)

if __name__ == "__main__":
    main()
