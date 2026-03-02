import argparse, json, os, re
from collections import Counter, defaultdict
from datetime import datetime, timezone

# -----------------------------
# Normalization helpers
# -----------------------------

WS_RE = re.compile(r"\s+")
TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.IGNORECASE)

# minimal + high ROI street suffix normalization
SUFFIX_MAP = {
  "AVENUE":"AVE","AVE":"AVE",
  "STREET":"ST","ST":"ST",
  "ROAD":"RD","RD":"RD",
  "DRIVE":"DR","DR":"DR",
  "LANE":"LN","LN":"LN",
  "LA":"LN",  # Hampden index uses LA often
  "COURT":"CT","CT":"CT",
  "PLACE":"PL","PL":"PL",
  "TERRACE":"TER","TER":"TER",
  "CIRCLE":"CIR","CIR":"CIR",
  "BOULEVARD":"BLVD","BLVD":"BLVD",
  "HIGHWAY":"HWY","HWY":"HWY",
  "HGY":"HWY",
  "PARKWAY":"PKWY","PKWY":"PKWY",
}

UNIT_TOKENS = [" APT ", " UNIT ", " #", " STE ", " SUITE ", " FLOOR ", " FL "]

ADDR_LINE_RE = re.compile(r"Addr:\s*([^\r\n]+)", re.IGNORECASE)
TOWN_LINE_RE = re.compile(r"Town:\s*([A-Z \-]+?)\s+Addr:", re.IGNORECASE)
RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s+(.*)$")

LOT_HEURISTIC_RE = re.compile(r"\bLOT\b|\bPL/W/CTF\b|\bCTF\b|\bPAR\b", re.IGNORECASE)

def collapse_ws(s: str) -> str:
    return WS_RE.sub(" ", (s or "").strip())

def strip_trailing_y(s: str) -> str:
    return TRAILING_Y_RE.sub("", (s or "").strip())

def clean_town(t: str) -> str:
    t = collapse_ws(strip_trailing_y(t))
    # remove trailing artifacts like "Addr"
    t = re.sub(r"\bADDR\b", "", t, flags=re.IGNORECASE)
    return collapse_ws(t).upper()

def clean_addr(a: str) -> str:
    a = collapse_ws(strip_trailing_y(a))
    a = a.upper()

    # remove doubled spaces
    a = collapse_ws(a)

    # normalize common punctuation
    a = a.replace(" ,", ",").replace(" .", ".")
    a = a.replace("’", "'")

    # normalize suffix token (last word) when present
    parts = a.split(" ")
    if len(parts) >= 2:
        last = parts[-1]
        if last in SUFFIX_MAP:
            parts[-1] = SUFFIX_MAP[last]
            a = " ".join(parts)

    return a

def addr_has_unit(addr: str) -> bool:
    a = " " + (addr or "").upper() + " "
    return any(tok in a for tok in UNIT_TOKENS)

def strip_unit(addr: str) -> str:
    """
    Conservative unit-strip:
    If ' UNIT ' or ' APT ' etc exists, keep base address before that token.
    """
    a = (addr or "").upper()
    for tok in [" UNIT ", " APT ", " STE ", " SUITE ", " FLOOR ", " FL "]:
        idx = a.find(tok)
        if idx > 0:
            return a[:idx].strip()
    # handle "#310" style
    if " #" in a:
        return a.split(" #", 1)[0].strip()
    return a.strip()

def expand_range(addr: str, max_span: int = 6):
    """
    Expand numeric ranges like '123-125 RANNEY ST' into candidates:
    123 RANNEY ST, 125 RANNEY ST (and odd/even in between if span small).
    Only expand if span <= max_span to avoid explosion.
    """
    m = RANGE_RE.match(addr or "")
    if not m:
        return []

    a0 = int(m.group(1))
    a1 = int(m.group(2))
    rest = m.group(3).strip()

    lo, hi = (a0, a1) if a0 <= a1 else (a1, a0)
    span = hi - lo
    if span < 1 or span > max_span:
        return []

    # step 2 preserves parity (most US ranges)
    step = 2 if (lo % 2) == (hi % 2) else 1
    return [f"{n} {rest}".strip() for n in range(lo, hi + 1, step)]

def extract_addresses_from_raw_block(raw_block: str):
    """
    Pull repeated 'Addr:' lines from raw_block (multi-address deeds).
    """
    if not raw_block:
        return []
    addrs = []
    for m in ADDR_LINE_RE.finditer(raw_block):
        val = m.group(1).strip()
        # stop at trailing artifacts if present
        addrs.append(val)
    # de-dupe preserving order
    seen=set()
    out=[]
    for a in addrs:
        k=collapse_ws(a).upper()
        if k in seen: 
            continue
        seen.add(k)
        out.append(a)
    return out

def should_skip_as_postal_locator(addr: str) -> bool:
    """
    Some deed lines are legal descriptions not postal addresses: 'AUTUMN RIDGE RD LOT 54'
    These will not match spine. We keep as UNKNOWN (by design).
    """
    if not addr:
        return True
    a = addr.upper()
    # strong signal of non-postal locator in this dataset
    if " LOT " in (" " + a + " "):
        return True
    if LOT_HEURISTIC_RE.search(a):
        return True
    return False

# -----------------------------
# IO helpers
# -----------------------------

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            yield json.loads(line)

def resolve_spine_path(spine_path: str) -> str:
    """
    Your CURRENT spine pointer is a small JSON file containing a ndjson path in 'properties_ndjson'.
    """
    if spine_path.lower().endswith(".ndjson"):
        return spine_path

    with open(spine_path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    nd = obj.get("properties_ndjson")
    if not nd:
        raise RuntimeError(f"Spine pointer JSON missing properties_ndjson: {spine_path}")
    return nd

def build_spine_index(spine_path_ndjson: str, allowed_towns: set):
    """
    Index keys: TOWN|ADDR  -> property_id
    """
    idx = {}
    meta = {"spine_rows_seen": 0, "spine_rows_indexed": 0, "spine_index_keys": 0}

    for p in iter_ndjson(spine_path_ndjson):
        meta["spine_rows_seen"] += 1

        town_raw = p.get("town") or p.get("TOWN") or ""
        town = clean_town(town_raw)
        if allowed_towns and town not in allowed_towns:
            continue

        # prefer full_address; avoid address_label when it is "poison"
        addr_raw = p.get("full_address") or p.get("FULL_ADDRESS") or p.get("address") or p.get("ADDRESS") or ""
        addr = clean_addr(addr_raw)

        if not town or not addr:
            continue

        key = f"{town}|{addr}"
        pid = p.get("property_id") or p.get("PROPERTY_ID")
        if not pid:
            continue

        idx[key] = pid
        meta["spine_rows_indexed"] += 1

    meta["spine_index_keys"] = len(idx)
    return idx, meta

def load_allowed_towns_from_events(events_dir: str):
    """
    Institutional: restrict to towns present in events so spine indexing is bounded.
    """
    towns=set()
    for fn in os.listdir(events_dir):
        if not fn.endswith(".ndjson"):
            continue
        for ev in iter_ndjson(os.path.join(events_dir, fn)):
            pr = ev.get("property_ref") or {}
            t = pr.get("town_raw") or pr.get("town") or ""
            t = clean_town(t)
            if t:
                towns.add(t)
    return towns

# -----------------------------
# Candidate generator
# -----------------------------

def build_candidates(ev):
    pr = ev.get("property_ref") or {}

    town_raw = pr.get("town_raw") or pr.get("town") or ""
    addr_raw = pr.get("address_raw") or pr.get("address") or ""

    raw_block = (ev.get("document") or {}).get("raw_block") or pr.get("raw_block") or ""
    # multi-address extraction from raw_block
    raw_addrs = extract_addresses_from_raw_block(raw_block)
    if raw_addrs:
        base_addrs = raw_addrs
    else:
        base_addrs = [addr_raw]

    town = clean_town(town_raw)
    if not town:
        return [], {"town_raw": town_raw, "addr_raw": addr_raw}

    candidates = []
    seen=set()

    for a in base_addrs:
        a0 = clean_addr(a)
        if not a0:
            continue

        # If this looks like legal description "LOT ..." keep it out of attach attempts (remains UNKNOWN)
        if should_skip_as_postal_locator(a0):
            continue

        # 1) direct
        for tag, addr_variant in [("direct", a0)]:
            key = f"{town}|{addr_variant}"
            if key not in seen:
                seen.add(key)
                candidates.append({"key": key, "town": town, "addr": addr_variant, "method": tag})

        # 2) unit strip
        if addr_has_unit(a0):
            base = strip_unit(a0)
            base = clean_addr(base)
            if base:
                key = f"{town}|{base}"
                if key not in seen:
                    seen.add(key)
                    candidates.append({"key": key, "town": town, "addr": base, "method": "strip_unit"})

        # 3) range expand
        expanded = expand_range(a0, max_span=6)
        for ex in expanded:
            exn = clean_addr(ex)
            if exn:
                key = f"{town}|{exn}"
                if key not in seen:
                    seen.add(key)
                    candidates.append({"key": key, "town": town, "addr": exn, "method": "range_expand"})

    return candidates, {"town_raw": town_raw, "addr_raw": addr_raw}

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eventsDir", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    allowed_towns = load_allowed_towns_from_events(args.eventsDir)
    spine_resolved = resolve_spine_path(args.spine)

    spine_idx, spine_meta = build_spine_index(spine_resolved, allowed_towns)

    counts = Counter()
    match_methods = Counter()
    unknown_reason = Counter()

    samples = {
        "unknown_other_key_mismatch": [],
        "unknown_lot_like": [],
        "unknown_no_candidate": [],
    }

    events_total = 0

    with open(args.out, "w", encoding="utf-8") as out_f:
        for fn in os.listdir(args.eventsDir):
            if not fn.endswith(".ndjson"):
                continue
            src_path = os.path.join(args.eventsDir, fn)
            for ev in iter_ndjson(src_path):
                events_total += 1

                # default attach object
                ev["attach"] = ev.get("attach") or {}
                ev["attach"]["status"] = None
                ev["attach"]["property_id"] = None
                ev["attach"]["attach_method"] = None
                ev["attach"]["matched_key"] = None
                ev["attach"]["candidates_considered"] = 0

                pr = ev.get("property_ref") or {}
                town_raw = pr.get("town_raw") or pr.get("town") or ""
                addr_raw = pr.get("address_raw") or pr.get("address") or ""

                if not (town_raw and addr_raw):
                    ev["attach"]["status"] = "MISSING_TOWN_OR_ADDRESS"
                    counts["MISSING_TOWN_OR_ADDRESS"] += 1
                    out_f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                    continue

                candidates, dbg = build_candidates(ev)
                ev["attach"]["candidates_considered"] = len(candidates)

                if not candidates:
                    ev["attach"]["status"] = "UNKNOWN"
                    counts["UNKNOWN"] += 1
                    unknown_reason["no_candidates"] += 1
                    if len(samples["unknown_no_candidate"]) < 10:
                        samples["unknown_no_candidate"].append({"town_raw": town_raw, "addr_raw": addr_raw, "doc": (ev.get("recording") or {}).get("document_number")})
                    out_f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                    continue

                matched = None
                for c in candidates:
                    pid = spine_idx.get(c["key"])
                    if pid:
                        matched = (pid, c)
                        break

                if matched:
                    pid, c = matched
                    ev["attach"]["status"] = "ATTACHED_A"
                    ev["attach"]["property_id"] = pid
                    ev["attach"]["attach_method"] = c["method"]
                    ev["attach"]["matched_key"] = c["key"]
                    counts["ATTACHED_A"] += 1
                    match_methods[c["method"]] += 1
                else:
                    ev["attach"]["status"] = "UNKNOWN"
                    counts["UNKNOWN"] += 1
                    unknown_reason["key_mismatch"] += 1
                    if len(samples["unknown_other_key_mismatch"]) < 10:
                        samples["unknown_other_key_mismatch"].append({"town_raw": town_raw, "addr_raw": addr_raw, "doc": (ev.get("recording") or {}).get("document_number")})

                out_f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "events_dir": args.eventsDir,
        "spine_path_input": args.spine,
        "spine_path_resolved": spine_resolved,
        "allowed_towns_count": len(allowed_towns),
        "spine_meta": spine_meta,
        "events_total": events_total,
        "attach_status_counts": dict(counts),
        "match_methods": dict(match_methods),
        "unknown_reason": dict(unknown_reason),
        "samples": samples,
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2, ensure_ascii=False)

    print("[done] allowed_towns_count:", len(allowed_towns))
    print("[done] spine_path_input:", args.spine)
    print("[done] spine_path_resolved:", spine_resolved)
    print("[done] spine_rows_seen:", spine_meta["spine_rows_seen"])
    print("[done] spine_rows_indexed:", spine_meta["spine_rows_indexed"])
    print("[done] spine_index_keys:", spine_meta["spine_index_keys"])
    print("[done] events_total:", events_total)
    print("[done] attach_status_counts:", dict(counts))
    print("[done] match_methods:", dict(match_methods))
    print("[done] out:", args.out)
    print("[done] audit:", args.audit)

if __name__ == "__main__":
    main()
