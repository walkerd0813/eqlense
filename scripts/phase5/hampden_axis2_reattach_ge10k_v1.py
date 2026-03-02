import json, os, re
from collections import Counter, defaultdict

IN_EVENTS = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/events_attached_DEED_ONLY_v1_8_1_MULTI.ndjson"
IN_CAND  = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k.ndjson"

OUT_DIR  = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_2_AXIS2"
OUT_ND   = os.path.join(OUT_DIR, "events_axis2_upgrades_ge10k.ndjson")
OUT_REP  = os.path.join(OUT_DIR, "axis2_report_ge10k.json")

# deterministic-only gates
MIN_AMOUNT = 10000.0
TARGET_STATUSES = {"UNKNOWN", "PARTIAL_MULTI"}

# small, deterministic suffix normalization (match the style of your pipeline)
SUFFIX_MAP = {
  "LA": "LN",
  "LANE": "LN",
  "LN": "LN",
  "TERR": "TER",
  "TERRACE": "TER",
  "TER": "TER",
  "PKY": "PKWY",
  "PARKWAY": "PKWY",
  "PKWY": "PKWY",
  "BLVD": "BLVD",
  "ST": "ST",
  "AVE": "AVE",
  "RD": "RD",
  "DR": "DR",
  "CT": "CT",
  "CIR": "CIR",
}

RE_WS = re.compile(r"\s+")
RE_TRAIL_Y = re.compile(r"(?:\s+Y)+\s*$", re.IGNORECASE)
RE_UNIT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#)\s*[A-Z0-9\-]+\b", re.IGNORECASE)
RE_LOT  = re.compile(r"\bLOT\s*[A-Z0-9\-]+\b", re.IGNORECASE)
RE_RANGE = re.compile(r"^(?P<a>\d+)\s*-\s*(?P<b>\d+)\s+(?P<rest>.+)$")

def clean_addr(s: str) -> str:
    if not s: return ""
    s = s.strip().upper()
    s = RE_TRAIL_Y.sub("", s)          # kill the trailing Y poison
    s = RE_WS.sub(" ", s)

    # strip obvious unit / lot tokens (deterministic)
    s = RE_UNIT.sub("", s)
    s = RE_LOT.sub("", s)
    s = RE_WS.sub(" ", s).strip()

    # suffix normalize (last token only)
    parts = s.split(" ")
    if len(parts) >= 2:
        last = parts[-1]
        if last in SUFFIX_MAP:
            parts[-1] = SUFFIX_MAP[last]
    return " ".join(parts)

def expand_range(addr_norm: str):
    m = RE_RANGE.match(addr_norm)
    if not m:
        return [addr_norm]
    a = m.group("a")
    b = m.group("b")
    rest = m.group("rest")
    # deterministic: try endpoints only (no guessing evens/odds list)
    return [f"{a} {rest}", f"{b} {rest}", addr_norm]

def iter_event_variants(ev):
    pr = ev.get("property_ref") or {}
    town = (pr.get("town_norm") or "").strip().upper()
    variants = []

    # primary
    variants.append(pr.get("address_norm") or pr.get("address_raw") or "")

    # multi_address alternates
    for ma in (pr.get("multi_address") or []):
        variants.append(ma.get("address_norm") or ma.get("address_raw") or "")

    # also try raw_block extracted "Addr:" lines if present (still deterministic because it's inside the same record)
    raw_block = (ev.get("document") or {}).get("raw_block") or ""
    for line in raw_block.splitlines():
        if "ADDR:" in line.upper():
            # grab after Addr:
            try:
                after = line.split("Addr:", 1)[1]
            except Exception:
                continue
            variants.append(after)

    # clean + range expansions
    out = []
    for v in variants:
        c = clean_addr(str(v))
        if not c: 
            continue
        out.extend(expand_range(c))

    # de-dupe preserving order
    seen = set()
    dedup = []
    for x in out:
        if x not in seen:
            dedup.append(x); seen.add(x)
    return town, dedup

def load_spine_index():
    cur = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
    with open(cur, "r", encoding="utf-8") as f:
        ptr = json.load(f)

    spine_path = ptr.get("properties_ndjson")
    if not spine_path or not isinstance(spine_path, str):
        raise RuntimeError("CURRENT pointer JSON missing 'properties_ndjson' string.")

    if not os.path.exists(spine_path):
        raise RuntimeError(f"Spine ndjson path from CURRENT does not exist: {spine_path}")

    idx = {}
    collisions = 0
    rows = 0

    with open(spine_path, "r", encoding="utf-8") as fin:
        for line in fin:
            rows += 1
            r = json.loads(line)

            town = (r.get("town_norm") or r.get("town") or "").strip().upper()
            addr = r.get("address_norm")

            # spine sometimes stores address_norm as dict (e.g. street_no_fix). deterministic fallback:
            if isinstance(addr, dict):
                addr = r.get("address_raw") or r.get("address") or ""

            addr = clean_addr(str(addr))
            if not town or not addr:
                continue

            k = f"{town}|{addr}"
            pid = r.get("property_id")
            if not pid:
                continue

            if k in idx and idx[k] != pid:
                collisions += 1
                idx[k] = None
            else:
                idx[k] = pid

    return idx, {"spine_rows_scanned": rows, "spine_collisions": collisions, "spine_path": spine_path}   

os.makedirs(OUT_DIR, exist_ok=True)

def main():
    spine_idx, spine_stats = load_spine_index()

    # load candidates into a set for fast membership
    cand_ids = set()
    with open(IN_CAND, "r", encoding="utf-8") as fin:
        for line in fin:
            ev = json.loads(line)
            cand_ids.add(ev.get("event_id"))

    stats = Counter()
    examples = []

    with open(IN_EVENTS, "r", encoding="utf-8") as fin, open(OUT_ND, "w", encoding="utf-8") as fout:
        for line in fin:
            ev = json.loads(line)
            eid = ev.get("event_id")
            if eid not in cand_ids:
                continue

            # gate by amount/status
            amt = (ev.get("consideration") or {}).get("amount")
            st  = (ev.get("attach") or {}).get("attach_status")
            if amt is None or float(amt) < MIN_AMOUNT:
                continue
            if st not in TARGET_STATUSES:
                continue

            town, variants = iter_event_variants(ev)
            if not town or not variants:
                stats["skip_no_town_or_variants"] += 1
                continue

            # attempt deterministic matches
            hits = []
            for a in variants:
                k = f"{town}|{a}"
                pid = spine_idx.get(k)
                if pid:
                    hits.append((a, pid, k))

            # if single property event: accept only unique hit
            # if multi: upgrade only the UNKNOWN attachments that now match uniquely
            if not hits:
                stats["no_hits"] += 1
                continue

            # unique pids only
            uniq_pids = list({h[1] for h in hits})
            if len(uniq_pids) != 1:
                stats["multi_pid_hits_ambiguous"] += 1
                continue

            pid = uniq_pids[0]
            # record an axis2 upgrade payload (DO NOT overwrite the main attached file yet)
            out = {
                "event_id": eid,
                "town_norm": town,
                "axis2_property_id": pid,
                "axis2_hit_count": len(hits),
                "axis2_keys": [h[2] for h in hits[:10]],
                "prior_attach_status": st,
                "consideration_amount": amt,
            }
            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            stats["axis2_upgrades_written"] += 1

            if len(examples) < 25:
                examples.append(out)

    report = {
        "in_events": IN_EVENTS,
        "in_candidates": IN_CAND,
        "out_ndjson": OUT_ND,
        "min_amount": MIN_AMOUNT,
        "target_statuses": sorted(TARGET_STATUSES),
        "stats": dict(stats),
        "spine_stats": spine_stats,
        "sample_upgrades": examples,
    }
    with open(OUT_REP, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(report["stats"])
    print("[ok] wrote:", OUT_ND)
    print("[ok] report:", OUT_REP)

if __name__ == "__main__":
    main()





