import argparse, json, re, time
from collections import defaultdict, Counter

def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def up(s: str) -> str:
    return norm_ws(s).upper()

# Strict, auditable suffix aliasing only (no fuzzy spelling guesses)
SUFFIX_ALIAS = {
    # your proven win:
    "LA": "LN",
    # common USPS-ish expansions (safe token swaps)
    "TERR": "TER",
    "TERRACE": "TER",
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "DRIVE": "DR",
    "COURT": "CT",
    "CIRCLE": "CIR",
    "PLACE": "PL",
    "PARKWAY": "PKWY",
    "HIGHWAY": "HWY",
    "ROAD": "RD",
    "STREET": "ST",
}

UNIT_WORDS = {"UNIT", "APT", "APARTMENT", "#", "NO", "NUMBER"}

def normalize_addr_tokens(addr: str) -> str:
    """
    Normalize address string into uppercase token form with suffix alias mapping.
    DOES NOT invent tokens. Only swaps known suffix tokens.
    """
    s = up(addr)
    # keep alnum, spaces, and hyphen (for ranges), drop other punctuation
    s = re.sub(r"[^A-Z0-9\s\-]", " ", s)
    s = norm_ws(s)
    toks = s.split(" ")
    out = []
    for t in toks:
        out.append(SUFFIX_ALIAS.get(t, t))
    return " ".join(out)

def split_num_street_unit(addr_norm: str):
    """
    Try to extract leading number and unit token.
    Returns (num, street_rest, unit_norm or "").
    """
    s = addr_norm
    # normalize unit patterns: "UNIT 203" / "#203" / "APT 203"
    unit = ""
    # convert "# 203" => "UNIT 203"
    s = re.sub(r"\s#\s*([A-Z0-9\-]+)\b", r" UNIT \1", s)
    # collapse "APARTMENT" => "APT"
    s = s.replace("APARTMENT ", "APT ")

    m_unit = re.search(r"\b(UNIT|APT)\s+([A-Z0-9\-]+)\b", s)
    if m_unit:
        unit = f"{m_unit.group(1)} {m_unit.group(2)}"
        # remove unit from base street portion
        s_wo = (s[:m_unit.start()] + " " + s[m_unit.end():]).strip()
        s_wo = norm_ws(s_wo)
    else:
        s_wo = s

    m = re.match(r"^([0-9]+[A-Z]?)\s+(.*)$", s_wo)
    if not m:
        return ("", s_wo, unit)
    num = m.group(1)
    rest = norm_ws(m.group(2))
    return (num, rest, unit)

def make_full_key(town: str, full_addr: str) -> str:
    return f"{town}|{full_addr}"

def make_street_key(town: str, num: str, street_rest: str) -> str:
    return f"{town}|{num}|{street_rest}"

def make_street_unit_key(town: str, num: str, street_rest: str, unit: str) -> str:
    return f"{town}|{num}|{street_rest}|{unit}"

def read_events(path: str):
    rows = []
    towns = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            pr = r.get("property_ref") or {}
            town = up(pr.get("town_norm") or pr.get("town_raw") or "")
            if town:
                towns.add(town)
            rows.append(r)
    return rows, towns

def build_spine_index(spine_path: str, towns_needed: set):
    """
    Build town-filtered indexes:
      - full_key -> property_id (only if unique)
      - street_key -> property_id (only if unique)
      - street_unit_key -> property_id (only if unique)
      - street_only_count per town+street_rest to support gated NO_NUM attach
    """
    t0 = time.time()
    full_map = defaultdict(list)
    street_map = defaultdict(list)
    street_unit_map = defaultdict(list)
    street_only_counts = Counter()

    debug = {"scanned_rows": 0, "kept_rows": 0, "no_key": 0, "town_skip": 0}

    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            debug["scanned_rows"] += 1
            r = json.loads(line)
            town = up(r.get("town") or "")
            if towns_needed and town not in towns_needed:
                debug["town_skip"] += 1
                continue

            fa = r.get("full_address") or ""
            fa_norm = normalize_addr_tokens(fa)
            if not town or not fa_norm:
                debug["no_key"] += 1
                continue

            debug["kept_rows"] += 1
            pid = r.get("property_id") or r.get("property_uid") or r.get("parcel_id") or ""
            if not pid:
                continue

            full_map[make_full_key(town, fa_norm)].append(pid)

            num = up(r.get("street_no") or "")
            street_rest = normalize_addr_tokens(r.get("street_name") or "")
            unit = up(r.get("unit") or "")
            if num and street_rest:
                street_map[make_street_key(town, num, street_rest)].append(pid)
                if unit:
                    street_unit_map[make_street_unit_key(town, num, street_rest, f"UNIT {unit}".replace("UNIT UNIT","UNIT"))].append(pid)
            if street_rest:
                street_only_counts[f"{town}|{street_rest}"] += 1

            if debug["scanned_rows"] % 400000 == 0:
                elapsed = time.time() - t0
                print(f"[progress] scanned_rows={debug['scanned_rows']} kept_rows={debug['kept_rows']} town_skip={debug['town_skip']} elapsed_s={elapsed:.1f}")

    def uniq_map(m):
        out = {}
        for k, v in m.items():
            uv = sorted(set(v))
            if len(uv) == 1:
                out[k] = uv[0]
        return out

    full_u = uniq_map(full_map)
    street_u = uniq_map(street_map)
    street_unit_u = uniq_map(street_unit_map)

    elapsed = time.time() - t0
    print(f"[ok] spine index built full={len(full_u)} street={len(street_u)} street_unit={len(street_unit_u)} debug={debug} elapsed_s={elapsed:.1f}")
    return full_u, street_u, street_unit_u, street_only_counts, debug

def try_attach_single(town: str, addr_raw: str, full_u, street_u, street_unit_u, street_only_counts):
    addr_norm = normalize_addr_tokens(addr_raw)
    num, street_rest, unit = split_num_street_unit(addr_norm)

    # 1) full address exact
    k_full = make_full_key(town, addr_norm)
    pid = full_u.get(k_full)
    if pid:
        return ("ATTACHED_A", "AXIS2_FULL_ADDRESS_EXACT", pid, None)

    # 2) street+unit exact (if unit present)
    if num and street_rest and unit:
        k_su = make_street_unit_key(town, num, street_rest, unit)
        pid = street_unit_u.get(k_su)
        if pid:
            return ("ATTACHED_A", "AXIS2_STREET+UNIT_EXACT", pid, None)

    # 3) street exact
    if num and street_rest:
        k_s = make_street_key(town, num, street_rest)
        pid = street_u.get(k_s)
        if pid:
            return ("ATTACHED_A", "AXIS2_STREET_UNIQUE_EXACT", pid, None)

    # 4) gated street-only (NO_NUM cases only)
    if (not num) and street_rest:
        k_so = f"{town}|{street_rest}"
        if street_only_counts.get(k_so, 0) == 1:
            # find the property by scanning street_u keys (cheap enough for small event set)
            # fallback: no attach if not discoverable deterministically
            # (we do NOT guess)
            return ("UNKNOWN", "NO_NUM", None, "street_only_unique_unresolved")

        return ("UNKNOWN", "NO_NUM", None, "no_num")

    return ("UNKNOWN", "NO_MATCH", None, "no_match")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    print("=== AXIS2 REATTACH (>=10k) v1_23 (MULTI attach + suffix/unit normalization) ===")
    print("[info] events:", args.events)
    print("[info] spine :", args.spine)

    events, towns_needed = read_events(args.events)
    print(f"[info] events rows: {len(events)} towns_needed: {len(towns_needed)}")

    print("[info] building spine index (town-filtered)...")
    full_u, street_u, street_unit_u, street_only_counts, spine_debug = build_spine_index(args.spine, towns_needed)

    stats = Counter()
    out_rows = 0

    with open(args.out, "w", encoding="utf-8") as w:
        for r in events:
            pr = r.get("property_ref") or {}
            town = up(pr.get("town_norm") or pr.get("town_raw") or "")
            addr_raw = pr.get("address_raw") or ""
            multi = pr.get("multi_address")

            attached = False

            # MULTI handling (deterministic):
            if isinstance(multi, list) and len(multi) > 0:
                pids = []
                methods = []
                for a in multi:
                    st, mm, pid, why = try_attach_single(town, a, full_u, street_u, street_unit_u, street_only_counts)
                    if pid:
                        pids.append(pid)
                        methods.append(mm)
                uniq = sorted(set(pids))
                if len(uniq) == 1:
                    r["attach"] = {"attach_status": "ATTACHED_A", "property_id": uniq[0]}
                    r["match_method"] = "AXIS2_MULTI_ALL_SAME"
                    r["why"] = None
                    stats["attached_a"] += 1
                    attached = True
                elif len(uniq) > 1:
                    # partial multi: we found conflicting deterministic hits
                    r["attach"] = {"attach_status": "PARTIAL_MULTI", "property_ids": uniq}
                    r["match_method"] = "AXIS2_MULTI_CONFLICT"
                    r["why"] = "multi_conflict"
                    stats["partial_multi"] += 1
                    attached = True
                else:
                    # leave as-is; will count later
                    stats["multi_unknown"] += 1

            if not attached:
                st, mm, pid, why = try_attach_single(town, addr_raw, full_u, street_u, street_unit_u, street_only_counts)
                if st == "ATTACHED_A":
                    r["attach"] = {"attach_status": "ATTACHED_A", "property_id": pid}
                    r["match_method"] = mm.lower()
                    r["why"] = None
                    stats["attached_a"] += 1
                else:
                    # keep UNKNOWN explicitly (do not pretend)
                    r["attach"] = {"attach_status": "UNKNOWN"}
                    r["match_method"] = mm.lower()
                    r["why"] = why
                    stats["still_unknown"] += 1

            w.write(json.dumps(r, ensure_ascii=False) + "\n")
            out_rows += 1

    audit = {
        "script": "hampden_axis2_reattach_ge10k_v1_23.py",
        "events_in": args.events,
        "spine_in": args.spine,
        "out": args.out,
        "stats": dict(stats),
        "spine_debug": spine_debug,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(f"[done] wrote out_rows={out_rows} stats={dict(stats)} audit={args.audit}")

if __name__ == "__main__":
    main()
