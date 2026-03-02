import argparse, json, re, time, os
from collections import defaultdict

def load_ndjson(path):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line: continue
            yield json.loads(line)

def norm_ws(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip())

# Deterministic suffix aliasing (defensible, no guessing)
SUFFIX_ALIAS = {
    "LA": "LN",      # Lane
    "LANE": "LN",
    "LN": "LN",

    "PKY": "PKWY",
    "PKWY": "PKWY",

    "TERR": "TER",
    "TERRACE": "TER",
    "TER": "TER",

    "AV": "AVE",
    "AVENUE": "AVE",
    "AVE": "AVE",

    "STREET": "ST",
    "ST": "ST",

    "ROAD": "RD",
    "RD": "RD",

    "DRIVE": "DR",
    "DR": "DR",

    "COURT": "CT",
    "CT": "CT",
}

def tokenize_addr(s: str):
    s = norm_ws(s).upper()
    if not s:
        return []
    # keep alnum and spaces; drop punctuation
    s = re.sub(r'[^A-Z0-9\s]', ' ', s)
    s = norm_ws(s)
    return s.split(' ') if s else []

def apply_suffix_alias(tokens):
    if not tokens:
        return tokens
    # If last token is a suffix-like word, normalize it.
    last = tokens[-1]
    rep = SUFFIX_ALIAS.get(last)
    if rep:
        tokens = tokens[:-1] + [rep]
    return tokens

def split_num_and_rest(tokens):
    # expects address like: ["10V","FEDERAL","LA"] or ["2173","MAIN","ST"]
    if not tokens:
        return ("", [])
    num = tokens[0]
    rest = tokens[1:]
    return (num, rest)

def num_variants(num: str):
    # Allow exact numeric; and also strip trailing alpha suffix (10V -> 10)
    num = (num or "").strip().upper()
    if not num:
        return []
    out = [num]
    m = re.match(r'^(\d+)[A-Z]+$', num)
    if m:
        out.append(m.group(1))
    # dash ranges: 26-28 -> treat as both ends only if present
    m2 = re.match(r'^(\d+)\s*-\s*(\d+)$', num)
    if m2:
        out.append(m2.group(1))
        out.append(m2.group(2))
    return list(dict.fromkeys(out))

def street_key_from_rest(rest_tokens):
    # Remove unit tokens and known unit markers
    if not rest_tokens:
        return ""
    toks = []
    i = 0
    while i < len(rest_tokens):
        t = rest_tokens[i]
        if t in ("UNIT","APT","#"):
            break
        toks.append(t)
        i += 1
    toks = apply_suffix_alias(toks)
    return " ".join(toks).strip()

def make_full_key(town: str, num: str, street: str):
    town = norm_ws(town).upper()
    street = norm_ws(street).upper()
    num = (num or "").strip().upper()
    if not town or not street or not num:
        return ""
    return f"{town}|{num}|{street}"

def make_street_only_key(town: str, street: str):
    town = norm_ws(town).upper()
    street = norm_ws(street).upper()
    if not town or not street:
        return ""
    return f"{town}|{street}"

def build_spine_index(spine_path, towns_needed_set):
    idx_full = defaultdict(list)
    idx_street = defaultdict(list)

    scanned = 0
    kept = 0
    no_key = 0
    town_skip = 0
    t0 = time.time()

    with open(spine_path, 'r', encoding='utf-8') as f:
        for line in f:
            scanned += 1
            if not line.strip():
                continue
            r = json.loads(line)

            town = (r.get("town") or "").strip().upper()
            if towns_needed_set and town not in towns_needed_set:
                town_skip += 1
                continue

            full_addr = r.get("full_address") or ""
            full_addr = norm_ws(full_addr).upper()
            if not full_addr:
                no_key += 1
                continue

            # Build from spine's own fields if present (preferred)
            street_no = (r.get("street_no") or "").strip().upper()
            street_name = (r.get("street_name") or "").strip().upper()

            # fallback: parse full_address if needed
            if not street_no or not street_name:
                toks = tokenize_addr(full_addr)
                if not toks:
                    no_key += 1
                    continue
                street_no, rest = split_num_and_rest(toks)
                street_name = street_key_from_rest(rest)

            # Normalize suffix on street_name tokens
            street_name = street_key_from_rest(tokenize_addr(street_name))

            if not town or not street_no or not street_name:
                no_key += 1
                continue

            kfull = make_full_key(town, street_no, street_name)
            kstreet = make_street_only_key(town, street_name)

            if kfull:
                idx_full[kfull].append(r.get("property_id"))
            if kstreet:
                idx_street[kstreet].append(r.get("property_id"))

            kept += 1
            if scanned % 200000 == 0:
                print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} idx_full={len(idx_full)} elapsed_s={time.time()-t0:.1f}")

    debug = {"scanned_rows": scanned, "kept_rows": kept, "no_key": no_key, "town_skip": town_skip}
    return idx_full, idx_street, debug

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    print("=== AXIS2 REATTACH (>=10k) v1_22b (suffix alias incl LA->LN) ===")
    print("[info] events:", args.events)
    print("[info] spine :", args.spine)

    events = list(load_ndjson(args.events))
    towns_needed = sorted({(e.get("property_ref") or {}).get("town_norm","").strip().upper() for e in events if (e.get("property_ref") or {}).get("town_norm")})
    towns_needed_set = set([t for t in towns_needed if t])

    print(f"[info] events rows: {len(events)} towns_needed: {len(towns_needed_set)}")

    print("[info] building spine index (town-filtered)...")
    t0 = time.time()
    idx_full, idx_street, debug = build_spine_index(args.spine, towns_needed_set)
    print(f"[ok] spine index built full={len(idx_full)} street={len(idx_street)} debug={debug} elapsed_s={time.time()-t0:.1f}")

    out_rows = 0
    attached = 0
    still_unknown = 0

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.out, 'w', encoding='utf-8') as fo:
        for e in events:
            pr = e.get("property_ref") or {}
            town = (pr.get("town_norm") or pr.get("town_raw") or "").strip().upper()
            addr = (e.get("addr") or pr.get("address_raw") or pr.get("address_norm") or "").strip().upper()
            toks = tokenize_addr(addr)
            num, rest = split_num_and_rest(toks)
            street = street_key_from_rest(rest)

            # Try full-key, then street-only if unique
            prop_id = None
            method = None

            for nv in num_variants(num):
                k = make_full_key(town, nv, street)
                if k and k in idx_full and len(idx_full[k]) == 1:
                    prop_id = idx_full[k][0]
                    method = "axis2_full_unique_suffix_alias"
                    break

            if not prop_id:
                ks = make_street_only_key(town, street)
                if ks and ks in idx_street and len(idx_street[ks]) == 1:
                    prop_id = idx_street[ks][0]
                    method = "axis2_street_unique_suffix_alias"

            if prop_id:
                e.setdefault("attach", {})
                e["attach"]["attach_status"] = "ATTACHED_A"
                e["attach"]["attach_method"] = method
                e["attach"]["property_id"] = prop_id
                attached += 1
            else:
                # keep existing status if present; else mark unknown
                e.setdefault("attach", {})
                if (e["attach"].get("attach_status") or "").upper() != "ATTACHED_A":
                    e["attach"]["attach_status"] = "UNKNOWN"
                still_unknown += 1

            fo.write(json.dumps(e, ensure_ascii=False) + "\n")
            out_rows += 1

    audit = {
        "in_events": args.events,
        "in_spine": args.spine,
        "out": args.out,
        "stats": {"out_rows": out_rows, "attached_a": attached, "still_unknown": still_unknown},
        "spine_index_debug": debug,
    }
    with open(args.audit, 'w', encoding='utf-8') as fa:
        json.dump(audit, fa, indent=2)

    print(f"[done] wrote out_rows={out_rows} stats={{'attached_a': {attached}, 'still_unknown': {still_unknown}}} audit={args.audit}")

if __name__ == "__main__":
    main()
