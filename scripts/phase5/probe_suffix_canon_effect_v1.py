import json, re, argparse
from collections import Counter, defaultdict



def as_str(x):
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (int, float)):
        return str(x)
    if isinstance(x, dict):
        # common patterns we’ve seen in EquityLens spine rows
        for k in ["norm","normalized","value","text","raw","full","address","address_norm","addr","line1","display"]:
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v
        # sometimes nested
        for k in ["address","addr"]:
            v = x.get(k)
            if isinstance(v, dict):
                for kk in ["norm","normalized","value","text","raw","full","line1","display"]:
                    vv = v.get(kk)
                    if isinstance(vv, str) and vv.strip():
                        return vv
        try:
            return json.dumps(x, ensure_ascii=False)
        except Exception:
            return str(x)
    if isinstance(x, (list, tuple)):
        # join simple string tokens if present
        parts = []
        for it in x:
            s = as_str(it)
            if s:
                parts.append(s)
        return " ".join(parts)
    return str(x)# --- Canonicalization (deterministic) ---
SUFFIX_CANON = {
  "ST": "ST", "STREET":"ST",
  "RD":"RD", "ROAD":"RD",
  "DR":"DR", "DRIVE":"DR",
  "AVE":"AVE", "AV":"AVE", "AVENUE":"AVE",
  "BLVD":"BLVD", "BOULEVARD":"BLVD",
  "LN":"LN", "LANE":"LN", "LA":"LN",
  "TER":"TER", "TERR":"TER", "TERRACE":"TER",
  "CIR":"CIR", "CIRCLE":"CIR", "CI":"CIR",
  "CT":"CT", "COURT":"CT",
  "PL":"PL", "PLACE":"PL",
  "PKY":"PKY", "PARKWAY":"PKY",
  "WAY":"WAY",
}

UNIT_MARKERS = {"UNIT","APT","APARTMENT","STE","SUITE","#"}

def norm_spaces(s:str)->str:
    return re.sub(r"\s+"," ",(s or "").strip())

def strip_trailing_y(s:str)->str:
    # you have '... Y' artifacts in raw blocks sometimes
    return re.sub(r"\s+Y$","",s.strip())

def split_unit(addr_tokens):
    # returns (base_tokens, unit_tokens)
    toks = addr_tokens[:]
    base=[]
    unit=[]
    i=0
    while i < len(toks):
        t=toks[i]
        if t in UNIT_MARKERS:
            unit = toks[i:]  # keep marker+value
            break
        base.append(t)
        i += 1
    return base, unit

def canon_suffix(tokens):
    if not tokens: return tokens
    t = tokens[-1]
    c = SUFFIX_CANON.get(t, t)
    return tokens[:-1] + [c]

def canon_addr(addr_norm:str)->str:
    a = norm_spaces(strip_trailing_y(addr_norm.upper()))
    toks = a.split(" ")
    base, unit = split_unit(toks)
    base = canon_suffix(base)
    # normalize UNIT marker to "UNIT" + value if we have one
    if unit:
        # best-effort: UNIT <value>
        val = unit[1] if len(unit) > 1 else ""
        unit_norm = ["UNIT"] + ([val] if val else [])
        toks2 = base + unit_norm
    else:
        toks2 = base
    return " ".join([t for t in toks2 if t])

def it(p):
    with open(p,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if line:
                yield json.loads(line)

def load_spine(spine_path):
    # Build counts so we can do UNIQUE-only safely
    full_counts = Counter()
    full_to_pid = {}
    full_to_multi = defaultdict(list)

    with open(spine_path,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line: continue
            r = json.loads(line)
            pr = r.get("property_ref") or r.get("ref") or {}
            town = as_str(r.get("town_norm") or pr.get("town_norm") or r.get("town")).upper()
            addr = as_str(r.get("address_norm") or pr.get("address_norm") or r.get("address")).upper()
            if not town or not addr: 
                continue
            key = f"{town}|{norm_spaces(addr)}"
            pid = r.get("property_id") or r.get("id")
            full_counts[key] += 1
            if pid:
                full_to_multi[key].append(pid)
                if full_counts[key] == 1:
                    full_to_pid[key] = pid
                else:
                    # collision -> remove "unique" pointer
                    if key in full_to_pid:
                        del full_to_pid[key]

    return full_counts, full_to_pid

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--max", type=int, default=10)
    args=ap.parse_args()

    full_counts, full_unique = load_spine(args.spine)

    stats = Counter()
    examples = []

    for ev in it(args.events):
        a = ev.get("attach") or {}
        if (a.get("attach_scope") or "").upper() != "SINGLE": 
            continue
        if (a.get("attach_status") or "").upper() != "UNKNOWN":
            continue
        if (a.get("match_method") or "").lower() != "no_match":
            continue

        pr = ev.get("property_ref") or {}
        town = (pr.get("town_norm") or pr.get("town_raw") or "").upper()
        addr = pr.get("address_norm") or pr.get("address_raw") or ""
        addr = norm_spaces(strip_trailing_y(addr.upper()))
        key_raw = f"{town}|{addr}"
        key_can = f"{town}|{canon_addr(addr)}"

        raw_hit = key_raw in full_counts
        can_hit = key_can in full_counts
        can_unique = key_can in full_unique

        stats["unknown_no_match_total"] += 1
        if raw_hit: stats["raw_key_exists_in_spine"] += 1
        if can_hit: stats["canon_key_exists_in_spine"] += 1
        if can_unique: stats["canon_key_unique_in_spine"] += 1

        if can_unique and not raw_hit and len(examples) < args.max:
            examples.append({
                "event_id": ev.get("event_id"),
                "town": town,
                "addr_raw": addr,
                "addr_can": canon_addr(addr),
                "would_attach_property_id": full_unique.get(key_can),
            })

    print(json.dumps({"events": args.events, "spine": args.spine, "stats": dict(stats)}, indent=2))
    if examples:
        print("\n--- examples (canon unique fixes) ---")
        for ex in examples:
            print(json.dumps(ex, ensure_ascii=False))

if __name__=="__main__":
    main()

