import os, re, json, argparse
from collections import Counter, defaultdict

EVENTS_DEFAULT = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_15.ndjson"
OUT_DEFAULT    = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_16.ndjson"
AUD_DEFAULT    = r"publicData/_audit/registry/hampden_axis2_reattach_ge10k_v1_16.json"

SPINE_DEFAULT  = r"publicData/properties/_attached/phase4_assessor_unknown_classify_v1/properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"

def it(p):
    with open(p,'r',encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def write_ndjson(p, rows):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p,'w',encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def as_str(x):
    if x is None: return ""
    if isinstance(x, str): return x
    if isinstance(x, (int,float)): return str(x)
    if isinstance(x, dict):
        for k in ["norm","normalized","value","text","raw","full","address","address_norm","addr","line1","display"]:
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v
        # nested
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
    if isinstance(x, (list,tuple)):
        parts=[]
        for itx in x:
            s=as_str(itx)
            if s: parts.append(s)
        return " ".join(parts)
    return str(x)

SUFFIX_CANON = {
  "STREET":"ST","ST":"ST","ST.":"ST",
  "ROAD":"RD","RD":"RD","RD.":"RD",
  "DRIVE":"DR","DR":"DR","DR.":"DR",
  "CIRCLE":"CIR","CIR":"CIR","CIR.":"CIR","CI":"CIR","CI.":"CIR",
  "TERRACE":"TERR","TERR":"TERR","TERR.":"TERR",
  "AVENUE":"AVE","AVE":"AVE","AVE.":"AVE","AV":"AVE","AV.":"AVE",
  "BOULEVARD":"BLVD","BLVD":"BLVD","BLVD.":"BLVD",
  "LANE":"LA","LA":"LA","LA.":"LA","LN":"LA","LN.":"LA",
  "COURT":"CT","CT":"CT","CT.":"CT",
  "PLACE":"PL","PL":"PL","PL.":"PL",
  "WAY":"WAY","PKWY":"PKY","PARKWAY":"PKY","PKY":"PKY",
}

UNIT_PAT = re.compile(r"\b(APT|UNIT|#|SUITE|STE)\b", re.I)

def canon_suffix_tokens(addr_up):
    toks = [t for t in addr_up.split() if t]
    if not toks:
        return addr_up
    last = toks[-1]
    repl = SUFFIX_CANON.get(last)
    if repl:
        toks[-1] = repl
    return " ".join(toks)

def strip_trailing_unit(addr_up):
    # only strip if we see explicit unit marker and it appears late
    if not UNIT_PAT.search(addr_up):
        return addr_up
    toks = addr_up.split()
    # find last unit marker
    idx = None
    for i,t in enumerate(toks):
        if t in ("APT","UNIT","#","SUITE","STE"):
            idx = i
    if idx is None:
        return addr_up
    return " ".join(toks[:idx]).strip()

def parse_num_and_street(addr_up):
    # expects leading number
    toks = addr_up.split()
    if not toks:
        return None, None
    if not toks[0].isdigit():
        return None, None
    num = toks[0]
    street = " ".join(toks[1:]).strip()
    return num, street if street else None

def build_spine_suffix_canon_index(spine_path, need_towns):
    counts = Counter()
    first_pid = {}
    for r in it(spine_path):
        pr = r.get("property_ref") or r.get("ref") or {}
        town = as_str(r.get("town_norm") or pr.get("town_norm") or r.get("town") or pr.get("town")).upper().strip()
        if not town or town not in need_towns:
            continue

        addr = as_str(r.get("address_norm") or pr.get("address_norm") or r.get("address") or pr.get("address")).upper().strip()
        if not addr:
            continue
        # normalize units away for matching (we only use this for NO_MATCH upgrades)
        base = strip_trailing_unit(addr)
        base = canon_suffix_tokens(base)

        num, street = parse_num_and_street(base)
        if not num or not street:
            continue

        key = f"{town}|{num}|{street}"
        pid = r.get("property_id") or pr.get("property_id")
        if not pid:
            continue

        counts[key] += 1
        if counts[key] == 1:
            first_pid[key] = pid
    return counts, first_pid

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", default=EVENTS_DEFAULT)
    ap.add_argument("--spine", default=SPINE_DEFAULT)
    ap.add_argument("--out", default=OUT_DEFAULT)
    ap.add_argument("--audit", default=AUD_DEFAULT)
    ap.add_argument("--max_upgrades", type=int, default=999999)
    args = ap.parse_args()

    print("=== AXIS2 REATTACH (>=10k) v1_16 (SUFFIX CANON, PRESERVE MULTI) ===")
    print("[info] events:", args.events)
    print("[info] spine :", args.spine)

    rows = list(it(args.events))
    stats = Counter()

    # figure towns we need (ONLY for SINGLE UNKNOWN no_match)
    need_towns = set()
    for ev in rows:
        a = (ev.get("attach") or {})
        if (a.get("attach_scope") == "SINGLE" and (a.get("attach_status") or "").upper() == "UNKNOWN" and (a.get("match_method") == "no_match")):
            pr = ev.get("property_ref") or {}
            town = (pr.get("town_norm") or pr.get("town_raw") or pr.get("town") or "").upper().strip()
            if town:
                need_towns.add(town)

    print("[info] need_towns:", len(need_towns))
    spine_counts, spine_first = build_spine_suffix_canon_index(args.spine, need_towns)
    print("[ok] built suffix-canon spine index keys:", len(spine_counts))

    out_rows = []
    upgrades = 0

    for ev in rows:
        a = (ev.get("attach") or {})
        scope = a.get("attach_scope")
        status = (a.get("attach_status") or "")
        mm = a.get("match_method")

        # preserve multi/partial-multi exactly
        if scope == "MULTI" or status in ("PARTIAL_MULTI",):
            stats["preserved_multi_or_partial_multi"] += 1
            out_rows.append(ev)
            continue

        # only attempt upgrades for SINGLE UNKNOWN no_match
        if not (scope == "SINGLE" and status.upper() == "UNKNOWN" and mm == "no_match"):
            stats["pass_through_other"] += 1
            out_rows.append(ev)
            continue

        pr = ev.get("property_ref") or {}
        town = (pr.get("town_norm") or pr.get("town_raw") or pr.get("town") or "").upper().strip()
        addr = (pr.get("address_norm") or pr.get("address_raw") or pr.get("address") or "").upper().strip()

        # safety
        base = strip_trailing_unit(addr)
        base = canon_suffix_tokens(base)

        num, street = parse_num_and_street(base)
        if not num or not street or not town:
            stats["single_still_unknown__no_num_or_bad_addr"] += 1
            out_rows.append(ev)
            continue

        key = f"{town}|{num}|{street}"
        if spine_counts.get(key, 0) == 1 and key in spine_first and upgrades < args.max_upgrades:
            pid = spine_first[key]
            # update attach block defensibly
            ev2 = ev
            ev2.setdefault("attach", {})
            ev2["attach"]["attach_scope"] = "SINGLE"
            ev2["attach"]["attach_status"] = "ATTACHED_A"
            ev2["attach"]["property_id"] = pid
            ev2["attach"]["match_method"] = "axis2_suffix_canon_unique"
            ev2["attach"]["match_key"] = key
            ev2["attach"]["why"] = None
            ev2.setdefault("meta", {})
            ev2["meta"].setdefault("qa_flags", [])
            stats["single_upgraded_to_attached"] += 1
            upgrades += 1
            out_rows.append(ev2)
        else:
            # keep unknown
            stats["single_still_unknown__no_match"] += 1
            out_rows.append(ev)

    write_ndjson(args.out, out_rows)

    audit = {
        "events": args.events,
        "spine": args.spine,
        "out": args.out,
        "stats": dict(stats),
        "need_towns": sorted(list(need_towns))[:50],
        "note": "v1_16 only upgrades SINGLE UNKNOWN no_match using suffix-canon unique-only key town|num|street; preserves MULTI/PARTIAL_MULTI."
    }
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit,'w',encoding='utf-8') as f:
        json.dump(audit, f, indent=2)

    print(json.dumps({"out": args.out, "audit": args.audit, "stats": dict(stats)}, indent=2))

if __name__ == "__main__":
    main()
