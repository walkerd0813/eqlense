#!/usr/bin/env python3
import argparse, json, os, re, hashlib
from datetime import datetime

NUMWORD = {"ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5","SIX":"6","SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10"}

SUFFIX_ALIAS = {
  "AVE":"AV","AV":"AVE",
  "ST":"STREET","STREET":"ST",
  "RD":"ROAD","ROAD":"RD",
  "DR":"DRIVE","DRIVE":"DR",
  "LN":"LANE","LANE":"LN",
  "PKY":"PKWY","PKWY":"PKY",
  "BLVD":"BOULEVARD","BOULEVARD":"BLVD",
  "TER":"TERRACE","TERRACE":"TER",
  "CT":"COURT","COURT":"CT",
  "PL":"PLACE","PLACE":"PL",
  "CIR":"CIRCLE","CIRCLE":"CIR",
  "HWY":"HIGHWAY","HIGHWAY":"HWY",
  "HGY":"HWY",
}

RE_MULTI_RANGE = re.compile(r"^(?P<a>\d+)\s*-\s*(?P<b>\d+)\s*(?P<rest>.+)$")
RE_UNIT = re.compile(r"\b(UNIT|APT|APARTMENT|#|PH|PENTHOUSE|BSMT|BASEMENT|FL|FLOOR|RM|ROOM|STE|SUITE)\b\s*([A-Z0-9\-]+)?\b", re.IGNORECASE)
RE_LOT  = re.compile(r"\bLOT\b[\s#]*([0-9]+|[A-Z])?\b", re.IGNORECASE)

def sha256_file(path:str)->str:
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda:f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def norm_town(t):
    if not t: return None
    return re.sub(r"\s+"," ", str(t).strip()).upper()

def norm_addr(a):
    if a is None: return None
    if not isinstance(a, str):
        a = str(a)
    s = re.sub(r"\s+"," ", a.strip()).upper()
    s = re.sub(r"\s+Y$", "", s).strip()
    parts = s.split(" ")
    if parts and parts[0] in NUMWORD:
        parts[0] = NUMWORD[parts[0]]
    return " ".join(parts)

def pick_spine_addr_str(r: dict):
    # only accept real strings (avoid dict metadata fields)
    for k in ("address_norm","address","address_raw","address_full","site_address","full_address","address1"):
        v = r.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return None

def strip_unit(addr_norm):
    if not addr_norm: return addr_norm
    s = re.sub(RE_UNIT, "", addr_norm).strip()
    return re.sub(r"\s+"," ", s).strip()

def strip_lot(addr_norm):
    if not addr_norm: return addr_norm
    s = re.sub(RE_LOT, "", addr_norm).strip()
    return re.sub(r"\s+"," ", s).strip()

def expand_range(addr_norm, max_span=10):
    m = RE_MULTI_RANGE.match(addr_norm or "")
    if not m: return []
    a = int(m.group("a")); b = int(m.group("b"))
    rest = m.group("rest").strip()
    lo = min(a,b); hi = max(a,b)
    span = hi - lo
    if span <= max_span:
        return [f"{n} {rest}" for n in range(lo, hi+1)]
    return [f"{lo} {rest}", f"{hi} {rest}"]

def suffix_alias_variants(addr_norm):
    toks = (addr_norm or "").split(" ")
    if not toks: return []
    last = toks[-1]
    out=[]
    if last in SUFFIX_ALIAS:
        out.append(" ".join(toks[:-1] + [SUFFIX_ALIAS[last]]))
    if last == "HGY":
        out += [" ".join(toks[:-1] + ["HWY"]), " ".join(toks[:-1] + ["HIGHWAY"])]
    if last == "HWY":
        out += [" ".join(toks[:-1] + ["HIGHWAY"]), " ".join(toks[:-1] + ["HGY"])]
    if last == "HIGHWAY":
        out += [" ".join(toks[:-1] + ["HWY"])]
    seen=set(); ded=[]
    for v in out:
        if v and v not in seen:
            seen.add(v); ded.append(v)
    return ded

def condo_cdr_variants(addr_norm):
    if not addr_norm: return []
    toks = addr_norm.split(" ")
    if "CDR" not in toks: return []
    i = toks.index("CDR")
    cands = [
        " ".join(toks[:i] + ["C","DR"] + toks[i+1:]),
        " ".join(toks[:i] + ["CIR","DR"] + toks[i+1:]),
        " ".join(toks[:i] + ["CIRCLE","DR"] + toks[i+1:]),
    ]
    seen=set(); out=[]
    for v in cands:
        if v and v not in seen:
            seen.add(v); out.append(v)
    return out

def addr_variants_event_side(addr_norm, max_range_span=10):
    """
    IMPORTANT: variants are generated ONLY for the EVENT address,
    not for the spine index. This keeps the spine index small.
    Returns list of (variant, tier, method_tag)
      Tier A: direct
      Tier B: deterministic transforms (unit/lot/suffix/range/cdr)
    """
    out=[]; seen=set()
    def add(v, tier, tag):
        if not v or v in seen: return
        seen.add(v); out.append((v, tier, tag))

    if not addr_norm: return out

    add(addr_norm, "A", "direct")

    su = strip_unit(addr_norm)
    if su != addr_norm: add(su, "B", "strip_unit")

    sl = strip_lot(addr_norm)
    if sl != addr_norm: add(sl, "B", "strip_lot")

    sul = strip_lot(su)
    if sul and sul not in (addr_norm, su, sl): add(sul, "B", "strip_unit+lot")

    for v in expand_range(addr_norm, max_span=max_range_span):
        add(v, "B", "range_expand")

    for v in suffix_alias_variants(addr_norm):
        add(v, "B", "suffix_alias")

    for v in condo_cdr_variants(addr_norm):
        add(v, "B", "condo_cdr_expand")

    # combos
    for base, tag in [(su,"strip_unit"), (sl,"strip_lot"), (sul,"strip_unit+lot")]:
        if not base: continue
        for v in suffix_alias_variants(base):
            add(v, "B", f"{tag}+suffix_alias")
        for v in condo_cdr_variants(base):
            add(v, "B", f"{tag}+condo_cdr")

    return out

def load_spine_pointer(pointer_json):
    with open(pointer_json, "r", encoding="utf-8") as f:
        raw = f.read().strip()

    # direct path
    cand = raw.strip().strip('"')
    if cand.lower().endswith(".ndjson") and os.path.exists(cand):
        return cand

    try:
        obj = json.loads(raw)
    except Exception:
        raise RuntimeError("Spine CURRENT pointer is not JSON and not a direct .ndjson path.")

    found=[]
    def walk(x):
        if isinstance(x, dict):
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)
        elif isinstance(x, str):
            s = x.strip().strip('"')
            if s.lower().endswith(".ndjson"):
                found.append(s)
    walk(obj)

    for p in found:
        if os.path.exists(p):
            return p

    raise RuntimeError("Could not resolve spine ndjson path from CURRENT pointer JSON.")

def build_spine_index(spine_ndjson, towns_set=None, collision_cap=5):
    """
    Spine index is CANONICAL ONLY: town|address_norm -> property_id or collision list marker.
    We do NOT generate variants for spine rows.
    """
    index={}
    collisions=0
    rows_seen=0
    rows_indexed=0
    collision_samples={}

    with open(spine_ndjson, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            rows_seen += 1
            try:
                r=json.loads(line)
            except Exception:
                continue

            town = norm_town(r.get("town_norm") or r.get("town") or r.get("municipality") or r.get("city"))
            if not town: 
                continue
            if towns_set is not None and town not in towns_set:
                continue

            addr_raw = pick_spine_addr_str(r)
            if not addr_raw:
                continue

            addr = norm_addr(addr_raw)
            pid = r.get("property_id")
            if not addr or not pid:
                continue

            rows_indexed += 1
            key = f"{town}|{addr}"

            if key not in index:
                index[key] = pid
            else:
                # collision: store marker + sample pids
                if index[key] != "__COLLISION__":
                    # first time we detect collision
                    prev = index[key]
                    index[key] = "__COLLISION__"
                    collision_samples[key] = [prev]
                collisions += 1
                # keep sample of pids
                lst = collision_samples.get(key, [])
                if len(lst) < collision_cap and pid not in lst:
                    lst.append(pid)
                collision_samples[key] = lst

    stats = {
        "spine_rows_seen": rows_seen,
        "spine_rows_indexed": rows_indexed,
        "spine_keys": len(index),
        "collision_events": collisions,
        "collision_keys_sampled": len(collision_samples)
    }
    return index, stats, collision_samples

def attach_one(town_norm, addr_norm, index, max_range_span=10):
    if not town_norm or not addr_norm:
        return ("MISSING_TOWN_OR_ADDRESS", None, None, None, None)

    for av, tier, tag in addr_variants_event_side(addr_norm, max_range_span=max_range_span):
        key = f"{town_norm}|{av}"
        if key in index:
            if index[key] == "__COLLISION__":
                return ("UNKNOWN", None, tier, "collision", key)
            return ("ATTACHED_A", index[key], tier, tag, key)

    return ("UNKNOWN", None, None, "no_match", f"{town_norm}|{addr_norm}")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)   # CURRENT pointer json
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", default="events.registry_attach_to_spine_deterministic_v1_0")
    ap.add_argument("--county", default="")
    ap.add_argument("--towns", default="")  # optional CSV list
    ap.add_argument("--max_range_span", type=int, default=10)
    ap.add_argument("--collision_cap", type=int, default=5)
    args=ap.parse_args()

    # towns scope
    towns_set=None
    if args.towns.strip():
        towns_set=set(norm_town(x) for x in args.towns.split(",") if x.strip())

    spine_ndjson = load_spine_pointer(args.spine)

    # if towns not provided, derive from events (deterministic)
    if towns_set is None:
        towns=set()
        with open(args.events, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try:
                    ev=json.loads(line)
                except Exception:
                    continue
                pr = ev.get("property_ref") or {}
                t = norm_town(pr.get("town_norm") or pr.get("town_raw") or pr.get("town_code") or pr.get("town"))
                if t: towns.add(t)
        towns_set = towns

    index, spine_stats, collision_samples = build_spine_index(
        spine_ndjson, towns_set=towns_set, collision_cap=args.collision_cap
    )

    ev_sha = sha256_file(args.events)
    run_id = f"{args.engine_id}|{args.county}|{ev_sha[:12]}|{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"

    audit = {
        "run_id": run_id,
        "engine_id": args.engine_id,
        "created_at_utc": datetime.utcnow().isoformat()+"Z",
        "county": args.county,
        "events_in": args.events,
        "events_sha256": ev_sha,
        "spine_pointer": args.spine,
        "spine_ndjson": spine_ndjson,
        "towns_used_count": len(towns_set),
        "spine_stats": spine_stats,
        "collision_keys_sampled": list(collision_samples.items())[:50],  # cap output
        "events_total": 0,
        "attach_status_counts": {},
        "attach_tier_counts": {},
        "attach_scope_counts": {"SINGLE": 0, "MULTI": 0},
        "method_counts": {}
    }

    def bump(d,k,n=1):
        d[k]=d.get(k,0)+n

    with open(args.events, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line: continue
            audit["events_total"] += 1
            ev=json.loads(line)

            pr = ev.get("property_ref") or {}
            town_norm = norm_town(pr.get("town_norm") or pr.get("town_raw") or pr.get("town_code") or pr.get("town"))
            addr_norm = norm_addr(pr.get("address_norm") or pr.get("address_raw") or pr.get("address"))

            multi = bool(pr.get("primary_is_multi") is True and isinstance(pr.get("multi_address"), list) and len(pr.get("multi_address"))>0)
            ev.setdefault("attach", {})

            if multi:
                audit["attach_scope_counts"]["MULTI"] += 1
                ev["attach"]["attach_scope"] = "MULTI"
                attachments=[]

                st,pid,tier,meth,mkey = attach_one(town_norm, addr_norm, index, max_range_span=args.max_range_span)
                attachments.append({"town_norm":town_norm,"address_norm":addr_norm,"attach_status":st,"property_id":pid,"attach_tier":tier,"match_method":meth,"match_key":mkey})
                bump(audit["method_counts"], meth or "none")
                bump(audit["attach_tier_counts"], tier or "none")

                for item in pr.get("multi_address", []):
                    t2 = norm_town(item.get("town_norm") or item.get("town_raw") or item.get("town_code") or item.get("town"))
                    a2 = norm_addr(item.get("address_norm") or item.get("address_raw") or item.get("address"))
                    st2,pid2,tier2,meth2,mkey2 = attach_one(t2, a2, index, max_range_span=args.max_range_span)
                    attachments.append({"town_norm":t2,"address_norm":a2,"attach_status":st2,"property_id":pid2,"attach_tier":tier2,"match_method":meth2,"match_key":mkey2})
                    bump(audit["method_counts"], meth2 or "none")
                    bump(audit["attach_tier_counts"], tier2 or "none")

                attached_ct = sum(1 for a in attachments if a["attach_status"]=="ATTACHED_A")
                if attached_ct == len(attachments): overall="ATTACHED_A"
                elif attached_ct > 0: overall="PARTIAL_MULTI"
                else: overall="UNKNOWN"

                ev["attach"]["attachments"]=attachments
                ev["attach"]["attach_status"]=overall
                ev["attach"]["property_id"]=None
                ev["attach"]["attach_tier"]=None
                ev["attach"]["match_method"]=None
                ev["attach"]["match_key"]=None

                bump(audit["attach_status_counts"], overall)

            else:
                audit["attach_scope_counts"]["SINGLE"] += 1
                ev["attach"]["attach_scope"]="SINGLE"

                st,pid,tier,meth,mkey = attach_one(town_norm, addr_norm, index, max_range_span=args.max_range_span)
                ev["attach"]["attach_status"]=st
                ev["attach"]["property_id"]=pid
                ev["attach"]["attach_tier"]=tier
                ev["attach"]["match_method"]=meth
                ev["attach"]["match_key"]=mkey
                ev["attach"]["attachments"]=[]

                bump(audit["attach_status_counts"], st)
                bump(audit["method_counts"], meth or "none")
                bump(audit["attach_tier_counts"], tier or "none")

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps(audit["attach_status_counts"], indent=2))
    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)

if __name__ == "__main__":
    main()

