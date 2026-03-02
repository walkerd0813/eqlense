#!/usr/bin/env python3
import argparse, json, os, re
from datetime import datetime

# ============================================================
# Phase 5 Step 2 (Hampden) - Deterministic-only attachment
# MULTI-aware (one event can attach to multiple properties)
# Schema-first: never guess; collisions -> UNKNOWN
# ============================================================

NUMWORD = {
  "ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5","SIX":"6","SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10"
}

# Suffix aliases (deterministic)
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

  # Highway variants + known typo from index
  "HWY":"HIGHWAY","HIGHWAY":"HWY",
  "HGY":"HWY",
}

RE_MULTI_RANGE = re.compile(r"^(?P<a>\d+)\s*-\s*(?P<b>\d+)\s*(?P<rest>.+)$")
RE_UNIT = re.compile(r"\b(UNIT|APT|APARTMENT|#|PH|PENTHOUSE|BSMT|BASEMENT|FL|FLOOR|RM|ROOM|STE|SUITE)\b\s*([A-Z0-9\-]+)?\b", re.IGNORECASE)
RE_LOT  = re.compile(r"\bLOT\b[\s#]*([0-9]+|[A-Z])?\b", re.IGNORECASE)

def norm_town(t):
    if not t: return None
    return re.sub(r"\s+"," ", t.strip()).upper()

def norm_addr(a):
    """
    Normalize an address-like value into deterministic uppercase string.
    NOTE: for EVENTS, address fields should be strings. For SPINE rows,
    we only feed norm_addr() a string returned by pick_spine_addr_str().
    """
    if a is None: return None
    if not isinstance(a, str):
        a = str(a)
    s = re.sub(r"\s+"," ", a.strip()).upper()

    # common index artifact: trailing verification token 'Y'
    s = re.sub(r"\s+Y$", "", s).strip()

    parts = s.split(" ")
    if parts and parts[0] in NUMWORD:
        parts[0] = NUMWORD[parts[0]]
    return " ".join(parts)

def pick_spine_addr_str(r: dict):
    """
    Spine is not guaranteed to store address fields as strings.
    We ONLY index rows with a real string address. If address_norm is a dict
    (like address_norm.street_no_fix metadata), we skip it.
    """
    candidates = [
        r.get("address_norm"),
        r.get("address"),
        r.get("address_raw"),
        r.get("address_full"),
        r.get("site_address"),
        r.get("full_address"),
        r.get("address1"),
    ]
    for v in candidates:
        if isinstance(v, str) and v.strip():
            return v
    return None

def strip_unit(addr_norm):
    if not addr_norm: return addr_norm
    s = addr_norm
    # remove unit-ish fragments anywhere (condos often embed UNIT mid-string)
    s = re.sub(RE_UNIT, "", s).strip()
    s = re.sub(r"\s+"," ", s).strip()
    return s

def strip_lot(addr_norm):
    if not addr_norm: return addr_norm
    s = re.sub(RE_LOT, "", addr_norm).strip()
    s = re.sub(r"\s+"," ", s).strip()
    return s

def expand_range(addr_norm):
    """
    Deterministic range policy:
      - if range span <= 10: expand all integers
      - else: endpoints only (avoid explosion)
    """
    m = RE_MULTI_RANGE.match(addr_norm or "")
    if not m: return []
    a = int(m.group("a")); b = int(m.group("b"))
    rest = m.group("rest").strip()
    lo = min(a,b); hi = max(a,b)
    span = hi - lo
    if span <= 10:
        return [f"{n} {rest}" for n in range(lo, hi+1)]
    return [f"{lo} {rest}", f"{hi} {rest}"]

def suffix_alias_variants(addr_norm):
    toks = (addr_norm or "").split(" ")
    if not toks: return []
    last = toks[-1]
    out = []
    if last in SUFFIX_ALIAS:
        out.append(" ".join(toks[:-1] + [SUFFIX_ALIAS[last]]))
    # HWY/HGY/HIGHWAY expansion completeness
    if last == "HGY":
        out.append(" ".join(toks[:-1] + ["HWY"]))
        out.append(" ".join(toks[:-1] + ["HIGHWAY"]))
    if last == "HWY":
        out.append(" ".join(toks[:-1] + ["HIGHWAY"]))
        out.append(" ".join(toks[:-1] + ["HGY"]))
    if last == "HIGHWAY":
        out.append(" ".join(toks[:-1] + ["HWY"]))
    # de-dupe
    seen=set(); ded=[]
    for v in out:
        if v and v not in seen:
            seen.add(v); ded.append(v)
    return ded

def condo_cdr_variants(addr_norm):
    """
    Index sometimes uses 'CDR' for 'CIRCLE DR' condos.
    Deterministic expansions only.
    """
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

def addr_variants(addr_norm):
    """
    Deterministic-only variants. No fuzzy, no nearest.
    Returns list of (variant, method_tag)
    """
    out=[]
    seen=set()
    def add(v, tag):
        if not v: return
        if v in seen: return
        seen.add(v); out.append((v, tag))

    if not addr_norm:
        return out

    add(addr_norm, "direct")

    su = strip_unit(addr_norm)
    if su != addr_norm: add(su, "strip_unit")

    sl = strip_lot(addr_norm)
    if sl != addr_norm: add(sl, "strip_lot")

    sul = strip_lot(su)
    if sul and sul not in (addr_norm, su, sl): add(sul, "strip_unit+lot")

    for v in expand_range(addr_norm):
        add(v, "range_expand")

    for v in suffix_alias_variants(addr_norm):
        add(v, "suffix_alias")

    for v in condo_cdr_variants(addr_norm):
        add(v, "condo_cdr_expand")

    # combos
    for base, tag in [(su,"strip_unit"), (sl,"strip_lot"), (sul,"strip_unit+lot")]:
        if not base: continue
        for v in suffix_alias_variants(base):
            add(v, f"{tag}+suffix_alias")
        for v in condo_cdr_variants(base):
            add(v, f"{tag}+condo_cdr")

    return out

def load_spine_pointer(spine_current_json):
    """
    Resolves the actual spine ndjson from CURRENT pointer JSON.
    If the file itself contains/equals a direct ndjson path, return it.
    Otherwise crawl JSON strings for a path that exists and endswith .ndjson.
    """
    with open(spine_current_json, "r", encoding="utf-8") as f:
        raw = f.read().strip()

    # direct ndjson path in file
    if raw.lower().endswith(".ndjson") and os.path.exists(raw.strip().strip('"')):
        return raw.strip().strip('"')

    try:
        obj = json.loads(raw)
    except Exception:
        raise RuntimeError("Could not parse spine CURRENT pointer JSON and it wasn't a direct .ndjson path.")

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

def build_spine_index(spine_ndjson, towns_set):
    index={}
    collisions=0
    rows_seen=0
    rows_indexed=0

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
            if not town or town not in towns_set:
                continue

            addr_raw = pick_spine_addr_str(r)
            if not addr_raw:
                continue

            addr = norm_addr(addr_raw)
            pid = r.get("property_id")

            if not addr or not pid:
                continue

            rows_indexed += 1

            for av, _tag in addr_variants(addr):
                key = f"{town}|{av}"
                if key in index and index[key] != pid:
                    index[key] = "__COLLISION__"
                    collisions += 1
                else:
                    index[key] = pid

    return index, {
        "spine_rows_seen": rows_seen,
        "spine_rows_indexed": rows_indexed,
        "spine_keys": len(index),
        "collision_events": collisions
    }

def attach_one(town_norm, addr_norm, index):
    if not town_norm or not addr_norm:
        return ("MISSING_TOWN_OR_ADDRESS", None, None, None)

    for av, tag in addr_variants(addr_norm):
        key = f"{town_norm}|{av}"
        if key in index:
            if index[key] == "__COLLISION__":
                return ("UNKNOWN", None, "collision", key)
            return ("ATTACHED_A", index[key], tag, key)

    return ("UNKNOWN", None, "no_match", f"{town_norm}|{addr_norm}")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args=ap.parse_args()

    # deterministic town scope: towns present in events
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
            t = norm_town(pr.get("town_norm") or pr.get("town_raw"))
            if t: towns.add(t)

    spine_ndjson = load_spine_pointer(args.spine)
    index, spine_stats = build_spine_index(spine_ndjson, towns)

    audit = {
        "run_id": "hampden_step2_attach_DEED_ONLY_v1_8_0_MULTI",
        "created_at_utc": datetime.utcnow().isoformat()+"Z",
        "events_in": args.events,
        "spine_pointer": args.spine,
        "spine_ndjson": spine_ndjson,
        "towns_used_count": len(towns),
        "spine_stats": spine_stats,
        "events_total": 0,
        "attach_status_counts": {},
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
            town_norm = norm_town(pr.get("town_norm") or pr.get("town_raw"))
            addr_norm = norm_addr(pr.get("address_norm") or pr.get("address_raw"))

            multi = bool(pr.get("primary_is_multi") is True and isinstance(pr.get("multi_address"), list) and len(pr.get("multi_address"))>0)
            ev.setdefault("attach", {})

            if multi:
                audit["attach_scope_counts"]["MULTI"] += 1
                ev["attach"]["attach_scope"] = "MULTI"
                attachments=[]

                st,pid,meth,mkey = attach_one(town_norm, addr_norm, index)
                attachments.append({"town_norm":town_norm,"address_norm":addr_norm,"attach_status":st,"property_id":pid,"match_method":meth,"match_key":mkey})
                bump(audit["method_counts"], meth or "none")

                for item in pr.get("multi_address", []):
                    t2 = norm_town(item.get("town_norm") or item.get("town_raw"))
                    a2 = norm_addr(item.get("address_norm") or item.get("address_raw"))
                    st2,pid2,meth2,mkey2 = attach_one(t2, a2, index)
                    attachments.append({"town_norm":t2,"address_norm":a2,"attach_status":st2,"property_id":pid2,"match_method":meth2,"match_key":mkey2})
                    bump(audit["method_counts"], meth2 or "none")

                attached_ct = sum(1 for a in attachments if a["attach_status"]=="ATTACHED_A")
                if attached_ct == len(attachments): overall="ATTACHED_A"
                elif attached_ct > 0: overall="PARTIAL_MULTI"
                else: overall="UNKNOWN"

                ev["attach"]["attachments"]=attachments
                ev["attach"]["attach_status"]=overall
                ev["attach"]["property_id"]=None
                ev["attach"]["match_method"]=None
                ev["attach"]["match_key"]=None

                bump(audit["attach_status_counts"], overall)

            else:
                audit["attach_scope_counts"]["SINGLE"] += 1
                ev["attach"]["attach_scope"]="SINGLE"

                st,pid,meth,mkey = attach_one(town_norm, addr_norm, index)
                ev["attach"]["attach_status"]=st
                ev["attach"]["property_id"]=pid
                ev["attach"]["match_method"]=meth
                ev["attach"]["match_key"]=mkey
                ev["attach"]["attachments"]=[]

                bump(audit["attach_status_counts"], st)
                bump(audit["method_counts"], meth or "none")

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps(audit["attach_status_counts"], indent=2))
    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)

if __name__ == "__main__":
    main()
