import json, re
from collections import Counter, defaultdict

CUR = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
CAND = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k.ndjson"
OUT  = r"publicData/_audit/registry/streetcore_probe_ge10k_v1.json"

RE_SPACE = re.compile(r"\s+")
RE_TRAIL_Y = re.compile(r"\s+Y\s*$")
RE_UNIT_ANY = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|NO\.|STE|SUITE|FL|FLOOR)\b", re.I)
RE_LOT_ANY  = re.compile(r"\b(?:LOT|PAR|PARCEL)\b", re.I)
RE_NUM = re.compile(r"^(?P<num>\d+)\s+(?P<rest>.+)$")
RE_RANGE = re.compile(r"^(?P<a>\d+)\s*-\s*(?P<b>\d+)\s+(?P<rest>.+)$")

SUFFIXES = {"ST","RD","AVE","BLVD","DR","LN","LA","TER","TERR","PKY","PKWY","CT","CIR","WAY","PL","HWY","HGY"}

def norm_line(s: str) -> str:
    if not s: return ""
    s = s.upper()
    s = RE_TRAIL_Y.sub("", s)
    s = RE_SPACE.sub(" ", s).strip()
    return s

def strip_unit_lot_tokens(s: str) -> str:
    # remove everything after unit marker OR lot marker (conservative, deterministic)
    toks = s.split()
    cut = None
    for i,t in enumerate(toks):
        if RE_UNIT_ANY.fullmatch(t) or RE_LOT_ANY.fullmatch(t):
            cut = i
            break
    if cut is not None:
        toks = toks[:cut]
    return " ".join(toks).strip()

def street_core(addr: str):
    s = norm_line(addr)
    s = strip_unit_lot_tokens(s)
    # handle range (e.g. 19-21 THOMAS AVE)
    m = RE_RANGE.match(s)
    if m:
        rest = m.group("rest")
        rest = norm_line(rest)
        rest_toks = rest.split()
        if rest_toks and rest_toks[-1] in SUFFIXES:
            rest_nosuf = " ".join(rest_toks[:-1])
        else:
            rest_nosuf = rest
        return {"kind":"range", "nums":[m.group("a"), m.group("b")], "rest":rest, "rest_nosuf":rest_nosuf}

    m = RE_NUM.match(s)
    if not m:
        return {"kind":"no_num", "raw":s}

    num = m.group("num")
    rest = norm_line(m.group("rest"))
    rest_toks = rest.split()
    if rest_toks and rest_toks[-1] in SUFFIXES:
        rest_nosuf = " ".join(rest_toks[:-1])
        suf = rest_toks[-1]
    else:
        rest_nosuf = rest
        suf = None
    return {"kind":"single", "nums":[num], "rest":rest, "rest_nosuf":rest_nosuf, "suf":suf}

def iter_ndjson(path):
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            yield json.loads(line)

def load_spine_path():
    with open(CUR,"r",encoding="utf-8") as f:
        ptr=json.load(f)
    p=ptr.get("properties_ndjson")
    if not p: raise RuntimeError("CURRENT pointer missing properties_ndjson")
    return p

def main():
    spine_path = load_spine_path()

    # Build: town|rest_nosuf -> counts + a few sample addresses
    sc_counts = Counter()
    sc_samples = defaultdict(list)

    for row in iter_ndjson(spine_path):
        town = (row.get("town_norm") or row.get("town") or row.get("address",{}).get("town_norm") or "").upper().strip()
        addr = row.get("address_norm") or row.get("address") or row.get("address_line1") or row.get("address_full") or row.get("address",{}).get("address_norm")
        if isinstance(addr, dict):
            addr = addr.get("value") or addr.get("text") or ""
        if not town or not addr:
            continue
        sc = street_core(str(addr))
        if sc.get("kind") not in ("single","range"):
            continue
        key = f"{town}|{sc.get('rest_nosuf','')}"
        if not sc.get("rest_nosuf"):
            continue
        sc_counts[key] += 1
        if len(sc_samples[key]) < 3:
            sc_samples[key].append(norm_line(str(addr)))

    stats = Counter()
    examples = []

    for ev in iter_ndjson(CAND):
        town = (ev.get("property_ref",{}).get("town_norm") or "").upper().strip()
        addr = ev.get("property_ref",{}).get("address_norm") or ev.get("property_ref",{}).get("address_raw") or ""
        amt  = ev.get("consideration",{}).get("amount")
        if not town or not addr:
            continue

        sc = street_core(str(addr))
        kind = sc.get("kind")
        if kind == "no_num":
            stats["cand_no_num"] += 1
            if len(examples) < 25:
                examples.append({"event_id": ev.get("event_id"), "town": town, "addr": norm_line(str(addr)), "amount": amt, "kind":"no_num"})
            continue

        rest_nosuf = sc.get("rest_nosuf","")
        key = f"{town}|{rest_nosuf}" if rest_nosuf else None
        if not key:
            stats["cand_bad_rest"] += 1
            continue

        hit_count = sc_counts.get(key, 0)
        if hit_count == 0:
            stats["streetcore_missing_in_spine_for_town"] += 1
            if len(examples) < 25:
                examples.append({"event_id": ev.get("event_id"), "town": town, "addr": norm_line(str(addr)), "amount": amt, "kind": kind, "streetcore": rest_nosuf, "spine_hits": 0})
        else:
            stats["streetcore_exists_in_spine_for_town"] += 1
            if len(examples) < 25:
                examples.append({"event_id": ev.get("event_id"), "town": town, "addr": norm_line(str(addr)), "amount": amt, "kind": kind, "streetcore": rest_nosuf, "spine_hits": hit_count, "spine_sample_addrs": sc_samples.get(key, [])})

    report = {
        "candidates_file": CAND,
        "min_consideration": 10000,
        "stats": dict(stats),
        "examples_first_25": examples
    }

    with open(OUT,"w",encoding="utf-8") as f:
        json.dump(report,f,ensure_ascii=False,indent=2)

    print("=== STREETCORE PROBE (>=10k candidates) ===")
    print(json.dumps(report["stats"], ensure_ascii=False))
    print(f"[ok] wrote: {OUT}")

if __name__ == "__main__":
    main()
