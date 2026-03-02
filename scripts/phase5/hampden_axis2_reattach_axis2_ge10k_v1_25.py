#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, re, hashlib
from collections import Counter, defaultdict
from datetime import datetime, timezone

# -------------------------
# Deterministic normalization
# -------------------------

SUFFIX_MAP = {
    "TERR": "TERR", "TER": "TERR", "TERRACE": "TERR",
    "CIR": "CIR", "CIRCLE": "CIR",
    "CT": "CT", "COURT": "CT",
    "AVE": "AVE", "AVENUE": "AVE",
    "ST": "ST", "STREET": "ST",
    "RD": "RD", "ROAD": "RD",
    "DR": "DR", "DRIVE": "DR",
    # Hampden towns vary: LA vs LN vs LANE
    "LN": "LN", "LANE": "LN", "LA": "LN",
    "BLVD": "BLVD", "BOULEVARD": "BLVD",
    "PL": "PL", "PLACE": "PL",
    "PKY": "PKWY", "PKWY": "PKWY", "PKWAY": "PKWY",
}

DIR_MAP = {"NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W","N":"N","S":"S","E":"E","W":"W"}

UNIT_PAT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|SUITE|NO|NUMBER)\s*([A-Z0-9\-]+)\b", re.I)

WORD_NUM = {
    "ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5","SIX":"6","SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10",
    "ELEVEN":"11","TWELVE":"12","THIRTEEN":"13","FOURTEEN":"14","FIFTEEN":"15","SIXTEEN":"16","SEVENTEEN":"17",
    "EIGHTEEN":"18","NINETEEN":"19","TWENTY":"20"
}

def norm_tokens(s: str) -> str:
    if not s:
        return ""
    s = str(s).upper().strip()
    s = re.sub(r"[.,;:]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_town(t: str) -> str:
    t = norm_tokens(t)
    if not t:
        return ""
    t = re.sub(r"^(CITY OF|TOWN OF)\s+", "", t).strip()
    t = re.sub(r",\s*MA$", "", t).strip()
    t = re.sub(r"\s+MA$", "", t).strip()
    return t

def norm_unit_value(u: str) -> str:
    u = norm_tokens(u).replace(" ", "")
    if u in WORD_NUM:
        return WORD_NUM[u]
    return u

def norm_street_name(raw_street: str):
    """
    Returns (street_norm, alias_applied)
    Suffix aliasing is applied ONLY to the last token.
    """
    s = norm_tokens(raw_street)
    if not s:
        return "", False

    toks = s.split(" ")
    toks = [DIR_MAP.get(t, t) for t in toks]

    alias_applied = False
    if toks:
        last = toks[-1]
        if last in SUFFIX_MAP:
            new_last = SUFFIX_MAP[last]
            if new_last != last:
                alias_applied = True
            toks[-1] = new_last

    return " ".join(toks).strip(), alias_applied

def parse_event_address(addr_raw: str):
    """
    Parse event address into:
      street_no (int or None),
      street_norm (str),
      unit_norm (str or ""),
      alias_applied (bool),
      street_no_range (bool)
    """
    s = norm_tokens(addr_raw)

    unit = ""
    m = UNIT_PAT.search(s)
    if m:
        unit = norm_unit_value(m.group(1))
        s = UNIT_PAT.sub(" ", s)
        s = re.sub(r"\s+", " ", s).strip()

    # support 19-21 style: take first number deterministically, flag range
    street_no = None
    street_no_range = False

    m2 = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s+(.*)$", s)
    if m2:
        street_no_range = True
        try:
            n = int(m2.group(1))
            if n > 0:
                street_no = n
        except Exception:
            street_no = None
        street_part = m2.group(3).strip()
    else:
        m3 = re.match(r"^\s*(\d+)\s+(.*)$", s)
        if m3:
            try:
                n = int(m3.group(1))
                if n > 0:
                    street_no = n
            except Exception:
                street_no = None
            street_part = m3.group(2).strip()
        else:
            street_part = s

    street_norm, alias_applied = norm_street_name(street_part)
    return street_no, street_norm, unit, alias_applied, street_no_range

def parse_spine_unit(u):
    if not u:
        return ""
    return norm_unit_value(u)

# -------------------------
# Schema extraction (CRITICAL FIX)
# -------------------------

def get_evt_town(evt: dict):
    pr = evt.get("property_ref") if isinstance(evt.get("property_ref"), dict) else {}
    # observed: property_ref.town_raw
    for v in [evt.get("town"),
              pr.get("town"),
              pr.get("town_raw"),
              (pr.get("address_norm") or {}).get("town") if isinstance(pr.get("address_norm"), dict) else None]:
        t = norm_town(v)
        if t:
            return t
    return ""

def get_evt_addr(evt: dict):
    pr = evt.get("property_ref") if isinstance(evt.get("property_ref"), dict) else {}
    for v in [evt.get("addr"),
              pr.get("address_raw"),
              pr.get("address"),
              pr.get("full_address")]:
        v = (v or "").strip() if isinstance(v, str) else v
        if v:
            return str(v)
    return ""

# -------------------------
# Indexing
# -------------------------

def keyA(town, no, street, unit): return f"{town}|{no}|{street}|{unit}"
def keyB(town, no, street):      return f"{town}|{no}|{street}"

def add_unique(idx: dict, k: str, pid: str):
    cur = idx.get(k)
    if cur is None:
        idx[k] = pid
    else:
        if cur == pid:
            return
        idx[k] = "__COLLISION__"

def resolve(idx, k):
    v = idx.get(k)
    if v is None:
        return (None, "NO_MATCH")
    if v == "__COLLISION__":
        return (None, "COLLISION")
    return (v, "UNIQUE")

def build_indexes(spine_path: str, towns_needed: set):
    idxA, idxB = {}, {}
    kept_rows = 0

    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            r = json.loads(line)

            town = norm_town(r.get("town") or "")
            if not town or town not in towns_needed:
                continue

            pid = r.get("property_id") or r.get("id") or ""
            if not pid:
                continue

            street_no = r.get("street_no")
            if isinstance(street_no, str) and re.fullmatch(r"\d+", street_no.strip()):
                street_no = int(street_no.strip())
            if not isinstance(street_no, int) or street_no <= 0:
                street_no = None

            street_name = r.get("street_name") or ""
            street_norm, _ = norm_street_name(street_name)

            unit_norm = parse_spine_unit(r.get("unit") or "")

            kept_rows += 1

            if street_no is not None and street_norm:
                add_unique(idxB, keyB(town, street_no, street_norm), pid)
                if unit_norm:
                    add_unique(idxA, keyA(town, street_no, street_norm, unit_norm), pid)

    stats = {
        "kept_rows": kept_rows,
        "keysA_total": len(idxA),
        "keysB_total": len(idxB),
        "keysA_unique": sum(1 for v in idxA.values() if v != "__COLLISION__"),
        "keysB_unique": sum(1 for v in idxB.values() if v != "__COLLISION__"),
    }
    return idxA, idxB, stats

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

# -------------------------
# Main
# -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--max_samples", type=int, default=30)
    args = ap.parse_args()

    events = []
    towns_needed = set()
    town_source_counts = Counter()
    addr_source_counts = Counter()

    def town_source(evt):
        pr = evt.get("property_ref") if isinstance(evt.get("property_ref"), dict) else {}
        if evt.get("town"): return "town"
        if pr.get("town"): return "property_ref.town"
        if pr.get("town_raw"): return "property_ref.town_raw"
        if isinstance(pr.get("address_norm"), dict) and pr.get("address_norm").get("town"): return "property_ref.address_norm.town"
        return "none"

    def addr_source(evt):
        pr = evt.get("property_ref") if isinstance(evt.get("property_ref"), dict) else {}
        if evt.get("addr"): return "addr"
        if pr.get("address_raw"): return "property_ref.address_raw"
        if pr.get("address"): return "property_ref.address"
        if pr.get("full_address"): return "property_ref.full_address"
        return "none"

    with open(args.events, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            e = json.loads(line)
            events.append(e)
            t = get_evt_town(e)
            if t:
                towns_needed.add(t)
            town_source_counts[town_source(e)] += 1
            addr_source_counts[addr_source(e)] += 1

    idxA, idxB, spine_stats = build_indexes(args.spine, towns_needed)

    buckets = Counter()
    samples = defaultdict(list)
    improved = 0
    attached_a_added = 0

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as w:
        for e in events:
            prior_status = e.get("attach_status") or ""
            if isinstance(prior_status, str) and prior_status.startswith("ATTACHED"):
                w.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue

            town = get_evt_town(e)
            addr = get_evt_addr(e)

            street_no, street_norm, unit_norm, alias_applied, street_no_range = parse_event_address(addr)

            if not town or not street_norm:
                e["attach_scope"] = "SINGLE"
                e["attach_status"] = "UNKNOWN"
                e["why"] = "no_match"
                e["match_method"] = "no_match"
                buckets["SINGLE|UNKNOWN|NO_MATCH|MISSING_TOWN_OR_STREET"] += 1
                w.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue

            if street_no is None or street_no == 0:
                e["attach_scope"] = "SINGLE"
                e["attach_status"] = "UNKNOWN"
                e["why"] = "no_num"
                e["match_method"] = "no_match"
                buckets["SINGLE|UNKNOWN|NO_NUM|NO_NUM"] += 1
                w.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue

            # Try A (unit)
            if unit_norm:
                kA = keyA(town, street_no, street_norm, unit_norm)
                pid, code = resolve(idxA, kA)
                if pid:
                    e["property_id"] = pid
                    e["attach_scope"] = "SINGLE"
                    e["attach_status"] = "ATTACHED_A"
                    e["why"] = None
                    e["match_method"] = "axis2_street+unit_exact"
                    e["match_meta"] = {"street_no_range": street_no_range}
                    buckets["SINGLE|ATTACHED_A|AXIS2_STREET+UNIT_EXACT|NONE"] += 1
                    improved += 1
                    attached_a_added += 1
                    w.write(json.dumps(e, ensure_ascii=False) + "\n")
                    continue

            # Try B (no unit)
            kB = keyB(town, street_no, street_norm)
            pid, code = resolve(idxB, kB)
            if pid:
                e["property_id"] = pid
                e["attach_scope"] = "SINGLE"
                e["attach_status"] = "ATTACHED_A"
                e["why"] = None
                e["match_method"] = "axis2_street_unique_suffix_alias" if alias_applied else "axis2_street_unique_exact"
                e["match_meta"] = {"street_no_range": street_no_range}
                buckets[f"SINGLE|ATTACHED_A|{e['match_method'].upper()}|NONE"] += 1
                improved += 1
                attached_a_added += 1
                w.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue

            # Unknown
            e["attach_scope"] = "SINGLE"
            e["attach_status"] = "UNKNOWN"
            e["why"] = "collision" if code == "COLLISION" else "no_match"
            e["match_method"] = "no_match"
            e["match_meta"] = {"street_no_range": street_no_range}
            buckets[f"SINGLE|UNKNOWN|{code}|STREET_KEY"] += 1

            sk = f"SINGLE|UNKNOWN|{e['why'].upper()}|SAMPLE"
            if len(samples[sk]) < args.max_samples:
                samples[sk].append({
                    "event_id": e.get("event_id"),
                    "town": town,
                    "addr": addr,
                    "parsed": {
                        "street_no": street_no,
                        "street_name_norm": street_norm,
                        "unit_norm": unit_norm,
                        "alias_applied": alias_applied,
                        "street_no_range": street_no_range
                    }
                })

            w.write(json.dumps(e, ensure_ascii=False) + "\n")

    audit = {
        "run": {"script": os.path.basename(__file__), "version": "v1_25", "ran_at": datetime.now(timezone.utc).isoformat()},
        "inputs": {
            "events_path": args.events, "spine_path": args.spine,
            "events_sha256": sha256_file(args.events), "spine_sha256": sha256_file(args.spine),
            "towns_needed_n": len(towns_needed),
            "town_source_counts": dict(town_source_counts),
            "addr_source_counts": dict(addr_source_counts),
            "towns_needed_sample": sorted(list(towns_needed))[:25],
        },
        "spine_index": spine_stats,
        "stats": {"events": len(events), "improved_new_attaches": improved, "attached_a_added": attached_a_added},
        "buckets": dict(buckets.most_common()),
        "samples": dict(samples),
        "notes": [
            "Schema fix: town from property_ref.town_raw, addr from property_ref.address_raw (fallbacks included).",
            "LA/LN/LANE normalized to LN as suffix-only (Hampden varies by town/source).",
            "Hyphen street ranges: '19-21' uses 19 with street_no_range=True (still deterministic)."
        ]
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.audit)), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print("[done] v1_25")
    print("  events:", len(events))
    print("  improved (new attaches):", improved)
    print("  spine kept_rows:", spine_stats["kept_rows"])
    print("  keysA unique:", spine_stats["keysA_unique"], "total:", spine_stats["keysA_total"])
    print("  keysB unique:", spine_stats["keysB_unique"], "total:", spine_stats["keysB_total"])
    print("  town_source_counts:", dict(town_source_counts))
    print("  addr_source_counts:", dict(addr_source_counts))

if __name__ == "__main__":
    main()
