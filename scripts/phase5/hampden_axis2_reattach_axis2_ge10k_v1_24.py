#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, re, hashlib
from collections import Counter, defaultdict
from datetime import datetime, timezone

# -------------------------
# Deterministic normalization
# -------------------------

# Hampden reality:
# - Lane appears as LA, LN, LANE depending on town/source
# We normalize suffix-only (last token) to LN.
SUFFIX_MAP = {
    "TERR": "TERR", "TER": "TERR", "TERRACE": "TERR",
    "CIR": "CIR", "CIRCLE": "CIR",
    "CT": "CT", "COURT": "CT",
    "AVE": "AVE", "AVENUE": "AVE",
    "ST": "ST", "STREET": "ST",
    "RD": "RD", "ROAD": "RD",
    "DR": "DR", "DRIVE": "DR",
    "LN": "LN", "LANE": "LN",
    "LA": "LN",  # critical: deed-index uses LA for Lane in many Hampden towns
    "BLVD": "BLVD", "BOULEVARD": "BLVD",
    "PL": "PL", "PLACE": "PL",
}

DIR_MAP = {"NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W","N":"N","S":"S","E":"E","W":"W"}

# Include common unit markers; add NO/NUMBER as deterministic markers too
UNIT_PAT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|SUITE|NO|NUMBER)\s*([A-Z0-9\-]+)\b", re.I)

WORD_NUM = {
    "ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5","SIX":"6","SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10",
    "ELEVEN":"11","TWELVE":"12","THIRTEEN":"13","FOURTEEN":"14","FIFTEEN":"15","SIXTEEN":"16","SEVENTEEN":"17","EIGHTEEN":"18","NINETEEN":"19","TWENTY":"20"
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
    # minimal + safe
    t = re.sub(r"^(CITY OF|TOWN OF)\s+", "", t).strip()
    t = re.sub(r",\s*MA$", "", t).strip()
    t = re.sub(r"\s+MA$", "", t).strip()
    return t

def norm_unit_value(u: str) -> str:
    u = norm_tokens(u).replace(" ", "")
    # map ONE..TWENTY only (deterministic, low risk)
    if u in WORD_NUM:
        return WORD_NUM[u]
    return u

def norm_street_name(raw_street: str):
    """
    Returns:
      street_norm, suffix_alias_applied(bool)
    """
    s = norm_tokens(raw_street)
    if not s:
        return "", False

    toks = s.split(" ")

    # directionals as standalone tokens only
    toks2 = []
    for t in toks:
        toks2.append(DIR_MAP.get(t, t))
    toks = toks2

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
      unit_norm (str or "")
    """
    s = norm_tokens(addr_raw)

    unit = ""
    m = UNIT_PAT.search(s)
    if m:
        unit = norm_unit_value(m.group(1))
        s = UNIT_PAT.sub(" ", s)
        s = re.sub(r"\s+", " ", s).strip()

    street_no = None
    m2 = re.match(r"^\s*(\d+)\s+(.*)$", s)
    if m2:
        try:
            n = int(m2.group(1))
            if n > 0:
                street_no = n
        except Exception:
            street_no = None
        street_part = m2.group(2).strip()
    else:
        street_part = s

    street_norm, alias_applied = norm_street_name(street_part)
    return street_no, street_norm, unit, alias_applied

def parse_spine_unit(u):
    if not u:
        return ""
    return norm_unit_value(u)

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

    # load events + towns needed
    events = []
    towns_needed = set()

    with open(args.events, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            e = json.loads(line)
            events.append(e)

            t = norm_town(e.get("town") or (e.get("property_ref") or {}).get("town") or "")
            if t:
                towns_needed.add(t)

    # build indexes from spine (town-filtered streaming)
    idxA, idxB, spine_stats = build_indexes(args.spine, towns_needed)

    buckets = Counter()
    samples = defaultdict(list)
    improved = 0
    attached_a = attached_b = 0

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as w:
        for e in events:
            # preserve prior attaches
            prior_status = e.get("attach_status") or ""
            if isinstance(prior_status, str) and prior_status.startswith("ATTACHED"):
                w.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue

            town = norm_town(e.get("town") or (e.get("property_ref") or {}).get("town") or "")
            addr = e.get("addr") or (e.get("property_ref") or {}).get("address_raw") or (e.get("property_ref") or {}).get("address") or ""
            addr = str(addr)

            street_no, street_norm, unit_norm, alias_applied = parse_event_address(addr)

            # default remains unknown if missing essentials
            if not town or not street_norm:
                e["attach_scope"] = "SINGLE"
                e["attach_status"] = "UNKNOWN"
                e["why"] = "no_match"
                e["match_method"] = None
                buckets["SINGLE|UNKNOWN|NO_MATCH|MISSING_TOWN_OR_STREET"] += 1
                w.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue

            if street_no is None:
                e["attach_scope"] = "SINGLE"
                e["attach_status"] = "UNKNOWN"
                e["why"] = "no_num"
                e["match_method"] = None
                buckets["SINGLE|UNKNOWN|NO_NUM|NO_NUM"] += 1
                w.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue

            # Try Index A if unit exists
            if unit_norm:
                kA = keyA(town, street_no, street_norm, unit_norm)
                pid, code = resolve(idxA, kA)
                if pid:
                    e["property_id"] = pid
                    e["attach_scope"] = "SINGLE"
                    e["attach_status"] = "ATTACHED_A"
                    e["why"] = None
                    e["match_method"] = "axis2_street+unit_exact"
                    buckets["SINGLE|ATTACHED_A|AXIS2_STREET+UNIT_EXACT|NONE"] += 1
                    improved += 1
                    attached_a += 1
                    w.write(json.dumps(e, ensure_ascii=False) + "\n")
                    continue
                else:
                    # collision stays honest, but we'll still attempt B
                    if code == "COLLISION":
                        buckets["SINGLE|UNKNOWN|COLLISION|UNIT_KEY"] += 1

            # Try Index B (no unit)
            kB = keyB(town, street_no, street_norm)
            pid, code = resolve(idxB, kB)
            if pid:
                e["property_id"] = pid
                e["attach_scope"] = "SINGLE"
                e["attach_status"] = "ATTACHED_A"
                e["why"] = None
                if alias_applied:
                    e["match_method"] = "axis2_street_unique_suffix_alias"
                    buckets["SINGLE|ATTACHED_A|AXIS2_STREET_UNIQUE_SUFFIX_ALIAS|NONE"] += 1
                else:
                    e["match_method"] = "axis2_street_unique_exact"
                    buckets["SINGLE|ATTACHED_A|AXIS2_STREET_UNIQUE_EXACT|NONE"] += 1
                improved += 1
                attached_a += 1
                w.write(json.dumps(e, ensure_ascii=False) + "\n")
                continue
            else:
                e["attach_scope"] = "SINGLE"
                e["attach_status"] = "UNKNOWN"
                e["why"] = "collision" if code == "COLLISION" else "no_match"
                e["match_method"] = None
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
                            "alias_applied": alias_applied
                        }
                    })

                w.write(json.dumps(e, ensure_ascii=False) + "\n")

    audit = {
        "run": {
            "script": os.path.basename(__file__),
            "version": "v1_24",
            "ran_at": datetime.now(timezone.utc).isoformat(),
        },
        "inputs": {
            "events_path": args.events,
            "spine_path": args.spine,
            "events_sha256": sha256_file(args.events),
            "spine_sha256": sha256_file(args.spine),
            "towns_needed_n": len(towns_needed),
            "towns_needed_sample": sorted(list(towns_needed))[:25],
        },
        "spine_index": spine_stats,
        "stats": {
            "events": len(events),
            "improved": improved,
            "attached_a_added": attached_a,
            "unknown_remaining_est": None,
        },
        "buckets": dict(buckets.most_common()),
        "samples": dict(samples),
        "notes": [
            "LA/LANE/LN normalized to LN as suffix-only (Hampden varies by town/source).",
            "Unit markers supported: UNIT/APT/APARTMENT/#/SUITE/NO/NUMBER; ONE..TWENTY mapped to digits deterministically.",
            "Existing ATTACHED_* rows are preserved; v1_24 only attempts to improve UNKNOWN rows.",
        ]
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.audit)), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print("[done] v1_24")
    print("  events:", len(events))
    print("  improved (new attaches):", improved)
    print("  spine kept_rows:", spine_stats["kept_rows"])
    print("  keysA unique:", spine_stats["keysA_unique"], "total:", spine_stats["keysA_total"])
    print("  keysB unique:", spine_stats["keysB_unique"], "total:", spine_stats["keysB_total"])

if __name__ == "__main__":
    main()
