#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, re, hashlib
from collections import Counter, defaultdict
from datetime import datetime, timezone

# -------------------------
# Normalization
# -------------------------

SUFFIX_MAP = {
    "TERR": "TERR", "TER": "TERR", "TERRACE": "TERR",
    "CIR": "CIR", "CIRCLE": "CIR",
    "CT": "CT", "COURT": "CT",
    "AVE": "AVE", "AVENUE": "AVE",
    "ST": "ST", "STREET": "ST",
    "RD": "RD", "ROAD": "RD",
    "DR": "DR", "DRIVE": "DR",
    "LN": "LN", "LANE": "LN",
    "LA": "LN",  # deed-index "LA" used as Lane; canonicalize suffix-only -> LN
    "BLVD": "BLVD", "BOULEVARD": "BLVD",
    "PL": "PL", "PLACE": "PL",
}

DIR_MAP = {"NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W","N":"N","S":"S","E":"E","W":"W"}

# include SUITE as deterministic unit token (still safe)
UNIT_PAT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|SUITE)\s*([A-Z0-9\-]+)\b", re.I)

def norm_tokens(s: str) -> str:
    if not s:
        return ""
    s = str(s).upper().strip()
    s = re.sub(r"[.,;:]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_town(t: str) -> str:
    """
    Town canonicalization to align event + spine:
    - uppercase + collapse spaces/punct
    - strip leading CITY OF / TOWN OF
    - drop trailing ', MA' or ' MA'
    """
    t = norm_tokens(t)
    if not t:
        return ""
    t = re.sub(r"^(CITY OF|TOWN OF)\s+", "", t).strip()
    t = re.sub(r",\s*MA$", "", t).strip()
    t = re.sub(r"\s+MA$", "", t).strip()
    return t

def norm_unit(u: str) -> str:
    u = norm_tokens(u)
    u = u.replace(" ", "")
    return u

def norm_street_name(raw_street: str) -> str:
    s = norm_tokens(raw_street)
    if not s:
        return ""
    toks = s.split(" ")
    toks2 = []
    for t in toks:
        toks2.append(DIR_MAP.get(t, t))
    toks = toks2
    if toks:
        last = toks[-1]
        if last in SUFFIX_MAP:
            toks[-1] = SUFFIX_MAP[last]
    return " ".join(toks).strip()

def parse_event_address(addr_raw: str):
    s = norm_tokens(addr_raw)
    unit = ""
    m = UNIT_PAT.search(s)
    if m:
        unit = norm_unit(m.group(1))
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

    street_name_norm = norm_street_name(street_part)
    return street_no, street_name_norm, unit

def recover_street_no_from_street_no_fix(evt: dict):
    pr = evt.get("property_ref") or {}
    an = pr.get("address_norm")
    if not isinstance(an, dict):
        return None
    snf = an.get("street_no_fix")
    if not isinstance(snf, dict):
        return None
    before = snf.get("before")
    if before is None:
        return None
    if isinstance(before, int):
        return before if before > 0 else None
    if isinstance(before, str):
        b = before.strip()
        if re.fullmatch(r"\d+", b):
            n = int(b)
            return n if n > 0 else None
    return None

# -------------------------
# Indexing
# -------------------------

def keyA(town, no, street, unit): return f"{town}|{no}|{street}|{unit}"
def keyB(town, no, street):      return f"{town}|{no}|{street}"
def keyC(town, street):         return f"{town}|{street}"

def add_unique_index(idx: dict, k: str, pid: str):
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
    idxA, idxB, idxC = {}, {}, {}
    kept_rows = 0

    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
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

            street_norm = norm_street_name(r.get("street_name") or "")
            unit_norm = norm_unit(r.get("unit") or "") if (r.get("unit") or "") else ""

            kept_rows += 1

            if street_no is not None and street_norm and unit_norm:
                add_unique_index(idxA, keyA(town, street_no, street_norm, unit_norm), pid)
            if street_no is not None and street_norm:
                add_unique_index(idxB, keyB(town, street_no, street_norm), pid)
            if street_norm:
                add_unique_index(idxC, keyC(town, street_norm), pid)

    stats = {
        "kept_rows": kept_rows,
        "keysA_total": len(idxA),
        "keysB_total": len(idxB),
        "keysC_total": len(idxC),
        "keysA_unique": sum(1 for v in idxA.values() if v != "__COLLISION__"),
        "keysB_unique": sum(1 for v in idxB.values() if v != "__COLLISION__"),
        "keysC_unique": sum(1 for v in idxC.values() if v != "__COLLISION__"),
    }
    return idxA, idxB, idxC, stats

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

    with open(args.events, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            e = json.loads(line)
            events.append(e)

            t = e.get("town") or (e.get("property_ref") or {}).get("town") or ""
            t = norm_town(t)
            if t:
                towns_needed.add(t)

    idxA, idxB, idxC, spine_stats = build_indexes(args.spine, towns_needed)

    buckets = Counter()
    samples = defaultdict(list)
    attached_a = attached_b = 0

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as w:
        for e in events:
            town = norm_town(e.get("town") or (e.get("property_ref") or {}).get("town") or "")
            addr = e.get("addr") or (e.get("property_ref") or {}).get("address_raw") or (e.get("property_ref") or {}).get("address") or ""
            addr = str(addr)

            street_no, street_norm, unit = parse_event_address(addr)

            recovered = False
            if street_no is None:
                rec = recover_street_no_from_street_no_fix(e)
                if isinstance(rec, int) and rec > 0:
                    street_no = rec
                    recovered = True

            # default result
            attach_scope = "AXIS2_GE10K_V1_23"
            attach_status = "UNKNOWN"
            match_method = None
            why = None

            # deterministic gates
            if not town or not street_norm:
                attach_status = "UNKNOWN"
                match_method = None
                why = "missing_town_or_street"
                buckets[f"{attach_scope}|UNKNOWN|NO_MATCH|MISSING_TOWN_OR_STREET"] += 1
            elif street_no is None:
                why = "no_num_recovered" if recovered else "no_num"
                buckets[f"{attach_scope}|UNKNOWN|NO_NUM|{why.upper()}"] += 1
            else:
                # A (unit key)
                if unit:
                    kA = keyA(town, street_no, street_norm, unit)
                    pid, code = resolve(idxA, kA)
                    if pid:
                        e["property_id"] = pid
                        attach_status = "ATTACHED_A"
                        match_method = "axis2_street+unit_exact"
                        buckets[f"{attach_scope}|ATTACHED_A|AXIS2_STREET+UNIT_EXACT|NONE"] += 1
                        attached_a += 1
                    else:
                        buckets[f"{attach_scope}|UNKNOWN|{code}|A"] += 1

                # B (no-unit)
                if attach_status == "UNKNOWN":
                    kB = keyB(town, street_no, street_norm)
                    pid, code = resolve(idxB, kB)
                    if pid:
                        e["property_id"] = pid
                        attach_status = "ATTACHED_A"
                        match_method = "axis2_street_unique_exact"
                        buckets[f"{attach_scope}|ATTACHED_A|AXIS2_STREET_UNIQUE_EXACT|NONE"] += 1
                        attached_a += 1
                    else:
                        buckets[f"{attach_scope}|UNKNOWN|{code}|B"] += 1
                        why = "collision" if code == "COLLISION" else "no_match"

                # C (street-only unique)
                if attach_status == "UNKNOWN":
                    kC = keyC(town, street_norm)
                    pid, code = resolve(idxC, kC)
                    if pid:
                        e["property_id"] = pid
                        attach_status = "ATTACHED_B"
                        match_method = "axis2_street_name_unique"
                        buckets[f"{attach_scope}|ATTACHED_B|AXIS2_STREET_NAME_UNIQUE|NONE"] += 1
                        attached_b += 1
                    else:
                        buckets[f"{attach_scope}|UNKNOWN|{code}|C"] += 1
                        why = why or ("collision" if code == "COLLISION" else "no_match")

            # IMPORTANT: write back to the *top-level fields* your probe expects
            e["attach_scope"] = attach_scope
            e["attach_status"] = attach_status
            e["match_method"] = match_method
            e["why"] = why

            # lightweight sample capture (for unknowns)
            if attach_status == "UNKNOWN":
                sk = f"{attach_scope}|UNKNOWN|{(why or 'NONE').upper()}|SAMPLE"
                if len(samples[sk]) < args.max_samples:
                    samples[sk].append({
                        "event_id": e.get("event_id"),
                        "town": town,
                        "addr": addr,
                        "parsed": {"street_no": street_no, "street_name_norm": street_norm, "unit_norm": unit, "street_no_recovered": recovered},
                        "why": why
                    })

            w.write(json.dumps(e, ensure_ascii=False) + "\n")

    audit = {
        "run": {
            "script": os.path.basename(__file__),
            "version": "v1_23",
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
            "attached_a": attached_a,
            "attached_b": attached_b,
            "unknown": len(events) - attached_a - attached_b
        },
        "buckets": dict(buckets.most_common()),
        "samples": dict(samples),
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.audit)), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print("[done] v1_23")
    print("  events:", len(events))
    print("  attached_a:", attached_a)
    print("  attached_b:", attached_b)
    print("  unknown:", len(events) - attached_a - attached_b)
    print("  spine kept_rows:", spine_stats["kept_rows"])
    print("  keysA unique:", spine_stats["keysA_unique"], "total:", spine_stats["keysA_total"])
    print("  keysB unique:", spine_stats["keysB_unique"], "total:", spine_stats["keysB_total"])
    print("  keysC unique:", spine_stats["keysC_unique"], "total:", spine_stats["keysC_total"])

if __name__ == "__main__":
    main()
