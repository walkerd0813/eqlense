#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime, timezone


def nowz() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def up(s: str) -> str:
    return (s or '').strip().upper()


def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or '').strip())


def norm_punct(s: str) -> str:
    s = up(s)
    # Keep dash for ranges like 35-37
    s = s.replace("'", "")
    s = re.sub(r"[\.,;:#]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


BOSTON_NEIGHBORHOOD_ALIASES = {
    # common Boston neighborhoods that appear in some sources as "town"
    "MATTAPAN": "BOSTON",
    "HYDE PARK": "BOSTON",
    "DORCHESTER": "BOSTON",
    "JAMAICA PLAIN": "BOSTON",
    "ROSLINDALE": "BOSTON",
    "ROXBURY": "BOSTON",
    "WEST ROXBURY": "BOSTON",
    "EAST BOSTON": "BOSTON",
    "CHARLESTOWN": "BOSTON",
    "ALLSTON": "BOSTON",
    "BRIGHTON": "BOSTON",
    "SOUTH BOSTON": "BOSTON",
    "SOUTH END": "BOSTON",
    "BACK BAY": "BOSTON",
    "MISSION HILL": "BOSTON",
    "FENWAY": "BOSTON",
}


def norm_town(town: str) -> str:
    t = norm_punct(town)
    return BOSTON_NEIGHBORHOOD_ALIASES.get(t, t)


def street_variants(street_name_norm: str):
    """Generate a small deterministic set of variants."""
    s = norm_punct(street_name_norm)
    out = {s}

    # SAINT <-> ST (ONLY when ST is the saint token, not STREET)
    # If pattern like: ST JOSEPH ... => SAINT JOSEPH ...
    out.add(re.sub(r"\bST\s+", "SAINT ", s))
    out.add(re.sub(r"\bSAINT\s+", "ST ", s))

    # STREET <-> ST (saint fix for bad normalizers: "ST" expanded to "STREET")
    out.add(re.sub(r"\bSTREET\s+", "ST ", s))
    out.add(re.sub(r"\bSTREET\s+", "SAINT ", s))


    # ROSEBERY <-> ROSEBERRY
    if "ROSEBERY" in s:
        out.add(s.replace("ROSEBERY", "ROSEBERRY"))
    if "ROSEBERRY" in s:
        out.add(s.replace("ROSEBERRY", "ROSEBERY"))

    # O DONNELL / ODONNELL (apostrophe already removed)
    if "O DONNELL" in s:
        out.add(s.replace("O DONNELL", "ODONNELL"))
    if "ODONNELL" in s:
        out.add(s.replace("ODONNELL", "O DONNELL"))

    return [x for x in out if x]


def parse_addr(addr_norm: str):
    """Return (street_no_raw, street_name_raw, flags)."""
    a = norm_punct(addr_norm)
    flags = {"is_range": False, "is_half": False}
    if not a:
        return None, None, {"bad": True}

    # capture leading number or range like 35-37 or 35 37
    m = re.match(r"^(\d+(?:\s*[-]\s*\d+)?)(?:\s+(.*))?$", a)
    if not m:
        return None, None, {"bad": True}

    street_no = (m.group(1) or "").strip()
    rest = (m.group(2) or "").strip()

    # normalize range formatting
    if "-" in street_no:
        flags["is_range"] = True
        street_no = re.sub(r"\s*[-]\s*", "-", street_no)

    # handle "11 1/2" style (rare in registry index)
    if rest.startswith("1/2 ") or rest.endswith(" 1/2") or " 1/2 " in a:
        flags["is_half"] = True

    if not rest:
        return street_no, None, {"bad": True}

    return street_no, rest, flags


def spine_keys_from_row(r: dict):
    """Extract town, street_no, street_name from spine row.

    Prefer explicit fields; fallback to address_key pattern: A|<no>|<street>|<town>|<zip>
    """
    town = up(r.get("town") or "")
    street_no = (r.get("street_no") or "")
    street_name = (r.get("street_name") or "")

    if town and street_no and street_name:
        return norm_town(town), up(str(street_no)), norm_punct(street_name)

    ak = (r.get("address_key") or "").strip()
    if ak:
        parts = ak.split("|")
        if len(parts) >= 5:
            # parts[0] tier
            sn = parts[1]
            st = parts[2]
            tw = parts[3]
            if sn and st and tw:
                return norm_town(tw), up(sn), norm_punct(st)

    # as last resort, try address_label like "366 W SECOND ST 6, BOSTON, MA 02127"
    al = (r.get("address_label") or "")
    if al:
        # take left side before comma
        left = al.split(",")[0]
        left = norm_punct(left)
        sn, st, f = parse_addr(left)
        if not f.get("bad"):
            return norm_town(town), up(sn), norm_punct(st)

    return None, None, None


def build_base_index(spine_path: str):
    base_index = {}  # key -> row OR list (collision)
    stats = {"rows_scanned": 0, "keys": 0, "collisions": 0}

    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            stats["rows_scanned"] += 1
            r = json.loads(line)
            town, street_no, street_name = spine_keys_from_row(r)
            if not town or not street_no or not street_name:
                continue
            key = f"{town}|{street_no}|{street_name}"
            stats["keys"] += 1
            if key not in base_index:
                base_index[key] = r
            else:
                # promote to list
                prev = base_index[key]
                if isinstance(prev, list):
                    prev.append(r)
                else:
                    base_index[key] = [prev, r]
                stats["collisions"] += 1

    return base_index, stats


def apply_attach(ev: dict, spine_row: dict, status: str, method: str, match_key_used: str):
    a = ev.get("attach") or {}
    a["attach_status"] = status
    a["property_id"] = spine_row.get("property_id") or spine_row.get("property_uid") or spine_row.get("parcel_id")
    a["match_method"] = method
    a["match_key_used"] = match_key_used
    ev["attach"] = a


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", required=True)
    args = ap.parse_args()

    audit = {
        "engine_id": args.engine_id,
        "infile": args.infile,
        "spine": args.spine,
        "out": args.out,
        "audit": args.audit,
        "started_at": nowz(),
        "rows_scanned": 0,
        "rows_unknown_in": 0,
        "rows_attached": 0,
        "rows_no_match": 0,
        "rows_collision_blocked": 0,
        "rows_bad_addr": 0,
        "detail_counts": {},
    }

    def bump(k: str):
        audit["detail_counts"][k] = audit["detail_counts"].get(k, 0) + 1

    base_index, spine_stats = build_base_index(args.spine)
    audit["spine_stats"] = spine_stats

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            audit["rows_scanned"] += 1
            ev = json.loads(line)

            status = up((ev.get("attach") or {}).get("attach_status") or ev.get("attach_status") or "")
            if status and status != "UNKNOWN":
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            audit["rows_unknown_in"] += 1

            pref = ev.get("property_ref") or {}
            town = norm_town(pref.get("town_code") or ev.get("town") or "")
            addr = pref.get("address_norm") or pref.get("address_raw") or ev.get("full_address") or ""
            addr = norm_punct(addr)

            if not town or not addr:
                audit["rows_bad_addr"] += 1
                bump("missing_town_or_addr")
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            street_no_raw, street_name_raw, flags = parse_addr(addr)
            if flags.get("bad") or not street_no_raw or not street_name_raw:
                audit["rows_bad_addr"] += 1
                bump("addr_parse_fail")
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            # for ranges, try the first number only (deterministic, conservative)
            street_no_try = street_no_raw
            if flags.get("is_range"):
                street_no_try = street_no_raw.split("-")[0]

            attached = False
            for stname in street_variants(street_name_raw):
                key = f"{town}|{up(street_no_try)}|{stname}"
                got = base_index.get(key)
                if not got:
                    continue
                if isinstance(got, list):
                    audit["rows_collision_blocked"] += 1
                    bump("collision")
                    attached = True  # stop searching; keep UNKNOWN but we learned it's a collision
                    break
                apply_attach(ev, got, "ATTACHED_A", "postfix|property_ref_rescue_v3", f"{town}|{street_no_try} {stname}")
                audit["rows_attached"] += 1
                bump("attached")
                attached = True
                break

            if not attached:
                audit["rows_no_match"] += 1
                bump("no_match")

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit["finished_at"] = nowz()
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
