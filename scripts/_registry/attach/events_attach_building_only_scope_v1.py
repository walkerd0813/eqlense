from __future__ import annotations

import argparse
import json
import time
import hashlib
import re
from collections import defaultdict, Counter


_LEAD_NUM_RE = re.compile(r"^\s*\d+[A-Z]?\s+")
_WS_RE = re.compile(r"\s+")


def norm_street_for_building(street: str) -> str:
    """
    Deterministic building-base street:
    - uppercase
    - collapse whitespace
    - strip leading street number (e.g., "25 MT VERNON ST" -> "MT VERNON ST")
    """
    if not street:
        return ""
    s = street.strip().upper()
    s = _WS_RE.sub(" ", s)
    s = _LEAD_NUM_RE.sub("", s).strip()
    return s


def parse_match_key(match_key: str):
    """
    match_key format observed: "TOWN|<street...>" (no state/zip)
    """
    if not match_key or "|" not in match_key:
        return None, None
    town, street = match_key.split("|", 1)
    town = (town or "").strip().upper()
    street = (street or "").strip().upper()
    street = _WS_RE.sub(" ", street)
    return town, street


def stable_building_id(building_key: str) -> str:
    h = hashlib.sha1(building_key.encode("utf-8")).hexdigest()[:24]
    return f"ma:building:{h}"


def load_authority_from_canon(canon_path: str, max_rows: int = 0):
    """
    Build TWO authority maps from ATTACHED_A canon rows:
      A) exact base_key: town|street_as_is
      B) building_key:  town|street_without_leading_number
    Both map to a stable set of parcel_ids.
    """
    exact_to_parcels = defaultdict(set)
    building_to_parcels = defaultdict(set)
    seen = 0
    json_err = 0

    with open(canon_path, "r", encoding="utf-8") as f:
        for ln in f:
            if not ln.strip():
                continue
            try:
                r = json.loads(ln)
            except Exception:
                json_err += 1
                continue

            a = r.get("attach") or {}
            st = (a.get("status") or a.get("attach_status") or "").upper()
            if st != "ATTACHED_A":
                continue

            mk = a.get("match_key")
            pid = a.get("property_id")
            if not mk or not pid:
                continue

            town, street = parse_match_key(mk)
            if not town or not street:
                continue

            exact_key = f"{town}|{street}"
            exact_to_parcels[exact_key].add(pid)

            bstreet = norm_street_for_building(street)
            if bstreet:
                bkey = f"{town}|{bstreet}"
                building_to_parcels[bkey].add(pid)

            seen += 1
            if max_rows and seen >= max_rows:
                break

    # convert sets -> sorted lists (deterministic)
    exact_to_parcels2 = {k: sorted(v) for k, v in exact_to_parcels.items()}
    building_to_parcels2 = {k: sorted(v) for k, v in building_to_parcels.items()}
    return exact_to_parcels2, building_to_parcels2, json_err


def is_building_only_row(a: dict) -> bool:
    st = (a.get("status") or "").upper()
    st2 = (a.get("attach_status") or "").upper()
    m = (a.get("method") or "").lower()
    return (st == "BUILDING_ONLY") or (st2 == "BUILDING_ONLY") or (m == "building_base_collision")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--canon", required=True)
    ap.add_argument("--events", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--canon_max_rows", type=int, default=0)
    args = ap.parse_args()

    t0 = time.time()

    exact_auth, building_auth, canon_json_err = load_authority_from_canon(args.canon, args.canon_max_rows)

    counts = Counter()
    counts["canon_authority_exact_keys"] = len(exact_auth)
    counts["canon_authority_building_keys"] = len(building_auth)

    out_rows = 0
    json_err = 0

    with open(args.out, "w", encoding="utf-8") as fo:
        with open(args.events, "r", encoding="utf-8") as f:
            for ln in f:
                if not ln.strip():
                    continue
                try:
                    r = json.loads(ln)
                except Exception:
                    json_err += 1
                    continue

                counts["events_rows_seen"] += 1
                a = r.get("attach") or {}

                if not is_building_only_row(a):
                    counts["events_non_building_only_skipped"] += 1
                    continue

                mk = a.get("match_key") or a.get("match_variant_addr")
                if not mk:
                    counts["events_no_match_key"] += 1
                    continue

                town, street = parse_match_key(mk)
                if not town or not street:
                    counts["events_bad_match_key"] += 1
                    continue

                exact_key = f"{town}|{street}"
                building_key = f"{town}|{norm_street_for_building(street)}"

                parcel_ids = exact_auth.get(exact_key)
                if parcel_ids:
                    counts["matched_on_exact_key"] += 1
                    used_key = exact_key
                else:
                    parcel_ids = building_auth.get(building_key)
                    if parcel_ids:
                        counts["matched_on_building_key"] += 1
                        used_key = building_key
                    else:
                        counts["building_no_authority_match"] += 1
                        continue

                building_id = stable_building_id(used_key)

                a2 = dict(a)
                a2["status"] = "ATTACHED_BUILDING"
                a2["attach_status"] = "ATTACHED_BUILDING"
                a2["attach_scope"] = "BUILDING"
                a2["property_id"] = building_id
                a2["method"] = "building_scope_authority"

                ev = dict(a2.get("evidence") or {})
                ev["building_key_used"] = used_key
                ev["building_parcel_ids"] = parcel_ids
                ev["building_parcel_count"] = len(parcel_ids)
                ev["original_match_key"] = mk
                a2["evidence"] = ev

                flags = list(a2.get("flags") or [])
                if "ATTACHED_BUILDING_SCOPE" not in flags:
                    flags.append("ATTACHED_BUILDING_SCOPE")
                a2["flags"] = flags

                r2 = dict(r)
                r2["attach"] = a2

                fo.write(json.dumps(r2, ensure_ascii=False) + "\n")
                out_rows += 1
                counts["upgraded_attached_building"] += 1

                if args.limit and counts["upgraded_attached_building"] >= args.limit:
                    break

    counts["json_errors_skipped"] = json_err

    audit = {
        "engine": "events_attach_building_only_scope_v1_2",
        "inputs": {"canon": args.canon, "events": args.events},
        "params": {"limit": args.limit, "canon_max_rows": args.canon_max_rows},
        "counts": dict(counts),
        "seconds": round(time.time() - t0, 2),
        "canon_json_errors_skipped": canon_json_err,
        "out_rows_written": out_rows,
        "out": args.out,
    }

    with open(args.audit, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, ensure_ascii=False, indent=2)

    print("[ok]", json.dumps(audit, ensure_ascii=False))


if __name__ == "__main__":
    main()
