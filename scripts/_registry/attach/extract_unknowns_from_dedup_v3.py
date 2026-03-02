#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, os, hashlib
from datetime import datetime, timezone

def utc_iso():
    return datetime.now(timezone.utc).isoformat()

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def get_status(r: dict) -> str:
    a = r.get("attach") or {}
    s = a.get("status") or a.get("attach_status") or r.get("attach_status")
    return s if isinstance(s, str) else "UNKNOWN"

def get_town(r: dict) -> str:
    # prefer authoritative town fields; do not use neighborhood
    pref = (r.get("property_ref") or {})
    for k in ("town_raw", "town", "city_raw", "city"):
        v = pref.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # fallback: top-level city/town if present
    for k in ("town_raw", "town", "city_raw", "city"):
        v = r.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out_unknown_all", required=True)
    ap.add_argument("--out_unknown_boston", required=True)
    ap.add_argument("--out_building_only", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    infile = args.infile
    out_unknown_all = args.out_unknown_all
    out_unknown_boston = args.out_unknown_boston
    out_building_only = args.out_building_only
    audit_path = args.audit

    for p in (out_unknown_all, out_unknown_boston, out_building_only, audit_path):
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)

    started = utc_iso()

    rows_in = 0
    parse_errors = 0
    unknown_all = 0
    unknown_boston = 0
    building_only = 0
    statuses = {}

    with open(infile, "r", encoding="utf-8") as fin, \
         open(out_unknown_all, "w", encoding="utf-8") as f_ua, \
         open(out_unknown_boston, "w", encoding="utf-8") as f_ub, \
         open(out_building_only, "w", encoding="utf-8") as f_bo:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            rows_in += 1
            try:
                r = json.loads(line)
            except Exception:
                parse_errors += 1
                continue

            st = get_status(r)
            statuses[st] = statuses.get(st, 0) + 1

            if st == "UNKNOWN":
                unknown_all += 1
                f_ua.write(json.dumps(r, ensure_ascii=False) + "\n")
                town = get_town(r).upper()
                if town == "BOSTON":
                    unknown_boston += 1
                    f_ub.write(json.dumps(r, ensure_ascii=False) + "\n")
            elif st == "BUILDING_ONLY":
                building_only += 1
                f_bo.write(json.dumps(r, ensure_ascii=False) + "\n")

    finished = utc_iso()

    audit = {
        "engine_id": "registry.extract_unknowns_from_dedup_v3",
        "started_utc": started,
        "finished_utc": finished,
        "infile": infile,
        "rows_in": rows_in,
        "parse_errors": parse_errors,
        "counts": {
            "unknown_all": unknown_all,
            "unknown_boston": unknown_boston,
            "building_only": building_only
        },
        "statuses": statuses,
        "outputs": {
            "out_unknown_all": out_unknown_all,
            "out_unknown_boston": out_unknown_boston,
            "out_building_only": out_building_only
        },
        "sha256": {
            "out_unknown_all": sha256_file(out_unknown_all) if os.path.exists(out_unknown_all) else None,
            "out_unknown_boston": sha256_file(out_unknown_boston) if os.path.exists(out_unknown_boston) else None,
            "out_building_only": sha256_file(out_building_only) if os.path.exists(out_building_only) else None
        }
    }

    with open(audit_path, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, ensure_ascii=False, indent=2)

    # Exit 0 unless input had parse errors (keep strict if you want)
    return 2 if parse_errors > 0 else 0

if __name__ == "__main__":
    raise SystemExit(main())
