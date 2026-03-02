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

def get_attach(r: dict) -> dict:
    a = r.get("attach")
    if isinstance(a, dict):
        return a
    a = {}
    r["attach"] = a
    return a

def get_property_id(r: dict) -> str | None:
    pid = r.get("property_id")
    return pid if isinstance(pid, str) and pid.strip() else None

def get_building_key(r: dict) -> str | None:
    # We attach at building scope only if an explicit building_key exists.
    # Prefer property_ref.building_key if present, else top-level building_key.
    pr = r.get("property_ref") or {}
    if isinstance(pr, dict):
        bk = pr.get("building_key") or pr.get("building_key_norm")
        if isinstance(bk, str) and bk.strip():
            return bk.strip()
    bk = r.get("building_key") or r.get("building_key_norm")
    return bk.strip() if isinstance(bk, str) and bk.strip() else None

def is_building_only(r: dict) -> bool:
    a = r.get("attach") or {}
    st = a.get("status") or a.get("attach_status") or r.get("attach_status")
    return st == "BUILDING_ONLY"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    infile = args.infile
    out = args.out
    audit_path = args.audit

    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(audit_path) or ".", exist_ok=True)

    started = utc_iso()

    rows_in = 0
    rows_written = 0
    parse_errors = 0
    missing_building_key = 0
    missing_property_id = 0

    with open(infile, "r", encoding="utf-8") as fin, open(out, "w", encoding="utf-8") as fout:
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

            if not is_building_only(r):
                # This script is meant to run on BUILDING_ONLY slice,
                # but if upstream passes mixed rows, ignore non-building-only.
                continue

            pid = get_property_id(r)
            if not pid:
                missing_property_id += 1
                continue

            bk = get_building_key(r)
            if not bk:
                missing_building_key += 1
                continue

            a = get_attach(r)

            # deterministic building-scope upgrade marker
            a["status"] = "ATTACHED_BUILDING"
            a["attach_scope"] = "BUILDING"
            a["attach_precision"] = "BUILDING"
            a["attach_method"] = a.get("attach_method") or "building_key_exact"
            a["building_key"] = bk

            # flag so promotion step can safely elevate without lying
            flags = a.get("flags")
            if not isinstance(flags, list):
                flags = []
                a["flags"] = flags
            if "ATTACHED_A_BUILDING_SCOPE" not in flags:
                flags.append("ATTACHED_A_BUILDING_SCOPE")

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")
            rows_written += 1

    finished = utc_iso()

    audit = {
        "engine_id": "registry.attach_building_only_scope_v1_2",
        "started_utc": started,
        "finished_utc": finished,
        "infile": infile,
        "out": out,
        "rows_in": rows_in,
        "rows_written": rows_written,
        "parse_errors": parse_errors,
        "missing_property_id": missing_property_id,
        "missing_building_key": missing_building_key,
        "sha256_out": sha256_file(out) if os.path.exists(out) else None
    }

    with open(audit_path, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, ensure_ascii=False, indent=2)

    return 2 if parse_errors > 0 else 0

if __name__ == "__main__":
    raise SystemExit(main())
