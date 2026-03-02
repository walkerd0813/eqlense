#!/usr/bin/env python3
"""events_dedupe_by_eventid_v3.py

Deterministic registry events dedupe by event_id.
- Streams NDJSON
- Keeps first occurrence of each event_id (stable)
- Writes duplicates to quarantine NDJSON
- Writes audit JSON

Usage:
  python scripts/_registry/attach/events_dedupe_by_eventid_v3.py \
    --infile <in.ndjson> \
    --out <out.ndjson> \
    --quarantine <dupes.ndjson> \
    --audit <audit.json>
"""
from __future__ import annotations
import argparse, json, os, hashlib
from datetime import datetime

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="Input NDJSON")
    ap.add_argument("--out", required=True, help="Output NDJSON (deduped)")
    ap.add_argument("--quarantine", required=True, help="Quarantine NDJSON for duplicates")
    ap.add_argument("--audit", required=True, help="Audit JSON output")
    args = ap.parse_args()

    infile = args.infile
    out = args.out
    quarantine = args.quarantine
    audit_path = args.audit

    rows_in = 0
    rows_out = 0
    dupes = 0
    parse_errors = 0
    missing_event_id = 0

    seen = set()

    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(quarantine) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(audit_path) or ".", exist_ok=True)

    started = datetime.utcnow().isoformat() + "Z"

    with open(infile, "r", encoding="utf-8") as fin, \
         open(out, "w", encoding="utf-8") as fout, \
         open(quarantine, "w", encoding="utf-8") as fbad:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            rows_in += 1
            try:
                r = json.loads(line)
            except Exception:
                parse_errors += 1
                fbad.write(line + "\n")
                continue

            eid = r.get("event_id")
            if not eid or not isinstance(eid, str):
                missing_event_id += 1
                fbad.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            if eid in seen:
                dupes += 1
                fbad.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            seen.add(eid)
            fout.write(json.dumps(r, ensure_ascii=False) + "\n")
            rows_out += 1

    finished = datetime.utcnow().isoformat() + "Z"

    audit = {
        "engine_id": "registry.dedupe_events_by_eventid_v3",
        "started_utc": started,
        "finished_utc": finished,
        "infile": infile,
        "out": out,
        "quarantine": quarantine,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "duplicates_quarantined": dupes,
        "parse_errors_quarantined": parse_errors,
        "missing_event_id_quarantined": missing_event_id,
        "sha256_out": sha256_file(out) if os.path.exists(out) else None,
        "sha256_quarantine": sha256_file(quarantine) if os.path.exists(quarantine) else None,
    }

    with open(audit_path, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, ensure_ascii=False, indent=2)

    return 2 if parse_errors > 0 else 0

if __name__ == "__main__":
    raise SystemExit(main())
