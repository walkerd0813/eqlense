#!/usr/bin/env python3
"""
Hampden STEP 1 v1.3 — Normalize + Classify (from index NDJSON, NO ATTACHING)

Fixes v1.2 issue:
- v1.2 expected exact filename versions and missed v1_4 / v1_7 files.
- v1.3 globs ALL: *_index_raw_v1_*.ndjson in inDir and infers type from:
  (1) row.event_type, (2) row.source.file_key, (3) filename prefix

Inputs:
  backend/publicData/registry/hampden/_raw_from_index_v1/*_index_raw_v1_*.ndjson

Outputs (in outDir):
  deed_events.ndjson
  mortgage_events.ndjson
  assignment_events.ndjson
  lien_events.ndjson
  release_events.ndjson
  lis_pendens_events.ndjson
  foreclosure_events.ndjson

Also writes audit JSON with counts + deed transaction_class distribution.
"""

import argparse, os, json, re, glob
from datetime import datetime, timezone
from collections import Counter, defaultdict

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

FILE_RE = re.compile(r"^(?P<key>[a-z0-9_]+)_index_raw_v1_", re.IGNORECASE)

KEY_TO_EVENT = {
    "deed": "DEED",
    "mortgage": "MORTGAGE",
    "mortgage_landcourt": "MORTGAGE",
    "assignment": "ASSIGNMENT",
    "assignment_landcourt": "ASSIGNMENT",
    "lien": "LIEN",
    "lien_ma": "LIEN",
    "lien_fed": "LIEN",
    "lien_landcourt": "LIEN",
    "lien_ma_landcourt": "LIEN",
    "lien_fed_landcourt": "LIEN",
    "release": "RELEASE",
    "discharge": "RELEASE",
    "release_landcourt": "RELEASE",
    "discharge_landcourt": "RELEASE",
    "lis_pendens": "LIS_PENDENS",
    "lis_pendens_landcourt": "LIS_PENDENS",
    "foreclosure": "FORECLOSURE",
    "foreclosure_landcourt": "FORECLOSURE",
}

def infer_file_key_from_name(path):
    fn=os.path.basename(path)
    m=FILE_RE.match(fn)
    if not m:
        return None
    return m.group("key").lower()

def infer_event_type(row, file_key):
    et=(row.get("event_type") or "").upper().strip()
    if et:
        return et
    sk=((row.get("source") or {}).get("file_key") or "").lower().strip()
    if sk and sk in KEY_TO_EVENT:
        return KEY_TO_EVENT[sk]
    if file_key and file_key in KEY_TO_EVENT:
        return KEY_TO_EVENT[file_key]
    return "UNKNOWN"

def deed_tx_class(row):
    # Index rows are thin. We keep it conservative and only tag distress when it's clearly foreclosure.
    et=(row.get("event_type") or "").upper()
    if et == "FORECLOSURE":
        return ("distress_transfer", 0.95, ["event_type=FORECLOSURE"])
    # Sometimes foreclosure-like clues appear in source file_key
    fk=((row.get("source") or {}).get("file_key") or "").lower()
    if "foreclos" in fk or "sheriff" in fk:
        return ("distress_transfer", 0.85, ["file_key indicates foreclosure"])
    # Otherwise unknown until we enrich with full deed docs later
    return ("unknown", 0.40, ["index_only_no_price_parties"])

def write_ndjson(path, rows):
    with open(path,"w",encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--inDir", required=True)
    ap.add_argument("--outDir", required=True)
    ap.add_argument("--audit", required=True)
    args=ap.parse_args()

    os.makedirs(args.outDir, exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    files=sorted(glob.glob(os.path.join(args.inDir, "*_index_raw_v1_*.ndjson")))
    if not files:
        raise SystemExit(f"[error] No *_index_raw_v1_*.ndjson files found in {args.inDir}")

    buckets=defaultdict(list)
    deed_tx_counts=Counter()
    file_counts={}

    for fp in files:
        file_key=infer_file_key_from_name(fp)
        n=0
        with open(fp,"r",encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: 
                    continue
                try:
                    row=json.loads(line)
                except Exception:
                    continue
                et=infer_event_type(row, file_key)
                # normalize into lightweight event record
                rec={
                    "event_id": row.get("event_id"),
                    "event_type": et,
                    "county": row.get("county","hampden"),
                    "recording": row.get("recording") or {},
                    "property_ref": row.get("property_ref") or {},
                    "source": row.get("source") or {},
                    "meta": row.get("meta") or {},
                }

                if et == "DEED":
                    tx, conf, reasons = deed_tx_class(rec)
                    rec["transaction_class"]=tx
                    rec["transaction_class_confidence"]=conf
                    rec["transaction_class_reasons"]=reasons
                    deed_tx_counts[tx]+=1

                buckets[et].append(rec)
                n+=1
        file_counts[os.path.basename(fp)]={"rows": n, "file_key": file_key}

    # Write outputs
    out_paths={
        "DEED": os.path.join(args.outDir,"deed_events.ndjson"),
        "MORTGAGE": os.path.join(args.outDir,"mortgage_events.ndjson"),
        "ASSIGNMENT": os.path.join(args.outDir,"assignment_events.ndjson"),
        "LIEN": os.path.join(args.outDir,"lien_events.ndjson"),
        "RELEASE": os.path.join(args.outDir,"release_events.ndjson"),
        "LIS_PENDENS": os.path.join(args.outDir,"lis_pendens_events.ndjson"),
        "FORECLOSURE": os.path.join(args.outDir,"foreclosure_events.ndjson"),
        "UNKNOWN": os.path.join(args.outDir,"unknown_events.ndjson"),
    }
    for k,p in out_paths.items():
        write_ndjson(p, buckets.get(k, []))

    audit={
        "created_at": now_iso(),
        "inDir": args.inDir,
        "outDir": args.outDir,
        "files_scanned": file_counts,
        "counts": {k: len(v) for k,v in buckets.items()},
        "deed_tx_class_counts": dict(deed_tx_counts),
        "outputs": out_paths,
        "note": "v1.3 globs all *_index_raw_v1_*.ndjson and is version-agnostic."
    }
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    # Console summary in your style
    print("[start] Hampden STEP 1 v1.3 - Normalize + Classify (from index)")
    print("[info] inDir:", args.inDir)
    print("[info] outDir:", args.outDir)
    print("[done] deed_events:", len(buckets.get("DEED",[])))
    print("[done] deed_tx_class_counts:", dict(deed_tx_counts))
    print("[done] mortgage_events:", len(buckets.get("MORTGAGE",[])))
    print("[done] assignment_events:", len(buckets.get("ASSIGNMENT",[])))
    print("[done] lien_events:", len(buckets.get("LIEN",[])))
    print("[done] release_events:", len(buckets.get("RELEASE",[])))
    print("[done] lis_pendens_events:", len(buckets.get("LIS_PENDENS",[])))
    print("[done] foreclosure_events:", len(buckets.get("FORECLOSURE",[])))
    print("[done] unknown_events:", len(buckets.get("UNKNOWN",[])))
    print("[done] audit:", args.audit)
    print("[done] STEP 1 v1.3 complete.")
    print("[next] STEP 2 - Attach events to Property Spine (confidence-gated).")

if __name__=="__main__":
    main()
