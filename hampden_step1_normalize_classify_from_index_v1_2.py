#!/usr/bin/env python3
"""
Hampden STEP 1 v1.2 — Normalize & Classify (from index NDJSON, NO ATTACHING)

Reads every *_index_raw_*.ndjson in:
  backend/publicData/registry/hampden/_raw_from_index_v1

Writes event tables into:
  backend/publicData/registry/hampden/_events_v1

Event tables:
  deed_events.ndjson
  mortgage_events.ndjson
  assignment_events.ndjson
  lien_events.ndjson
  release_events.ndjson
  lis_pendens_events.ndjson
  foreclosure_events.ndjson

Classification requirement:
- This step assigns transaction_class BEFORE attachment.
- We only use what the index provides (grantor/grantee overlap, foreclosure file_key, nominal amount).
- UNKNOWN is first-class.

"""
from __future__ import annotations
import argparse, json, os, re, glob
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def load_ndjson(path: str) -> List[Dict[str, Any]]:
    out=[]
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            out.append(json.loads(line))
    return out

def norm_name(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r'[^A-Z0-9 ]+',' ',s)
    s = re.sub(r'\s+',' ',s).strip()
    return s

def classify_deed(rec: Dict[str, Any]) -> Tuple[str, float, List[str]]:
    """
    Returns (transaction_class, confidence_score, reasons[])
    """
    reasons=[]
    # Distress by file_key or doc_type hints
    fk = (rec.get("file_key") or "").lower()
    if fk == "foreclosure":
        return ("distress_transfer", 0.95, ["file_key=foreclosure"])
    desc = norm_name(rec.get("description_raw",""))
    if "FORECLOSURE" in desc:
        reasons.append("description_contains_foreclosure")
        return ("distress_transfer", 0.85, reasons)

    # Related/Restructure by party overlap
    g1 = [norm_name(x) for x in (rec.get("grantors") or [])]
    g2 = [norm_name(x) for x in (rec.get("grantees") or [])]
    overlap = set(g1) & set(g2)
    if overlap:
        reasons.append("grantor_grantee_overlap")
        return ("internal_restructure", 0.8, reasons)

    # Nominal consideration heuristic if amount exists
    amt = rec.get("amount", None)
    if isinstance(amt,(int,float)) and amt <= 100.0:
        reasons.append("nominal_amount<=100")
        return ("related_party_transfer", 0.55, reasons)

    return ("unknown", 0.2, ["insufficient_index_signals"])

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inDir", required=True)
    ap.add_argument("--outDir", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    in_dir = args.inDir
    out_dir = args.outDir
    safe_mkdir(out_dir)
    safe_mkdir(os.path.dirname(args.audit))

    files = sorted(glob.glob(os.path.join(in_dir, "*_index_raw_*.ndjson")))
    if not files:
        print(f"[error] no index NDJSON files found in {in_dir}")
        return 2

    buckets = {
        "deed": [],
        "mortgage": [],
        "assignment": [],
        "lien": [],
        "release": [],
        "discharge": [],
        "lis_pendens": [],
        "foreclosure": [],
    }

    for fp in files:
        for rec in load_ndjson(fp):
            fk = (rec.get("file_key") or "").lower()
            # Some step0 files encode lien variants as lien_ma/lien_fed — bucket them as lien with subtype
            if fk.startswith("lien"):
                rec["lien_subtype"] = fk
                buckets["lien"].append(rec)
            elif fk in buckets:
                buckets[fk].append(rec)
            else:
                # fall back on doc_type
                dt = (rec.get("doc_type") or "").upper()
                if dt == "MTG":
                    buckets["mortgage"].append(rec)
                else:
                    # unknown bucket is ignored for now (keeps audit visibility)
                    pass

    # Write event tables
    def write(path: str, rows: List[Dict[str, Any]]) -> None:
        with open(path,"w",encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Deeds + classification
    deed_events=[]
    tx_counts={}
    for r in buckets["deed"]:
        tx, conf, reasons = classify_deed(r)
        r["transaction_class"]=tx
        r["confidence_score"]=conf
        r["tx_reasons"]=reasons
        deed_events.append(r)
        tx_counts[tx]=tx_counts.get(tx,0)+1

    write(os.path.join(out_dir,"deed_events.ndjson"), deed_events)
    write(os.path.join(out_dir,"mortgage_events.ndjson"), buckets["mortgage"])
    write(os.path.join(out_dir,"assignment_events.ndjson"), buckets["assignment"])
    write(os.path.join(out_dir,"lien_events.ndjson"), buckets["lien"])

    # release/discharge collapse to release_events
    release_events = buckets["release"] + buckets["discharge"]
    write(os.path.join(out_dir,"release_events.ndjson"), release_events)
    write(os.path.join(out_dir,"lis_pendens_events.ndjson"), buckets["lis_pendens"])
    write(os.path.join(out_dir,"foreclosure_events.ndjson"), buckets["foreclosure"])

    audit = {
        "created_at": utc_iso(),
        "inDir": os.path.abspath(in_dir),
        "outDir": os.path.abspath(out_dir),
        "files": [os.path.basename(f) for f in files],
        "counts": {
            "deed_events": len(deed_events),
            "mortgage_events": len(buckets["mortgage"]),
            "assignment_events": len(buckets["assignment"]),
            "lien_events": len(buckets["lien"]),
            "release_events": len(release_events),
            "lis_pendens_events": len(buckets["lis_pendens"]),
            "foreclosure_events": len(buckets["foreclosure"]),
        },
        "deed_tx_class_counts": tx_counts,
    }
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("[done] deed_events:", len(deed_events))
    print("[done] deed_tx_class_counts:", tx_counts)
    print("[done] mortgage_events:", len(buckets["mortgage"]))
    print("[done] assignment_events:", len(buckets["assignment"]))
    print("[done] lien_events:", len(buckets["lien"]))
    print("[done] release_events:", len(release_events))
    print("[done] lis_pendens_events:", len(buckets["lis_pendens"]))
    print("[done] foreclosure_events:", len(buckets["foreclosure"]))
    print("[done] audit:", args.audit)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
