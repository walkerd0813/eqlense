#!/usr/bin/env python3
"""
Phase 5 — Hampden County
STEP 1 (From Index PDFs): Normalize & Classify (NO ATTACHING)

Inputs:
  backend/publicData/registry/hampden/_raw_from_index_v1/*_index_raw_v1_2.ndjson
    - deed_index_raw_v1_2.ndjson
    - mortgage_index_raw_v1_2.ndjson
    - assignment_index_raw_v1_2.ndjson
    - lien_index_raw_v1_2.ndjson
    - discharge_index_raw_v1_2.ndjson
    - lis_pendens_index_raw_v1_2.ndjson
    - foreclosure_index_raw_v1_2.ndjson

Outputs (standardized tables):
  backend/publicData/registry/hampden/_events_v1/
    - deed_events.ndjson            (includes transaction_class + confidence)
    - mortgage_events.ndjson
    - assignment_events.ndjson
    - lien_events.ndjson
    - release_events.ndjson         (from discharge/release)
    - lis_pendens_events.ndjson
    - foreclosure_events.ndjson
  Audit:
    backend/publicData/_audit/registry/hampden_events_v1_from_index_audit.json

This step ONLY:
- normalizes schema
- classifies deeds into:
    arms_length_sale | related_party_transfer | internal_restructure | distress_transfer | unknown
- NEVER attaches to parcel/property_id
"""

import argparse, json, os, re, hashlib
from datetime import datetime, timezone
from collections import Counter, defaultdict

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            yield json.loads(line)

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def as_str(x):
    if x is None:
        return None
    if isinstance(x, str):
        x=x.strip()
        return x or None
    return str(x).strip() or None

def norm_name(name):
    if not name: return None
    s = re.sub(r"[^A-Z0-9\s&\-']", " ", str(name).upper())
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def parties_from_raw(raw_list):
    # index rows don't know grantor/grantee; keep as participants
    out=[]
    if isinstance(raw_list, list):
        for p in raw_list:
            if isinstance(p, dict):
                nm = norm_name(p.get("name"))
                if nm:
                    out.append({"name": nm, "raw": p.get("raw")})
    return out

def parse_amount_text(amt_text):
    if not amt_text: return None
    s=str(amt_text)
    s=s.replace(",","").replace("$","").strip()
    m=re.search(r"(\d+(\.\d+)?)", s)
    if not m: return None
    try:
        v=float(m.group(1))
        # keep int if whole
        return int(v) if abs(v-int(v))<1e-9 else v
    except Exception:
        return None

NOMINAL_AMOUNTS={1,10,100}

def shared_token_overlap(names):
    # for index-only deeds we only have participant list; overlap is unknown
    # keep for future, return 0
    return 0

def classify_deed_from_index(row):
    """
    With index-only data, we don't have clear grantor vs grantee.
    So classification relies on:
      - source filename cues (foreclosure_deeds)
      - document_type tokens if present in raw_lines
      - nominal/blank consideration/amount_text (weak)
    Returns (class, conf, evidence[])
    """
    evidence=[]
    srcpdf=((row.get("source") or {}).get("source_pdf") or "").lower()
    raw_lines=" ".join((row.get("raw_lines") or [])).upper()

    # distress
    if "forcl" in srcpdf or "foreclos" in srcpdf or "sheriff" in srcpdf or "REO" in raw_lines or "FORECLOS" in raw_lines:
        evidence.append("source_or_text_foreclosure")
        return ("distress_transfer", 0.90, evidence)

    # internal restructure hints
    internal_kw=["CONFIRMATORY","CORRECTION","RECTIF","REORGAN","CONSOLIDAT"]
    if any(k in raw_lines for k in internal_kw):
        evidence.append("internal_keyword")
        amt=parse_amount_text(row.get("amount_text"))
        if amt in NOMINAL_AMOUNTS:
            evidence.append("nominal_amount")
            return ("internal_restructure", 0.88, evidence)
        return ("internal_restructure", 0.75, evidence)

    amt=parse_amount_text(row.get("amount_text"))
    if amt is not None and amt >= 1000:
        evidence.append("non_nominal_amount")
        return ("arms_length_sale", 0.70, evidence)

    if amt in NOMINAL_AMOUNTS:
        evidence.append("nominal_amount")
        return ("internal_restructure", 0.65, evidence)

    return ("unknown", 0.40, ["insufficient_signals_index_only"])

def dataset_hash_for_files(paths):
    h=hashlib.sha256()
    for p in sorted(paths):
        st=os.stat(p)
        h.update(f"{p}|{st.st_size}|{int(st.st_mtime)}".encode("utf-8"))
    return h.hexdigest()

def find_inputs(in_dir):
    found={}
    for fn in os.listdir(in_dir):
        if not fn.endswith(".ndjson"): continue
        f=fn.lower()
        full=os.path.join(in_dir, fn)
        if "deed_index_raw" in f or (f.startswith("deed_") and "index_raw" in f):
            found["deed"]=full
        elif "mortgage_index_raw" in f:
            found["mortgage"]=full
        elif "assignment_index_raw" in f:
            found["assignment"]=full
        elif "lien_index_raw" in f:
            found["lien"]=full
        elif "discharge_index_raw" in f or "release_index_raw" in f:
            found["release"]=full
        elif "lis_pendens_index_raw" in f:
            found["lis_pendens"]=full
        elif "foreclosure_index_raw" in f:
            found["foreclosure"]=full
    return found

def normalize_row(row, event_type, dataset_version, dataset_hash, as_of_date):
    rec = row.get("recording") or {}
    pref = row.get("property_ref") or {}
    src = row.get("source") or {}

    base={
        "event_id": row.get("event_id"),
        "event_type": event_type,
        "county": "hampden",
        "recording": {
            "recording_date": as_str(rec.get("recording_date")),
            "recording_time": as_str(rec.get("recording_time")),
            "book": as_str(rec.get("book")),
            "page": as_str(rec.get("page")),
            "document_number": as_str(rec.get("document_number")),
            "document_type": as_str(rec.get("document_type")),
        },
        "property_ref": {
            "town": as_str(pref.get("town")) or as_str(pref.get("city")),
            "address": as_str(pref.get("address")),
        },
        "participants": parties_from_raw(row.get("parties_raw")),
        "amount": parse_amount_text(row.get("amount_text")),
        "source": {
            "source_system": as_str(src.get("source_system")),
            "source_pdf": as_str(src.get("source_pdf")),
            "page_no": src.get("page_no"),
            "file_key": as_str(src.get("file_key")),
        },
        "raw_lines": row.get("raw_lines") or [],
        "meta": {
            "as_of_date": as_of_date,
            "dataset_version": dataset_version,
            "dataset_hash": dataset_hash,
            "normalized_at": now_iso()
        }
    }
    return base

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--inDir", required=True, help=".../registry/hampden/_raw_from_index_v1")
    ap.add_argument("--outDir", required=True, help=".../registry/hampden/_events_v1")
    ap.add_argument("--audit", required=True)
    ap.add_argument("--datasetVersion", default="hampden_events_v1_from_index")
    ap.add_argument("--asOfDate", default=datetime.now(timezone.utc).date().isoformat())
    args=ap.parse_args()

    inputs=find_inputs(args.inDir)
    if "deed" not in inputs:
        raise SystemExit("[error] deed_index_raw_v1_2.ndjson not found in --inDir. Run STEP 0 first for deeds.")

    dataset_hash=dataset_hash_for_files(list(inputs.values()))

    os.makedirs(args.outDir, exist_ok=True)
    audit={
        "created_at": now_iso(),
        "as_of_date": args.asOfDate,
        "dataset_version": args.datasetVersion,
        "dataset_hash": dataset_hash,
        "inputs": inputs,
        "counts": {},
        "deed_transaction_class_counts": {},
        "warnings": []
    }

    # deeds with classification
    deed_out=os.path.join(args.outDir,"deed_events.ndjson")
    deed_rows=[]
    tx_counts=Counter()
    for r in read_ndjson(inputs["deed"]):
        norm=normalize_row(r,"DEED",args.datasetVersion,dataset_hash,args.asOfDate)
        tx_class, tx_conf, tx_evidence = classify_deed_from_index(r)
        norm["transaction"]={
            "transaction_class": tx_class,
            "transaction_confidence": round(float(tx_conf),3),
            "transaction_evidence": tx_evidence
        }
        deed_rows.append(norm)
        tx_counts[tx_class]+=1
    write_ndjson(deed_out, deed_rows)
    audit["counts"]["deed_events"]=len(deed_rows)
    audit["deed_transaction_class_counts"]=dict(tx_counts)

    # other event passthroughs
    mapping=[
        ("mortgage","MORTGAGE","mortgage_events.ndjson"),
        ("assignment","ASSIGNMENT","assignment_events.ndjson"),
        ("lien","LIEN","lien_events.ndjson"),
        ("release","RELEASE","release_events.ndjson"),
        ("lis_pendens","LIS_PENDENS","lis_pendens_events.ndjson"),
        ("foreclosure","FORECLOSURE","foreclosure_events.ndjson"),
    ]

    for key, et, outname in mapping:
        if key not in inputs:
            continue
        outp=os.path.join(args.outDir,outname)
        rows=[]
        for r in read_ndjson(inputs[key]):
            rows.append(normalize_row(r, et, args.datasetVersion, dataset_hash, args.asOfDate))
        write_ndjson(outp, rows)
        audit["counts"][outname.replace(".ndjson","")]=len(rows)

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("[start] Hampden STEP 1 — Normalize & Classify (from index)")
    print("[info] inDir:", args.inDir)
    print("[info] outDir:", args.outDir)
    print("[done] deed_events:", len(deed_rows))
    print("[done] deed_tx_class_counts:", dict(tx_counts))
    for k,v in audit["counts"].items():
        if k!="deed_events":
            print(f"[done] {k}: {v}")
    print("[done] audit:", args.audit)

if __name__=="__main__":
    main()
