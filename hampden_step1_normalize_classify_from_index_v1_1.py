#!/usr/bin/env python3
"""
Phase 5 — Hampden County
STEP 1 v1.1 (From Index PDFs): Normalize & Classify (NO ATTACHING)

Reads ALL *_index_raw_v1_4.ndjson files in inDir and produces standardized event tables.
Prevents "overwrite" problems and supports multiple lien/mortgage subtypes.

Outputs:
  _events_v1/
    deed_events.ndjson           (classified)
    mortgage_events.ndjson       (all mortgage* files merged)
    assignment_events.ndjson
    lien_events.ndjson           (all lien* files merged)
    release_events.ndjson        (release+discharge merged)
    lis_pendens_events.ndjson
    foreclosure_events.ndjson
"""

import argparse, json, os, re, hashlib
from datetime import datetime, timezone
from collections import Counter, defaultdict

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

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
    if x is None: return None
    if isinstance(x,str):
        x=x.strip()
        return x or None
    return str(x).strip() or None

def norm_name(name):
    if not name: return None
    s=re.sub(r"[^A-Z0-9\s&\-']", " ", str(name).upper())
    s=re.sub(r"\s+"," ",s).strip()
    return s or None

def parties_from_raw(raw_list):
    out=[]
    if isinstance(raw_list,list):
        for p in raw_list:
            if isinstance(p,dict):
                nm=norm_name(p.get("name"))
                if nm:
                    out.append({"name": nm, "raw": p.get("raw")})
    return out

def parse_amount_text(amt_text):
    if not amt_text: return None
    s=str(amt_text).replace(",","").replace("$","").strip()
    m=re.search(r"(\d+(\.\d+)?)", s)
    if not m: return None
    try:
        v=float(m.group(1))
        return int(v) if abs(v-int(v))<1e-9 else v
    except Exception:
        return None

NOMINAL_AMOUNTS={1,10,100}

def classify_deed_index_only(row):
    evidence=[]
    srcpdf=((row.get("source") or {}).get("source_pdf") or "").lower()
    raw_lines=" ".join((row.get("raw_lines") or [])).upper()

    if "foreclos" in srcpdf or "sheriff" in srcpdf or "reo" in raw_lines or "FORECLOS" in raw_lines or "FORCL" in srcpdf:
        evidence.append("source_or_text_foreclosure")
        return ("distress_transfer", 0.90, evidence)

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
        h.update(f"{os.path.basename(p)}|{st.st_size}|{int(st.st_mtime)}".encode("utf-8"))
    return h.hexdigest()

def normalize_row(row, event_type, dataset_version, dataset_hash, as_of_date):
    rec=row.get("recording") or {}
    pref=row.get("property_ref") or {}
    src=row.get("source") or {}
    return {
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

def group_inputs(in_dir):
    groups=defaultdict(list)
    all_paths=[]
    for fn in os.listdir(in_dir):
        if not fn.endswith(".ndjson"): continue
        f=fn.lower()
        if "_index_raw_v1_4" not in f:
            continue
        full=os.path.join(in_dir, fn)
        all_paths.append(full)
        if f.startswith("deed_"):
            groups["deed"].append(full)
        elif f.startswith("mortgage_"):
            groups["mortgage"].append(full)
        elif f.startswith("assignment_"):
            groups["assignment"].append(full)
        elif f.startswith("lien_"):
            groups["lien"].append(full)
        elif f.startswith("release_") or f.startswith("discharge_"):
            groups["release"].append(full)
        elif f.startswith("lis_pendens_"):
            groups["lis_pendens"].append(full)
        elif f.startswith("foreclosure_"):
            groups["foreclosure"].append(full)
        else:
            groups["other"].append(full)
    return groups, all_paths

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--inDir", required=True)
    ap.add_argument("--outDir", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--datasetVersion", default="hampden_events_v1_from_index_v1_1")
    ap.add_argument("--asOfDate", default=datetime.now(timezone.utc).date().isoformat())
    args=ap.parse_args()

    groups, all_paths = group_inputs(args.inDir)
    if not groups.get("deed"):
        raise SystemExit("[error] No deed*_index_raw_v1_4.ndjson found. Rerun STEP 0 v1.4 for deeds.")

    dataset_hash=dataset_hash_for_files(all_paths)

    os.makedirs(args.outDir, exist_ok=True)
    audit={
        "created_at": now_iso(),
        "as_of_date": args.asOfDate,
        "dataset_version": args.datasetVersion,
        "dataset_hash": dataset_hash,
        "inputs_grouped": {k:[os.path.basename(p) for p in v] for k,v in groups.items()},
        "counts": {},
        "deed_transaction_class_counts": {},
        "warnings": []
    }

    # deed (merge all deed files, classify)
    deed_rows=[]
    tx_counts=Counter()
    for p in groups["deed"]:
        for r in read_ndjson(p):
            norm=normalize_row(r,"DEED",args.datasetVersion,dataset_hash,args.asOfDate)
            tx_class, tx_conf, tx_ev = classify_deed_index_only(r)
            norm["transaction"]={
                "transaction_class": tx_class,
                "transaction_confidence": round(float(tx_conf),3),
                "transaction_evidence": tx_ev
            }
            deed_rows.append(norm)
            tx_counts[tx_class]+=1
    write_ndjson(os.path.join(args.outDir,"deed_events.ndjson"), deed_rows)
    audit["counts"]["deed_events"]=len(deed_rows)
    audit["deed_transaction_class_counts"]=dict(tx_counts)

    def emit(group_key, event_type, outname):
        paths=groups.get(group_key, [])
        if not paths:
            audit["counts"][outname.replace(".ndjson","")]=0
            return 0
        rows=[]
        for p in paths:
            for r in read_ndjson(p):
                rows.append(normalize_row(r, event_type, args.datasetVersion, dataset_hash, args.asOfDate))
        write_ndjson(os.path.join(args.outDir,outname), rows)
        audit["counts"][outname.replace(".ndjson","")]=len(rows)
        return len(rows)

    emit("mortgage","MORTGAGE","mortgage_events.ndjson")
    emit("assignment","ASSIGNMENT","assignment_events.ndjson")
    emit("lien","LIEN","lien_events.ndjson")
    emit("release","RELEASE","release_events.ndjson")
    emit("lis_pendens","LIS_PENDENS","lis_pendens_events.ndjson")
    emit("foreclosure","FORECLOSURE","foreclosure_events.ndjson")

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("[start] Hampden STEP 1 v1.1 — Normalize & Classify (from index v1.4)")
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
