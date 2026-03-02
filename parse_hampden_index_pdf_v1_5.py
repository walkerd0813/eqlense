#!/usr/bin/env python3
"""
Hampden Registry Index PDF Parser v1.5 (TEXT LAYER, NO OCR)

Fix for mortgage PDFs that don't use "2+ spaces" column splits.
v1.5 detects record header rows using a single REGEX over the raw line:
  DATE  TIME  BOOK  PAGE  DOCNO ...

Supported:
- Date: MM-DD-YYYY or MM/DD/YYYY
- Time: HH:MM or HH:MM:SS with optional a/p or am/pm or none

Writes distinct output file per inferred subtype (no overwrites):
  deed_index_raw_v1_5.ndjson
  mortgage_index_raw_v1_5.ndjson
  assignment_index_raw_v1_5.ndjson
  lien_index_raw_v1_5.ndjson / lien_ma_... / lien_fed_...
  release_index_raw_v1_5.ndjson / discharge_index_raw_v1_5.ndjson
  foreclosure_index_raw_v1_5.ndjson
  lis_pendens_index_raw_v1_5.ndjson

NO ATTACHING.
"""

import argparse, json, os, re, hashlib
from datetime import datetime, timezone

try:
    import fitz  # PyMuPDF
except Exception as e:
    raise SystemExit("[error] Missing dependency PyMuPDF. Run: pip install pymupdf") from e

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def sha256_file(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

# One-pass header regex (works with single or multiple spaces/tabs)
HDR_RE = re.compile(
    r"^\s*(?P<date>\d{2}[-/]\d{2}[-/]\d{4})\s+"
    r"(?P<time>\d{1,2}:\d{2}(?::\d{2})?(?:[ap]|am|pm)?)\s+"
    r"(?P<book>\d+)\s+"
    r"(?P<page>\d+)\s+"
    r"(?P<docno>[0-9A-Z\-]+)\b",
    re.IGNORECASE
)

def infer_keys(fn_lower: str):
    lc=fn_lower
    is_landcourt = "landcourt" in lc or "(landcourt" in lc

    if "mortgage" in lc:
        return ("MORTGAGE", "mortgage_landcourt" if is_landcourt else "mortgage")

    if "assign" in lc:
        return ("ASSIGNMENT", "assignment_landcourt" if is_landcourt else "assignment")

    if "lis" in lc and "pend" in lc:
        return ("LIS_PENDENS", "lis_pendens_landcourt" if is_landcourt else "lis_pendens")

    if "foreclos" in lc or "forcl" in lc or "sheriff" in lc or "reo" in lc:
        return ("FORECLOSURE", "foreclosure_landcourt" if is_landcourt else "foreclosure")

    if "discharge" in lc:
        return ("RELEASE", "discharge_landcourt" if is_landcourt else "discharge")

    if "release" in lc:
        return ("RELEASE", "release_landcourt" if is_landcourt else "release")

    if "lien" in lc:
        if "fed" in lc:
            return ("LIEN", "lien_fed_landcourt" if is_landcourt else "lien_fed")
        if "mass" in lc or "ma_" in lc or "commonwealth" in lc:
            return ("LIEN", "lien_ma_landcourt" if is_landcourt else "lien_ma")
        return ("LIEN", "lien_landcourt" if is_landcourt else "lien")

    if "deed" in lc:
        return ("DEED", "deed_landcourt" if is_landcourt else "deed")

    return ("UNKNOWN", "unknown")

def stable_event_id(event_type, county, recording_date, recording_time, book, page, docno, extra=""):
    key="|".join([
        "MA","registry",event_type.lower(),county.lower(),
        str(recording_date or ""),
        str(recording_time or ""),
        str(book or ""),
        str(page or ""),
        str(docno or ""),
        str(extra or ""),
    ])
    return f"MA|registry|{event_type.lower()}|{county.lower()}|" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]

def parse_blocks_from_lines(lines):
    blocks=[]
    current=[]
    for ln in lines:
        ln=ln.rstrip("\n")
        if not ln.strip():
            continue
        s=ln.strip()
        up=s.upper()
        if up.startswith("HAMPDEN REGISTRY OF DEEDS") or up.startswith("DOC TYPES") or up.startswith("DATE/TIME") or up.startswith("RECORDED"):
            continue
        if HDR_RE.match(ln):
            if current:
                blocks.append(current)
            current=[ln]
        else:
            if current:
                current.append(ln)
    if current:
        blocks.append(current)
    return blocks

def extract_town_addr(block_lines):
    town=None; addr=None
    for ln in block_lines:
        if "Town:" in ln or "Addr:" in ln or "TOWN:" in ln or "ADDR:" in ln:
            m=re.search(r"Town:\s*([A-Z0-9 \-']+)", ln, re.IGNORECASE)
            if m: town=m.group(1).strip()
            m2=re.search(r"Addr:\s*([0-9A-Z \-'.#\/]+)", ln, re.IGNORECASE)
            if m2: addr=m2.group(1).strip()
            if town or addr:
                break
    return town, addr

def parse_parties(block_lines):
    parties=[]
    for ln in block_lines:
        if re.match(r"^\s*\d+\s+P\s+", ln):
            parties.append({"raw": ln.strip()})
    return parties

def parse_event(block_lines, event_type, file_key, pdf_path, page_no):
    m=HDR_RE.match(block_lines[0])
    if not m:
        return None
    recording_date=m.group("date").replace("/","-")
    recording_time=m.group("time")
    book=m.group("book")
    page=m.group("page")
    docno=m.group("docno")

    u0=" ".join(block_lines).upper()
    doc_type=None
    if " MORTGAGE" in u0: doc_type="MORTGAGE"
    elif " DEED" in u0: doc_type="DEED"
    elif " DISCHARGE" in u0 or " RELEASE" in u0: doc_type="RELEASE"
    elif " LIEN" in u0: doc_type="LIEN"
    elif " ASSIGN" in u0: doc_type="ASSIGNMENT"
    elif " LIS PENDENS" in u0: doc_type="LIS_PENDENS"
    elif " FORECLOS" in u0 or " SHERIFF" in u0: doc_type="FORECLOSURE"

    town, addr = extract_town_addr(block_lines)
    parties = parse_parties(block_lines)

    eid=stable_event_id(event_type,"hampden",recording_date,recording_time,book,page,docno,extra=(addr or town or ""))
    return {
        "event_id": eid,
        "event_type": event_type,
        "county": "hampden",
        "recording": {
            "recording_date": recording_date,
            "recording_time": recording_time,
            "book": book,
            "page": page,
            "document_number": docno,
            "document_type": doc_type,
        },
        "property_ref": {"town": town, "address": addr},
        "parties_raw": parties,
        "source": {
            "source_system": "hampden_registry_index_pdf",
            "source_pdf": os.path.basename(pdf_path),
            "page_no": page_no,
            "file_key": file_key,
        },
        "raw_lines": [l.rstrip("\n") for l in block_lines],
        "meta": {"parsed_at": now_iso()}
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--outDir", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--append", action="store_true")
    args=ap.parse_args()

    fn=os.path.basename(args.pdf)
    inferred_type, file_key = infer_keys(fn.lower())
    event_type=inferred_type
    pdf_hash=sha256_file(args.pdf)

    os.makedirs(args.outDir, exist_ok=True)
    out_path=os.path.join(args.outDir, f"{file_key}_index_raw_v1_5.ndjson")

    doc=fitz.open(args.pdf)

    rows=[]
    skipped=0
    header_hits=0

    for pno in range(len(doc)):
        text=doc[pno].get_text("text") or ""
        if not text.strip():
            continue
        lines=text.splitlines()
        blocks=parse_blocks_from_lines(lines)
        header_hits += len(blocks)
        for b in blocks:
            ev=parse_event(b, event_type, file_key, args.pdf, pno+1)
            if ev: rows.append(ev)
            else: skipped += 1

    mode="a" if args.append else "w"
    with open(out_path, mode, encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    audit={
        "created_at": now_iso(),
        "pdf": fn,
        "pdf_sha256": pdf_hash,
        "pages": len(doc),
        "event_type": event_type,
        "file_key": file_key,
        "header_blocks_found": header_hits,
        "rows_written": len(rows),
        "rows_skipped": skipped,
        "out": out_path,
    }
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("[start] Hampden index PDF parse v1.5 (text-based, no OCR)")
    print("[info] pages:", len(doc))
    print("[info] inferred:", inferred_type, "file_key:", file_key)
    print("[done] header_blocks_found:", header_hits)
    print("[done] rows_written:", len(rows))
    print("[done] rows_skipped:", skipped)
    print("[done] out:", out_path)
    print("[done] audit:", args.audit)

if __name__=="__main__":
    main()
