#!/usr/bin/env python3
"""
Hampden Registry Index PDF Parser v1.2 (TEXT LAYER, NO OCR)
STEP 0 — Parse registry index exports into NDJSON raw event rows (NO ATTACHING)

Splits records by detecting repeating header rows:
  MM-DD-YYYY  HH:MM:SSa/p  <book> <page> <doc#> ...

Infers event type by filename keywords unless overridden.
"""

import argparse, json, os, re, hashlib
from datetime import datetime, timezone

try:
    import fitz  # PyMuPDF
except Exception as e:
    raise SystemExit("[error] Missing dependency PyMuPDF. Run: pip install pymupdf") from e

DATE_RE = re.compile(r"^\d{2}-\d{2}-\d{4}$")
TIME_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}[ap]$", re.IGNORECASE)

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def sha256_file(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def split_cols(line):
    return [c for c in re.split(r"\s{2,}", line.strip()) if c]

def infer_event_type_from_filename(fn_lower: str):
    if "discharge" in fn_lower or "release" in fn_lower or "disch" in fn_lower:
        return ("RELEASE", "discharge")
    if "mortgage" in fn_lower:
        return ("MORTGAGE", "mortgage")
    if "assign" in fn_lower:
        return ("ASSIGNMENT", "assignment")
    if "lien" in fn_lower:
        return ("LIEN", "lien")
    if "lis" in fn_lower and "pend" in fn_lower:
        return ("LIS_PENDENS", "lis_pendens")
    if "foreclos" in fn_lower or "reo" in fn_lower or "sheriff" in fn_lower or "forcl" in fn_lower:
        return ("FORECLOSURE", "foreclosure")
    return ("DEED", "deed")

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

def is_header_line(cols):
    if len(cols) < 5:
        return False
    return bool(DATE_RE.match(cols[0]) and TIME_RE.match(cols[1]))

def parse_blocks_from_lines(lines):
    blocks=[]
    current=[]
    for ln in lines:
        ln = ln.rstrip("\n")
        if not ln.strip():
            continue
        s = ln.strip()
        if s.startswith("Hampden Registry of Deeds") or s.startswith("DOC TYPES") or s.startswith("DATE/TIME") or s.startswith("RECORDED"):
            continue
        cols = split_cols(ln)
        if is_header_line(cols):
            if current:
                blocks.append(current)
            current=[ln]
        else:
            if current:
                current.append(ln)
    if current:
        blocks.append(current)
    return blocks

def extract_town_addr_from_block(block_lines):
    town=None; addr=None
    for ln in block_lines:
        if "Town:" in ln or "Addr:" in ln:
            m=re.search(r"Town:\s*([A-Z0-9 \-']+)", ln, re.IGNORECASE)
            if m: town=m.group(1).strip()
            m2=re.search(r"Addr:\s*([0-9A-Z \-'.#\/]+)", ln, re.IGNORECASE)
            if m2: addr=m2.group(1).strip()
            if town or addr:
                break
    return town, addr

def parse_parties_from_block(block_lines):
    parties=[]
    for ln in block_lines:
        if re.match(r"^\s*\d+\s+P\s+", ln):
            cols=split_cols(ln)
            name = cols[-1] if cols else None
            parties.append({"name": name, "raw": ln.strip()})
    return parties

def parse_event_from_block(block_lines, event_type, file_key, pdf_path, page_no):
    header_cols = split_cols(block_lines[0])
    if not is_header_line(header_cols):
        return None
    recording_date = header_cols[0]
    recording_time = header_cols[1]
    book = header_cols[2]
    page = header_cols[3]
    docno = header_cols[4]

    doc_type=None
    for ln in block_lines:
        u = ln.upper()
        if " DEED" in u: doc_type="DEED"
        if " MORTGAGE" in u: doc_type="MORTGAGE"
        if " DISCHARGE" in u or " RELEASE" in u: doc_type="DISCHARGE"
        if doc_type: break

    town, addr = extract_town_addr_from_block(block_lines)
    parties = parse_parties_from_block(block_lines)

    eid = stable_event_id(event_type, "hampden", recording_date, recording_time, book, page, docno, extra=(addr or town or ""))

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
        "property_ref": {
            "town": town,
            "address": addr,
        },
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
    ap.add_argument("--eventType", default="", help="Override inferred event type (DEED, RELEASE, MORTGAGE, ...)")
    args=ap.parse_args()

    pdf_hash = sha256_file(args.pdf)
    fn_lower = os.path.basename(args.pdf).lower()
    inferred_type, file_key = infer_event_type_from_filename(fn_lower)
    event_type = args.eventType.strip().upper() or inferred_type

    os.makedirs(args.outDir, exist_ok=True)
    out_path = os.path.join(args.outDir, f"{file_key}_index_raw_v1_2.ndjson")

    doc=fitz.open(args.pdf)

    out_rows=[]
    skipped=0
    for pno in range(len(doc)):
        text = doc[pno].get_text("text") or ""
        if not text.strip():
            continue
        lines=text.splitlines()
        blocks = parse_blocks_from_lines(lines)
        for b in blocks:
            ev = parse_event_from_block(b, event_type, file_key, args.pdf, pno+1)
            if ev: out_rows.append(ev)
            else: skipped += 1

    with open(out_path, "w", encoding="utf-8") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    audit={
        "created_at": now_iso(),
        "pdf": os.path.basename(args.pdf),
        "pdf_sha256": pdf_hash,
        "pages": len(doc),
        "event_type": event_type,
        "file_key": file_key,
        "rows_written": len(out_rows),
        "rows_skipped": skipped,
        "out": out_path,
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[start] Hampden index PDF parse v1.2 (text-based, no OCR)")
    print("[info] pages:", len(doc))
    print("[info] inferred:", inferred_type, "file_key:", file_key)
    print("[done] rows_written:", len(out_rows))
    print("[done] rows_skipped:", skipped)
    print("[done] out:", out_path)
    print("[done] audit:", args.audit)

if __name__=="__main__":
    main()
