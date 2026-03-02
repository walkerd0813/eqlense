#!/usr/bin/env python3
"""
Hampden Registry Index PDF Parser (Text-Based, NO OCR)
STEP 0 — Parse index PDF export into NDJSON "raw events" (still NO attaching)

Input:
  A Hampden index PDF (text layer present) like:
    hamden_deeds 12-31-20-12-30-24.pdf

Output:
  backend/publicData/registry/hampden/_raw_from_index_v1/deeds_index_raw_v1.ndjson
  backend/publicData/_audit/registry/hampden_deeds_index_raw_v1_audit.json

Notes:
- This parser is intentionally conservative.
- It extracts a per-record "recording header" + "town/addr" + party name lines.
- It keeps raw_lines[] for full auditability.
"""

import argparse, json, os, re, hashlib
from datetime import datetime
import fitz  # PyMuPDF

SEP_RE = re.compile(r"^-{10,}")
DATE_RE = re.compile(r"^\d{2}-\d{2}-\d{4}$")
TIME_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}[ap]$", re.IGNORECASE)

def now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def sha256_file(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def split_cols(line):
    # split by 2+ spaces, drop empty
    return [c for c in re.split(r"\s{2,}", line.strip()) if c]

def extract_addr_line(line):
    # Example: "Town: SOUTHWICK...       Addr:16 RENNY AVE"
    town=None; addr=None
    m=re.search(r"Town:\s*([A-Z0-9 \-']+)", line, re.IGNORECASE)
    if m:
        town=m.group(1).strip()
    m2=re.search(r"Addr:\s*([0-9A-Z \-'.#\/]+)", line, re.IGNORECASE)
    if m2:
        addr=m2.group(1).strip()
    return town, addr

def parse_party_line(line):
    # Conservative: keep full line, try extract trailing name after multiple spaces
    # Example: "1 P SEDOR ...                   DEBORAH I"
    cols=split_cols(line)
    # often last col is name
    name = cols[-1] if cols else None
    # sometimes first cols are like ["1","P","SEDOR",...,"DEBORAH I"]
    role_hint = None
    if cols and cols[0].isdigit():
        role_hint = cols[0]
    return {"name": name, "raw": line.strip(), "role_hint": role_hint}

def stable_event_id(book, page, docno, date, time, town, addr):
    key="|".join([
        "MA","registry","deed","hampden",
        str(book or ""),
        str(page or ""),
        str(docno or ""),
        str(date or ""),
        str(time or ""),
        str(town or ""),
        str(addr or ""),
    ])
    return "MA|registry|deed|hampden|" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]

def parse_record(block_lines, source_pdf, page_no):
    # Find first line with date/time/book/page/doc
    header=None
    for ln in block_lines:
        cols=split_cols(ln)
        if len(cols) >= 5 and DATE_RE.match(cols[0]) and TIME_RE.match(cols[1]):
            header=cols
            break
    if not header:
        return None

    recording_date = header[0]
    recording_time = header[1]
    book = header[2] if len(header) > 2 else None
    page = header[3] if len(header) > 3 else None
    docno = header[4] if len(header) > 4 else None

    # Find doc type + amount line (often contains "DEED")
    doc_type=None
    amount_text=None
    street_hint=None
    for ln in block_lines:
        cols=split_cols(ln)
        if "DEED" in (ln.upper()):
            # find "DEED" token in cols and pull next token if it looks numeric
            for i,c in enumerate(cols):
                if c.upper() == "DEED":
                    doc_type="DEED"
                    if i+1 < len(cols) and re.match(r"^\d+(\.\d+)?$", cols[i+1].replace(",","")):
                        amount_text=cols[i+1]
                    break
            # also capture a street-ish hint if present (often a col like "RENNY AVE")
            for c in cols:
                if re.search(r"\b(AVE|ST|RD|DR|LN|BLVD|CT|WAY|PL)\b", c.upper()):
                    street_hint=c
                    break

    town=None; addr=None
    for ln in block_lines:
        if "Town:" in ln or "Addr:" in ln:
            town, addr = extract_addr_line(ln)
            if town or addr:
                break

    parties=[]
    for ln in block_lines:
        # party lines often start with spaces then digit
        if re.match(r"^\s*\d+\s+P\s+", ln):
            parties.append(parse_party_line(ln))

    event = {
        "event_id": stable_event_id(book, page, docno, recording_date, recording_time, town, addr),
        "event_type": "DEED",
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
            "street_hint": street_hint,
        },
        "amount_text": amount_text,  # ambiguous: fee/consideration in index export
        "parties_raw": parties,
        "source": {
            "source_system": "hampden_registry_index_pdf",
            "source_pdf": os.path.basename(source_pdf),
            "page_no": page_no,
        },
        "raw_lines": [l.rstrip("\n") for l in block_lines],
        "meta": {
            "parsed_at": now_iso()
        }
    }
    return event

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="Path to Hampden index PDF")
    ap.add_argument("--out", required=True, help="NDJSON output path")
    ap.add_argument("--audit", required=True, help="Audit JSON output path")
    args=ap.parse_args()

    pdf_hash = sha256_file(args.pdf)
    doc=fitz.open(args.pdf)

    out_rows=[]
    skipped=0
    for pno in range(len(doc)):
        text = doc[pno].get_text("text") or ""
        if not text.strip():
            continue
        lines=text.splitlines()
        block=[]
        for ln in lines:
            if SEP_RE.match(ln.strip()):
                if block:
                    ev=parse_record(block, args.pdf, pno+1)
                    if ev: out_rows.append(ev)
                    else: skipped += 1
                    block=[]
                continue
            # skip obvious report headers
            if ln.strip().startswith("Hampden Registry of Deeds"):
                continue
            if ln.strip().startswith("DOC TYPES"):
                continue
            if ln.strip().startswith("CONSIDERATION:"):
                continue
            if ln.strip().startswith("DATE/TIME"):
                continue
            if ln.strip().startswith("RECORDED"):
                continue
            if ln.strip().startswith("--------------------"):
                continue
            if not ln.strip():
                continue
            block.append(ln)

        if block:
            ev=parse_record(block, args.pdf, pno+1)
            if ev: out_rows.append(ev)
            else: skipped += 1

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    audit={
        "created_at": now_iso(),
        "pdf": os.path.basename(args.pdf),
        "pdf_sha256": pdf_hash,
        "pages": len(doc),
        "rows_written": len(out_rows),
        "rows_skipped": skipped,
        "out": args.out,
    }
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[start] Hampden index PDF parse (text-based, no OCR)")
    print("[info] pages:", len(doc))
    print("[done] rows_written:", len(out_rows))
    print("[done] rows_skipped:", skipped)
    print("[done] out:", args.out)
    print("[done] audit:", args.audit)

if __name__=="__main__":
    main()
