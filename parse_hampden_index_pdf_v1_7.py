#!/usr/bin/env python3
"""
Hampden STEP 0 v1.7 — Parse Registry "Index" PDFs (text-based, NO OCR, NO ATTACHING)

Why v1.7 exists:
- Hampden "mortgage" index PDFs are LAND COURT-style tables (MTG rows) whose layout differs from deeds/assignments/etc.
- Prior parsers expected other header patterns and returned rows_written=0.
- This parser uses pypdf text extraction (works even when pages are visually rotated) and parses table rows robustly.

Outputs:
- NDJSON rows into backend/publicData/registry/hampden/_raw_from_index_v1/<file_key>_index_raw_v1_7.ndjson
- Audit JSON into backend/publicData/_audit/registry/hampden_index_raw_v1_7_<file_key>_audit.json

Notes:
- This is an INDEX PARSE, not a deed-document OCR.
- Fields are "best effort" and include the raw block for traceability.
"""
from __future__ import annotations
import argparse, json, os, re, sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pypdf import PdfReader

REC_START_RE = re.compile(r'^\s*(\d{1,3}(?:,\d{3})*)\s+(\d+)\s+([A-Z]{2,6})\s+([A-Z]{2,5})\s+(\d{1,2}:\d{2})\s+(.*)$')
ENTRY_DATE_RE = re.compile(r'DAILY ENTRY SHEET FOR\.\.(.+)$')
INDEX_SELECTION_RE = re.compile(r'INDEX SELECTION\.+(.+)$')
DOC_TYPES_RE = re.compile(r'DOC TYPES\.+([A-Z0-9, ]+)$')
PRINTED_RE = re.compile(r'PRINTED:(\d{2}/\d{2}/\d{2})\s+(\d{2}:\d{2}:\d{2})')

ADDR_AMOUNT_RE = re.compile(r'^\s{10,}(.+?)\s+([\d,]+\.\d{2})\s*$')
DOC_DATE_RE = re.compile(r'DOC DATE->\s*([0-9\-\/]+)')
FAVOR_RE = re.compile(r'FAVOR OF->\s*(.+)$')
GTOR_RE = re.compile(r'^\s*GRANTORS:\s+\d+\s+[PC]\s+(.+)$')
GTEE_RE = re.compile(r'^\s*GRANTEES:\s+\d+\s+[PC]\s+(.+)$')

def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="Path to Hampden index PDF")
    ap.add_argument("--out", required=True, help="Output NDJSON path")
    ap.add_argument("--audit", required=True, help="Audit JSON path")
    ap.add_argument("--fileKey", default="", help="Force file_key (deed|mortgage|assignment|lien|lien_ma|lien_fed|release|discharge|lis_pendens|foreclosure)")
    ap.add_argument("--maxPages", type=int, default=0, help="0 = all pages")
    return ap.parse_args()

def infer_file_key(text0: str, forced: str) -> str:
    if forced:
        return forced
    # fall back: infer from DOC TYPES line if present
    m = DOC_TYPES_RE.search(text0)
    if m:
        dt = m.group(1)
        if "MTG" in dt:
            return "mortgage"
        if "ASSIGN" in dt or "ASN" in dt:
            return "assignment"
        if "LIS" in dt:
            return "lis_pendens"
    return "unknown"

def enrich_block(rec: Dict[str, Any]) -> None:
    lines = rec.get("raw_block","").splitlines()
    for ln in lines:
        m = ADDR_AMOUNT_RE.match(ln)
        if m and "property_address" not in rec:
            rec["property_address"] = m.group(1).strip()
            try:
                rec["amount"] = float(m.group(2).replace(",",""))
            except:
                pass
        m = DOC_DATE_RE.search(ln)
        if m and "doc_date_raw" not in rec:
            rec["doc_date_raw"] = m.group(1)
        m = FAVOR_RE.search(ln)
        if m and "favor_of" not in rec:
            rec["favor_of"] = m.group(1).strip()
        m = GTOR_RE.match(ln)
        if m:
            rec.setdefault("grantors", []).append(m.group(1).strip())
        m = GTEE_RE.match(ln)
        if m:
            rec.setdefault("grantees", []).append(m.group(1).strip())

def main() -> int:
    args = parse_args()
    pdf_path = args.pdf
    if not os.path.exists(pdf_path):
        print(f"[error] PDF not found: {pdf_path}")
        return 2

    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)
    max_pages = total_pages if args.maxPages <= 0 else min(total_pages, args.maxPages)

    # Header inference from page 1
    p0 = reader.pages[0].extract_text() or ""
    file_key = infer_file_key(p0, args.fileKey)

    audit = {
        "created_at": utc_iso(),
        "pdf": os.path.abspath(pdf_path),
        "pages_total": total_pages,
        "pages_parsed": max_pages,
        "file_key": file_key,
        "rows_written": 0,
        "rows_skipped": 0,
        "header_blocks_found": 0,
    }

    safe_mkdir(os.path.dirname(args.out))
    safe_mkdir(os.path.dirname(args.audit))

    out_f = open(args.out, "w", encoding="utf-8")

    entry_date = None
    index_selection = None
    printed = None

    current = None
    block: List[str] = []

    def flush():
        nonlocal current, block
        if not current:
            return
        current["raw_block"] = "\n".join(block).rstrip()
        enrich_block(current)
        out_f.write(json.dumps(current, ensure_ascii=False) + "\n")
        audit["rows_written"] += 1
        current = None
        block = []

    for pi in range(max_pages):
        text = reader.pages[pi].extract_text() or ""
        if pi == 0:
            m = INDEX_SELECTION_RE.search(text)
            if m: index_selection = m.group(1).strip()
            m = PRINTED_RE.search(text)
            if m: printed = {"date": m.group(1), "time": m.group(2)}
            # rough "header_blocks_found": presence of the daily sheet header line
            if "DAILY ENTRY SHEET FOR.." in text:
                audit["header_blocks_found"] += 1

        for line in text.splitlines():
            m = ENTRY_DATE_RE.search(line)
            if m:
                entry_date = m.group(1).strip()
                continue

            m = REC_START_RE.match(line)
            if not m:
                if current is not None:
                    block.append(line)
                continue

            doc_no = m.group(1)
            seq = int(m.group(2))
            doc_type = m.group(3)
            town = m.group(4)
            tm = m.group(5)
            rest = m.group(6).strip()

            # Gate: only accept rows consistent with the file_key we're parsing.
            # (If you pass --fileKey it will be strict; otherwise "unknown" accepts all.)
            if file_key == "mortgage" and doc_type != "MTG":
                if current is not None:
                    block.append(line)
                continue
            if file_key == "assignment" and doc_type not in ("ASN","ASSIGN","ASGN"):
                if current is not None:
                    block.append(line)
                continue

            flush()
            current = {
                "source": {
                    "county": "hampden",
                    "pdf": os.path.basename(pdf_path),
                    "page": pi + 1,
                    "line0": line.strip(),
                },
                "file_key": file_key,
                "index_selection": index_selection,
                "printed": printed,
                "entry_date_raw": entry_date,
                "document_number_raw": doc_no,
                "document_number": doc_no.replace(",",""),
                "seq": seq,
                "doc_type": doc_type,
                "town_code": town,
                "recorded_time_raw": tm,
                "description_raw": rest,
            }
            block = [line]

    flush()
    out_f.close()

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(f"[done] pages: {max_pages}")
    print(f"[done] inferred: {file_key}")
    print(f"[done] rows_written: {audit['rows_written']}")
    print(f"[done] out: {args.out}")
    print(f"[done] audit: {args.audit}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
