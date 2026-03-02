#!/usr/bin/env python3
"""
Hampden Registry Index PDF Parser v1.4 (TEXT LAYER, NO OCR)

Fixes vs v1.2:
- More permissive DATE/TIME detection (supports MM-DD-YYYY or MM/DD/YYYY; time with/without seconds; with a/p, am/pm, or none)
- Writes distinct output files per subtype to avoid overwriting:
    lien_index_raw_v1_4.ndjson
    lien_ma_index_raw_v1_4.ndjson
    lien_fed_index_raw_v1_4.ndjson
    mortgage_index_raw_v1_4.ndjson
    mortgage_landcourt_index_raw_v1_4.ndjson
    release_index_raw_v1_4.ndjson / discharge_index_raw_v1_4.ndjson (if both exist)
- Adds --append option to accumulate across multiple PDFs (safe, idempotent per run is handled upstream by later dedupe)

NOTE: This is STILL "index-only" parsing — it extracts recording block + parties/town/addr when present.
NO ATTACHING.
"""

import argparse, json, os, re, hashlib
from datetime import datetime, timezone

try:
    import fitz  # PyMuPDF
except Exception as e:
    raise SystemExit("[error] Missing dependency PyMuPDF. Run: pip install pymupdf") from e

DATE_RE = re.compile(r"^(\d{2}[-/]\d{2}[-/]\d{4})$")
# Accept:
#  1) HH:MM:SSa / HH:MM:SSp  (a/p)
#  2) HH:MM:SSam / pm
#  3) HH:MM:SS
#  4) HH:MMa / p
#  5) HH:MMam / pm
#  6) HH:MM
TIME_RE = re.compile(r"^\d{1,2}:\d{2}(:\d{2})?([ap]|am|pm)?$", re.IGNORECASE)

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

def infer_keys(fn_lower: str):
    # returns (event_type, file_key)
    # file_key is used for output filename; it must be stable and avoid collisions/overwrites.
    lc = fn_lower

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
        # subtypes
        if "fed" in lc:
            return ("LIEN", "lien_fed_landcourt" if is_landcourt else "lien_fed")
        if "mass" in lc or "ma_" in lc or "commonwealth" in lc:
            return ("LIEN", "lien_ma_landcourt" if is_landcourt else "lien_ma")
        return ("LIEN", "lien_landcourt" if is_landcourt else "lien")

    # default
    if "deed" in lc:
        # distinguish foreclosure_deeds via above "foreclos"
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

def is_header_line(cols):
    if len(cols) < 5:
        return False
    d = cols[0].strip()
    t = cols[1].strip().lower()
    if not DATE_RE.match(d):
        return False
    if not TIME_RE.match(t):
        return False
    # book/page/doc#
    # allow alnum for doc#
    if not re.match(r"^\d+$", cols[2]):
        return False
    if not re.match(r"^\d+$", cols[3]):
        return False
    if not re.match(r"^[0-9A-Z\-]+$", cols[4], re.IGNORECASE):
        return False
    return True

def parse_blocks_from_lines(lines):
    blocks=[]
    current=[]
    for ln in lines:
        ln=ln.rstrip("\n")
        if not ln.strip():
            continue
        s=ln.strip()
        # skip obvious headers
        if s.upper().startswith("HAMPDEN REGISTRY OF DEEDS") or s.upper().startswith("DOC TYPES") or s.upper().startswith("DATE/TIME"):
            continue
        cols=split_cols(ln)
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
        if "Town:" in ln or "Addr:" in ln or "TOWN:" in ln or "ADDR:" in ln:
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

    recording_date = header_cols[0].replace("/","-")
    recording_time = header_cols[1]
    book = header_cols[2]
    page = header_cols[3]
    docno = header_cols[4]

    doc_type=None
    u0=" ".join(block_lines).upper()
    if " DEED" in u0: doc_type="DEED"
    if " MORTGAGE" in u0: doc_type="MORTGAGE"
    if " DISCHARGE" in u0 or " RELEASE" in u0: doc_type="RELEASE"
    if " LIEN" in u0: doc_type="LIEN"
    if " ASSIGN" in u0: doc_type="ASSIGNMENT"
    if " LIS PENDENS" in u0: doc_type="LIS_PENDENS"
    if " FORECLOS" in u0 or " SHERIFF" in u0: doc_type="FORECLOSURE"

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
    ap.add_argument("--append", action="store_true")
    args=ap.parse_args()

    pdf_hash=sha256_file(args.pdf)
    fn=os.path.basename(args.pdf)
    inferred_type, file_key=infer_keys(fn.lower())
    event_type=inferred_type

    os.makedirs(args.outDir, exist_ok=True)
    out_path=os.path.join(args.outDir, f"{file_key}_index_raw_v1_4.ndjson")

    doc=fitz.open(args.pdf)

    out_rows=[]
    skipped=0
    for pno in range(len(doc)):
        text=doc[pno].get_text("text") or ""
        if not text.strip():
            continue
        blocks=parse_blocks_from_lines(text.splitlines())
        for b in blocks:
            ev=parse_event_from_block(b, event_type, file_key, args.pdf, pno+1)
            if ev: out_rows.append(ev)
            else: skipped += 1

    mode="a" if args.append else "w"
    with open(out_path, mode, encoding="utf-8") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    audit={
        "created_at": now_iso(),
        "pdf": fn,
        "pdf_sha256": pdf_hash,
        "pages": len(doc),
        "event_type": event_type,
        "file_key": file_key,
        "append": bool(args.append),
        "rows_written": len(out_rows),
        "rows_skipped": skipped,
        "out": out_path,
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("[start] Hampden index PDF parse v1.4 (text-based, no OCR)")
    print("[info] pages:", len(doc))
    print("[info] inferred:", inferred_type, "file_key:", file_key)
    print("[done] rows_written:", len(out_rows))
    print("[done] rows_skipped:", skipped)
    print("[done] out:", out_path)
    print("[done] audit:", args.audit)

if __name__=="__main__":
    main()
