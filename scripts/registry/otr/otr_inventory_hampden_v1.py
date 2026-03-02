#!/usr/bin/env python
"""Inventory + preflight of Hampden registry OTR PDFs.

- Computes sha256
- Detects if pages are text-extractable (OTR) vs image-only (OCR needed)
- Detects rotation hints
- Extracts high-level header fields from page 1

Writes an NDJSON inventory plus a JSON audit.
"""

import argparse, hashlib, json, os, re, sys
from datetime import datetime, timezone

import fitz  # PyMuPDF

HEADER_RE = {
    "scope": re.compile(r"(HAMPDEN\s+REGISTRY\s+OF\s+DEEDS|HAMPDEN\s+COUNTY\s+LAND\s+REGISTRATION).*?(RECORDED\s+LAND|REGISTERED\s+LAND).*?(DOCUMENTS|BY\s+RECORDING\s+DATE)?", re.I),
    "doc_types": re.compile(r"DOC TYPES\.{2,}(.+)$", re.I),
    "date_range": re.compile(r"Dates\s+([0-9\-]+)\s+through\s+([0-9\-]+)", re.I),
    "printed": re.compile(r"PRINTED:([0-9/]+)\s+([0-9:]+)", re.I),
}


def sha256_file(path: str, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
    return h.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def analyze_pdf(path: str, max_pages_probe: int = 8):
    doc = fitz.open(path)
    n = doc.page_count

    # Header parse from first page
    head = doc[0].get_text("text") or ""
    head_one = " ".join(head.split())

    scope = None
    m = HEADER_RE["scope"].search(head_one)
    if m:
        scope = "REGISTERED_LAND" if "REGISTERED" in m.group(1).upper() else "RECORDED_LAND"

    doc_types_raw = None
    m = HEADER_RE["doc_types"].search(head)
    if m:
        doc_types_raw = " ".join(m.group(1).split())

    date_from = date_to = None
    m = HEADER_RE["date_range"].search(head_one)
    if m:
        date_from, date_to = m.group(1), m.group(2)

    printed_date = printed_time = None
    m = HEADER_RE["printed"].search(head_one)
    if m:
        printed_date, printed_time = m.group(1), m.group(2)

    # Probe pages for text extractability + rotation
    text_pages = 0
    image_only_pages = 0
    rotations = {0: 0, 90: 0, 180: 0, 270: 0}

    probe = min(n, max_pages_probe)
    for i in range(probe):
        p = doc[i]
        rot = int(p.rotation) % 360
        if rot in rotations:
            rotations[rot] += 1
        t = (p.get_text("text") or "").strip()
        if len(re.sub(r"\s+", "", t)) >= 50:
            text_pages += 1
        else:
            image_only_pages += 1

    text_ratio = text_pages / probe if probe else 0.0

    return {
        "path": path,
        "filename": os.path.basename(path),
        "bytes": os.path.getsize(path),
        "sha256": sha256_file(path),
        "pages": n,
        "scope": scope,
        "doc_types_raw": doc_types_raw,
        "date_from_raw": date_from,
        "date_to_raw": date_to,
        "printed_date_raw": printed_date,
        "printed_time_raw": printed_time,
        "probe_pages": probe,
        "probe_text_pages": text_pages,
        "probe_image_only_pages": image_only_pages,
        "probe_text_ratio": round(text_ratio, 4),
        "probe_rotations": rotations,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_dir", required=True, help="Folder containing Hampden PDFs")
    ap.add_argument("--out_inventory", required=True, help="NDJSON inventory output")
    ap.add_argument("--audit", required=True, help="JSON audit output")
    ap.add_argument("--glob", default="*.pdf")
    args = ap.parse_args()

    in_dir = args.in_dir
    if not os.path.isdir(in_dir):
        raise SystemExit(f"Not a directory: {in_dir}")

    import glob

    pdfs = sorted(glob.glob(os.path.join(in_dir, args.glob)))
    started = utc_now()

    rows = []
    errs = []

    os.makedirs(os.path.dirname(os.path.abspath(args.out_inventory)), exist_ok=True)
    with open(args.out_inventory, "w", encoding="utf-8") as out:
        for p in pdfs:
            try:
                rec = analyze_pdf(p)
                rows.append(rec)
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            except Exception as e:
                errs.append({"path": p, "error": repr(e)})

    audit = {
        "engine_id": "registry.hampden_otr_inventory_v1",
        "started_utc": started,
        "finished_utc": utc_now(),
        "in_dir": os.path.abspath(in_dir),
        "glob": args.glob,
        "pdfs_found": len(pdfs),
        "rows_written": len(rows),
        "errors": errs,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.audit)), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps({"ok": True, "pdfs": len(pdfs), "rows": len(rows), "errors": len(errs)}))


if __name__ == "__main__":
    raise SystemExit(main())

