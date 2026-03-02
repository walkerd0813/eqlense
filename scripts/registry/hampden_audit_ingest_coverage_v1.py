#!/usr/bin/env python3
import argparse, json, os, re, hashlib
from datetime import datetime, timezone

def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def sha256_file(path, block=1024*1024):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(block)
            if not b: break
            h.update(b)
    return h.hexdigest()

def count_lines(path):
    n=0
    with open(path, "rb") as f:
        for _ in f: n+=1
    return n

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdfDir", required=True)
    ap.add_argument("--rawDir", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # Lazy import after installation
    from PyPDF2 import PdfReader

    pdf_dir = args.pdfDir
    raw_dir = args.rawDir

    pdfs = []
    for fn in os.listdir(pdf_dir):
        if fn.lower().endswith(".pdf") and "hamden" in fn.lower() or "hampden" in fn.lower():
            pdfs.append(os.path.join(pdf_dir, fn))
    pdfs.sort()

    raws = []
    if os.path.isdir(raw_dir):
        for fn in os.listdir(raw_dir):
            if fn.lower().endswith(".ndjson"):
                raws.append(os.path.join(raw_dir, fn))
    raws.sort()

    raw_index = {}
    for p in raws:
        raw_index[os.path.basename(p)] = {
            "path": p,
            "rows": count_lines(p),
            "bytes": os.path.getsize(p),
        }

    # Text coverage audit per PDF
    results = []
    total_pages = 0
    total_empty = 0
    header_hits = 0

    header_re = re.compile(r"(Recorded\s+Land\s+by\s+Recording\s+Date|Doc\s+Type|Consideration|GRP-SEQ|Instr)", re.I)

    for pdf in pdfs:
        try:
            reader = PdfReader(pdf)
            pages = len(reader.pages)
        except Exception as e:
            results.append({
                "pdf": pdf,
                "error": str(e),
            })
            continue

        empty = 0
        with_text = 0
        hdr = 0
        sample_pages = []
        for i in range(pages):
            try:
                txt = reader.pages[i].extract_text() or ""
            except Exception:
                txt = ""
            t = txt.strip()
            if not t:
                empty += 1
                continue
            with_text += 1
            if header_re.search(t):
                hdr += 1
            if len(sample_pages) < 3:
                sample_pages.append({"p": i+1, "chars": len(t), "head": t[:120].replace("\n"," ")})
        total_pages += pages
        total_empty += empty
        header_hits += hdr

        results.append({
            "pdf": pdf,
            "pdf_bytes": os.path.getsize(pdf),
            "pdf_sha256": sha256_file(pdf),
            "pages": pages,
            "pages_with_text": with_text,
            "empty_pages": empty,
            "empty_pct": round((empty / pages) * 100, 2) if pages else None,
            "header_like_pages": hdr,
            "samples": sample_pages
        })

    out = {
        "created_at": now_utc_iso(),
        "pdf_dir": pdf_dir,
        "raw_dir": raw_dir,
        "pdf_count": len(pdfs),
        "raw_files": raw_index,
        "totals": {
            "total_pages": total_pages,
            "total_empty_pages": total_empty,
            "total_empty_pct": round((total_empty/total_pages)*100,2) if total_pages else None,
            "total_header_like_pages": header_hits
        },
        "pdf_results": results
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print("[done] pdf_count:", len(pdfs))
    print("[done] total_pages:", total_pages)
    print("[done] empty_pages:", total_empty)
    print("[done] empty_pct:", out["totals"]["total_empty_pct"])
    print("[done] wrote:", args.out)

if __name__ == "__main__":
    main()
