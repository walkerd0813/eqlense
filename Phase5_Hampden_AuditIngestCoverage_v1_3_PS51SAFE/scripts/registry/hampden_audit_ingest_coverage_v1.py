import os
import json
import glob
from datetime import datetime, timezone

from PyPDF2 import PdfReader


def iso_utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')


def get_text_len(page):
    try:
        txt = page.extract_text() or ""
    except Exception:
        txt = ""
    txt = (txt or "").strip()
    return len(txt)


def audit_pdf(pdf_path: str, min_chars: int = 10):
    reader = PdfReader(pdf_path)
    total = len(reader.pages)
    empty = 0
    empty_pages = []
    sample_empty = []
    for i, page in enumerate(reader.pages):
        n = get_text_len(page)
        if n < min_chars:
            empty += 1
            empty_pages.append(i + 1)  # 1-indexed
            if len(sample_empty) < 10:
                sample_empty.append(i + 1)
    return {
        "file": os.path.basename(pdf_path),
        "path": pdf_path,
        "pages": total,
        "empty_pages": empty,
        "empty_pct": (empty / total) if total else 0.0,
        "sample_empty_pages": sample_empty,
    }


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--pdfDir', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--minChars', type=int, default=10)
    args = ap.parse_args()

    pdf_dir = os.path.abspath(args.pdfDir)
    pdfs = sorted(glob.glob(os.path.join(pdf_dir, '*.pdf')))
    if not pdfs:
        raise SystemExit(f"No PDFs found in {pdf_dir}")

    per_pdf = []
    total_pages = 0
    total_empty = 0
    for pdf in pdfs:
        stats = audit_pdf(pdf, min_chars=args.minChars)
        per_pdf.append(stats)
        total_pages += stats['pages']
        total_empty += stats['empty_pages']

    out_obj = {
        "created_at": iso_utc_now(),
        "pdf_dir": pdf_dir,
        "min_chars": args.minChars,
        "totals": {
            "pdf_count": len(per_pdf),
            "total_pages": total_pages,
            "empty_pages": total_empty,
            "empty_pct": (total_empty / total_pages) if total_pages else 0.0,
        },
        "per_pdf": per_pdf,
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(out_obj, f, indent=2)

    print('[done] pdf_count:', out_obj['totals']['pdf_count'])
    print('[done] total_pages:', out_obj['totals']['total_pages'])
    print('[done] empty_pages:', out_obj['totals']['empty_pages'])
    print('[done] empty_pct:', round(out_obj['totals']['empty_pct'], 4))
    print('[done] out:', os.path.abspath(args.out))


if __name__ == '__main__':
    main()
