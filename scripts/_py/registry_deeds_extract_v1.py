import os, json, argparse, re, datetime
from datetime import timezone
from pypdf import PdfReader

def norm_addr(s: str):
    if not s: return None
    s = s.upper()
    s = re.sub(r"[.]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s*,\s*", ", ", s)
    return s

def parse_fields(text: str):
    t = text or ""
    low = t.lower()

    out = {
        "recorded_date": None,
        "instrument_type": None,
        "book": None,
        "page": None,
        "doc_id": None,
        "consideration": None,
        "grantors": [],
        "grantees": [],
        "address_raw": None,
        "address_norm": None,
        "flags": []
    }

    # Recorded date
    m = re.search(r"\b(?:recorded|recording date)\s*[:\-]?\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\b", t, re.I)
    if not m:
        m = re.search(r"\b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\b", t)
    if m:
        mm, dd, yy = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        if len(yy) == 2: yy = "20" + yy
        out["recorded_date"] = f"{yy}-{mm}-{dd}"
    else:
        out["flags"].append("NO_RECORDED_DATE")

    # Instrument type (very conservative)
    if "quitclaim" in low or "quit claim" in low:
        out["instrument_type"] = "QUITCLAIM_DEED"
    elif "warranty deed" in low:
        out["instrument_type"] = "WARRANTY_DEED"
    elif "deed" in low:
        out["instrument_type"] = "DEED"
    else:
        out["flags"].append("NO_INSTRUMENT_TYPE")

    # Book/Page
    m = re.search(r"\bbook\s*[:#]?\s*(\d{1,7})\b[\s\S]{0,80}\bpage\s*[:#]?\s*(\d{1,7})\b", t, re.I)
    if not m:
        m = re.search(r"\bbk\.?\s*(\d{1,7})\b[\s\S]{0,40}\bpg\.?\s*(\d{1,7})\b", t, re.I)
    if m:
        out["book"], out["page"] = m.group(1), m.group(2)
    else:
        out["flags"].append("NO_BOOK_PAGE")

    # Doc / instrument number
    m = re.search(r"\b(?:doc(?:ument)?\s*(?:id|no\.?|number)|instrument\s*(?:no\.?|number))\s*[:#]?\s*([A-Z0-9\-]{6,})\b", t, re.I)
    if m:
        out["doc_id"] = m.group(1)

    # Consideration
    m = re.search(r"\b(?:consideration|amount)\s*[:\-]?\s*\$?\s*([0-9][0-9,]*)(?:\.\d{2})?\b", t, re.I)
    if not m:
        m = re.search(r"\$\s*([0-9][0-9,]*)(?:\.\d{2})?\b", t)
    if m:
        out["consideration"] = m.group(1).replace(",", "")
    else:
        out["flags"].append("NO_CONSIDERATION")

    # Grantor/Grantee blocks (only if labeled)
    def block_after(label_list):
        for lab in label_list:
            m = re.search(rf"\b{re.escape(lab)}\b\s*[:\-]?\s*([\s\S]{{0,300}})", t, re.I)
            if m:
                b = m.group(1)
                stop = re.search(r"\b(grantor|grantee|consideration|recorded|book|page|doc|instrument)\b", b, re.I)
                if stop:
                    b = b[:stop.start()]
                return b.strip()
        return None

    def split_people(s):
        if not s: return []
        s = re.sub(r"\n+", " ", s)
        parts = re.split(r";|, and | and |\|", s, flags=re.I)
        parts = [p.strip() for p in parts if 2 <= len(p.strip()) <= 120]
        return parts[:10]

    gtor = block_after(["grantor", "grantors", "grantor(s)"])
    gtee = block_after(["grantee", "grantees", "grantee(s)"])
    out["grantors"] = split_people(gtor)
    out["grantees"] = split_people(gtee)
    if not out["grantors"]: out["flags"].append("NO_GRANTOR")
    if not out["grantees"]: out["flags"].append("NO_GRANTEE")

    # Address (Mass-specific heuristic)
    m = re.search(r"\b(?:property\s*address|address)\s*[:\-]?\s*([0-9]{1,6}\s+[A-Z0-9 .#\-/]+,\s*[A-Z .]+,\s*MA\s*\d{5})\b", t, re.I)
    if m:
        out["address_raw"] = m.group(1).strip()
        out["address_norm"] = norm_addr(out["address_raw"])
    else:
        out["flags"].append("NO_ADDRESS")

    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--rawDir", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--outExtracted", required=True)
    ap.add_argument("--outErrors", required=True)
    ap.add_argument("--outAudit", required=True)
    ap.add_argument("--max", type=int, default=0)
    args = ap.parse_args()

    with open(args.manifest, "r", encoding="utf-8-sig") as f:
        manifest = json.load(f)
    file_hash = { x["file"]: x["sha256"] for x in manifest.get("files", []) }

    raw_dir = args.rawDir
    pdfs = [f for f in os.listdir(raw_dir) if f.lower().endswith(".pdf")]
    pdfs.sort()
    if args.max and args.max > 0:
        pdfs = pdfs[:args.max]

    os.makedirs(os.path.dirname(args.outExtracted), exist_ok=True)
    os.makedirs(os.path.dirname(args.outErrors), exist_ok=True)
    os.makedirs(os.path.dirname(args.outAudit), exist_ok=True)

    out_ok = open(args.outExtracted, "w", encoding="utf-8")
    out_err = open(args.outErrors, "w", encoding="utf-8")

    audit = {
        "created_at": datetime.datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "raw_dir": raw_dir,
        "manifest": args.manifest,
        "pdfs_seen": len(pdfs),
        "extracted_ok": 0,
        "errors": 0,
        "text_pdf": 0,
        "needs_ocr": 0,
        "missing_rates": {},
        "examples_needs_ocr": [],
        "examples_errors": []
    }

    miss_counts = {
        "NO_ADDRESS": 0,
        "NO_BOOK_PAGE": 0,
        "NO_RECORDED_DATE": 0,
        "NO_GRANTOR": 0,
        "NO_GRANTEE": 0,
        "NO_CONSIDERATION": 0,
        "NO_INSTRUMENT_TYPE": 0
    }

    for i, fn in enumerate(pdfs, start=1):
        p = os.path.join(raw_dir, fn)
        try:
            reader = PdfReader(p)
            pages = []
            total_len = 0
            for pi, page in enumerate(reader.pages):
                try:
                    txt = page.extract_text() or ""
                except Exception:
                    txt = ""
                if len(txt) > 20000:
                    txt = txt[:20000]
                total_len += len(txt)
                pages.append({"page_index": pi, "text_len": len(txt), "text": txt})

            page_count = len(pages)
            avg = (total_len / page_count) if page_count else 0.0
            needs_ocr = (page_count > 0 and (avg < 60 or total_len < 200))

            # parse only if we have enough text to be meaningful
            merged_text = "\n".join([x["text"] for x in pages]) if not needs_ocr else ""
            parsed = parse_fields(merged_text) if merged_text else {"flags": ["NEEDS_OCR_TEXT_TOO_THIN"]}

            for k in list(miss_counts.keys()):
                if k in parsed.get("flags", []):
                    miss_counts[k] += 1

            rec = {
                "doc_type": "DEED",
                "source": {
                    "file": fn,
                    "rel_path": "publicData/registry/raw_pdfs/deeds/" + fn,
                    "sha256": file_hash.get(fn),
                    "page_count": page_count,
                    "text_total_len": total_len,
                    "avg_text_per_page": round(avg, 2),
                    "is_text_pdf": (not needs_ocr),
                    "needs_ocr": needs_ocr,
                    "extract_method": "pypdf_text_v1" if not needs_ocr else "pypdf_text_insufficient_v1"
                },
                "extracted": parsed
            }

            if needs_ocr:
                audit["needs_ocr"] += 1
                if len(audit["examples_needs_ocr"]) < 10:
                    audit["examples_needs_ocr"].append({"file": fn, "pages": page_count, "totalTextLen": total_len, "avg": round(avg,2)})
            else:
                audit["text_pdf"] += 1

            out_ok.write(json.dumps(rec, ensure_ascii=False) + "\n")
            audit["extracted_ok"] += 1

            if i % 250 == 0:
                print(f"[progress] {i}/{len(pdfs)}")

        except Exception as e:
            audit["errors"] += 1
            err_rec = {
                "file": fn,
                "rel_path": "publicData/registry/raw_pdfs/deeds/" + fn,
                "error": str(e)[:800]
            }
            out_err.write(json.dumps(err_rec, ensure_ascii=False) + "\n")
            if len(audit["examples_errors"]) < 10:
                audit["examples_errors"].append(err_rec)

    out_ok.close()
    out_err.close()

    # missing rates
    total = max(1, audit["extracted_ok"])
    audit["missing_rates"] = { k: round(v / total, 4) for k, v in miss_counts.items() }

    with open(args.outAudit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

if __name__ == "__main__":
    main()


