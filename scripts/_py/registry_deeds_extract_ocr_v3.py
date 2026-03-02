import os, json, argparse, re, hashlib
import datetime
from datetime import timezone

import fitz  # pymupdf
import pytesseract
from PIL import Image

# Hard-wire tesseract path (stable, no PATH dependence)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

STREET_WORD = r"(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|LN|LANE|BLVD|BOULEVARD|CT|COURT|PL|PLACE|TER|TERRACE|WAY|PKWY|PARKWAY|HWY|HIGHWAY)"

def clean_text(s: str) -> str:
    if not s: return ""
    # normalize curly quotes / garbage
    s = s.replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')
    s = s.replace("â€™", "'").replace("â€œ", '"').replace("â€�", '"')
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def norm_addr(s: str):
    if not s: return None
    s = s.upper()
    s = re.sub(r"[.]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s*,\s*", ", ", s)
    return s

def best_address(text: str):
    t = text or ""
    # 1) LOCUS:
    m = re.search(r"\bLOCUS\b\s*[:\-]?\s*([0-9]{1,6}\s+[^,\n]{2,80}\b" + STREET_WORD + r"\b[^,\n]*,\s*[^,\n]{2,50},\s*MA\s*\d{5})\b", t, re.I)
    if m: return m.group(1).strip()

    # 2) "Known as" / "Premises known as" / "situated at" / "located at"
    m = re.search(r"\b(?:premises\s+known\s+as|known\s+as|situat(?:ed|e)\s+at|located\s+at)\b\s*[:\-]?\s*([0-9]{1,6}\s+[^,\n]{2,80}\b" + STREET_WORD + r"\b[^,\n]*,\s*[^,\n]{2,50},\s*MA\s*\d{5})\b", t, re.I)
    if m: return m.group(1).strip()

    # 3) Generic MA address line anywhere (Boston area docs)
    m = re.search(r"\b([0-9]{1,6}\s+[^,\n]{2,80}\b" + STREET_WORD + r"\b[^,\n]*,\s*[^,\n]{2,50},\s*MA\s*\d{5})\b", t, re.I)
    if m: return m.group(1).strip()

    return None

def extract_block(t: str, label_regex: str, stop_regex: str, max_len: int = 700):
    m = re.search(label_regex, t, re.I)
    if not m: return None
    s = t[m.end():]
    s = s[:max_len]
    stop = re.search(stop_regex, s, re.I)
    if stop:
        s = s[:stop.start()]
    return s.strip()

def split_people(block: str):
    if not block: return []
    b = block
    b = re.sub(r"\n+", " ", b)
    b = re.sub(r"\s{2,}", " ", b).strip()

    # Remove obvious non-name boilerplate chunks
    junk = ["acting by", "hereby releases", "homestead", "public facilities commission", "mayor", "office of housing"]
    for j in junk:
        b = re.sub(rf".*{re.escape(j)}.*", " ", b, flags=re.I)

    parts = re.split(r";|\band\b|&|\|", b, flags=re.I)
    parts = [p.strip(" ,.-") for p in parts if len(p.strip()) >= 3]

    # Filter for name-like strings (letters heavy)
    out = []
    for p in parts:
        letters = sum(ch.isalpha() for ch in p)
        if letters < 4: 
            continue
        if len(p) > 140:
            continue
        out.append(p)
        if len(out) >= 10:
            break
    return out

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

    # Instrument type (conservative)
    if "quitclaim" in low or "quit claim" in low:
        out["instrument_type"] = "QUITCLAIM_DEED"
    elif "warranty deed" in low:
        out["instrument_type"] = "WARRANTY_DEED"
    elif "deed" in low:
        out["instrument_type"] = "DEED"
    else:
        out["flags"].append("NO_INSTRUMENT_TYPE")

    # Book/Page (more forgiving)
    m = re.search(r"\bbook\s*[:#]?\s*(\d{1,8})\b[\s\S]{0,120}\bpage\s*[:#]?\s*(\d{1,8})\b", t, re.I)
    if not m:
        m = re.search(r"\bbk\.?\s*(\d{1,8})\b[\s\S]{0,80}\bpg\.?\s*(\d{1,8})\b", t, re.I)
    if m:
        out["book"], out["page"] = m.group(1), m.group(2)
    else:
        out["flags"].append("NO_BOOK_PAGE")

    # Doc/instrument number
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

    # Party blocks: tolerate OCR label variants
    stop = r"\b(consideration|recorded|recording|book|page|doc|instrument|property\s*address|address|locus|premises|known\s+as)\b"

    gtor_block = extract_block(t, r"\bgrantor(?:s|\(s\))?\b\s*[:\-]?\s*", stop)
    gtee_block = extract_block(t, r"\bgrantee(?:s|\(s\))?\b\s*[:\-]?\s*", stop)

    # Some OCR variants: "GRAN TEE", "GRANIEE"
    if not gtee_block:
        gtee_block = extract_block(t, r"\bgran[\s\-]*tee(?:s|\(s\))?\b\s*[:\-]?\s*", stop)
    if not gtee_block:
        gtee_block = extract_block(t, r"\bgrani[e]?e(?:s|\(s\))?\b\s*[:\-]?\s*", stop)

    out["grantors"] = split_people(gtor_block)
    out["grantees"] = split_people(gtee_block)

    if not out["grantors"]: out["flags"].append("NO_GRANTOR")
    if not out["grantees"]: out["flags"].append("NO_GRANTEE")

    addr = best_address(t)
    if addr:
        out["address_raw"] = addr
        out["address_norm"] = norm_addr(addr)
    else:
        out["flags"].append("NO_ADDRESS")

    return out

def sha1(s: str):
    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()

def render_region(page, rect, dpi):
    mat = fitz.Matrix(dpi/72.0, dpi/72.0)
    clip = fitz.Rect(rect)
    pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return img

def ocr_regions(pdf_path: str, page_limit: int, dpi: int, config: str):
    doc = fitz.open(pdf_path)
    all_txt = []
    total_len = 0

    n = min(page_limit, doc.page_count)
    for i in range(n):
        page = doc.load_page(i)
        w, h = page.rect.width, page.rect.height

        # Regions: top band, left margin, mid band (where LOCUS often appears), full page fallback
        regions = [
            ("top",    (0, 0, w, h*0.28)),
            ("left",   (0, 0, w*0.22, h)),
            ("mid",    (0, h*0.25, w, h*0.75)),
            ("full",   (0, 0, w, h))
        ]

        for name, r in regions:
            img = render_region(page, r, dpi)
            txt = pytesseract.image_to_string(img, lang="eng", config=config) or ""
            txt = clean_text(txt)
            if len(txt) > 0:
                block = f"\n\n===PAGE {i} REGION {name}===\n{txt}\n"
                all_txt.append(block)
                total_len += len(txt)

    doc.close()
    merged = "\n".join(all_txt)
    avg = total_len / max(1, page_limit)
    return merged, total_len, avg

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rawDir", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--outExtracted", required=True)
    ap.add_argument("--outErrors", required=True)
    ap.add_argument("--outAudit", required=True)
    ap.add_argument("--max", type=int, default=0)
    ap.add_argument("--pageLimit", type=int, default=2)
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--psm", type=int, default=6)
    ap.add_argument("--oem", type=int, default=1)
    args = ap.parse_args()

    with open(args.manifest, "r", encoding="utf-8-sig") as f:
        manifest = json.load(f)
    file_hash = { x["file"]: x["sha256"] for x in manifest.get("files", []) }

    pdfs = [f for f in os.listdir(args.rawDir) if f.lower().endswith(".pdf")]
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
        "raw_dir": args.rawDir,
        "manifest": args.manifest,
        "pdfs_seen": len(pdfs),
        "extracted_ok": 0,
        "errors": 0,
        "ocr_page_limit": args.pageLimit,
        "ocr_dpi": args.dpi,
        "tesseract_config": f"--oem {args.oem} --psm {args.psm}",
        "missing_rates": {},
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

    config = f"--oem {args.oem} --psm {args.psm}"

    for i, fn in enumerate(pdfs, start=1):
        p = os.path.join(args.rawDir, fn)
        try:
            merged, total_len, avg = ocr_regions(p, args.pageLimit, args.dpi, config)
            parsed = parse_fields(merged)

            for k in miss_counts.keys():
                if k in parsed.get("flags", []):
                    miss_counts[k] += 1

            rec = {
                "doc_type": "DEED",
                "source": {
                    "file": fn,
                    "rel_path": "publicData/registry/raw_pdfs/deeds/" + fn,
                    "sha256": file_hash.get(fn),
                    "extract_method": "ocr_tesseract_regions_v3",
                    "ocr_page_limit": args.pageLimit,
                    "ocr_dpi": args.dpi,
                    "tesseract_config": config,
                    "ocr_text_total_len": total_len,
                    "ocr_avg_text_per_page": round(avg, 2),
                    "ocr_text_fingerprint": sha1(merged[:2000])
                },
                "extracted": parsed
            }

            out_ok.write(json.dumps(rec, ensure_ascii=False) + "\n")
            audit["extracted_ok"] += 1

            if i % 200 == 0:
                print(f"[progress] {i}/{len(pdfs)}")

        except Exception as e:
            audit["errors"] += 1
            err_rec = {"file": fn, "rel_path": "publicData/registry/raw_pdfs/deeds/" + fn, "error": str(e)[:900]}
            out_err.write(json.dumps(err_rec, ensure_ascii=False) + "\n")
            if len(audit["examples_errors"]) < 10:
                audit["examples_errors"].append(err_rec)

    out_ok.close()
    out_err.close()

    total = max(1, audit["extracted_ok"])
    audit["missing_rates"] = { k: round(v / total, 4) for k, v in miss_counts.items() }

    with open(args.outAudit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

if __name__ == "__main__":
    main()
