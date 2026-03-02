import argparse, os, json, hashlib, datetime, re
from typing import Dict, Any, List, Set, Tuple

import fitz  # PyMuPDF
from PIL import Image
import pytesseract

# Hard-wire tesseract path for Windows
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def now_utc_iso() -> str:
    # timezone-aware UTC (no utcnow deprecation)
    return datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00","Z")

def read_json_allow_bom(path: str) -> Any:
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def load_done_files(out_ndjson: str, err_ndjson: str) -> Set[str]:
    done: Set[str] = set()
    for p in [out_ndjson, err_ndjson]:
        if not os.path.exists(p):
            continue
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    fn = obj.get("source", {}).get("file")
                    if fn:
                        done.add(fn)
                except Exception:
                    continue
    return done

def iter_pdfs(raw_dir: str) -> List[str]:
    pdfs = []
    for root, _, files in os.walk(raw_dir):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                pdfs.append(os.path.join(root, fn))
    pdfs.sort(key=lambda p: os.path.basename(p))
    return pdfs

def safe_relpath(full: str, anchor: str) -> str:
    try:
        rp = os.path.relpath(full, anchor).replace("\\", "/")
        return rp
    except Exception:
        return os.path.basename(full)

def ocr_page_pix(doc: fitz.Document, page_index: int, dpi: int) -> Image.Image:
    page = doc.load_page(page_index)
    mat = fitz.Matrix(dpi/72, dpi/72)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    mode = "RGB" if pix.n < 4 else "RGBA"
    img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
    if img.mode != "RGB":
        img = img.convert("RGB")
    return img

def ocr_regions(pdf_path: str, page_limit: int, dpi: int, tconfig: str, timeout_sec: int) -> Tuple[str, List[Dict[str, Any]], int]:
    doc = fitz.open(pdf_path)
    pages = min(page_limit, doc.page_count)
    blocks: List[Dict[str, Any]] = []
    merged_parts: List[str] = []
    total_len = 0

    for i in range(pages):
        img = ocr_page_pix(doc, i, dpi)
        txt = pytesseract.image_to_string(img, lang="eng", config=tconfig, timeout=timeout_sec) or ""
        txt = txt.replace("\x00", " ")
        merged_parts.append(txt)
        total_len += len(txt)
        blocks.append({"page": i+1, "text_len": len(txt)})
    doc.close()
    merged = "\n".join(merged_parts)
    return merged, blocks, total_len

# --- Candidate extraction + scoring (your path) ---
FULL_ADDR_RE = re.compile(r"\b(\d{1,6}\s+[A-Z0-9][A-Z0-9\s\.\-']+?\s+(?:ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|CT|COURT|PL|PLACE|LN|LANE|BLVD|BOULEVARD|PKWY|PARKWAY|HWY|HIGHWAY|WAY|TER|TERRACE|CIR|CIRCLE))\b", re.IGNORECASE)
ZIP_RE = re.compile(r"\b(MA)\s+(\d{5})(?:-\d{4})?\b", re.IGNORECASE)

POS_CUES = [
    ("PROPERTY ADDRESS", 18),
    ("PREMISES KNOWN AS", 16),
    ("LOCATED AT", 8),
    ("SITUATED AT", 8),
    ("BEING THE PREMISES", 10),
    ("THE LAND WITH BUILDINGS THEREON", 10),
    ("VACANT LAND LOCATED AT", 10),
    ("STREET ADDRESS", 10),
]
NEG_CUES = [
    ("PLEASE RETURN TO", -18),
    ("RETURN TO", -14),
    ("NOW OF", -10),
    ("WITH AN ADDRESS OF", -12),
    ("ADDRESS OF", -6),
    ("MAILING ADDRESS", -12),
    ("WHOSE ADDRESS IS", -10),
]

def extract_address_candidates(text: str) -> List[Dict[str, Any]]:
    cands: List[Dict[str, Any]] = []
    upper = text.upper()

    # full-ish street phrases
    for m in FULL_ADDR_RE.finditer(upper):
        val = m.group(1).strip()
        start = m.start(1)
        # try to extend to include city/state/zip if nearby
        tail = upper[m.end(1): m.end(1) + 80]
        zipm = ZIP_RE.search(tail)
        if zipm:
            val2 = (val + ", " + tail[:zipm.end()].strip()).replace(" ,", ",")
            cands.append({"value": val2.strip(), "strength": "ma_zip", "start": start})
        cands.append({"value": val.strip(), "strength": "weak", "start": start})

    # de-dupe preserving order
    seen = set()
    out = []
    for c in cands:
        key = c["value"]
        if key not in seen:
            seen.add(key)
            out.append(c)
    return out

def score_candidate(text: str, cand: Dict[str, Any]) -> Dict[str, Any]:
    start = cand.get("start", 0)
    lo = max(0, start - 180)
    hi = min(len(text), start + 220)
    ctx = text[lo:hi]
    ctxU = ctx.upper()

    score = 0
    hits = []

    for cue, pts in POS_CUES:
        if cue in ctxU:
            score += pts
            hits.append({"cue": cue, "pts": pts})
    for cue, pts in NEG_CUES:
        if cue in ctxU:
            score += pts
            hits.append({"cue": cue, "pts": pts})

    # locality boosts
    if "BOSTON" in ctxU:
        score += 2
        hits.append({"cue": "LOCALITY_BOSTON", "pts": 2})

    # frequency in doc
    freq = text.upper().count(cand["value"].upper())
    if freq >= 2:
        score += 2
        hits.append({"cue": "FREQ", "pts": 2})

    cand2 = dict(cand)
    cand2["score"] = score
    cand2["hits"] = hits
    cand2["context"] = ctx[:400]
    return cand2

def pick_property_address(scored: List[Dict[str, Any]]) -> Tuple[str, str, List[str]]:
    # returns (raw, norm, flags)
    if not scored:
        return None, None, ["NO_PROPERTY_ADDRESS"]

    scored_sorted = sorted(scored, key=lambda c: c["score"], reverse=True)
    top = scored_sorted[0]
    runner = scored_sorted[1] if len(scored_sorted) > 1 else None

    # Thresholding + ambiguity
    if top["score"] < 14:
        return None, None, ["NO_PROPERTY_ADDRESS"]
    if runner and (top["score"] - runner["score"]) < 6:
        return None, None, ["AMBIGUOUS_ADDRESS"]

    raw = top["value"]
    norm = raw.upper().strip()
    norm = re.sub(r"\s+", " ", norm)
    return raw, norm, []

def write_ndjson_line(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rawDir", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--outExtracted", required=True)
    ap.add_argument("--outErrors", required=True)
    ap.add_argument("--outAudit", required=True)
    ap.add_argument("--max", type=int, default=0)
    ap.add_argument("--pageLimit", type=int, default=3)
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--psm", type=int, default=6)
    ap.add_argument("--oem", type=int, default=1)
    ap.add_argument("--ocrTimeoutSec", type=int, default=25)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    manifest = read_json_allow_bom(args.manifest)
    raw_dir = args.rawDir

    done = set()
    if args.resume:
        done = load_done_files(args.outExtracted, args.outErrors)

    pdfs = iter_pdfs(raw_dir)
    if args.max and args.max > 0:
        pdfs = pdfs[:args.max]

    tconfig = f"--oem {args.oem} --psm {args.psm}"

    stats = {
        "created_at": now_utc_iso(),
        "raw_dir": raw_dir,
        "manifest": args.manifest,
        "pdfs_total": len(pdfs),
        "resume": bool(args.resume),
        "already_done": len(done),
        "pdfs_seen": 0,
        "extracted_ok": 0,
        "errors": 0,
        "ocr_page_limit": args.pageLimit,
        "ocr_dpi": args.dpi,
        "tesseract_config": tconfig,
        "missing_rates": {
            "NO_PROPERTY_ADDRESS": 0.0,
            "AMBIGUOUS_ADDRESS": 0.0,
            "NO_BOOK_PAGE": 0.0,
            "NO_RECORDED_DATE": 0.0,
        },
        "examples_errors": []
    }
    missing_counts = {k:0 for k in stats["missing_rates"].keys()}
    N = 0

    for idx, pdf_path in enumerate(pdfs, start=1):
        fname = os.path.basename(pdf_path)
        if args.resume and fname in done:
            continue

        N += 1
        stats["pdfs_seen"] += 1

        try:
            # OCR
            merged, blocks, total_len = ocr_regions(
                pdf_path, args.pageLimit, args.dpi, tconfig, args.ocrTimeoutSec
            )
            fp = sha1(merged[:20000])  # fingerprint
            rel_path = "publicData/registry/raw_pdfs/deeds/" + fname

            # VERY LIGHT deterministic fields (we can harden later):
            # recorded date + book/page often on cover sheet; keep optional
            rec_date = None
            mdate = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", merged)
            if mdate:
                mm, dd, yyyy = mdate.group(1), mdate.group(2), mdate.group(3)
                rec_date = f"{yyyy}-{int(mm):02d}-{int(dd):02d}"

            book = None
            page = None
            mbp = re.search(r"\b(\d{3,6})\s*/\s*(\d{1,4})\b", merged)
            if mbp:
                book, page = mbp.group(1), mbp.group(2)

            # candidates + scoring
            candidates = extract_address_candidates(merged)
            scored = [score_candidate(merged, c) for c in candidates]
            prop_raw, prop_norm, addr_flags = pick_property_address(scored)

            flags = []
            if not rec_date:
                flags.append("NO_RECORDED_DATE")
                missing_counts["NO_RECORDED_DATE"] += 1
            if not (book and page):
                flags.append("NO_BOOK_PAGE")
                missing_counts["NO_BOOK_PAGE"] += 1
            if addr_flags:
                flags.extend(addr_flags)
                for f in addr_flags:
                    if f in missing_counts:
                        missing_counts[f] += 1

            out_obj = {
                "doc_type": "DEED",
                "source": {
                    "file": fname,
                    "rel_path": rel_path,
                    "extract_method": "ocr_candidates_scored_v6",
                    "ocr_page_limit": args.pageLimit,
                    "ocr_dpi": args.dpi,
                    "tesseract_config": tconfig,
                    "ocr_timeout_sec": args.ocrTimeoutSec,
                    "ocr_text_total_len": total_len,
                    "ocr_text_fingerprint": fp,
                },
                "extracted": {
                    "recorded_date": rec_date,
                    "instrument_type": "DEED",
                    "book": book,
                    "page": page,
                    "property_address_raw": prop_raw,
                    "property_address_norm": prop_norm,
                    "flags": flags,
                    "address_candidates": sorted(scored, key=lambda c: c["score"], reverse=True)[:12],
                }
            }

            write_ndjson_line(args.outExtracted, out_obj)
            stats["extracted_ok"] += 1

        except Exception as e:
            err_obj = {
                "doc_type": "DEED",
                "source": {
                    "file": fname,
                    "rel_path": "publicData/registry/raw_pdfs/deeds/" + fname,
                    "extract_method": "ocr_candidates_scored_v6",
                },
                "error": str(e)
            }
            write_ndjson_line(args.outErrors, err_obj)
            stats["errors"] += 1
            if len(stats["examples_errors"]) < 5:
                stats["examples_errors"].append({"file": fname, "error": str(e)})

        if stats["pdfs_seen"] % 200 == 0:
            print(f"[progress] {stats['pdfs_seen']}/{len(pdfs)}")

    # missing rates
    if N > 0:
        for k in stats["missing_rates"].keys():
            stats["missing_rates"][k] = round(missing_counts[k] / N, 4)

    os.makedirs(os.path.dirname(args.outAudit), exist_ok=True)
    with open(args.outAudit, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print("[done] wrote:")
    print("  extracted:", args.outExtracted)
    print("  errors:   ", args.outErrors)
    print("  audit:    ", args.outAudit)

if __name__ == "__main__":
    main()
