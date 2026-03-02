#!/usr/bin/env python3
# registry_deeds_extract_ocr_v7_margin.py
# Goal: resilient deed OCR extractor focused on LEFT-MARGIN (rotated) "Property Address" patterns,
# plus fallback body cues like "land at", "situated at", etc.
#
# Usage example:
#   python scripts/_py/registry_deeds_extract_ocr_v7_margin.py ^
#     --rawDir "C:\seller-app\backend\publicData\registry\raw_pdfs\deeds" ^
#     --manifest "C:\seller-app\backend\publicData\registry\manifests\registry_deeds_manifest_v1.json" ^
#     --outExtracted "C:\seller-app\backend\publicData\registry\_extracted\deeds_extracted_v7_margin_ALL.ndjson" ^
#     --outAudit "C:\seller-app\backend\publicData\_audit\registry\registry_deeds_extract_v7_margin_audit__ALL.json" ^
#     --pageLimit 2 --dpi 300 --psm 6 --oem 1 --ocrTimeoutSec 25 --resume
#
# Notes:
# - This script is *resumable* (skips PDFs whose sha256 already appear in outExtracted).
# - OCR is targeted:
#     (A) Left margin strip (rotated both directions) => high hit-rate for "Property Address" printed vertically
#     (B) Small header strip for "Bk:" / "Pg:" / "Recorded:" patterns
#     (C) Fallback full-page OCR only if needed (still limited to pageLimit)
#
# Dependencies: pdf2image, pillow, pytesseract. Poppler must be available for pdf2image.

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from PIL import Image

import pytesseract
from pdf2image import convert_from_path

# ----------------------------
# Regex helpers
# ----------------------------

RE_DATE = re.compile(r'(?<!\d)(\d{1,2})/(\d{1,2})/(\d{4})(?!\d)')
RE_BKPG_1 = re.compile(r'\bBk[:\s]+(\d{3,7})\b.*?\bPg[:\s]+(\d{1,5})\b', re.IGNORECASE | re.DOTALL)
RE_BKPG_2 = re.compile(r'\bBook[:\s]+(\d{3,7})\b.*?\bPage[:\s]+(\d{1,5})\b', re.IGNORECASE | re.DOTALL)
RE_CONS = re.compile(r'\bCONS[:\s]+\$?\s*([0-9][0-9,]*(?:\.\d{2})?)', re.IGNORECASE)
RE_FEE = re.compile(r'\bFEE[:\s]+\$?\s*([0-9][0-9,]*(?:\.\d{2})?)', re.IGNORECASE)

# "Property Address:" and variants (common in first screenshot)
RE_PROP_ADDR_LABEL = re.compile(r'\bPROPERTY\s+ADDRESS\b\s*[:\-]?\s*(.+)', re.IGNORECASE)

# Left margin: "Property/Grantor Address:" and variants (common in third screenshot)
RE_PROP_GRANTOR_ADDR = re.compile(r'\bPROPERTY\s*/\s*GRANTOR\s+ADDRESS\b\s*[:\-]?\s*(.+)', re.IGNORECASE)

# Candidate address pattern (simple but effective)
RE_ADDR_LINE = re.compile(
    r'\b(\d{1,6})\s+([A-Z0-9][A-Z0-9\s\.\-]{2,40}?)\s+'
    r'(STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|LANE|LN|COURT|CT|PLACE|PL|WAY|BOULEVARD|BLVD|PARKWAY|PKWY|TERRACE|TER)\b'
    r'(?:\s*(?:,|\s)\s*([A-Z][A-Z\s]{1,25}))?'
    r'(?:\s*(?:,|\s)\s*(MA|MASSACHUSETTS))?'
    r'(?:\s+(\d{5})(?:-\d{4})?)?\b',
    re.IGNORECASE
)

# Cues for fallback body patterns
CUE_PATTERNS = [
    ("VACANT_LAND_LOCATED_AT", re.compile(r'\bVACANT\s+LAND\s+(?:LOCATED|SITUATED)\s+AT\b', re.IGNORECASE)),
    ("LAND_AT", re.compile(r'\bTHE\s+LAND\s+AT\b', re.IGNORECASE)),
    ("SITUATED_AT", re.compile(r'\bSITUATED\s+AT\b', re.IGNORECASE)),
    ("LOCATED_AT", re.compile(r'\bLOCATED\s+AT\b', re.IGNORECASE)),
    ("PREMISES_KNOWN_AS", re.compile(r'\bPREMISES\s+(?:KNOWN\s+AS|LOCATED\s+AT)\b', re.IGNORECASE)),
    ("WITH_BUILDINGS_THEREON", re.compile(r'\bWITH\s+THE\s+BUILDINGS\s+THEREON\b', re.IGNORECASE)),
    ("BUILDINGS_THEREON_SITUATED", re.compile(r'\bBUILDINGS\s+THEREON\s+SITUATED\b', re.IGNORECASE)),
]

PROPERTY_TYPE_RULES = [
    ("LAND", re.compile(r'\bVACANT\s+LAND\b|\bUNIMPROVED\b|\bPARCEL\s+OF\s+VACANT\s+LAND\b', re.IGNORECASE)),
    ("CONDO", re.compile(r'\bCONDOMINIUM\b|\bUNIT\b', re.IGNORECASE)),
    ("IMPROVED", re.compile(r'\bBUILDINGS?\b|\bIMPROVEMENTS?\b', re.IGNORECASE)),
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def safe_ocr(img: Image.Image, config: str, timeout_sec: int) -> str:
    try:
        return (pytesseract.image_to_string(img, lang="eng", config=config, timeout=timeout_sec) or "").strip()
    except RuntimeError as e:
        # pytesseract uses RuntimeError for timeouts; treat as empty
        return ""
    except Exception:
        return ""


def crop_left_margin(img: Image.Image, frac_w: float = 0.14) -> Image.Image:
    w, h = img.size
    mw = max(1, int(w * frac_w))
    return img.crop((0, 0, mw, h))


def crop_header_strip(img: Image.Image, frac_h: float = 0.18) -> Image.Image:
    w, h = img.size
    hh = max(1, int(h * frac_h))
    return img.crop((0, 0, w, hh))


def ocr_margin_both_rotations(img: Image.Image, config: str, timeout_sec: int) -> dict:
    margin = crop_left_margin(img)
    rot_cw = margin.rotate(-90, expand=True)
    rot_ccw = margin.rotate(90, expand=True)

    txt_cw = safe_ocr(rot_cw, config=config, timeout_sec=timeout_sec)
    txt_ccw = safe_ocr(rot_ccw, config=config, timeout_sec=timeout_sec)

    # pick richer text
    if len(txt_cw) >= len(txt_ccw):
        return {"rotation": "CW", "text": txt_cw}
    return {"rotation": "CCW", "text": txt_ccw}


def normalize_whitespace(s: str) -> str:
    s = s.replace("\u2019", "'").replace("\u2018", "'")
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def normalize_address(s: str) -> str:
    s = normalize_whitespace(s)
    s = s.replace(",", " ")
    s = re.sub(r'\s+', ' ', s).strip()
    return s.upper()


def extract_book_page(text: str):
    m = RE_BKPG_1.search(text) or RE_BKPG_2.search(text)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def extract_recorded_date(text: str):
    # prefer MM/DD/YYYY
    m = RE_DATE.search(text)
    if not m:
        return None
    mm, dd, yyyy = m.group(1), m.group(2), m.group(3)
    try:
        dt = datetime(int(yyyy), int(mm), int(dd))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def score_candidate(context: str, value: str, cues_hit: list, base: int) -> dict:
    score = base + sum(pts for _, pts in cues_hit)
    strength = "weak"
    if score >= 35:
        strength = "full"
    elif score >= 25:
        strength = "ma"
    return {
        "value": value,
        "strength": strength,
        "score": score,
        "hits": [{"cue": cue, "pts": pts} for cue, pts in cues_hit],
        "context": context[:600]
    }


def find_address_candidates(text: str, prefer_city=None):
    text_n = text
    cands = []

    # 1) Property Address label
    for m in RE_PROP_GRANTOR_ADDR.finditer(text_n):
        tail = m.group(1)
        # take first 2 lines-ish
        seg = tail.splitlines()
        seg = " ".join(seg[:3])
        seg = normalize_whitespace(seg)
        cands.append(("PROPERTY_GRANTOR_ADDRESS", seg, 40))

    for m in RE_PROP_ADDR_LABEL.finditer(text_n):
        tail = m.group(1)
        seg = " ".join(tail.splitlines()[:3])
        seg = normalize_whitespace(seg)
        cands.append(("PROPERTY_ADDRESS", seg, 40))

    # 2) General address lines
    for m in RE_ADDR_LINE.finditer(text_n.upper()):
        # rebuild
        num = m.group(1)
        street = m.group(2).strip()
        suf = m.group(3).strip()
        city = (m.group(4) or "").strip()
        state = (m.group(5) or "MA").strip()
        zipc = (m.group(6) or "").strip()
        val = f"{num} {street} {suf}"
        if city:
            val += f", {city}"
        if state:
            val += f", {state}"
        if zipc:
            val += f" {zipc}"
        cands.append(("ADDR_LINE", val, 18))

    # De-dupe while preserving order
    seen = set()
    out = []
    for tag, val, base in cands:
        key = normalize_address(val)
        if key in seen:
            continue
        seen.add(key)
        # attach cue hits based on nearby patterns
        hits = []
        # lightweight cue scoring: presence of label => big points
        if tag in ("PROPERTY_ADDRESS", "PROPERTY_GRANTOR_ADDRESS"):
            hits.append((tag, 20))
        # cue scoring
        for cue, rx in CUE_PATTERNS:
            if rx.search(text_n):
                # only small boost unless it's a direct address label
                pts = 10 if cue in ("VACANT_LAND_LOCATED_AT", "LAND_AT", "SITUATED_AT", "LOCATED_AT") else 4
                hits.append((cue, pts))
        # locality boost
        if prefer_city and prefer_city.upper() in key:
            hits.append((f"LOCALITY_{prefer_city.upper()}", 2))
        out.append(score_candidate(text_n, val, hits, base))
    return out


def infer_property_type(text: str) -> str | None:
    for ptype, rx in PROPERTY_TYPE_RULES:
        if rx.search(text):
            return ptype
    return None


def load_manifest_list(manifest_path: Path):
    if not manifest_path.exists():
        return None
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            j = json.load(f)
        # support either {"files":[...]} or raw array
        if isinstance(j, dict) and "files" in j and isinstance(j["files"], list):
            return j["files"]
        if isinstance(j, list):
            return j
    except Exception:
        return None
    return None


def read_processed_sha256(out_path: Path) -> set:
    done = set()
    if not out_path.exists():
        return done
    with open(out_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                sha = obj.get("source", {}).get("sha256")
                if sha:
                    done.add(str(sha).upper())
            except Exception:
                continue
    return done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rawDir", required=True)
    ap.add_argument("--manifest", required=False, default=None)
    ap.add_argument("--outExtracted", required=True)
    ap.add_argument("--outAudit", required=True)
    ap.add_argument("--pageLimit", type=int, default=2)
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--psm", type=int, default=6)
    ap.add_argument("--oem", type=int, default=1)
    ap.add_argument("--ocrTimeoutSec", type=int, default=25)
    ap.add_argument("--max", type=int, default=0, help="limit number of PDFs (0=all)")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    raw_dir = Path(args.rawDir)
    out_path = Path(args.outExtracted)
    audit_path = Path(args.outAudit)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    config = f"--oem {args.oem} --psm {args.psm}"

    # Determine input PDF list
    manifest_files = load_manifest_list(Path(args.manifest)) if args.manifest else None
    if manifest_files:
        pdf_paths = [raw_dir / Path(x).name for x in manifest_files]
    else:
        pdf_paths = sorted(raw_dir.glob("*.pdf"))

    pdfs_total = len(pdf_paths)
    processed_sha = read_processed_sha256(out_path) if (args.resume and out_path.exists()) else set()
    already_done = 0
    extracted_ok = 0
    errors = 0

    # missing counters
    miss_no_addr = 0
    miss_ambig = 0
    miss_no_bkpg = 0
    miss_no_date = 0

    t0 = time.time()
    mode = "append" if (args.resume and out_path.exists()) else "w"

    with open(out_path, mode, encoding="utf-8") as fout:
        seen = 0
        for i, pdf in enumerate(pdf_paths, start=1):
            if args.max and seen >= args.max:
                break
            if not pdf.exists():
                continue

            sha = sha256_file(pdf)
            if sha in processed_sha:
                already_done += 1
                continue

            seen += 1
            if seen % 200 == 0:
                print(f"[progress] {seen}/{pdfs_total}", flush=True)

            try:
                images = convert_from_path(str(pdf), dpi=args.dpi, first_page=1, last_page=min(args.pageLimit, 10))
            except Exception as e:
                errors += 1
                continue

            # Primary: first page margin + header
            page1 = images[0].convert("RGB")

            margin = ocr_margin_both_rotations(page1, config=config, timeout_sec=args.ocrTimeoutSec)
            header = crop_header_strip(page1)
            header_text = safe_ocr(header, config=config, timeout_sec=args.ocrTimeoutSec)

            margin_text = margin.get("text", "")
            # Fallback: if margin looks empty, try a bit more of body on page1 only
            body_text = ""
            if len(margin_text) < 25 or ("PROPERTY" not in margin_text.upper() and "ADDRESS" not in margin_text.upper()):
                body_text = safe_ocr(page1, config=config, timeout_sec=args.ocrTimeoutSec)

            merged_text = "\n".join([margin_text, header_text, body_text]).strip()
            merged_text = merged_text or ""

            book, page = extract_book_page(merged_text)
            rec_date = extract_recorded_date(merged_text)

            # Address candidates: prefer town/city if visible in margin/body (light heuristic)
            prefer_city = None
            if "BOSTON" in merged_text.upper():
                prefer_city = "BOSTON"
            elif "REVERE" in merged_text.upper():
                prefer_city = "REVERE"

            candidates = find_address_candidates(merged_text, prefer_city=prefer_city)

            # Choose best candidate
            best = None
            if candidates:
                best = max(candidates, key=lambda c: c.get("score", -999))

            flags = []
            raw_addr = None
            norm_addr = None
            confidence = None
            if best and best.get("score", 0) >= 25:
                raw_addr = best["value"]
                norm_addr = normalize_address(raw_addr)
                confidence = best.get("strength")
            else:
                flags.append("NO_PROPERTY_ADDRESS")
                miss_no_addr += 1

            # ambiguous if we have multiple similar high candidates
            if candidates:
                top_scores = sorted([c["score"] for c in candidates], reverse=True)[:3]
                if len(top_scores) >= 2 and top_scores[0] - top_scores[1] <= 4 and top_scores[0] >= 18:
                    flags.append("AMBIGUOUS_ADDRESS")
                    miss_ambig += 1

            if not book or not page:
                flags.append("NO_BOOK_PAGE")
                miss_no_bkpg += 1
            if not rec_date:
                flags.append("NO_RECORDED_DATE")
                miss_no_date += 1

            # property type inference from merged text
            prop_type = infer_property_type(merged_text)

            doc = {
                "doc_type": "DEED",
                "source": {
                    "file": pdf.name,
                    "rel_path": str(pdf).replace("\\", "/").split("backend/")[-1],
                    "sha256": sha,
                    "extract_method": "ocr_margin_first_v7",
                    "ocr_page_limit": args.pageLimit,
                    "ocr_dpi": args.dpi,
                    "tesseract_config": config,
                    "margin_rotation": margin.get("rotation"),
                    "ocr_text_total_len": len(merged_text),
                    "ocr_text_fingerprint": hashlib.sha1(merged_text.encode("utf-8", errors="ignore")).hexdigest(),
                },
                "extracted": {
                    "recorded_date": rec_date,
                    "instrument_type": "DEED",
                    "book": book,
                    "page": page,
                    "doc_id": None,
                    "consideration": None,
                    "grantors": [],
                    "grantees": [],
                    "property_address_raw": raw_addr,
                    "property_address_norm": norm_addr,
                    "property_type_hint": prop_type,
                    "confidence": confidence,
                    "flags": flags,
                    "address_candidates": candidates[:12],
                },
            }

            fout.write(json.dumps(doc, ensure_ascii=False) + "\n")
            extracted_ok += 1
            processed_sha.add(sha)

    dt = time.time() - t0
    pdfs_seen = extracted_ok + already_done
    missing_rates = {
        "NO_PROPERTY_ADDRESS": round(miss_no_addr / max(1, extracted_ok), 4),
        "AMBIGUOUS_ADDRESS": round(miss_ambig / max(1, extracted_ok), 4),
        "NO_BOOK_PAGE": round(miss_no_bkpg / max(1, extracted_ok), 4),
        "NO_RECORDED_DATE": round(miss_no_date / max(1, extracted_ok), 4),
    }
    audit = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "raw_dir": str(raw_dir),
        "pdfs_total": pdfs_total,
        "pdfs_seen": pdfs_seen,
        "extracted_ok": extracted_ok,
        "errors": errors,
        "already_done": already_done,
        "ocr_page_limit": args.pageLimit,
        "ocr_dpi": args.dpi,
        "tesseract_config": config,
        "ocr_timeout_sec": args.ocrTimeoutSec,
        "missing_rates": missing_rates,
        "runtime_sec": round(dt, 1),
    }
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:")
    print(f"  extracted: {out_path}")
    print(f"  audit:     {audit_path}")


if __name__ == "__main__":
    main()
