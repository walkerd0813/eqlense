#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hampden RG370RP (Recorded Land) Index-PDF extractor for DOC TYPES = MTG.

Bible rules enforced:
- bounded zone extraction (no full-page text parsing)
- reconstruct rows via y-band clustering of word fragments
- prefer embedded text; OCR only when embedded text quality is very low
- never mix embedded+OCR within the same page
- multi-property safe: emit event.property_refs[] (not a single property_ref)
"""

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

DATE_ROW_RE = re.compile(
    r"^(?P<mdy>\d{2}-\d{2}-\d{4})\s+(?P<hms>\d{1,2}:\d{2}:\d{2}[ap])\s+"
    r"(?P<book>\d{3,6})\s+(?P<page>\d{1,6})\s+(?P<inst>\d{1,8})\b"
)
TOWN_ADDR_RE = re.compile(r"Town:\s*([A-Z \-\'&]+)\s+Addr:\s*(.+)$", re.I)
PARTY_RE = re.compile(r"^(?P<side>[12])\s+(?P<pc>[PC])\s+(?P<rest>.+)$")
CONSID_RE = re.compile(r"\bMTG\b\s+([0-9][0-9,]*\.[0-9]{2})\b")
DOC_TYPE_CODE_EXPECTED = "MTG"

def now_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "__hampden__indexpdf_v1_6"

def sha256_file(path: str, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def norm_ws(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip()

def norm_town(s: Optional[str]) -> Optional[str]:
    s = norm_ws(s) if s else None
    return s.upper() if s else None

def norm_addr(s: Optional[str]) -> Optional[str]:
    s = norm_ws(s) if s else None
    return s.upper() if s else None

def parse_money(m: Optional[str]) -> Optional[int]:
    if not m:
        return None
    try:
        return int(round(float(m.replace(",", ""))))
    except Exception:
        return None

def parse_date_mdy(mdy: str) -> Optional[str]:
    try:
        return datetime.strptime(mdy, "%m-%d-%Y").strftime("%Y-%m-%d")
    except Exception:
        return None

def classify_registry_office_page1(pdf: pdfplumber.PDF) -> Optional[str]:
    try:
        t = (pdf.pages[0].extract_text() or "").upper()
        if "RECORDED LAND" in t:
            return "RECORDED_LAND"
        if "LAND REGISTRATION" in t or "REGISTERED LAND" in t:
            return "REGISTERED_LAND"
    except Exception:
        pass
    return None

def extract_doc_type_code_page1(pdf: pdfplumber.PDF) -> Optional[str]:
    try:
        page = pdf.pages[0]
        w, h = page.width, page.height
        bbox = (0, 0, w * 0.60, h * 0.25)
        txt = page.within_bbox(bbox).extract_text() or ""
        m = re.search(r"DOC\s+TYPES\.*\s*([A-Z]{2,6})", txt.upper())
        return m.group(1).strip() if m else None
    except Exception:
        return None

@dataclass
class Word:
    text: str
    x0: float
    x1: float
    top: float
    bottom: float

def words_in_bbox_embedded(page: pdfplumber.page.Page, bbox: Tuple[float, float, float, float]) -> List[Word]:
    ws = page.within_bbox(bbox).extract_words(
        use_text_flow=True,
        keep_blank_chars=False,
        extra_attrs=["x0", "x1", "top", "bottom"],
    )
    out: List[Word] = []
    for w in ws:
        t = (w.get("text") or "").strip()
        if not t:
            continue
        out.append(Word(text=t, x0=float(w["x0"]), x1=float(w["x1"]), top=float(w["top"]), bottom=float(w["bottom"])))
    return out

def embedded_quality_score(words: List[Word]) -> float:
    if not words:
        return 0.0
    sample = words[:800]
    good = 0
    alpha = 0
    lens = []
    for w in sample:
        lens.append(len(w.text))
        alnum = sum(ch.isalnum() for ch in w.text)
        if alnum >= 2:
            good += 1
        if any(ch.isalpha() for ch in w.text):
            alpha += 1
    frac_good = good / max(1, len(sample))
    frac_alpha = alpha / max(1, len(sample))
    avg_len = sum(lens) / max(1, len(lens))
    score = 0.55 * frac_good + 0.35 * frac_alpha + 0.10 * min(1.0, avg_len / 3.0)
    return max(0.0, min(1.0, score))

def words_in_bbox_ocr(page: pdfplumber.page.Page, bbox: Tuple[float, float, float, float]) -> List[Word]:
    try:
        import pytesseract  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "OCR required but pytesseract not available. Install Tesseract (Windows) + add to PATH, then: python -m pip install pytesseract. "
            f"Import error: {e}"
        )
    im = page.to_image(resolution=300).original  # PIL
    x0, y0, x1, y1 = bbox
    Wp, Hp = page.width, page.height
    Wi, Hi = im.size
    px0 = int(max(0, min(Wi, (x0 / Wp) * Wi)))
    px1 = int(max(0, min(Wi, (x1 / Wp) * Wi)))
    py0 = int(max(0, min(Hi, (y0 / Hp) * Hi)))
    py1 = int(max(0, min(Hi, (y1 / Hp) * Hi)))
    crop = im.crop((px0, py0, px1, py1))

    data = pytesseract.image_to_data(crop, output_type=pytesseract.Output.DICT, config="--psm 6")
    out: List[Word] = []
    n = len(data.get("text", []))
    for i in range(n):
        t = (data["text"][i] or "").strip()
        if not t:
            continue
        try:
            conf = float(data.get("conf", ["-1"])[i])
        except Exception:
            conf = -1
        if conf < 40:
            continue
        left = float(data["left"][i]); top = float(data["top"][i])
        width = float(data["width"][i]); height = float(data["height"][i])
        x0p = x0 + (left / max(1.0, (px1 - px0))) * (x1 - x0)
        x1p = x0 + ((left + width) / max(1.0, (px1 - px0))) * (x1 - x0)
        top_p = y0 + (top / max(1.0, (py1 - py0))) * (y1 - y0)
        bottom_p = y0 + ((top + height) / max(1.0, (py1 - py0))) * (y1 - y0)
        out.append(Word(text=t, x0=x0p, x1=x1p, top=top_p, bottom=bottom_p))
    return out

def cluster_rows(words: List[Word], y_tol: float = 2.8) -> List[List[Word]]:
    if not words:
        return []
    words_sorted = sorted(words, key=lambda w: (w.top, w.x0))
    rows: List[List[Word]] = []
    cur: List[Word] = [words_sorted[0]]
    y0 = words_sorted[0].top
    for w in words_sorted[1:]:
        if abs(w.top - y0) <= y_tol:
            cur.append(w)
        else:
            rows.append(sorted(cur, key=lambda ww: ww.x0))
            cur = [w]
            y0 = w.top
    rows.append(sorted(cur, key=lambda ww: ww.x0))
    return rows

def row_text(row: List[Word]) -> str:
    return norm_ws(" ".join(w.text for w in row if w.text)) or ""

def make_event_id(county: str, book: str, page: str, inst: str, seq: int) -> str:
    base = f"MA|registry|indexpdf|{county}|{book}|{page}|{inst}|{seq}"
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()
    return f"MA|registry|indexpdf|{county}|{h}"

def new_event(county: str, registry_office: Optional[str], doc_type_code: Optional[str],
              run_id: str, pdf_path: str, pdf_hash: str, recording: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema": {"name": "equitylens.registry_event", "version": "mim_v1_0"},
        "event_id": None,
        "event_type": "MORTGAGE",
        "county": county,
        "registry_system": "registry_index_pdf",
        "registry_office": registry_office,
        "doc_type_code": doc_type_code,
        "doc_type_desc": "MTG",
        "source": {
            "run_id": run_id,
            "dataset_hash": pdf_hash,
            "as_of_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "uri": "file://" + os.path.abspath(pdf_path),
        },
        "recording": recording,
        "parties": [],
        "property_refs": [],
        "consideration": {"raw_text": None, "amount": None, "parse_status": "MISSING", "flags": [], "source": "INDEXPDF"},
        "attach": {"status": "UNKNOWN", "attach_scope": "MULTI", "flags": []},
        "meta": {"property_ref_count": 0, "has_multiple_properties": False},
        "mortgage": None,
        "deed": None,
        "assignment": None,
        "satisfaction": None,
        "lien": None,
        "lis_pendens": None,
        "foreclosure": None,
        "arms_length": None,
    }

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--run-id", default="")
    ap.add_argument("--county", default="hampden")
    ap.add_argument("--page-start", type=int, default=0)
    ap.add_argument("--page-end", type=int, default=-1)  # inclusive
    ap.add_argument("--progress-every", type=int, default=25)
    ap.add_argument("--debug-lines", type=int, default=0)
    args = ap.parse_args()

    run_id = args.run_id.strip() or now_run_id()
    county = args.county.strip().lower()
    pdf_hash = sha256_file(args.pdf)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    stats: Dict[str, Any] = {
        "run_id": run_id,
        "pdf": args.pdf,
        "pdf_sha256": pdf_hash,
        "events_written": 0,
        "pages_total": 0,
        "pages_processed": 0,
        "registry_office": None,
        "doc_type_code": None,
        "warnings": [],
        "mode_per_page": {},
        "date_rows_seen": 0,
        "town_addr_seen": 0,
    }

    events: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    with pdfplumber.open(args.pdf) as pdf:
        stats["pages_total"] = len(pdf.pages)
        stats["registry_office"] = classify_registry_office_page1(pdf)
        stats["doc_type_code"] = extract_doc_type_code_page1(pdf) or None

        if stats["doc_type_code"] and stats["doc_type_code"] != DOC_TYPE_CODE_EXPECTED:
            stats["warnings"].append(f"DOC_TYPE_CODE_PAGE1_IS_{stats['doc_type_code']}_EXPECTED_{DOC_TYPE_CODE_EXPECTED}")
        if not stats["doc_type_code"]:
            stats["warnings"].append("DOC_TYPE_CODE_NOT_FOUND_PAGE1_HEADER")

        page_start = max(0, args.page_start)
        page_end = args.page_end if args.page_end >= 0 else (len(pdf.pages) - 1)
        page_end = min(page_end, len(pdf.pages) - 1)
        page_start = min(page_start, page_end)

        for pi in range(page_start, page_end + 1):
            page = pdf.pages[pi]
            w, h = page.width, page.height
            body_bbox = (0, h * 0.20, w, h * 0.94)

            embedded = words_in_bbox_embedded(page, body_bbox)
            q = embedded_quality_score(embedded)
            use_ocr = q < 0.20

            try:
                if use_ocr:
                    words = words_in_bbox_ocr(page, body_bbox)
                    stats["mode_per_page"][str(pi)] = {"mode": "ocr", "embedded_quality": q, "words": len(words)}
                else:
                    words = embedded
                    stats["mode_per_page"][str(pi)] = {"mode": "embedded", "embedded_quality": q, "words": len(words)}
            except Exception as e:
                stats["mode_per_page"][str(pi)] = {"mode": "ocr_failed", "embedded_quality": q, "error": str(e)}
                stats["warnings"].append(f"OCR_FAILED_PAGE_{pi}: {e}")
                words = embedded

            rows = cluster_rows(words, y_tol=2.8)

            if args.debug_lines and pi == page_start:
                stats["debug_page0_rows"] = [row_text(r) for r in rows[:args.debug_lines]]

            for row in rows:
                txt = row_text(row)
                if not txt:
                    continue

                m = DATE_ROW_RE.match(txt)
                if m:
                    stats["date_rows_seen"] += 1
                    if cur is not None:
                        cur["meta"]["property_ref_count"] = len(cur["property_refs"])
                        cur["meta"]["has_multiple_properties"] = len(cur["property_refs"]) > 1
                        events.append(cur)
                        cur = None

                    mdy = m.group("mdy"); hms = m.group("hms")
                    book = m.group("book"); page_no = m.group("page"); inst = m.group("inst")
                    recording = {
                        "recorded_at_raw": f"{mdy} {hms}",
                        "recording_date_raw": mdy,
                        "recording_date": parse_date_mdy(mdy),
                        "recording_time": hms,
                        "book": book,
                        "page": page_no,
                        "instrument_number_raw": inst,
                        "seq": 1,
                    }
                    cur = new_event(county, stats["registry_office"], stats["doc_type_code"], run_id, args.pdf, pdf_hash, recording)
                    cur["event_id"] = make_event_id(county, book, page_no, inst, 1)
                    continue

                if cur is None:
                    continue

                if " MTG " in f" {txt} ":
                    cm = CONSID_RE.search(txt)
                    if cm:
                        raw = cm.group(1)
                        cur["consideration"]["raw_text"] = raw
                        cur["consideration"]["amount"] = parse_money(raw)
                        cur["consideration"]["parse_status"] = "PARSED"
                        if cur["consideration"]["amount"] in (0, 1):
                            cur["consideration"]["flags"] = ["ZERO_OR_NOMINAL"]
                    continue

                ta = TOWN_ADDR_RE.search(txt)
                if ta:
                    stats["town_addr_seen"] += 1
                    town = norm_town(ta.group(1))
                    addr_raw = norm_ws(ta.group(2))
                    addr_norm = norm_addr(addr_raw)
                    lot_ref_raw = addr_raw if addr_raw and " LOT " in addr_raw.upper() else None
                    cur["property_refs"].append({
                        "ref_index": len(cur["property_refs"]) + 1,
                        "town": town,
                        "addr_raw": addr_raw,
                        "addr_norm": addr_norm,
                        "unit_hint": None,
                        "lot_ref_raw": lot_ref_raw,
                        "ref_anchor_type": "ADDRESS",
                        "attach_status": "UNKNOWN",
                        "attach_confidence": None,
                        "evidence": {"page_index": pi},
                    })
                    continue

                pm = PARTY_RE.match(txt)
                if pm:
                    side = pm.group("side")
                    pc = pm.group("pc").upper()
                    rest = norm_ws(pm.group("rest")) or ""
                    role = "BORROWER" if side == "1" else "LENDER"
                    cur["parties"].append({
                        "role": role,
                        "name_raw": rest,
                        "name_norm": rest.upper(),
                        "entity_type": "PERSON" if pc == "P" else "ORG",
                        "mailing_address_raw": None,
                    })
                    continue

            stats["pages_processed"] += 1
            if args.progress_every and ((pi - page_start + 1) % args.progress_every == 0):
                print(f"[progress] page {pi - page_start + 1}/{page_end - page_start + 1} events_so_far={len(events)}")

        if cur is not None:
            cur["meta"]["property_ref_count"] = len(cur["property_refs"])
            cur["meta"]["has_multiple_properties"] = len(cur["property_refs"]) > 1
            events.append(cur)

    with open(args.out, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "
")
    stats["events_written"] = len(events)

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"[done] wrote {len(events)} events")
    print(f"[audit] {args.audit}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
