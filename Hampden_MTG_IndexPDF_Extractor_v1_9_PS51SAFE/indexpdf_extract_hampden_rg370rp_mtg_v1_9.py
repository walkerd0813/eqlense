#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hampden Registry Index-PDF Extractor (Recorded Land / Land Court) — MTG flavor v1_9.

Goal: stop fighting PowerShell patching. Ship a clean Python extractor + a minimal PS runner.
Always writes audit + ndjson, even if 0 events.

Usage:
  python indexpdf_extract_hampden_rg370rp_mtg_v1_9.py --pdf IN.pdf --out OUT.ndjson --audit AUD.json
    [--page-start N --page-end M] [--run-id RUN] [--progress-every 25]
    [--debug-rows 0] [--debug-words 0] [--scan-pages K]

"""
import argparse
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

MONEY_RE = re.compile(r"(?P<amt>\d{1,3}(?:,\d{3})*(?:\.\d{2})?)")
DATE_LINE_RE = re.compile(
    r"^(?P<mdy>\d{2}[-/]\d{2}[-/]\d{4})\s+(?P<hms>\d{1,2}:\d{2}:\d{2}[apAP]?)\s+(?P<inst>\d{2,10})\s+(?P<book>\d{1,10})\s+(?P<page>\d{1,10})\b"
)
DATE_WEAK_RE = re.compile(r"^(?P<mdy>\d{2}[-/]\d{2}[-/]\d{4})\s+(?P<hms>\d{1,2}:\d{2}:\d{2}[apAP]?)\b")
TOWN_ADDR_RE = re.compile(r"Town:\s*(?P<town>[A-Z0-9 \-'\.&]+)\s+Addr:\s*(?P<addr>.+)$", re.I)
PARTY_RE = re.compile(r"^(?P<idx>\d+)\s+(?P<etype>[PC])\s+(?P<surname>[A-Z0-9'\-\.&/ ]+?)(?:\s{2,}|\s+)(?P<given>[A-Z0-9'\-\.&/ ]+)?$", re.I)

def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def now_run_id() -> str:
    return f"{utc_now_compact()}__hampden__indexpdf_v1_9"

def sha256_file(path: str) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_mkdir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)

def extract_doc_type_code_from_page1(pdf: pdfplumber.PDF) -> Optional[str]:
    try:
        page = pdf.pages[0]
    except Exception:
        return None
    txt = page.extract_text() or ""
    m = re.search(r"DOC\s+TYPES\.*\s*([A-Z0-9]{2,6})\b", txt)
    if m:
        return m.group(1).strip().upper()
    if "\nMTG" in txt or " MTG" in txt:
        return "MTG"
    return None

def classify_registry_office(pdf: pdfplumber.PDF) -> str:
    try:
        txt = (pdf.pages[0].extract_text() or "").upper()
    except Exception:
        return "UNKNOWN"
    if "LAND REGISTRATION" in txt or "REGISTERED LAND DOCUMENTS" in txt or "LAND COURT" in txt:
        return "LAND_COURT"
    if "RECORDED LAND" in txt:
        return "RECORDED_LAND"
    return "UNKNOWN"

def words_in_bbox(page: pdfplumber.page.Page, bbox: Optional[Tuple[float, float, float, float]]) -> List[Dict[str, Any]]:
    p = page
    if bbox is not None:
        try:
            p = page.crop(bbox)
        except Exception:
            try:
                p = page.within_bbox(bbox)
            except Exception:
                p = page
    words = p.extract_words(x_tolerance=1, y_tolerance=2, keep_blank_chars=False, use_text_flow=True)
    out = []
    for w in words:
        if "text" not in w:
            continue
        out.append({"text": w["text"], "x0": float(w.get("x0", 0.0)), "top": float(w.get("top", 0.0))})
    return out

def cluster_rows(words: List[Dict[str, Any]], y_tol: float = 2.6) -> List[List[Dict[str, Any]]]:
    if not words:
        return []
    words_sorted = sorted(words, key=lambda w: (w["top"], w["x0"]))
    rows: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    cur_y = None
    for w in words_sorted:
        y = w["top"]
        if cur_y is None:
            cur_y = y
            cur = [w]
            continue
        if abs(y - cur_y) <= y_tol:
            cur.append(w)
        else:
            rows.append(sorted(cur, key=lambda t: t["x0"]))
            cur = [w]
            cur_y = y
    if cur:
        rows.append(sorted(cur, key=lambda t: t["x0"]))
    return rows

def row_text(row: List[Dict[str, Any]]) -> str:
    txt = " ".join([t["text"] for t in row]).strip()
    return re.sub(r"\s+", " ", txt)

def parse_money_last(s: str) -> Optional[float]:
    m = None
    for m in MONEY_RE.finditer(s):
        pass
    if not m:
        return None
    raw = m.group("amt")
    try:
        return float(raw.replace(",", ""))
    except Exception:
        return None

def normalize_mdy(mdy: str) -> Optional[str]:
    mdy = mdy.strip()
    for fmt in ("%m-%d-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(mdy, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

def build_event_id(county: str, registry_office: str, doc_type_code: Optional[str], recording: Dict[str, Any]) -> str:
    parts = [
        "MA","registry","indexpdf",county.lower(),
        (registry_office or "UNKNOWN").lower(),
        (doc_type_code or "UNK").lower(),
        (recording.get("recording_date") or "unknown"),
        (recording.get("instrument_number_raw") or recording.get("document_number_raw") or "noid"),
        (recording.get("book") or "nobook"),
        (recording.get("page") or "nopage"),
    ]
    return "|".join(parts)

def new_base_event(county: str, registry_office: str, doc_type_code: Optional[str], run_id: str, pdf_path: str) -> Dict[str, Any]:
    return {
        "schema": {"name": "equitylens.registry_event", "version": "indexpdf_v1_9"},
        "event_id": None,
        "event_type": "MORTGAGE",
        "county": county.lower(),
        "registry_system": "registry_index_pdf",
        "registry_office": registry_office,
        "doc_type_code": doc_type_code,
        "doc_type_desc": "MORTGAGE" if (doc_type_code or "").upper() in ("MTG","MORT") else None,
        "source": {"run_id": run_id, "as_of_date": datetime.now().strftime("%Y-%m-%d"), "uri": f"file://{pdf_path}", "dataset_hash": sha256_file(pdf_path)},
        "recording": {},
        "consideration": {"raw_text": None, "amount": None, "parse_status": "UNKNOWN", "flags": [], "source": "INDEXPDF"},
        "property_refs": [],
        "parties": [],
        "meta": {"page_index": None, "warnings": [], "property_ref_count": 0, "has_multiple_properties": False},
        "attach": {"status": "UNKNOWN", "property_id": None, "method": None, "confidence": None,
                   "evidence": {"match_keys_used": [], "matched_address_norm": None, "matched_town_norm": None, "distance_m": None},
                   "flags": [], "attach_scope": "MULTI"},
    }

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--run-id", default="")
    ap.add_argument("--county", default="hampden")
    ap.add_argument("--page-start", type=int, default=0)
    ap.add_argument("--page-end", type=int, default=-1)
    ap.add_argument("--progress-every", type=int, default=25)
    ap.add_argument("--debug-rows", type=int, default=0)
    ap.add_argument("--debug-words", type=int, default=0)
    ap.add_argument("--scan-pages", type=int, default=0)
    args = ap.parse_args()

    run_id = (args.run_id or "").strip() or now_run_id()
    county = (args.county or "hampden").strip().lower()
    pdf_path = args.pdf

    safe_mkdir(os.path.dirname(args.out))
    safe_mkdir(os.path.dirname(args.audit))

    stats: Dict[str, Any] = {
        "run_id": run_id, "pdf": pdf_path, "pdf_sha256": None,
        "pages": 0, "page_start": args.page_start, "page_end": args.page_end,
        "events_written": 0, "doc_type_code": None, "registry_office": None,
        "warnings": [], "signals": [], "exceptions": []
    }

    events: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            stats["pages"] = len(pdf.pages)
            stats["pdf_sha256"] = sha256_file(pdf_path)

            registry_office = classify_registry_office(pdf)
            doc_type_code = extract_doc_type_code_from_page1(pdf)
            stats["registry_office"] = registry_office
            stats["doc_type_code"] = doc_type_code

            if args.scan_pages and args.scan_pages > 0:
                n = min(args.scan_pages, len(pdf.pages))
                date_sig = re.compile(r"\b\d{2}[-/]\d{2}[-/]\d{4}\b")
                for i in range(n):
                    t = pdf.pages[i].extract_text() or ""
                    has_town = "Town:" in t
                    has_addr = "Addr:" in t
                    has_date = bool(date_sig.search(t))
                    if has_town or has_addr or has_date:
                        stats["signals"].append({"page": i, "Town": has_town, "Addr": has_addr, "Date": has_date, "textlen": len(t)})
                print("[scan] pages_with_signals =", len(stats["signals"]))
                for row in stats["signals"][:50]:
                    print("[scan] page", row["page"], "Town", row["Town"], "Addr", row["Addr"], "Date", row["Date"], "textlen", row["textlen"])
                with open(args.audit, "w", encoding="utf-8") as f:
                    json.dump(stats, f, ensure_ascii=False, indent=2)
                return

            page_start = max(0, args.page_start)
            page_end = args.page_end if args.page_end >= 0 else (len(pdf.pages) - 1)
            page_end = min(page_end, len(pdf.pages) - 1)
            if page_end < page_start:
                raise ValueError(f"page_end < page_start ({page_end} < {page_start})")

            for pi in range(page_start, page_end + 1):
                page = pdf.pages[pi]
                w, h = page.width, page.height
                body_bbox = (0, h * 0.18, w, h * 0.94)

                words = words_in_bbox(page, body_bbox)

                if args.debug_words and pi == page_start:
                    print(f"[debug] page_index={pi} words={len(words)} sample={min(len(words), args.debug_words)}")
                    for j, ww in enumerate(words[: args.debug_words]):
                        print("  ", j, ww["text"], "x0=", round(ww["x0"], 1), "top=", round(ww["top"], 1))

                rows = cluster_rows(words, y_tol=2.6)

                if args.debug_rows and pi == page_start:
                    print(f"[debug] page_index={pi} rows={len(rows)} sample={min(len(rows), args.debug_rows)}")
                    for ri, row in enumerate(rows[: args.debug_rows]):
                        print(f"  {ri:03d} {row_text(row)}")

                for row in rows:
                    txt = row_text(row)
                    if not txt:
                        continue

                    m = DATE_LINE_RE.match(txt)
                    if m:
                        if cur is not None:
                            cur["meta"]["property_ref_count"] = len(cur["property_refs"])
                            cur["meta"]["has_multiple_properties"] = len(cur["property_refs"]) > 1
                            cur["attach"]["attach_scope"] = "SINGLE" if len(cur["property_refs"]) == 1 else "MULTI"
                            events.append(cur)

                        recording_date_raw = m.group("mdy")
                        time_raw = m.group("hms")
                        recording_date = normalize_mdy(recording_date_raw)

                        rec = {
                            "recorded_at_raw": f"{recording_date_raw} {time_raw}",
                            "recording_date_raw": recording_date_raw,
                            "recording_date": recording_date,
                            "recording_time_raw": time_raw,
                            "recording_time": time_raw,
                            "book": m.group("book"),
                            "page": m.group("page"),
                            "instrument_number_raw": m.group("inst"),
                            "document_number_raw": None,
                            "seq": None,
                        }

                        cur = new_base_event(county, registry_office, doc_type_code, run_id, pdf_path)
                        cur["recording"] = rec
                        cur["meta"]["page_index"] = pi

                        amt = parse_money_last(txt)
                        if amt is not None:
                            cur["consideration"]["raw_text"] = str(amt)
                            cur["consideration"]["amount"] = amt
                            cur["consideration"]["parse_status"] = "PARSED"
                            if amt == 0:
                                cur["consideration"]["flags"].append("ZERO_OR_NOMINAL")

                        cur["event_id"] = build_event_id(county, registry_office, doc_type_code, rec)
                        continue

                    m2 = DATE_WEAK_RE.match(txt)
                    if m2 and cur is None:
                        recording_date_raw = m2.group("mdy")
                        time_raw = m2.group("hms")
                        recording_date = normalize_mdy(recording_date_raw)
                        rec = {
                            "recorded_at_raw": f"{recording_date_raw} {time_raw}",
                            "recording_date_raw": recording_date_raw,
                            "recording_date": recording_date,
                            "recording_time_raw": time_raw,
                            "recording_time": time_raw,
                            "book": None, "page": None,
                            "instrument_number_raw": None, "document_number_raw": None,
                            "seq": None,
                        }
                        cur = new_base_event(county, registry_office, doc_type_code, run_id, pdf_path)
                        cur["recording"] = rec
                        cur["meta"]["page_index"] = pi
                        cur["meta"]["warnings"].append("WEAK_START_MISSING_IDS")
                        cur["event_id"] = build_event_id(county, registry_office, doc_type_code, rec)
                        continue

                    if cur is None:
                        continue

                    tm = TOWN_ADDR_RE.search(txt)
                    if tm:
                        cur["property_refs"].append({"town_raw": tm.group("town").strip().upper(), "address_raw": tm.group("addr").strip(),
                                                    "unit_raw": None, "state": "MA", "zip_raw": None,
                                                    "legal_desc_raw": None, "lot_raw": None})
                        continue

                    pm = PARTY_RE.match(txt)
                    if pm:
                        idx = int(pm.group("idx"))
                        etype = pm.group("etype").upper()
                        surname = (pm.group("surname") or "").strip()
                        given = (pm.group("given") or "").strip() if pm.group("given") else None
                        name_raw = surname if not given else f"{surname} {given}"
                        cur["parties"].append({"role": "BORROWER" if etype == "P" else "LENDER",
                                               "name_raw": name_raw, "name_norm": name_raw.upper(),
                                               "entity_type": "PERSON" if etype == "P" else "ORG",
                                               "mailing_address_raw": None, "idx": idx, "etype": etype})
                        continue

                if args.progress_every and ((pi - page_start + 1) % args.progress_every == 0):
                    print(f"[progress] page {pi - page_start + 1}/{page_end - page_start + 1} events_so_far={len(events)}")

            if cur is not None:
                cur["meta"]["property_ref_count"] = len(cur["property_refs"])
                cur["meta"]["has_multiple_properties"] = len(cur["property_refs"]) > 1
                cur["attach"]["attach_scope"] = "SINGLE" if len(cur["property_refs"]) == 1 else "MULTI"
                events.append(cur)

    except Exception as e:
        stats["exceptions"].append({"type": type(e).__name__, "msg": str(e)})

    try:
        with open(args.out, "w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        stats["events_written"] = len(events)
    except Exception as e:
        stats["exceptions"].append({"type": type(e).__name__, "msg": f"WRITE_OUT_FAILED: {e}"})

    try:
        with open(args.audit, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    print(f"[done] wrote {len(events)} events")
    if stats["exceptions"]:
        print("[warn] exceptions:", stats["exceptions"])

if __name__ == "__main__":
    main()
