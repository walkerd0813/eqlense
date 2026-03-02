#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hampden Registry of Deeds - Recorded Land "RECORDED LAND BY RECORDING DATE" (RG370RP) - MTG
Index-PDF extractor (Bible-compliant: bounded extraction; row reconstruction; no full-page naive splitlines).

Key improvement vs earlier versions:
- Use pdfplumber.extract_words (not extract_text tokens) + y-band clustering to rebuild visual rows
- More tolerant DATE row detection (date/time may be split across tokens)
- Debug modes: prints row samples and word diagnostics when requested

NOTE: This is an *index PDF* extractor. It produces 1 event per recorded instrument line.
Property refs are derived from subsequent Town/Addr lines and other row fragments.
"""

import argparse
import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

# -------------------------
# Regexes (tolerant)
# -------------------------

# Date/time at beginning of a record row, tolerant of spaces: 01-21-2021  1:17:39p
DATE_TIME_RE = re.compile(
    r'^(?P<mdy>\d{2}-\d{2}-\d{4})\s+(?P<hms>\d{1,2}:\d{2}:\d{2}[ap])\b',
    re.I
)

# Book page and instrument number usually appear on same row but can be missing on first row of page.
# Example: 23663  161   4605
BOOK_PAGE_INST_RE = re.compile(
    r'\b(?P<book>\d{3,6})\s+(?P<page>\d{1,5})\s+(?P<inst>\d{1,6})\b'
)

DOC_TYPE_TOKEN_RE = re.compile(r'\bMTG\b', re.I)

TOWN_ADDR_RE = re.compile(r'\bTown:\s*(?P<town>[A-Z \-\'&]+)\s+Addr:\s*(?P<addr>.+)$', re.I)
ADDR_ONLY_RE = re.compile(r'^\s*Addr:\s*(?P<addr>.+)$', re.I)
TOWN_ONLY_RE = re.compile(r'^\s*Town:\s*(?P<town>[A-Z \-\'&]+)\s*$', re.I)

# Party lines in this format:
# 1 P STEVENS PAUL R
# 2 C TD BANK
PARTY_RE = re.compile(r'^(?P<idx>\d+)\s+(?P<entity>[PC])\s+(?P<name>.+)$', re.I)

# Consideration money patterns: 161,250.00
MONEY_RE = re.compile(r'\b(\d{1,3}(?:,\d{3})+|\d+)\.\d{2}\b')

def now_run_id(prefix: str="hampden__indexpdf_v1_7") -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "__" + prefix

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def norm_ws(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip()

def norm_upper(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return norm_ws(s).upper()

def parse_money(s: str) -> Optional[float]:
    m = MONEY_RE.search(s)
    if not m:
        return None
    t = m.group(0).replace(",", "")
    try:
        return float(t)
    except Exception:
        return None

def classify_registry_office_from_page1(page1_text: str) -> str:
    t = (page1_text or "").upper()
    if "RECORDED LAND" in t:
        return "RECORDED_LAND"
    if "LAND REGISTRATION" in t or "REGISTERED LAND" in t or "LAND COURT" in t:
        return "REGISTERED_LAND"
    return "UNKNOWN"

def extract_doc_type_code_from_page1(page1_text: str) -> Optional[str]:
    # Bible rule: doc type comes from page 1 header area (we approximate via full page1 text scan)
    t = (page1_text or "").upper()
    # For MTG packets this should be "DOC TYPES.............MTG"
    m = re.search(r'DOC\s+TYPES\.*\s*([A-Z]{2,5})', t)
    if m:
        return m.group(1).strip()
    # fallback: MTG token exists
    if " MTG" in t or "\nMTG" in t:
        return "MTG"
    return None

# -------------------------
# Row reconstruction
# -------------------------

def extract_words_in_body(page, body_bbox: Tuple[float,float,float,float]) -> List[Dict[str, Any]]:
    """
    Use extract_words so we get word boxes with x0,y0,x1,y1,text.
    """
    words = page.extract_words(
        x_tolerance=1.5,
        y_tolerance=2.0,
        keep_blank_chars=False,
        use_text_flow=True
    )
    x0, y0, x1, y1 = body_bbox
    out = []
    for w in words:
        # pdfplumber uses top-origin coords; y increases down
        if (w["x0"] >= x0 and w["x1"] <= x1 and w["top"] >= y0 and w["bottom"] <= y1):
            txt = w.get("text","")
            if txt:
                out.append(w)
    return out

def cluster_words_to_rows(words: List[Dict[str, Any]], y_tol: float = 2.5) -> List[List[Dict[str, Any]]]:
    """
    Cluster by visual row using y coordinate (top).
    """
    if not words:
        return []
    # sort by top then x0
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
            rows.append(sorted(cur, key=lambda z: z["x0"]))
            cur = [w]
            cur_y = y
    if cur:
        rows.append(sorted(cur, key=lambda z: z["x0"]))
    return rows

def row_text(row: List[Dict[str, Any]]) -> str:
    # join with spaces, but collapse repeated hyphen blocks
    t = " ".join([w["text"] for w in row if w.get("text")])
    t = norm_ws(t) or ""
    return t

def rows_to_text(rows: List[List[Dict[str, Any]]]) -> List[str]:
    out = []
    for r in rows:
        t = row_text(r)
        if t:
            out.append(t)
    return out

# -------------------------
# Event building
# -------------------------

def new_event_base(run_id: str, county: str, registry_office: str, doc_type_code: Optional[str]) -> Dict[str, Any]:
    return {
        "schema": {"name": "equitylens.registry_event", "version": "mim_v1_0"},
        "event_id": None,  # set after we have stable parts
        "event_type": "MORTGAGE",  # MTG
        "county": county,
        "registry_system": "registry_index_pdf",
        "registry_office": registry_office,
        "doc_type_code": doc_type_code,
        "doc_type_desc": "MTG",
        "source": {
            "run_id": run_id,
            "dataset_hash": None,
            "as_of_date": datetime.utcnow().strftime("%Y-%m-%d"),
            "uri": None
        },
        "recording": {
            "recorded_at_raw": None,
            "recording_date": None,
            "recording_time": None,
            "book": None,
            "page": None,
            "instrument_number_raw": None,
            "seq": None,
            "doc_number_raw": None
        },
        "parties": [],
        "property_refs": [],
        "consideration": {
            "raw_text": None,
            "amount": None,
            "parse_status": "MISSING",
            "flags": [],
            "source": "INDEX_PDF"
        },
        "meta": {
            "has_multiple_properties": False,
            "property_ref_count": 0,
            "pdf_row_fingerprint": None
        }
    }

def finalize_event_id(ev: Dict[str, Any]) -> None:
    # Deterministic ID from county + doc-type + date + book/page/inst + row fingerprint
    county = ev.get("county","")
    dt = ev.get("recording",{}).get("recording_date") or ""
    book = ev.get("recording",{}).get("book") or ""
    page = ev.get("recording",{}).get("page") or ""
    inst = ev.get("recording",{}).get("instrument_number_raw") or ev.get("recording",{}).get("doc_number_raw") or ""
    fp = ev.get("meta",{}).get("pdf_row_fingerprint") or ""
    key = f"MA|registry|indexpdf|{county}|MTG|{dt}|{book}|{page}|{inst}|{fp}"
    ev["event_id"] = "MA|registry|indexpdf|" + county + "|" + sha256_text(key)[:24]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--run-id", default="")
    ap.add_argument("--county", default="hampden")
    ap.add_argument("--page-start", type=int, default=0)
    ap.add_argument("--page-end", type=int, default=-1)
    ap.add_argument("--progress-every", type=int, default=25)
    ap.add_argument("--debug-rows", type=int, default=0)      # print first N reconstructed rows per page
    ap.add_argument("--debug-words", type=int, default=0)     # print first N raw words per page
    args = ap.parse_args()

    in_pdf = args.pdf
    run_id = args.run_id.strip() or now_run_id()
    county = args.county.strip().lower()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    stats: Dict[str, Any] = {
        "run_id": run_id,
        "pdf": in_pdf,
        "events_written": 0,
        "pages_total": 0,
        "pages_processed": 0,
        "doc_type_code": None,
        "registry_office": None,
        "warnings": [],
        "date_rows_seen": 0,
        "town_addr_lines_seen": 0,
        "events_started": 0,
        "events_finalized": 0,
        "events_missing_bookpageinst": 0
    }

    events: List[Dict[str, Any]] = []

    with pdfplumber.open(in_pdf) as pdf:
        stats["pages_total"] = len(pdf.pages)

        # Page range
        page_start = max(0, args.page_start)
        if args.page_end is None or args.page_end < 0:
            page_end = len(pdf.pages) - 1
        else:
            page_end = min(args.page_end, len(pdf.pages) - 1)

        # classify + doc type from page 1 (index 0)
        page1_text = pdf.pages[0].extract_text() or ""
        registry_office = classify_registry_office_from_page1(page1_text)
        doc_type_code = extract_doc_type_code_from_page1(page1_text)

        stats["registry_office"] = registry_office
        stats["doc_type_code"] = doc_type_code

        if not doc_type_code:
            stats["warnings"].append("DOC_TYPE_CODE_NOT_FOUND_PAGE1")
        if doc_type_code and doc_type_code.upper() != "MTG":
            stats["warnings"].append(f"DOC_TYPE_CODE_UNEXPECTED:{doc_type_code}")

        cur: Optional[Dict[str, Any]] = None
        last_header_bookpageinst: Optional[Tuple[str,str,str]] = None  # carry when first row missing on page

        for pi in range(page_start, page_end + 1):
            page = pdf.pages[pi]
            stats["pages_processed"] += 1
            w, h = page.width, page.height

            # bounded body zone (avoid header/footer). tuned for RG370RP style.
            body_bbox = (0, h * 0.17, w, h * 0.94)

            words = extract_words_in_body(page, body_bbox)

            if args.debug_words:
                print(f"[debug] page_index={pi} words_in_body={len(words)} sample={min(args.debug_words,len(words))}")
                for i, wd in enumerate(sorted(words, key=lambda z: (z["top"], z["x0"]))[:args.debug_words]):
                    print(f"  W{i:03d} top={wd['top']:.1f} x0={wd['x0']:.1f} text={wd.get('text','')!r}")

            rows = cluster_words_to_rows(words, y_tol=2.6)
            lines = rows_to_text(rows)

            if args.debug_rows:
                print(f"[debug] page_index={pi} rows={len(lines)} sample={min(args.debug_rows,len(lines))}")
                for i, ln in enumerate(lines[:args.debug_rows]):
                    print(f"  {i:03d} {ln}")

            # Scan lines
            for ln in lines:
                # Start of a new record is a DATE/TIME row
                mdt = DATE_TIME_RE.match(ln)
                if mdt:
                    stats["date_rows_seen"] += 1

                    # finalize previous event
                    if cur is not None:
                        cur["meta"]["property_ref_count"] = len(cur["property_refs"])
                        cur["meta"]["has_multiple_properties"] = len(cur["property_refs"]) > 1
                        finalize_event_id(cur)
                        events.append(cur)
                        stats["events_finalized"] += 1
                        cur = None

                    cur = new_event_base(run_id, county, registry_office, doc_type_code)

                    recording_date_raw = mdt.group("mdy")
                    time_raw = mdt.group("hms")

                    recording_date = None
                    try:
                        recording_date = datetime.strptime(recording_date_raw, "%m-%d-%Y").strftime("%Y-%m-%d")
                    except Exception:
                        stats["warnings"].append(f"BAD_DATE:{recording_date_raw}")

                    cur["recording"]["recorded_at_raw"] = f"{recording_date_raw} {time_raw}"
                    cur["recording"]["recording_date"] = recording_date
                    cur["recording"]["recording_time"] = time_raw

                    # book/page/inst may be on same row
                    bpi = BOOK_PAGE_INST_RE.search(ln)
                    if bpi:
                        book = bpi.group("book")
                        page_no = bpi.group("page")
                        inst = bpi.group("inst")
                        cur["recording"]["book"] = book
                        cur["recording"]["page"] = page_no
                        cur["recording"]["instrument_number_raw"] = inst
                        last_header_bookpageinst = (book, page_no, inst)
                    else:
                        # carry from previous page footer if we saw it
                        if last_header_bookpageinst:
                            cur["recording"]["book"], cur["recording"]["page"], cur["recording"]["instrument_number_raw"] = last_header_bookpageinst
                        stats["events_missing_bookpageinst"] += 1

                    # fingerprint for debug/audit
                    cur["meta"]["pdf_row_fingerprint"] = sha256_text(ln)[:12]
                    stats["events_started"] += 1

                    # consideration maybe on same row
                    amt = parse_money(ln)
                    if amt is not None:
                        cur["consideration"]["raw_text"] = f"{amt:.2f}"
                        cur["consideration"]["amount"] = amt
                        cur["consideration"]["parse_status"] = "PARSED"
                    continue

                # If we haven't started an event yet, we still want to capture "book/page/inst" in case first entry spills
                if cur is None:
                    bpi2 = BOOK_PAGE_INST_RE.search(ln)
                    if bpi2:
                        last_header_bookpageinst = (bpi2.group("book"), bpi2.group("page"), bpi2.group("inst"))
                    continue

                # within an event: Town/Addr lines create property refs
                mta = TOWN_ADDR_RE.search(ln)
                if mta:
                    stats["town_addr_lines_seen"] += 1
                    town = norm_upper(mta.group("town"))
                    addr = norm_ws(mta.group("addr"))
                    unit_hint = None
                    # quick unit hint if "UNIT X" appears
                    um = re.search(r'\bUNIT\s+([A-Za-z0-9\-]+)\b', addr or "", re.I)
                    if um:
                        unit_hint = um.group(1)
                    cur["property_refs"].append({
                        "ref_index": len(cur["property_refs"]) + 1,
                        "town_raw": town,
                        "address_raw": addr,
                        "address_norm": norm_upper(addr),
                        "unit_hint": unit_hint,
                        "lot_ref_raw": None,
                        "ref_anchor_type": "ADDRESS",
                        "attach_status": "UNKNOWN",
                        "attach_confidence": None
                    })
                    continue

                # Sometimes Town and Addr break across multiple reconstructed rows (rare). Capture town then addr.
                mtown = TOWN_ONLY_RE.match(ln)
                if mtown:
                    cur.setdefault("_pending_town", norm_upper(mtown.group("town")))
                    continue
                maddronly = ADDR_ONLY_RE.match(ln)
                if maddronly and cur.get("_pending_town"):
                    stats["town_addr_lines_seen"] += 1
                    town = cur.pop("_pending_town")
                    addr = norm_ws(maddronly.group("addr"))
                    unit_hint = None
                    um = re.search(r'\bUNIT\s+([A-Za-z0-9\-]+)\b', addr or "", re.I)
                    if um:
                        unit_hint = um.group(1)
                    cur["property_refs"].append({
                        "ref_index": len(cur["property_refs"]) + 1,
                        "town_raw": town,
                        "address_raw": addr,
                        "address_norm": norm_upper(addr),
                        "unit_hint": unit_hint,
                        "lot_ref_raw": None,
                        "ref_anchor_type": "ADDRESS",
                        "attach_status": "UNKNOWN",
                        "attach_confidence": None
                    })
                    continue

                # Party lines: store raw; exact role mapping handled in NORMALIZE stage later
                mp = PARTY_RE.match(ln)
                if mp:
                    ent = mp.group("entity").upper()
                    name = norm_ws(mp.group("name"))
                    entity_type = "PERSON" if ent == "P" else "ORG"
                    cur["parties"].append({
                        "role": "UNKNOWN",
                        "name_raw": name,
                        "name_norm": norm_upper(name),
                        "entity_type": entity_type,
                        "mailing_address_raw": None
                    })
                    continue

                # Consideration line might appear as standalone money on a later row: store if missing
                if cur["consideration"]["amount"] is None:
                    amt2 = parse_money(ln)
                    if amt2 is not None and DOC_TYPE_TOKEN_RE.search(ln):
                        cur["consideration"]["raw_text"] = f"{amt2:.2f}"
                        cur["consideration"]["amount"] = amt2
                        cur["consideration"]["parse_status"] = "PARSED"

            if args.progress_every and ((pi - page_start + 1) % args.progress_every == 0):
                print(f"[progress] page {pi - page_start + 1}/{page_end - page_start + 1} events_so_far={len(events)}")

        # finalize last event
        if cur is not None:
            cur.pop("_pending_town", None)
            cur["meta"]["property_ref_count"] = len(cur["property_refs"])
            cur["meta"]["has_multiple_properties"] = len(cur["property_refs"]) > 1
            finalize_event_id(cur)
            events.append(cur)
            stats["events_finalized"] += 1

    # output NDJSON
    with open(args.out, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    stats["events_written"] = len(events)

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"[done] wrote {len(events)} events")

if __name__ == "__main__":
    main()
