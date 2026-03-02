#!/usr/bin/env python3
# Hampden Registry (Recorded Land) RG370RP - MTG index PDF extractor v1_5
# - Uses pdfplumber.extract_words + row clustering (Y-band) to reconstruct visual rows.
# - "One recorded instrument = one event"; can contain many property_refs (Town+Addr lines).
# - Doc type is extracted ONLY from page 1 header zone per the "Bible".
#
# Notes:
# - This script extracts "raw-ish canonical" events for downstream normalize/attach.
# - It does NOT attempt fuzzy matching, snapping, or inference.
# - UNKNOWN is first-class.

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()


def norm_ws(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None


def upper(s: Optional[str]) -> Optional[str]:
    s = norm_ws(s)
    return s.upper() if s else None


def money_to_amount(raw: Optional[str]) -> Tuple[Optional[float], str, List[str]]:
    if not raw:
        return (None, "MISSING", ["MISSING"])
    s = raw.strip()
    flags: List[str] = []
    s2 = s.replace("$", "").replace(",", "")
    try:
        amt = float(s2)
    except Exception:
        return (None, "UNPARSEABLE", ["UNPARSEABLE"])
    if abs(amt) < 1e-9:
        flags.append("ZERO_OR_NOMINAL")
    return (amt, "PARSED", flags)


def classify_registry_office_page1(text: str) -> Optional[str]:
    t = (text or "").upper()
    if "RECORDED LAND" in t:
        return "RECORDED_LAND"
    if "REGISTERED LAND" in t or "LAND REGISTRATION" in t or "LAND COURT" in t:
        return "LAND_COURT"
    return None


DATE_ROW_RE = re.compile(
    r"^(?P<mdy>\d{2}-\d{2}-\d{4})\s+"
    r"(?P<time>\d{1,2}:\d{2}:\d{2}[ap]?)\s+"
    r"(?P<book>\d+)\s+(?P<page>\d+)\s+(?P<inst>\d+)\b"
)

TOWN_ADDR_RE = re.compile(r"Town:\s*(?P<town>[A-Za-z \-'.&]+)\s+Addr:\s*(?P<addr>.+)$", re.I)


def extract_doc_type_code_page1(pdf: pdfplumber.PDF) -> Optional[str]:
    if not pdf.pages:
        return None
    page = pdf.pages[0]
    w, h = page.width, page.height
    bbox = (0, 0, w * 0.55, h * 0.22)
    txt = page.crop(bbox).extract_text() or ""
    txt_u = txt.upper()
    m = re.search(r"DOC\s+TYPES[^\n]*?([A-Z]{2,5})", txt_u)
    if m:
        return m.group(1).strip()
    m = re.search(r"DOC\s*TYPES\.*\s*([A-Z]{2,5})", txt_u)
    if m:
        return m.group(1).strip()
    return None


def words_in_bbox(page: pdfplumber.page.Page, bbox):
    cropped = page.crop(bbox)
    return cropped.extract_words(
        keep_blank_chars=False,
        use_text_flow=True,
        split_at_punctuation=False
    )


def cluster_rows(words: List[Dict[str, Any]], y_tol: float = 2.8) -> List[List[Dict[str, Any]]]:
    if not words:
        return []
    ws = sorted(words, key=lambda w: (float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
    rows: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    cur_y = None

    for w in ws:
        y = float(w.get("top", 0.0))
        if cur_y is None:
            cur_y = y
            cur = [w]
            continue
        if abs(y - cur_y) <= y_tol:
            cur.append(w)
        else:
            rows.append(cur)
            cur = [w]
            cur_y = y
    if cur:
        rows.append(cur)
    return rows


def row_text(row: List[Dict[str, Any]]) -> str:
    if not row:
        return ""
    row_s = sorted(row, key=lambda w: float(w.get("x0", 0.0)))
    parts = [w.get("text", "") for w in row_s if w.get("text")]
    txt = " ".join(parts)
    return norm_ws(txt) or ""


def page_rows(page: pdfplumber.page.Page) -> List[str]:
    w, h = page.width, page.height
    body_bbox = (0, h * 0.20, w, h * 0.94)
    words = words_in_bbox(page, body_bbox)
    rows = cluster_rows(words, y_tol=2.8)
    out = []
    for r in rows:
        t = row_text(r)
        if t:
            out.append(t)
    return out


def doc_type_to_event_type(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    code = code.upper().strip()
    mapping = {
        "MTG": "MORTGAGE",
        "MGT": "MORTGAGE",
        "ASN": "ASSIGNMENT",
        "REL": "RELEASE",
        "DM": "DISCHARGE_MORTGAGE",
        "DIS": "DISCHARGE",
        "DEED": "DEED",
        "FDD": "FORECLOSURE_DEED",
        "LP": "LIS_PENDENS",
        "MTL": "MA_TAX_LIEN",
        "FTL": "FEDERAL_TAX_LIEN",
        "ESMT": "EASEMENT",
        "LIEN": "LIEN",
        "MSDD": "MASTER_DEED",
    }
    return mapping.get(code, code)


def build_event_id(county: str, registry_office: str, doc_type_code: str, book: Optional[str], page: Optional[str], inst: Optional[str], seq: int) -> str:
    key = f"{county}|{registry_office}|{doc_type_code}|{book or ''}|{page or ''}|{inst or ''}|{seq}"
    return f"MA|registry|indexpdf|{county}|{sha1_hex(key)[:24]}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--run-id", default="")
    ap.add_argument("--county", default="hampden")
    ap.add_argument("--page-start", type=int, default=0)
    ap.add_argument("--page-end", type=int, default=-1)
    ap.add_argument("--progress-every", type=int, default=50)
    ap.add_argument("--debug-lines", type=int, default=0)
    args = ap.parse_args()

    in_pdf = args.pdf
    run_id = (args.run_id or "").strip() or f"{utc_now_compact()}__{args.county.lower()}__indexpdf_v1_5"
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
        "event_type": None,
        "registry_office": None,
        "warnings": [],
        "date_rows_seen": 0,
        "town_addr_seen": 0,
        "multi_property_events": 0,
        "continuation_rows_attached": 0,
    }

    events_out: List[Dict[str, Any]] = []

    with pdfplumber.open(in_pdf) as pdf:
        stats["pages_total"] = len(pdf.pages)
        if not pdf.pages:
            stats["warnings"].append("EMPTY_PDF")
        else:
            page1_text = pdf.pages[0].extract_text() or ""
            registry_office = classify_registry_office_page1(page1_text) or "RECORDED_LAND"
            stats["registry_office"] = registry_office

            doc_type_code = extract_doc_type_code_page1(pdf)
            stats["doc_type_code"] = doc_type_code
            stats["event_type"] = doc_type_to_event_type(doc_type_code)
            if not doc_type_code:
                stats["warnings"].append("DOC_TYPE_CODE_NOT_FOUND_PAGE1_HEADER")

            page_start = max(0, int(args.page_start))
            page_end = int(args.page_end)
            if page_end < 0:
                page_end = len(pdf.pages) - 1
            page_end = min(len(pdf.pages) - 1, page_end)

            current_event: Optional[Dict[str, Any]] = None
            last_date_row_ctx: Optional[Dict[str, str]] = None

            for pi in range(page_start, page_end + 1):
                page = pdf.pages[pi]
                rows = page_rows(page)

                if args.debug_lines and pi == page_start:
                    print(f"[debug] page_index={pi} reconstructed_rows={len(rows)}")
                    for i, t in enumerate(rows[:args.debug_lines]):
                        print(f"  {i:>3} {t}")

                for txt in rows:
                    m = DATE_ROW_RE.match(txt)
                    if m:
                        stats["date_rows_seen"] += 1
                        if current_event is not None:
                            events_out.append(current_event)
                            current_event = None

                        recording_date_raw = m.group("mdy")
                        time_raw = m.group("time")
                        book = m.group("book")
                        page_no = m.group("page")
                        inst = m.group("inst")

                        try:
                            dt = datetime.strptime(recording_date_raw, "%m-%d-%Y")
                            recording_date = dt.strftime("%Y-%m-%d")
                        except Exception:
                            recording_date = None
                            stats["warnings"].append(f"BAD_DATE:{recording_date_raw}")

                        last_date_row_ctx = {
                            "recording_date_raw": recording_date_raw,
                            "recording_date": recording_date or "",
                            "recording_time_raw": time_raw,
                            "book": book,
                            "page": page_no,
                            "instrument_number_raw": inst
                        }

                        current_event = {
                            "schema": {"name": "equitylens.registry_event", "version": "mim_v1_0"},
                            "event_id": build_event_id(county, registry_office, doc_type_code or "UNK", book, page_no, inst, seq=1),
                            "event_type": doc_type_to_event_type(doc_type_code) or "UNKNOWN",
                            "county": county,
                            "registry_system": "registry_index_pdf",
                            "registry_office": registry_office,
                            "doc_type_code": doc_type_code,
                            "doc_type_desc": doc_type_to_event_type(doc_type_code),
                            "source": {
                                "run_id": run_id,
                                "dataset_hash": None,
                                "as_of_date": datetime.now().strftime("%Y-%m-%d"),
                                "uri": f"file://{os.path.abspath(in_pdf)}",
                                "page_index": pi,
                            },
                            "recording": {
                                "recorded_at_raw": recording_date_raw,
                                "recording_date": recording_date or None,
                                "recording_time": time_raw,
                                "book": book,
                                "page": page_no,
                                "instrument_number_raw": inst,
                                "seq": None
                            },
                            "parties": [],
                            "property_refs": [],
                            "consideration": {"raw_text": None, "amount": None, "parse_status": "MISSING", "flags": ["MISSING"], "source": "INDEX"},
                            "attach": {"status": "UNKNOWN", "property_id": None, "method": None, "confidence": None, "evidence": {"match_keys_used": []}, "flags": [], "attach_scope": "MULTI", "attach_status": "UNKNOWN"},
                            "mortgage": None,
                            "deed": None,
                            "assignment": None,
                            "satisfaction": None,
                            "lien": None,
                            "lis_pendens": None,
                            "foreclosure": None,
                            "arms_length": None
                        }
                        continue

                    if current_event is None and last_date_row_ctx is not None and (("Town:" in txt) or ("Addr:" in txt) or (" MTG " in (" " + txt + " "))):
                        ctx = last_date_row_ctx
                        current_event = {
                            "schema": {"name": "equitylens.registry_event", "version": "mim_v1_0"},
                            "event_id": build_event_id(county, registry_office, doc_type_code or "UNK", ctx.get("book"), ctx.get("page"), ctx.get("instrument_number_raw"), seq=1),
                            "event_type": doc_type_to_event_type(doc_type_code) or "UNKNOWN",
                            "county": county,
                            "registry_system": "registry_index_pdf",
                            "registry_office": registry_office,
                            "doc_type_code": doc_type_code,
                            "doc_type_desc": doc_type_to_event_type(doc_type_code),
                            "source": {
                                "run_id": run_id,
                                "dataset_hash": None,
                                "as_of_date": datetime.now().strftime("%Y-%m-%d"),
                                "uri": f"file://{os.path.abspath(in_pdf)}",
                                "page_index": pi,
                                "notes": ["CONTINUATION_FROM_PREV_PAGE_DATE_ROW"]
                            },
                            "recording": {
                                "recorded_at_raw": ctx.get("recording_date_raw"),
                                "recording_date": ctx.get("recording_date") or None,
                                "recording_time": ctx.get("recording_time_raw"),
                                "book": ctx.get("book"),
                                "page": ctx.get("page"),
                                "instrument_number_raw": ctx.get("instrument_number_raw"),
                                "seq": None
                            },
                            "parties": [],
                            "property_refs": [],
                            "consideration": {"raw_text": None, "amount": None, "parse_status": "MISSING", "flags": ["MISSING"], "source": "INDEX"},
                            "attach": {"status": "UNKNOWN", "property_id": None, "method": None, "confidence": None, "evidence": {"match_keys_used": []}, "flags": [], "attach_scope": "MULTI", "attach_status": "UNKNOWN"},
                            "mortgage": None,
                            "deed": None,
                            "assignment": None,
                            "satisfaction": None,
                            "lien": None,
                            "lis_pendens": None,
                            "foreclosure": None,
                            "arms_length": None
                        }
                        stats["continuation_rows_attached"] += 1

                    if current_event is None:
                        continue

                    tm = TOWN_ADDR_RE.search(txt)
                    if tm:
                        stats["town_addr_seen"] += 1
                        town = upper(tm.group("town"))
                        addr = norm_ws(tm.group("addr"))
                        unit_hint = None
                        m_unit = re.search(r"\bUNIT\s+([A-Za-z0-9\-]+)\b", addr or "", re.I)
                        if m_unit:
                            unit_hint = m_unit.group(1)

                        lot_ref_raw = None
                        m_lot = re.search(r"\bLOT\b.+$", addr or "", re.I)
                        if m_lot:
                            lot_ref_raw = norm_ws(m_lot.group(0))

                        current_event["property_refs"].append({
                            "ref_index": len(current_event["property_refs"]) + 1,
                            "town_raw": town,
                            "address_raw": addr,
                            "address_norm": upper(addr),
                            "unit_hint": unit_hint,
                            "lot_ref_raw": lot_ref_raw,
                            "ref_anchor_type": "ADDRESS" if addr else "LOT_PLAN",
                            "attach_status": "UNKNOWN",
                            "attach_confidence": None,
                            "evidence": {"page_index": pi}
                        })
                        continue

                    if re.search(r"\bMTG\b", txt):
                        money_tokens = re.findall(r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d+\.\d{2})", txt)
                        if money_tokens:
                            money = money_tokens[-1]
                            amt, st, flags = money_to_amount(money)
                            current_event["consideration"] = {"raw_text": money, "amount": amt, "parse_status": st, "flags": flags, "source": "INDEX"}
                        continue

                    pm = re.match(r"^(?P<grp>\d+)\s+(?P<pc>[PC])\s+(?P<name>.+)$", txt)
                    if pm:
                        grp = int(pm.group("grp"))
                        pc = pm.group("pc").upper()
                        name_raw = norm_ws(pm.group("name")) or ""
                        entity_type = "PERSON" if pc == "P" else "ORG"
                        role = "MORTGAGOR" if grp == 1 else "MORTGAGEE"
                        current_event["parties"].append({
                            "role": role,
                            "name_raw": name_raw,
                            "name_norm": upper(name_raw),
                            "entity_type": entity_type,
                            "mailing_address_raw": None
                        })
                        continue

                stats["pages_processed"] += 1
                if args.progress_every and ((pi - page_start + 1) % args.progress_every == 0):
                    print(f"[progress] page {pi - page_start + 1}/{page_end - page_start + 1} events_so_far={len(events_out) + (1 if current_event else 0)} date_rows_seen={stats['date_rows_seen']} town_addr_seen={stats['town_addr_seen']}")

            if current_event is not None:
                events_out.append(current_event)

    for ev in events_out:
        if isinstance(ev.get("property_refs"), list) and len(ev["property_refs"]) > 1:
            ev["has_multiple_properties"] = True
            ev["property_ref_count"] = len(ev["property_refs"])
            stats["multi_property_events"] += 1
        else:
            ev["has_multiple_properties"] = False
            ev["property_ref_count"] = len(ev.get("property_refs") or [])

    with open(args.out, "w", encoding="utf-8") as f:
        for ev in events_out:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    stats["events_written"] = len(events_out)

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print(f"[done] wrote {len(events_out)} events")
    print(f"[audit] {args.audit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
