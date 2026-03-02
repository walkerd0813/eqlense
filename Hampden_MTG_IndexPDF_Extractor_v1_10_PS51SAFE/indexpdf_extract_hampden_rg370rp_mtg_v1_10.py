#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hampden Registry of Deeds - Recorded Land "RECORDED LAND BY RECORDING DATE" (RG370RP) - MTG
Index-PDF extractor.

Design goals (Bible-compliant):
- bounded zone extraction (no full-page splitlines parsing)
- rebuild visual rows by clustering glyphs (page.chars) into y-bands
- fixed row parsing based on anchored tokens (Town:/Addr:, MTG, date/time, book-page, inst/doc #)
- accept NULLs; never invent/guess; UNKNOWN is first-class
"""

from __future__ import annotations
import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    import pdfplumber
except Exception as e:
    raise SystemExit("Missing dependency pdfplumber. Install with: python -m pip install pdfplumber") from e


# -------------------------
# Helpers: header classification
# -------------------------

def classify_registry_office(pdf: "pdfplumber.PDF") -> str:
    """Detect Recorded Land vs Land Court using only page 1 header text."""
    try:
        txt = (pdf.pages[0].extract_text() or "").upper()
    except Exception:
        return "UNKNOWN"
    if "LAND REGISTRATION" in txt or "REGISTERED LAND" in txt or "LAND COURT" in txt:
        return "LAND_COURT"
    if "RECORDED LAND" in txt:
        return "RECORDED_LAND"
    return "UNKNOWN"


# -------------------------
# Bounded glyph extraction
# -------------------------

Token = Dict[str, Any]  # {text,x0,x1,top}

def tokens_in_bbox(page: "pdfplumber.page.Page",
                   bbox: Optional[Tuple[float, float, float, float]]) -> List[Token]:
    """
    Extract glyph-level tokens from bbox using page.chars to handle character-level PDFs reliably.
    Returns tokens with text, x0, x1, top.
    """
    try:
        p = page.within_bbox(bbox) if bbox is not None else page
    except Exception:
        p = page

    out: List[Token] = []
    for ch in getattr(p, "chars", []) or []:
        t = ch.get("text")
        if not t:
            continue
        # Normalize weird whitespace glyphs
        if t in ("\u00a0", "\u2007", "\u202f"):
            t = " "
        out.append({
            "text": t,
            "x0": float(ch.get("x0", 0.0)),
            "x1": float(ch.get("x1", float(ch.get("x0", 0.0)))),
            "top": float(ch.get("top", 0.0)),
        })
    return out


def cluster_rows(tokens: List[Token], y_tol: float = 2.6) -> List[List[Token]]:
    """Cluster glyph tokens into visual rows by y coordinate, then sort within row by x."""
    if not tokens:
        return []
    toks = sorted(tokens, key=lambda w: (w["top"], w["x0"]))
    rows: List[List[Token]] = []
    cur: List[Token] = []
    cur_y: Optional[float] = None
    for w in toks:
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


def row_text(row: List[Token]) -> str:
    """
    Reconstruct a readable line from a row of glyph tokens.
    Uses x-gaps; works even when every character is separate.
    """
    if not row:
        return ""

    row = sorted(row, key=lambda w: (w.get("x0", 0.0), w.get("top", 0.0)))
    texts = [str(w.get("text", "")).strip() for w in row if str(w.get("text", "")).strip()]
    if not texts:
        return ""

    # If mostly 1-char tokens, treat as char-mode.
    one_char = sum(1 for t in texts if len(t) == 1)
    char_mode = (one_char / max(1, len(texts))) >= 0.70

    out: List[str] = []
    prev_x1: Optional[float] = None

    for w in row:
        t = str(w.get("text", ""))
        if not t:
            continue

        # collapse internal whitespace glyphs
        if t.isspace():
            t = " "

        x0 = float(w.get("x0", 0.0))
        x1 = float(w.get("x1", x0))

        if prev_x1 is None:
            out.append(t)
            prev_x1 = x1
            continue

        gap = x0 - prev_x1

        if char_mode:
            # join tightly; insert space only for real gaps
            if gap > 2.0:
                out.append(" ")
            out.append(t)
        else:
            # word-ish mode; insert space for modest gaps
            if gap > 1.0:
                out.append(" ")
            out.append(t)

        prev_x1 = x1

    s = "".join(out)
    # normalize whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def is_rule_row(s: str) -> bool:
    """Detect horizontal rule rows (mostly dashes/underscores)."""
    if not s:
        return True
    if len(s) >= 30:
        dash = sum(1 for ch in s if ch in "-_—")
        if dash / max(1, len(s)) >= 0.60:
            return True
    return False


# -------------------------
# Row parsers
# -------------------------

DATE_RE = re.compile(
    r"(?P<mdy>\d{2}-\d{2}-\d{4})\s+(?P<hms>\d{1,2}:\d{2}:\d{2}[ap]?)\s+(?P<book>\d+)\s*-\s*(?P<page>\d+)\s+(?P<inst>\d+)",
    re.I
)

TOWN_RE = re.compile(r"\bTown:\s*(?P<town>[A-Z \-\'\.&]+)\b", re.I)
ADDR_RE = re.compile(r"\bAddr:\s*(?P<addr>.+)$", re.I)

AMOUNT_RE = re.compile(r"\b(?P<amt>\d{1,3}(?:,\d{3})*\.\d{2})\b")

def parse_town_addr(s: str) -> Tuple[Optional[str], Optional[str]]:
    town = None
    addr = None
    m = TOWN_RE.search(s)
    if m:
        town = m.group("town").strip().upper()
    m2 = ADDR_RE.search(s)
    if m2:
        addr = m2.group("addr").strip()
    return town, addr

def parse_amount(s: str) -> Optional[str]:
    # Hampden MTG shows amount in many rows; store as raw string; normalization later
    m = AMOUNT_RE.search(s.replace(" ", ""))
    if not m:
        return None
    return m.group("amt")

def parse_doc_type(s: str) -> Optional[str]:
    # Many RG prints include "MTG" token somewhere in row.
    if re.search(r"\bMTG\b", s):
        return "MTG"
    return None


def build_event_id(county: str, registry_office: str, doc_type_code: str, recording: Dict[str, Any]) -> str:
    # Deterministic key based on recording fields only.
    parts = [
        "MA", "registry", doc_type_code.lower(), county.lower(),
        registry_office.lower(),
        recording.get("recording_date") or recording.get("recording_date_raw") or "unknown_date",
        recording.get("recording_time") or recording.get("recording_time_raw") or "unknown_time",
        recording.get("book") or "unknown_book",
        recording.get("page") or "unknown_page",
        recording.get("instrument_number_raw") or recording.get("document_number_raw") or "unknown_inst",
    ]
    safe = "|".join(parts)
    return safe


# -------------------------
# Main
# -------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--county", default="hampden")
    ap.add_argument("--page-start", type=int, default=0)
    ap.add_argument("--page-end", type=int, default=None)
    ap.add_argument("--progress-every", type=int, default=0)
    ap.add_argument("--debug-rows", type=int, default=0)
    ap.add_argument("--debug-words", type=int, default=0)
    args = ap.parse_args()

    run_id = args.run_id or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "__hampden__indexpdf_v1_10"
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    stats: Dict[str, Any] = {
        "engine": "Hampden_MTg_IndexPDF_Extractor_v1_10",
        "run_id": run_id,
        "pdf": args.pdf,
        "pages": {"start": args.page_start, "end": args.page_end},
        "events_written": 0,
        "registry_office": None,
        "doc_type_code": "MTG",
        "warnings": [],
    }

    with pdfplumber.open(args.pdf) as pdf:
        registry_office = classify_registry_office(pdf)
        stats["registry_office"] = registry_office

        page_start = max(0, int(args.page_start))
        page_end = int(args.page_end) if args.page_end is not None else (len(pdf.pages) - 1)
        page_end = min(page_end, len(pdf.pages) - 1)

        events: List[Dict[str, Any]] = []
        cur: Optional[Dict[str, Any]] = None
        header_ctx: Optional[Dict[str, Any]] = None

        for pi in range(page_start, page_end + 1):
            page = pdf.pages[pi]
            w, h = page.width, page.height
            # Body bbox: skip top headers; keep bottom footer out.
            body_bbox = (0.0, h * 0.18, w, h * 0.94)

            toks = tokens_in_bbox(page, body_bbox)
            rows = cluster_rows(toks, y_tol=2.8)

            if args.debug_words:
                print(f"[debug] page_index={pi} tokens={len(toks)}")
                for t in toks[:min(args.debug_words, len(toks))]:
                    print("   ", t)

            # Debug sample of first N reconstructed rows
            if args.debug_rows:
                print(f"[debug] page_index={pi} rows={len(rows)} sample={min(args.debug_rows, len(rows))}")
                for ridx, r in enumerate(rows[:min(args.debug_rows, len(rows))]):
                    s = row_text(r)
                    s = s[:180]
                    print(f"  {ridx:03d} {s}")

            for r in rows:
                s = row_text(r)
                if not s or is_rule_row(s):
                    continue

                # DATE row
                m = DATE_RE.search(s)
                if m:
                    if cur is not None:
                        # finalize previous event
                        cur["meta"]["property_ref_count"] = len(cur.get("property_refs", []))
                        cur["meta"]["has_multiple_properties"] = len(cur.get("property_refs", [])) > 1
                        events.append(cur)

                    recording_date_raw = m.group("mdy")
                    time_raw = m.group("hms")
                    book = m.group("book")
                    page_no = m.group("page")
                    inst = m.group("inst")

                    try:
                        dt = datetime.strptime(recording_date_raw, "%m-%d-%Y")
                        recording_date = dt.strftime("%Y-%m-%d")
                    except Exception:
                        recording_date = None
                        stats["warnings"].append({"type": "BAD_DATE", "raw": recording_date_raw, "page": pi})

                    recording = {
                        "recorded_at_raw": f"{recording_date_raw} {time_raw}",
                        "recording_date_raw": recording_date_raw,
                        "recording_date": recording_date,
                        "recording_time_raw": time_raw,
                        "recording_time": time_raw,
                        "book": book,
                        "page": page_no,
                        # Recorded land prints: INST #
                        "instrument_number_raw": inst if registry_office != "LAND_COURT" else None,
                        # Land court prints: DOC #
                        "document_number_raw": inst if registry_office == "LAND_COURT" else None,
                        "seq": None,
                    }

                    header_ctx = {"recording": recording, "page_index": pi}

                    event_id = build_event_id(args.county, registry_office, "MTG", recording)
                    cur = {
                        "event_id": event_id,
                        "event_type": "MORTGAGE",
                        "county": args.county,
                        "document": {"doc_type_code": "MTG", "doc_type_raw": "MTG"},
                        "recording": recording,
                        "source": {
                            "source_system": "Hampden_RG370RP_IndexPDF",
                            "registry_office": registry_office,
                            "pdf": args.pdf,
                            "page_index": pi,
                        },
                        "property_refs": [],
                        "meta": {"run_id": run_id, "engine_version": "v1_10"},
                    }
                    continue

                # If we haven't started an event yet, ignore rows until DATE row.
                if cur is None:
                    continue

                # Parse Town/Addr row(s)
                if "Town:" in s or "Addr:" in s:
                    town, addr = parse_town_addr(s)
                    if town or addr:
                        cur.setdefault("property_refs", []).append({
                            "ref_index": len(cur.get("property_refs", [])) + 1,
                            "town_raw": town,
                            "address_raw": addr,
                            "unit_hint": None,
                            "attach_status": "UNKNOWN",
                            "attach_confidence": "C",
                        })
                    continue

                # Parse amount / parties / misc: store minimal evidence without guessing.
                amt = parse_amount(s)
                if amt and cur.get("transaction_semantics") is None:
                    cur["transaction_semantics"] = {"amount_raw": amt}

                # Parties: this MTG print often lists grantor/grantee style rows above Town.
                # We do not force a party grammar yet; store raw rows for later canonicalizer.
                cur.setdefault("document", {}).setdefault("raw_rows", []).append(s)

            if args.progress_every and ((pi - page_start + 1) % args.progress_every == 0):
                print(f"[progress] page {pi - page_start + 1}/{page_end - page_start + 1} events_so_far={len(events) + (1 if cur else 0)}")

        # finalize trailing event
        if cur is not None:
            cur["meta"]["property_ref_count"] = len(cur.get("property_refs", []))
            cur["meta"]["has_multiple_properties"] = len(cur.get("property_refs", [])) > 1
            events.append(cur)

    with open(args.out, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    stats["events_written"] = len(events)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"[done] wrote {len(events)} events")


if __name__ == "__main__":
    main()
