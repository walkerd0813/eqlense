#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
normalize_hampden_indexpdf_events_mtg_enrich_v1_1.py

Post-process Hampden Registry "Index-in-PDF" Mortgage extract outputs.

Fixes:
  1) Drops null/empty events (the "null event every ~5 rows" symptom).
  2) Extracts consideration amounts (money token parsing) into:
       consideration.raw_text, consideration.amount
  3) Attempts light heuristics for book/page (optional; never forces).
  4) Land Court handling: capture certificate_number_raw if present; prefer it as document_number_raw when document_number_raw is missing.
  5) Registry office classification: flags RECORDED_LAND vs LAND_COURT when detectable.

Constraints (Equity Lens data constitution)
- Deterministic, non-destructive: only enriches when evidence exists.
- Accept NULLs over polluted data.

Usage
python scripts/_registry/hampden/normalize_hampden_indexpdf_events_mtg_enrich_v1_1.py \
  --infile "path\...__CANON_v1_0.ndjson" \
  --out    "path\...__CANON_v1_1.ndjson" \
  --audit  "path\...__CANON_v1_1__audit.json"
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

MONEY_RE = re.compile(r"""
    (?<!\w)
    \$?
    (?:
        (?:\d{1,3}(?:,\d{3})+)|(?:\d+)
    )
    (?:\.\d{2})?
    (?!\w)
""", re.VERBOSE)

BOOK_HINT_RE = re.compile(r"^(?:BK|BOOK|B\s*K)$", re.IGNORECASE)
PAGE_HINT_RE = re.compile(r"^(?:PG|PAGE|P\s*G)$", re.IGNORECASE)
CERT_HINT_RE = re.compile(r"^(?:CERT|CERTIFICATE|CERT#|CERTIFICATE#)$", re.IGNORECASE)

LAND_COURT_HINT_RE = re.compile(r"LAND\s+COURT|LAN\s*CORT|LANCORT", re.IGNORECASE)
RECORDED_LAND_HINT_RE = re.compile(r"RECORDED\s+LAND|REC\s*LAND", re.IGNORECASE)

def _safe_json_loads(line: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        obj = json.loads(line)
        if obj is None:
            return None, None
        if not isinstance(obj, dict):
            return None, "non_dict_json"
        return obj, None
    except Exception:
        return None, "json_error"

def _is_effectively_empty_event(e: Dict[str, Any]) -> bool:
    if not e:
        return True
    meaningful = False
    for _, v in e.items():
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)) and str(v).strip() != "":
            meaningful = True
            break
        if isinstance(v, (list, dict)) and len(v) > 0:
            meaningful = True
            break
    return not meaningful

def _iter_strings(obj: Any) -> Iterable[str]:
    if obj is None:
        return
    if isinstance(obj, str):
        s = obj.strip()
        if s:
            yield s
        return
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _iter_strings(v)
        return
    if isinstance(obj, list):
        for v in obj:
            yield from _iter_strings(v)
        return

def _iter_tokens(e: Dict[str, Any]) -> List[str]:
    for key in ("tokens", "row_tokens", "raw_tokens"):
        v = e.get(key)
        if isinstance(v, list) and v and all(isinstance(x, str) for x in v):
            return [x.strip() for x in v if x and x.strip()]
    raw = e.get("raw") or {}
    if isinstance(raw, dict):
        for key in ("tokens", "row_tokens", "fragments"):
            v = raw.get(key)
            if isinstance(v, list) and v and all(isinstance(x, str) for x in v):
                return [x.strip() for x in v if x and x.strip()]
    return list(_iter_strings(e))

def _parse_money_candidates(tokens: List[str]) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for t in tokens:
        for m in MONEY_RE.finditer(t):
            raw = m.group(0)
            has_dollar = "$" in raw
            cleaned = raw.replace("$", "").replace(",", "")
            if cleaned.count(".") > 1:
                continue
            if "." in cleaned:
                whole, frac = cleaned.split(".", 1)
                if not whole.isdigit() or not frac.isdigit() or len(frac) != 2:
                    continue
                cents = int(whole) * 100 + int(frac)
            else:
                if not cleaned.isdigit():
                    continue
                if (not has_dollar) and (len(cleaned) <= 4):
                    continue
                cents = int(cleaned) * 100
            out.append((raw, cents))
    return out

def _pick_best_consideration(cands: List[Tuple[str, int]]) -> Optional[Tuple[str, float]]:
    if not cands:
        return None
    raw, cents = max(cands, key=lambda x: x[1])
    return raw, round(cents / 100.0, 2)

def _set_nested(d: Dict[str, Any], path: List[str], value: Any) -> None:
    cur = d
    for p in path[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[path[-1]] = value

def _get_first_key_ci(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    lower_map = {k.lower(): k for k in d.keys()}
    for cand in keys:
        k = lower_map.get(cand.lower())
        if k is not None:
            return k
    return None

def _infer_book_page(tokens: List[str]) -> Tuple[Optional[str], Optional[str], str]:
    if not tokens:
        return None, None, "none"
    book = page = None
    for i, t in enumerate(tokens):
        if BOOK_HINT_RE.match(t) and i + 1 < len(tokens):
            nxt = re.sub(r"[^\d]", "", tokens[i + 1])
            if nxt.isdigit():
                book = nxt
        if PAGE_HINT_RE.match(t) and i + 1 < len(tokens):
            nxt = re.sub(r"[^\d]", "", tokens[i + 1])
            if nxt.isdigit():
                page = nxt
    if book or page:
        return book, page, "marker_tokens"

    for i in range(len(tokens) - 1):
        a = re.sub(r"[^\d]", "", tokens[i])
        b = re.sub(r"[^\d]", "", tokens[i + 1])
        if a.isdigit() and b.isdigit() and len(a) >= 4 and len(b) >= 2:
            return a, b, "weak_numeric_pair"
    return None, None, "none"

def _infer_registry_office(tokens: List[str]) -> Optional[str]:
    joined = " ".join(tokens[:80])
    if LAND_COURT_HINT_RE.search(joined):
        return "LAND_COURT"
    if RECORDED_LAND_HINT_RE.search(joined):
        return "RECORDED_LAND"
    return None

def enrich_event(e: Dict[str, Any]) -> Dict[str, Any]:
    tokens = _iter_tokens(e)

    reg_office = _infer_registry_office(tokens)
    if reg_office:
        _set_nested(e, ["recording", "registry_office_raw"], reg_office)

    cert_raw: Optional[str] = None
    for container in (e.get("recording"), e.get("document"), e.get("source"), e):
        if isinstance(container, dict):
            k = _get_first_key_ci(container, ["certificate_number_raw", "certificate_raw", "cert_number_raw", "cert_no_raw"])
            if k:
                v = container.get(k)
                if isinstance(v, str) and v.strip():
                    cert_raw = v.strip()
                    break

    if cert_raw is None:
        for i, t in enumerate(tokens[:-1]):
            if CERT_HINT_RE.match(t):
                nxt = re.sub(r"[^\w\-]", "", tokens[i + 1]).strip()
                if nxt:
                    cert_raw = nxt
                    break

    if cert_raw:
        _set_nested(e, ["recording", "certificate_number_raw"], cert_raw)

    rec = e.get("recording")
    if not isinstance(rec, dict):
        rec = {}
        e["recording"] = rec

    docnum = rec.get("document_number_raw") or rec.get("document_number")
    if (not docnum) and cert_raw and rec.get("registry_office_raw") == "LAND_COURT":
        rec["document_number_raw"] = cert_raw

    has_cons = isinstance(e.get("consideration"), dict) and (
        e["consideration"].get("amount") is not None or (e["consideration"].get("raw_text") or "").strip()
    )
    if not has_cons:
        cands = _parse_money_candidates(tokens)
        best = _pick_best_consideration(cands)
        if best:
            raw_text, amount = best
            _set_nested(e, ["consideration", "raw_text"], raw_text)
            _set_nested(e, ["consideration", "amount"], amount)

    book_existing = rec.get("book") or rec.get("book_raw")
    page_existing = rec.get("page") or rec.get("page_raw")
    if not (book_existing and page_existing):
        book, page, method = _infer_book_page(tokens)
        if method == "marker_tokens":
            if not book_existing and book:
                rec["book"] = book
            if not page_existing and page:
                rec["page"] = page
            rec["book_page_infer_method"] = method
        elif method == "weak_numeric_pair" and (not book_existing) and (not page_existing) and book and page:
            rec["book"] = book
            rec["page"] = page
            rec["book_page_infer_method"] = method

    return e

def has_required_fields(e: Dict[str, Any]) -> bool:
    et = e.get("event_type") or e.get("event_type_raw")
    if isinstance(et, str):
        et = et.strip()
    if not et:
        doc = e.get("document")
        if isinstance(doc, dict):
            et = (doc.get("doc_type") or doc.get("doc_type_raw") or "").strip()
    if not et:
        return False

    rec = e.get("recording")
    if not isinstance(rec, dict):
        rec = {}
    dateish = rec.get("recording_date") or rec.get("recording_date_raw") or rec.get("recorded_at_raw") or rec.get("recorded_datetime_raw") or e.get("recorded_at_raw")
    docish = rec.get("document_number_raw") or rec.get("document_number") or rec.get("certificate_number_raw") or (rec.get("book") and rec.get("page"))
    if not dateish:
        return False
    if not docish:
        return False
    return True

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    audit: Dict[str, Any] = {
        "engine": "normalize_hampden_indexpdf_events_mtg_enrich_v1_1",
        "started_at": datetime.utcnow().isoformat() + "Z",
        "inputs": {"infile": os.path.abspath(args.infile)},
        "outputs": {"out": os.path.abspath(args.out), "audit": os.path.abspath(args.audit)},
        "counts": {
            "lines_read": 0,
            "json_errors": 0,
            "non_dict_json": 0,
            "null_or_empty_events_dropped": 0,
            "missing_required_fields_dropped": 0,
            "written": 0,
            "consideration_added": 0,
            "book_page_inferred": 0,
            "land_court_docnum_promoted": 0,
            "registry_office_classified": 0,
        },
        "notes": [
            "Hard gate: drops events that lack event_type + (recording_date-ish) + (doc# or (book+page) or cert#).",
            "Consideration extracted conservatively by picking the largest money-like token.",
            "Book/page inference is conservative; weak pairs are labeled via recording.book_page_infer_method.",
        ],
    }

    def _cons_added(before: Dict[str, Any], after: Dict[str, Any]) -> bool:
        b = before.get("consideration") if isinstance(before.get("consideration"), dict) else {}
        a = after.get("consideration") if isinstance(after.get("consideration"), dict) else {}
        b_has = bool(b.get("raw_text") or b.get("amount") is not None)
        a_has = bool(a.get("raw_text") or a.get("amount") is not None)
        return (not b_has) and a_has

    with open(args.infile, "r", encoding="utf-8") as f_in, open(args.out, "w", encoding="utf-8") as f_out:
        for line in f_in:
            audit["counts"]["lines_read"] += 1
            line = line.strip()
            if not line:
                continue
            obj, err = _safe_json_loads(line)
            if err == "json_error":
                audit["counts"]["json_errors"] += 1
                continue
            if err == "non_dict_json":
                audit["counts"]["non_dict_json"] += 1
                continue
            if obj is None or _is_effectively_empty_event(obj):
                audit["counts"]["null_or_empty_events_dropped"] += 1
                continue

            before = json.loads(json.dumps(obj))
            enrich_event(obj)

            if _cons_added(before, obj):
                audit["counts"]["consideration_added"] += 1

            rec = obj.get("recording") if isinstance(obj.get("recording"), dict) else {}
            if rec.get("book_page_infer_method") in ("marker_tokens", "weak_numeric_pair"):
                audit["counts"]["book_page_inferred"] += 1
            if rec.get("registry_office_raw") in ("LAND_COURT", "RECORDED_LAND"):
                audit["counts"]["registry_office_classified"] += 1
            if rec.get("registry_office_raw") == "LAND_COURT" and rec.get("certificate_number_raw"):
                b_rec = before.get("recording") if isinstance(before.get("recording"), dict) else {}
                if not (b_rec.get("document_number_raw") or b_rec.get("document_number")) and rec.get("document_number_raw") == rec.get("certificate_number_raw"):
                    audit["counts"]["land_court_docnum_promoted"] += 1

            if not has_required_fields(obj):
                audit["counts"]["missing_required_fields_dropped"] += 1
                continue

            f_out.write(json.dumps(obj, ensure_ascii=False) + "\n")
            audit["counts"]["written"] += 1

    audit["finished_at"] = datetime.utcnow().isoformat() + "Z"
    with open(args.audit, "w", encoding="utf-8") as f_a:
        json.dump(audit, f_a, indent=2, ensure_ascii=False)

    print("[ok]", json.dumps({"out": args.out, "audit": args.audit, "counts": audit["counts"]}, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
