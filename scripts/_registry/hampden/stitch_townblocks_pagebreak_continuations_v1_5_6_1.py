#!/usr/bin/env python3
"""
stitch_townblocks_pagebreak_continuations_v1_5_6.py

Wrapper over stitch_townblocks_pagebreak_continuations_v1_5_5_1.py
Adds SAME-PAGE ref repair for last-event candidates when refs are present
in same-page OCR but TownBlocks block parser missed them (e.g. UNIT483-2A).

Does NOT modify frozen TB/RowCtx/Join.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
from typing import Dict, List, Optional, Tuple

BASE_MOD = "stitch_townblocks_pagebreak_continuations_v1_5_5_1"

RE_TOWN_ADDR = re.compile(r"(?i)\bTown\s*:\s*(?P<town>[A-Z][A-Z\s\.\-']+?)\s+\bAddr\s*:\s*(?P<addr>.+)$")
# OCR variant: "CHICOPEE Addr:35 RUSSELL TERR" (Town label missing/split)
RE_TOWNLESS_ADDR = re.compile(r"(?i)^(?P<town>[A-Z][A-Z\s\.\-']{2,})\s+\bAddr\s*:\s*(?P<addr>.+)$")
RE_ADDR_ONLY = re.compile(r"(?i)\bAddr\s*:\s*(?P<addr>.+)$")
# UNIT with optional whitespace: "UNIT483-2A" or "UNIT 483-2A"
RE_UNIT = re.compile(r"(?i)\bUNIT\s*(?P<unit>[A-Z0-9\-]+)\b")

def safe_stitch_event(base, last_ev, *, from_page, into_page, refs_found, parties_found, captured, counts, samples, continuation_type: str):
    """
    Older frozen stitchers may not accept continuation_type kwarg.
    Try with it; on TypeError retry without; record type in meta.
    """
    try:
        safe_stitch_event(base, 
            last_ev,
            from_page=from_page,
            into_page=into_page,
            refs_found=refs_found,
            parties_found=parties_found,
            captured=captured,
            counts=counts,
            samples=samples,
            continuation_type=continuation_type,
        )
    except TypeError:
        safe_stitch_event(base, 
            last_ev,
            from_page=from_page,
            into_page=into_page,
            refs_found=refs_found,
            parties_found=parties_found,
            captured=captured,
            counts=counts,
            samples=samples,
        )
        try:
            last_ev.setdefault("meta", {}).setdefault("stitch", {})["continuation_type"] = continuation_type
        except Exception:
            pass

def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _strip_trailing_flags(addr: str) -> str:
    # Often lines end with " Y" verification flag. Keep address clean.
    addr = re.sub(r"\s+Y\s*$", "", addr.strip())
    return addr.strip()

def _extract_unit(addr: str) -> Tuple[str, Optional[str]]:
    """
    Returns (addr_wo_unit, unit_hint)
    """
    a = _strip_trailing_flags(addr)
    m = RE_UNIT.search(a)
    if not m:
        return a, None
    unit = m.group("unit").strip()
    # remove the UNIT... token from address string
    a2 = RE_UNIT.sub("", a)
    a2 = _clean_spaces(a2).strip(",")
    return a2, unit

def parse_refs_from_lines(lines: List[str]) -> List[Tuple[Optional[str], str]]:
    """
    Parse (town, address_raw) refs from arbitrary OCR lines.
    Universal, tolerant:
      - Town: X Addr: Y
      - X Addr: Y (Town label missing)
      - Addr: Y (uses last seen town in window)
    """
    refs: List[Tuple[Optional[str], str]] = []
    last_town: Optional[str] = None

    for raw in lines:
        line = _clean_spaces(raw)
        if not line:
            continue

        m = RE_TOWN_ADDR.search(line)
        if m:
            town = _clean_spaces(m.group("town")).upper()
            addr = _strip_trailing_flags(m.group("addr"))
            last_town = town
            refs.append((town, addr))
            continue

        m = RE_TOWNLESS_ADDR.search(line)
        if m:
            town = _clean_spaces(m.group("town")).upper()
            addr = _strip_trailing_flags(m.group("addr"))
            last_town = town
            refs.append((town, addr))
            continue

        m = RE_ADDR_ONLY.search(line)
        if m and last_town:
            addr = _strip_trailing_flags(m.group("addr"))
            refs.append((last_town, addr))
            continue

    # Deduplicate exact duplicates while preserving order
    seen = set()
    out: List[Tuple[Optional[str], str]] = []
    for t, a in refs:
        k = (t or "", a)
        if k in seen:
            continue
        seen.add(k)
        out.append((t, a))
    return out

def same_page_repair(lines_raw: List[str], max_bottom: int = 140) -> List[Tuple[Optional[str], str]]:
    """
    Look at bottom-of-page window for the last-event candidate, and try to harvest refs.
    This avoids capturing refs from other transactions above by restricting window.
    """
    if not lines_raw:
        return []

    tail = lines_raw[-max_bottom:]
    # Keep in natural order; parser expects town context.
    refs = parse_refs_from_lines(tail)

    # Extra normalization: fix UNIT spacing variants and split into clean addr + unit_hint if needed.
    # NOTE: We do NOT change returned structure here; stitcher downstream may store unit_hint separately.
    # We simply ensure addr tokens are stable.
    norm: List[Tuple[Optional[str], str]] = []
    for town, addr in refs:
        addr2 = _clean_spaces(addr)
        norm.append((town, addr2))
    return norm

def load_page_lines(raw_path: str) -> Dict[int, List[str]]:
    page_lines: Dict[int, List[str]] = {}
    with open(raw_path, "r", encoding="utf-8") as f:
        for line in f:
            o = json.loads(line)
            page_lines[int(o["page_index"])] = o.get("lines_raw") or []
    return page_lines

def main() -> None:
    base = importlib.import_module(BASE_MOD)

    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--qa", dest="qa_path", required=True)
    ap.add_argument("--raw", dest="raw_path", default=None)
    ap.add_argument("--raw_lines_ndjson", dest="raw_lines_ndjson", default=None)
    ap.add_argument("--max_scan", type=int, default=80)
    ap.add_argument("--same_page_bottom", type=int, default=140)
    args = ap.parse_args()

    in_path = args.in_path
    raw_path = args.raw_lines_ndjson or args.raw_path
    if not raw_path:
        raise SystemExit("Must provide --raw_lines_ndjson or --raw")

    # read events (input) and also keep base grouping logic
    events: List[dict] = []
    with open(in_path, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))

    by_page = base.index_events_by_page(events) if hasattr(base, "index_events_by_page") else None
    if by_page is None:
        # fallback: mimic prior behavior
        by_page = {}
        for ev in events:
            p = int(ev["meta"]["page_index"])
            by_page.setdefault(p, []).append(ev)

    page_lines = load_page_lines(raw_path)

    counts = {
        "pages_seen": 0,
        "candidate_pagebreaks": 0,
        "stitched": 0,
        "stitched_parties_only": 0,
        "same_page_repair": 0,
        "no_continuation_found": 0,
        "missing_raw_lines_for_next_page": 0,
        "notes": "v1_5_6 adds SAME_PAGE_REPAIR for missing refs; keeps v1_5_5_1 pagebreak stitching; supports --raw and --raw_lines_ndjson.",
    }
    samples: List[dict] = []

    pages = sorted(by_page.keys())
    counts["pages_seen"] = len(pages)

    for p in pages:
        next_p = p + 1
        last_ev = by_page[p][-1]

        # Candidate = last event missing addr
        if not base.event_missing_addr(last_ev):
            continue

        counts["candidate_pagebreaks"] += 1

        # 1) SAME-PAGE repair first
        same_refs = same_page_repair(page_lines.get(p) or [], max_bottom=args.same_page_bottom)
        if same_refs:
            # Reuse base extractor for parties from same-page bottom window, if helpful:
            # We'll pull parties using base.extract_top_continuation on tail chunk (safe enough).
            tail = (page_lines.get(p) or [])[-args.same_page_bottom:]
            refs_found = same_refs
            parties_found: List[dict] = []
            captured: List[str] = tail

            # Let base stitch logic write meta and merge logic
            safe_stitch_event(base, 
                last_ev,
                from_page=p,
                into_page=p,
                refs_found=refs_found,
                parties_found=parties_found,
                captured=captured,
                counts=counts,
                samples=samples,
                continuation_type="SAME_PAGE_REPAIR",
            )
            counts["same_page_repair"] += 1
            continue

        # 2) Otherwise try next-page continuation as usual (base behavior)
        if next_p not in page_lines:
            counts["missing_raw_lines_for_next_page"] += 1
            continue

        refs_found, parties_found, captured = base.extract_top_continuation(
            page_lines.get(next_p) or [],
            max_scan=args.max_scan
        )

        # v1_5_5_1 supports parties-only continuation; base will handle counts appropriately.
        if (not refs_found) and (not parties_found):
            counts["no_continuation_found"] += 1
            continue

        safe_stitch_event(base, 
            last_ev,
            from_page=p,
            into_page=next_p,
            refs_found=refs_found,
            parties_found=parties_found,
            captured=captured,
            counts=counts,
            samples=samples
        )

    # write
    os.makedirs(os.path.dirname(args.out_path), exist_ok=True)
    with open(args.out_path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    os.makedirs(os.path.dirname(args.qa_path), exist_ok=True)
    with open(args.qa_path, "w", encoding="utf-8") as f:
        json.dump({
            "engine": "stitch_townblocks_pagebreak_continuations_v1_5_6",
            "inputs": {"in": in_path, "raw": raw_path},
            "counts": counts,
            "samples": samples[:50],
        }, f, indent=2)

if __name__ == "__main__":
    main()

