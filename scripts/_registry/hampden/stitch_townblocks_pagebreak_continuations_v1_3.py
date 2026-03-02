#!/usr/bin/env python3
"""
stitch_townblocks_pagebreak_continuations_v1_3.py

Hampden Recorded Land TownBlocks stitcher:
- Detects when the LAST extracted event on page P has missing/blank address
- Looks at raw OCR lines for page P+1 and captures the *continuation* Town/Addr (and optionally parties)
- Patches the last event in page P:
    - fills property_refs (supports multiple Town/Addr pairs on the continuation)
    - merges party lines found on continuation
    - strips common OCR tail noise (e.g., trailing " Y")
    - normalizes known town OCR glitches (tight map only)
- Adds evidence + meta flags for auditability

This does NOT attempt to "fix" zoning/attach; it only repairs page-break continuations.
UNKNOWN/NULL remains acceptable; never invent data that isn't present.
"""

import argparse
import json
import os
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# ------------------------
# Regexes (tight + safe)
# ------------------------

# Stop when we hit the first "new row" description block; conservative anchor.
# In Hampden pages this is typically "FILE ..." / "ENV ATTY ..." / etc.
DESCR_LINE_RE = re.compile(r'^(FILE|ENV|AT+Y|ATTY|LAW|C\/O|PO BOX|P\.?\s*O\.?\s*BOX)\b', re.IGNORECASE)

RE_TOWN = re.compile(r'^\s*Town:\s*(.+?)\s*$', re.IGNORECASE)
RE_ADDR_ONLY = re.compile(r'^\s*Addr:\s*(.+?)\s*$', re.IGNORECASE)

# Sometimes Town and Addr appear on one line (rare but keep)
TOWN_ADDR_RE = re.compile(r'Town:\s*([A-Z\*\s]+?)\s+Addr:\s*(.+?)\s*$', re.IGNORECASE)

# Continuation parties lines: "1 P NAME" / "2 C TRUST" etc.
RE_PARTY = re.compile(r'^\s*(\d+)\s+([PC])\s+(.+?)\s*$', re.IGNORECASE)

# Trailing verify/status "Y" often OCRs into addresses
RE_TRAIL_Y = re.compile(r'\s+Y\s*$', re.IGNORECASE)

def is_blank(s: Any) -> bool:
    return s is None or str(s).strip() == ""

def normalize_spaces(s: str) -> str:
    return " ".join(str(s).strip().split())

def normalize_town(t: Optional[str]) -> Optional[str]:
    if not t:
        return t
    s = normalize_spaces(t).upper()

    # Very tight OCR-fix map (only apply when exact/near-exact)
    FIX = {
        "*ALIL": None,  # treat as garbage
        "*ALL": None,   # treat as garbage
        "TOWN: *ALIL": None,
        "TOWN: *ALL": None,
        "BAST LONGMEADOW": "EAST LONGMEADOW",
        "BAST  LONGMEADOW": "EAST LONGMEADOW",
    }
    return FIX.get(s, s)

def normalize_addr(a: Optional[str]) -> Optional[str]:
    if not a:
        return a
    s = normalize_spaces(a)
    # Strip trailing Y noise (verify/status column)
    s = RE_TRAIL_Y.sub("", s).strip()
    # Normalize "UNIT483-2A" -> "UNIT 483-2A"
    s = re.sub(r'\bUNIT(?=\d)', 'UNIT ', s, flags=re.IGNORECASE)
    # Collapse any doubled spaces again
    s = normalize_spaces(s)
    return s

def get_page_index(ev: Dict[str, Any]) -> Optional[int]:
    m = ev.get("meta") or {}
    return m.get("page_index")

def get_record_index(ev: Dict[str, Any]) -> Optional[int]:
    m = ev.get("meta") or {}
    return m.get("record_index")

def event_missing_addr(ev: Dict[str, Any]) -> bool:
    refs = ev.get("property_refs")
    if not refs or not isinstance(refs, list) or len(refs) == 0:
        return True
    addr = (refs[0] or {}).get("address_raw")
    return is_blank(addr)

def add_or_update_property_refs(ev: Dict[str, Any], refs_new: List[Tuple[Optional[str], str]]) -> None:
    """
    refs_new: list of (town, addr) from continuation top-of-page.
    """
    out: List[Dict[str, Any]] = []
    seen = set()

    for idx, (town, addr) in enumerate(refs_new):
        if is_blank(addr):
            continue
        t2 = normalize_town(town) if town else None
        a2 = normalize_addr(addr)
        key = (t2 or "", a2 or "")
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "ref_index": idx,
            "town": t2,
            "address_raw": a2,
            "unit_hint": None,
            "ref_role": "PRIMARY" if idx == 0 else "ADDITIONAL"
        })

    # If we produced any refs, set/overwrite property_refs (we are repairing a broken row)
    if out:
        ev["property_refs"] = out

def merge_parties(ev: Dict[str, Any], parties_new: List[Dict[str, str]]) -> None:
    if not parties_new:
        return
    parties_obj = ev.get("parties") or {}
    cur = parties_obj.get("parties_raw")
    if not isinstance(cur, list):
        cur = []
    seen = set()
    for p in cur:
        try:
            seen.add((str(p.get("side_code_raw","")).strip(), str(p.get("entity_type_raw","")).strip().upper(), str(p.get("name_raw","")).strip()))
        except Exception:
            pass

    for p in parties_new:
        key = (p["side_code_raw"], p["entity_type_raw"], p["name_raw"])
        if key in seen:
            continue
        seen.add(key)
        cur.append({
            "side_code_raw": p["side_code_raw"],
            "entity_type_raw": p["entity_type_raw"],
            "name_raw": p["name_raw"],
        })

    parties_obj["parties_raw"] = cur
    ev["parties"] = parties_obj

def extract_top_continuations(lines_raw: List[Any]) -> Tuple[List[Tuple[Optional[str], str]], List[Dict[str, str]], List[str]]:
    """
    Scan top-of-page raw OCR lines for the continuation of a split transaction.

    Returns:
      refs: list of (town, addr)
      parties: list of party dicts
      captured_lines: list of raw strings captured for evidence/QA
    """
    captured: List[str] = []
    refs: List[Tuple[Optional[str], str]] = []
    parties: List[Dict[str, str]] = []

    current_town: Optional[str] = None

    for ln in (lines_raw or []):
        s = str(ln).strip()
        if not s:
            continue

        # Stop at first new transaction description anchor
        if DESCR_LINE_RE.match(s):
            break

        # Keep evidence once we've begun capturing anything relevant
        # Town-only
        mt = RE_TOWN.match(s)
        if mt:
            current_town = mt.group(1).strip().upper()
            captured.append(s)
            continue

        # Addr-only
        ma = RE_ADDR_ONLY.match(s)
        if ma:
            addr = ma.group(1).strip()
            refs.append((current_town, addr))
            captured.append(s)
            continue

        # Combined line
        mc = TOWN_ADDR_RE.search(s)
        if mc:
            t = mc.group(1).strip().upper()
            a = mc.group(2).strip()
            current_town = t
            refs.append((current_town, a))
            captured.append(s)
            continue

        # Parties
        mp = RE_PARTY.match(s)
        if mp:
            parties.append({
                "side_code_raw": mp.group(1).strip(),
                "entity_type_raw": mp.group(2).strip().upper(),
                "name_raw": normalize_spaces(mp.group(3)),
            })
            captured.append(s)
            continue

        # If we already started capturing, keep a few extra lines (helps debug OCR oddities)
        if captured:
            captured.append(s)

    return refs, parties, captured

def load_ndjson(path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out

def write_ndjson(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def load_raw_lines_ndjson(path: str) -> Dict[int, List[str]]:
    """
    Reads raw_ocr_lines__ALLPAGES.ndjson format:
      {"page_index": 12, ..., "lines_raw": [...]} per line.
    """
    page_lines: Dict[int, List[str]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            p = obj.get("page_index")
            if p is None:
                continue
            lines = obj.get("lines_raw") or []
            page_lines[int(p)] = [str(x) for x in lines]
    return page_lines

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--raw", dest="raw_path", required=True, help="raw_ocr_lines__ALLPAGES.ndjson")
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--qa", dest="qa_path", required=True)
    args = ap.parse_args()

    events = load_ndjson(args.in_path)
    page_lines = load_raw_lines_ndjson(args.raw_path)

    # Index events by page
    by_page: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for ev in events:
        p = get_page_index(ev)
        if p is None:
            continue
        by_page[int(p)].append(ev)

    for p in by_page:
        by_page[p].sort(key=lambda e: (get_record_index(e) or 0))

    pages = sorted(by_page.keys())

    counts = {
        "pages_seen": len(pages),
        "candidate_pagebreaks": 0,
        "stitched": 0,
        "no_continuation_found": 0,
        "missing_raw_lines_for_next_page": 0,
    }
    samples: List[Dict[str, Any]] = []

    for p in pages:
        next_p = p + 1

        # last extracted event on page p
        last_ev = by_page[p][-1]
        if not event_missing_addr(last_ev):
            continue

        counts["candidate_pagebreaks"] += 1

        if next_p not in page_lines:
            counts["missing_raw_lines_for_next_page"] += 1
            continue

        refs_new, parties_new, captured = extract_top_continuations(page_lines.get(next_p, []))

        if not refs_new:
            counts["no_continuation_found"] += 1
            continue

        # Apply
        add_or_update_property_refs(last_ev, refs_new)
        merge_parties(last_ev, parties_new)

        # Evidence
        ev_evidence = last_ev.get("evidence") or {}
        lc = ev_evidence.get("lines_clean")
        if not isinstance(lc, list):
            lc = []
        # keep a short continuation preview for audit
        ev_evidence["pagebreak_continuation_lines"] = captured[:40]
        last_ev["evidence"] = ev_evidence

        # Meta flags
        meta = last_ev.get("meta") or {}
        meta["pagebreak_continuation"] = True
        meta["pagebreak_from_page"] = p
        meta["pagebreak_into_page"] = next_p
        last_ev["meta"] = meta

        counts["stitched"] += 1
        samples.append({
            "from_page": p,
            "into_page": next_p,
            "inst": (last_ev.get("recording") or {}).get("inst_raw"),
            "stitched_addr": (last_ev.get("property_refs") or [{}])[0].get("address_raw") if last_ev.get("property_refs") else None,
            "stitched_town": (last_ev.get("property_refs") or [{}])[0].get("town") if last_ev.get("property_refs") else None,
            "n_parties_added": len(parties_new),
            "captured_preview": captured[:8],
        })

    write_ndjson(args.out_path, events)

    os.makedirs(os.path.dirname(args.qa_path), exist_ok=True)
    with open(args.qa_path, "w", encoding="utf-8") as f:
        json.dump({"counts": counts, "samples": samples[:200]}, f, indent=2)

    print(f"[done] stitched={counts['stitched']} candidates={counts['candidate_pagebreaks']} out={args.out_path} qa={args.qa_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
