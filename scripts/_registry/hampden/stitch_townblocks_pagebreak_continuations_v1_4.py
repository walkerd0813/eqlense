#!/usr/bin/env python3
"""
stitch_townblocks_pagebreak_continuations_v1_4.py

Goal:
- Fix TownBlocks OCR events where the *last* transaction on a page is missing address / property_refs
  because its Town/Addr lines appear at the top of the next PDF page (after the page header).

v1_4 upgrades vs v1_3:
1) Capture *multiple* Town/Addr lines at the top of the next page and append them as property_refs
   (PRIMARY if missing, ADDITIONAL if already present).
2) Capture party lines at the top of the next page (e.g. "2 C SOME BANK") and append into
   parties.parties_raw for the continued event.
3) Normalize common OCR town glitches (e.g. "*ALIL" -> UNKNOWN, "BAST LONGMEADOW" -> "EAST LONGMEADOW").
4) Strip trailing " Y" artifacts from address lines ("... TERR Y") like TownBlocks does.
5) CLI compatibility: accepts BOTH (--raw) and (--raw_lines_ndjson) and BOTH (--in) and (--in_path).

PS51SAFE: deterministic, no external deps.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# -------------------------
# Regex (tight, conservative)
# -------------------------
RE_TOWN = re.compile(r'^\s*Town:\s*(.+?)\s*$', re.IGNORECASE)
# "Addr:..." line only
RE_ADDR_ONLY = re.compile(r'^\s*Addr:\s*(.+?)\s*$', re.IGNORECASE)
# Combined on one line in some OCR outputs: "Town: X Addr:Y"
TOWN_ADDR_RE = re.compile(r'Town:\s*(.+?)\s+Addr:\s*(.+?)\s*$', re.IGNORECASE)
# "SEQ DESCR/LOC/ ..." style header line (signals top-of-page header)
DESCR_LINE_RE = re.compile(r'^\s*SEQ\s+DESCR/LOC', re.IGNORECASE)

# Party line at top of next page, e.g. "2 C WILMINGTON SAVINGS FUND SOCIETY"
PARTY_LINE_RE = re.compile(r'^\s*(\d{1,2})\s+([PC])\s+(.+?)\s*$', re.IGNORECASE)

# When the real next transaction begins, TownBlocks uses the "FILE SIMPLIFILE ..." anchor often.
# We'll treat this as a hard stop for continuation capture (very conservative).
STOP_FILE_ANCHOR_RE = re.compile(r'^\s*FILE\s+SIMPLIF', re.IGNORECASE)

# Trailing " Y" is the right-edge column bleed (Yes/No column). Strip it.
RE_TRAILING_Y = re.compile(r'\s+Y\s*$', re.IGNORECASE)

# -------------------------
# IO helpers
# -------------------------
def read_ndjson(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def write_ndjson(path: str, rows: List[dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def is_blank(x) -> bool:
    return x is None or str(x).strip() == ""

def strip_trailing_yes(s: str) -> str:
    if s is None:
        return s
    return RE_TRAILING_Y.sub("", str(s)).strip()

# -------------------------
# Town normalization (tight map only)
# -------------------------
def normalize_town(t: Optional[str]) -> Optional[str]:
    if not t:
        return t
    s = str(t).strip().upper()
    s = " ".join(s.split())
    # remove leading/trailing asterisks that show up in OCR (*ALIL, *ALL, etc.)
    s = s.strip("*").strip()
    s = " ".join(s.split())
    FIX = {
        "BAST LONGMEADOW": "EAST LONGMEADOW",
        "BAST  LONGMEADOW": "EAST LONGMEADOW",
    }
    return FIX.get(s, s)

def is_probably_bad_town(t: Optional[str]) -> bool:
    if not t:
        return True
    s = str(t).strip()
    if "*" in s:
        return True
    if len(s) < 4:
        return True
    return False

# -------------------------
# Event access
# -------------------------
def get_page_index(ev: dict) -> Optional[int]:
    m = ev.get("meta") or {}
    return m.get("page_index")

def get_record_index(ev: dict) -> Optional[int]:
    m = ev.get("meta") or {}
    return m.get("record_index")

def event_missing_addr(ev: dict) -> bool:
    refs = ev.get("property_refs")
    if not refs or not isinstance(refs, list) or len(refs) == 0:
        return True
    addr = (refs[0] or {}).get("address_raw")
    return is_blank(addr)

def ensure_property_refs(ev: dict) -> List[dict]:
    refs = ev.get("property_refs")
    if not isinstance(refs, list):
        refs = []
        ev["property_refs"] = refs
    return refs

def ensure_parties(ev: dict) -> dict:
    p = ev.get("parties")
    if not isinstance(p, dict):
        p = {}
        ev["parties"] = p
    if "parties_raw" not in p or not isinstance(p.get("parties_raw"), list):
        p["parties_raw"] = []
    return p

# -------------------------
# Continuation extraction from top-of-next-page raw OCR lines
# -------------------------
def extract_top_continuation(lines_raw: List[str], max_scan: int = 80) -> Tuple[List[Tuple[Optional[str], str]], List[dict], List[str]]:
    """
    Returns:
      refs_found: list of (town, addr) pairs (addr required, town optional)
      parties_found: list of parties_raw dicts
      captured_lines: list[str] (for QA preview)
    """
    captured: List[str] = []
    refs_found: List[Tuple[Optional[str], str]] = []
    parties_found: List[dict] = []

    current_town: Optional[str] = None

    for ln in (lines_raw or [])[:max_scan]:
        s0 = str(ln).strip()
        if not s0:
            continue

        # Stop when next transaction likely begins
        if DESCR_LINE_RE.match(s0) or STOP_FILE_ANCHOR_RE.match(s0):
            break

        # collect party lines
        pm = PARTY_LINE_RE.match(s0)
        if pm:
            side = pm.group(1).strip()
            ent  = pm.group(2).strip().upper()
            name = strip_trailing_yes(pm.group(3).strip())
            parties_found.append({"side_code_raw": side, "entity_type_raw": ent, "name_raw": name})
            captured.append(s0)
            continue

        # Town line
        mt = RE_TOWN.match(s0)
        if mt:
            current_town = normalize_town(mt.group(1))
            captured.append(s0)
            continue

        # Addr-only line
        ma = RE_ADDR_ONLY.match(s0)
        if ma:
            addr = strip_trailing_yes(ma.group(1))
            if addr:
                refs_found.append((current_town, addr))
                captured.append(s0)
            continue

        # Combined Town/Addr
        mc = TOWN_ADDR_RE.search(s0)
        if mc:
            town = normalize_town(mc.group(1))
            addr = strip_trailing_yes(mc.group(2))
            if addr:
                refs_found.append((town, addr))
                current_town = town
                captured.append(s0)
            continue

        # allow additional lines only after we've started capturing something (keeps QA helpful)
        if captured:
            captured.append(s0)

    return refs_found, parties_found, captured

# -------------------------
# Stitch application
# -------------------------
def stitch_event(last_ev: dict, from_page: int, into_page: int, refs_found: List[Tuple[Optional[str], str]], parties_found: List[dict], captured: List[str], counts: dict, samples: List[dict]):
    # property refs
    refs = ensure_property_refs(last_ev)

    before_n_refs = len(refs)
    added_refs = 0

    # If the event has zero refs or blank addr, treat first found ref as PRIMARY.
    def has_primary_addr():
        if not refs:
            return False
        return not is_blank((refs[0] or {}).get("address_raw"))

    for i, (town, addr) in enumerate(refs_found):
        town_n = normalize_town(town)
        if is_probably_bad_town(town_n):
            town_n = None

        ref_obj = {
            "ref_index": len(refs),
            "town": town_n,
            "address_raw": addr,
            "unit_hint": None,
            "ref_role": "PRIMARY" if (i == 0 and not has_primary_addr()) else "ADDITIONAL",
        }

        # If we have 0 refs, create list and set PRIMARY
        if not refs:
            refs.append(ref_obj)
            added_refs += 1
            continue

        # If first addr blank, overwrite it with the first captured (PRIMARY)
        if is_blank((refs[0] or {}).get("address_raw")) and i == 0:
            refs[0]["town"] = ref_obj["town"]
            refs[0]["address_raw"] = ref_obj["address_raw"]
            refs[0]["unit_hint"] = refs[0].get("unit_hint")
            refs[0]["ref_role"] = "PRIMARY"
            added_refs += 1
            continue

        # avoid duplicates (same town+addr)
        dup = False
        for r in refs:
            if normalize_town(r.get("town")) == town_n and str(r.get("address_raw") or "").strip().upper() == addr.strip().upper():
                dup = True
                break
        if dup:
            continue

        refs.append(ref_obj)
        added_refs += 1

    # parties
    parties_added = 0
    if parties_found:
        p = ensure_parties(last_ev)
        existing = p.get("parties_raw") or []
        # build a simple dedupe key on (side, ent, name)
        existing_keys = set()
        for e in existing:
            existing_keys.add((str(e.get("side_code_raw") or "").strip(), str(e.get("entity_type_raw") or "").strip().upper(), str(e.get("name_raw") or "").strip().upper()))
        for pf in parties_found:
            k = (str(pf.get("side_code_raw") or "").strip(), str(pf.get("entity_type_raw") or "").strip().upper(), str(pf.get("name_raw") or "").strip().upper())
            if k in existing_keys:
                continue
            existing.append(pf)
            existing_keys.add(k)
            parties_added += 1
        p["parties_raw"] = existing

    # meta flags
    m = last_ev.get("meta")
    if not isinstance(m, dict):
        m = {}
        last_ev["meta"] = m

    m["pagebreak_continuation"] = True
    m["pagebreak_from_page"] = from_page
    m["pagebreak_into_page"] = into_page
    m["pagebreak_refs_added"] = added_refs
    m["pagebreak_parties_added"] = parties_added
    if captured:
        m["pagebreak_captured_preview"] = captured[:12]

    counts["stitched"] += 1
    if len(samples) < 50:
        samples.append({
            "from_page": from_page,
            "into_page": into_page,
            "refs_added": added_refs,
            "parties_added": parties_added,
            "stitched_addr": refs[0].get("address_raw") if refs else None,
            "stitched_town": refs[0].get("town") if refs else None,
            "captured_preview": captured[:12],
        })

# -------------------------
def main():
    ap = argparse.ArgumentParser()
    # input
    ap.add_argument("--in", dest="in_path", required=False, help="TownBlocks events ndjson")
    ap.add_argument("--in_path", dest="in_path_alt", required=False, help="alias")
    # raw lines
    ap.add_argument("--raw", dest="raw_path", required=False, help="raw_ocr_lines__ALLPAGES.ndjson")
    ap.add_argument("--raw_lines_ndjson", dest="raw_path_alt", required=False, help="alias used by older runners")
    # output
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--qa", dest="qa_path", required=True)
    ap.add_argument("--max_scan", type=int, default=80)
    args = ap.parse_args()

    in_path = args.in_path or args.in_path_alt
    raw_path = args.raw_path or args.raw_path_alt
    if not in_path:
        ap.error("missing --in (or --in_path)")
    if not raw_path:
        ap.error("missing --raw (or --raw_lines_ndjson)")

    events = list(read_ndjson(in_path))

    # raw lines indexed by page
    page_lines: Dict[int, List[str]] = {}
    for row in read_ndjson(raw_path):
        try:
            p = int(row.get("page_index"))
        except Exception:
            continue
        lines = row.get("lines_raw") or []
        if isinstance(lines, list):
            page_lines[p] = lines

    # index events by page
    by_page: Dict[int, List[dict]] = defaultdict(list)
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
        "notes": "v1_4 stitches refs+parties from top-of-next-page raw OCR lines; supports --raw and --raw_lines_ndjson."
    }
    samples: List[dict] = []

    for p in pages:
        next_p = p + 1

        last_ev = by_page[p][-1]
        if not event_missing_addr(last_ev):
            continue

        counts["candidate_pagebreaks"] += 1

        if next_p not in page_lines:
            counts["missing_raw_lines_for_next_page"] += 1
            continue

        refs_found, parties_found, captured = extract_top_continuation(page_lines.get(next_p) or [], max_scan=args.max_scan)

        if not refs_found:
            counts["no_continuation_found"] += 1
            continue

        stitch_event(last_ev, from_page=p, into_page=next_p, refs_found=refs_found, parties_found=parties_found, captured=captured, counts=counts, samples=samples)

    # write
    write_ndjson(args.out_path, events)
    os.makedirs(os.path.dirname(args.qa_path), exist_ok=True)
    with open(args.qa_path, "w", encoding="utf-8") as f:
        json.dump({
            "engine": "stitch_townblocks_pagebreak_continuations_v1_4",
            "inputs": {"in": in_path, "raw": raw_path},
            "counts": counts,
            "samples": samples,
        }, f, indent=2)

    print(f"[done] stitched={counts['stitched']} candidates={counts['candidate_pagebreaks']} out={args.out_path} qa={args.qa_path}")

if __name__ == "__main__":
    main()
