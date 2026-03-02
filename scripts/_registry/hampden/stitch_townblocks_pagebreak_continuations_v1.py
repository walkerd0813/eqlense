import argparse
import json
import os
import re
from collections import defaultdict


# Reuse the same patterns as the v1_11 townblocks extractor (keep conservative)
TOWN_ADDR_RE = re.compile(r"\bTown:\s*([A-Z][A-Z\s\-']+?)\s+Addr:\s*([0-9A-Z].*)$", re.IGNORECASE)
DESCR_LINE_RE = re.compile(r"^\s*(\d+)\s+(.*?)\s+\bDEED\b(.*)$", re.IGNORECASE)
PARTY_LINE_RE = re.compile(r"^\s*([12])\s+([PC])\s+(.*)$", re.IGNORECASE)
RE_ADDR_ONLY = re.compile(r'^\s*Addr:\s*(.+?)\s*$', re.I)
RE_TOWN = re.compile(r'^\s*Town:\s*(.+?)\s*$', re.I)


def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def is_blank(v):
    return v is None or (isinstance(v, str) and v.strip() == "")


def event_missing_addr(ev) -> bool:
    refs = ev.get("property_refs")
    if not refs or not isinstance(refs, list) or len(refs) == 0:
        return True
    addr = (refs[0] or {}).get("address_raw")
    return is_blank(addr)

def is_probably_bad_town(t: str) -> bool:
    if not t:
        return True
    s = str(t).strip()
    # OCR garbage often has asterisks or is too short
    if "*" in s:
        return True
    if len(s) < 4:
        return True
    return False

def normalize_town(t: str) -> str:
    if not t:
        return t
    s = str(t).strip().upper()
    s = " ".join(s.split())  # collapse whitespace

    # Very tight OCR-fix map (only apply when exact/near-exact)
    FIX = {
        "BAST LONGMEADOW": "EAST LONGMEADOW",
        "BAST  LONGMEADOW": "EAST LONGMEADOW",
    }
    return FIX.get(s, s)


def get_page_index(ev):
    m = ev.get("meta") or {}
    return m.get("page_index")


def get_record_index(ev):
    m = ev.get("meta") or {}
    return m.get("record_index")


def extract_top_continuation(lines_raw):
    """
    Return (town, addr, captured_lines) from top-of-page lines.

    NEW BEHAVIOR:
    - Accept Town-only lines
    - Accept Addr-only lines
    - Stop at first DESCR line
    - Stitch if addr is found (town optional)
    """
    captured = []
    town = None
    addr = None

    for ln in (lines_raw or []):
        s = str(ln).strip()
        if not s:
            continue

        # Stop at first new row description (new transaction)
        if DESCR_LINE_RE.match(s):
            break

        # Explicit Town: line
        mt = RE_TOWN.match(s)
        if mt and not town:
            town = mt.group(1).strip().upper()
            captured.append(s)
            continue

        # Explicit Addr: line
        ma = RE_ADDR_ONLY.match(s)
        if ma and not addr:
            addr = ma.group(1).strip()
            captured.append(s)
            continue

        # Combined Town/Addr on same line
        m = TOWN_ADDR_RE.search(s)
        if m:
            town = m.group(1).strip().upper()
            addr = m.group(2).strip()
            captured.append(s)
            continue

        if captured:
            captured.append(s)

    # Stitch allowed if we found addr (town optional)
    if addr:
        return town, addr, captured

    return None, None, captured



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True)
    ap.add_argument("--raw_lines_ndjson", required=True,
                    help="Path to raw_ocr_lines__ALLPAGES.ndjson emitted by townblocks v1_11")
    ap.add_argument("--out", required=True)
    ap.add_argument("--qa", required=True)
    args = ap.parse_args()

    events = list(read_ndjson(args.infile))

    # Map page_index -> lines_raw (from raw OCR dump)
    page_lines = {}
    for row in read_ndjson(args.raw_lines_ndjson):
        p = row.get("page_index")
        if p is None:
            continue
        page_lines[p] = row.get("lines_raw") or []

    # Index events by page
    by_page = defaultdict(list)
    for ev in events:
        p = get_page_index(ev)
        if p is None:
            continue
        by_page[p].append(ev)

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
    samples = []

    for p in pages:
        next_p = p + 1
        # We CAN stitch even if next_p has zero events, as long as raw OCR lines exist.


        last_ev = by_page[p][-1]
        if not event_missing_addr(last_ev):
            continue

        counts["candidate_pagebreaks"] += 1

        if next_p not in page_lines:
            counts["missing_raw_lines_for_next_page"] += 1
            continue

        town, addr, captured = extract_top_continuation(page_lines.get(next_p))
        # If next-page town looks like OCR garbage (e.g., "*ALIL"), ignore it
        # and fall back to the prior page's Town: line.
        if is_probably_bad_town(town):
            town = None

        if not town:
            for s in ((last_ev.get("evidence") or {}).get("lines_clean") or []):
                m = RE_TOWN.match(str(s))
                if m:
                    town = m.group(1).strip().upper()
                    break


        if not addr:
            counts["no_continuation_found"] += 1
            continue

        # Fallback: if town missing, try to get from prior event evidence
        if not town:
            for s in ((last_ev.get("evidence") or {}).get("lines_clean") or []):
                m = RE_TOWN.match(str(s))
                if m:
                    town = m.group(1).strip().upper()
                    break

        town = normalize_town(town)

        # Attach address into last_ev
        refs = last_ev.get("property_refs")
        if not refs or not isinstance(refs, list) or len(refs) == 0:
            refs = [{"ref_index": 0, "town": None, "address_raw": None, "unit_hint": None}]
            last_ev["property_refs"] = refs

        refs[0]["town"] = refs[0].get("town") or town
        refs[0]["address_raw"] = addr

        m = last_ev.get("meta")
        if not isinstance(m, dict):
            m = {}
            last_ev["meta"] = m
        m["pagebreak_continuation"] = True
        m["pagebreak_from_page"] = p
        m["pagebreak_into_page"] = next_p
        m["pagebreak_captured_lines"] = captured[:80]

        counts["stitched"] += 1
        if len(samples) < 20:
            samples.append({
                "from_page": p,
                "into_page": next_p,
                "inst": ((last_ev.get("recording") or {}).get("inst_raw")),
                "stitched_addr": addr,
                "captured_preview": captured[:10],
            })

    os.makedirs(os.path.dirname(args.qa), exist_ok=True)
    with open(args.qa, "w", encoding="utf-8") as f:
        json.dump({
            "engine": "stitch_townblocks_pagebreak_continuations_v1",
            "inputs": {"in": args.infile, "raw_lines_ndjson": args.raw_lines_ndjson},
            "counts": counts,
            "samples": samples,
            "note": "Stitches top-of-next-page Town/Addr continuation into last event of prior page ONLY when prior event has blank address. Conservative; no guessing without explicit Town/Addr."
        }, f, ensure_ascii=False, indent=2)

    write_ndjson(args.out, events)
    print(f"[done] stitched={counts['stitched']} candidates={counts['candidate_pagebreaks']} out={args.out} qa={args.qa}")


if __name__ == "__main__":
    main()
