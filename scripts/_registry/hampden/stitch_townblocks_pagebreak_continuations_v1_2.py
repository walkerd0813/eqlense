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


def get_page_index(ev):
    m = ev.get("meta") or {}
    return m.get("page_index")


def get_record_index(ev):
    m = ev.get("meta") or {}
    return m.get("record_index")


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


_RE_TRAILING_Y = re.compile(r"\s+Y\s*$", re.IGNORECASE)
_RE_UNIT_STUCK = re.compile(r"\bUNIT(?=[0-9A-Z])", re.IGNORECASE)


def clean_addr(addr: str) -> str:
    if not addr:
        return addr
    s = str(addr).strip()
    s = _RE_TRAILING_Y.sub("", s)   # stray verify-status 'Y' column
    s = _RE_UNIT_STUCK.sub("UNIT ", s)  # UNIT483-2A -> UNIT 483-2A
    s = " ".join(s.split())
    return s


PARTY_LINE_RE = re.compile(r"^\s*(\d+)\s+([PC])\s+(.+?)\s*$", re.IGNORECASE)


def parse_party_line(s: str):
    m = PARTY_LINE_RE.match(s or "")
    if not m:
        return None
    return {
        "side_code_raw": m.group(1).strip(),
        "entity_type_raw": m.group(2).strip().upper(),
        "name_raw": " ".join(m.group(3).strip().split()),
    }


def extract_top_continuation(lines_raw):
    """
    Return (town, addrs, parties, captured_lines) from top-of-page lines.

    Behavior:
    - Accept Town-only lines
    - Accept Addr-only lines
    - Accept multiple Addr lines (multi-property continuations)
    - Capture party lines if they continue on the next page
    - Stop at first DESCR line (start of next transaction)
    - Stitch if we find at least one addr (town optional)
    """
    captured = []
    town = None
    addrs = []
    parties = []

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

        # Explicit Addr: line(s)
        ma = RE_ADDR_ONLY.match(s)
        if ma:
            a = clean_addr(ma.group(1))
            if a and a not in addrs:
                addrs.append(a)
            captured.append(s)
            continue

        # Combined Town/Addr on same line
        m = TOWN_ADDR_RE.search(s)
        if m:
            if not town:
                town = m.group(1).strip().upper()
            a = clean_addr(m.group(2))
            if a and a not in addrs:
                addrs.append(a)
            captured.append(s)
            continue

        # Party lines on continuation page (when the parties block split across pages)
        p = parse_party_line(s)
        if p:
            parties.append(p)
            captured.append(s)
            continue

        if captured:
            captured.append(s)

    town = normalize_town(town) if town else town

    # Stitch allowed if we found >= 1 addr (town optional)
    if addrs:
        return town, addrs, parties, captured

    return None, [], parties, captured
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
        # We CAN stitch even if next_p has zero extracted events, as long as raw OCR lines exist.
        last_ev = by_page[p][-1]
        if not event_missing_addr(last_ev):
            continue

        counts["candidate_pagebreaks"] += 1

        if next_p not in page_lines:
            counts["missing_raw_lines_for_next_page"] += 1
            continue

        town, addrs, cont_parties, captured = extract_top_continuation(page_lines.get(next_p))
        if not addrs:
            counts["no_continuation_found"] += 1
            continue

        # Fallback: if town missing/bad, try to get from prior event evidence
        if is_probably_bad_town(town):
            for s in ((last_ev.get("evidence") or {}).get("lines_clean") or []):
                mt = RE_TOWN.match(str(s))
                if mt:
                    town = mt.group(1).strip().upper()
                    break
            town = normalize_town(town) if town else town

        # Attach property refs into last_ev (support multi-property continuations)
        def _split_unit(a: str):
            if not a:
                return a, None
            m = re.search(r"\bUNIT\s+(.+)$", a, flags=re.IGNORECASE)
            if not m:
                return a, None
            base = a[:m.start()].strip()
            unit = ("UNIT " + m.group(1).strip()).strip()
            # Avoid empty base; if base empty keep full string
            if not base:
                return a, None
            return base, unit

        new_refs = []
        for i_addr, a0 in enumerate(addrs):
            base_addr, unit_hint = _split_unit(a0)
            new_refs.append({
                "ref_index": i_addr,
                "town": normalize_town(town) if town else None,
                "address_raw": base_addr,
                "unit_hint": unit_hint,
                "ref_role": "PRIMARY" if i_addr == 0 else "ADDITIONAL",
            })

        last_ev["property_refs"] = new_refs

        # Attach continuation parties into last_ev if present (de-dupe)
        if cont_parties:
            parties_obj = last_ev.get("parties")
            if not isinstance(parties_obj, dict):
                parties_obj = {}
                last_ev["parties"] = parties_obj
            pr = parties_obj.get("parties_raw")
            if not isinstance(pr, list):
                pr = []
                parties_obj["parties_raw"] = pr

            seen = set()
            for x in pr:
                try:
                    seen.add((x.get("side_code_raw"), x.get("entity_type_raw"), x.get("name_raw")))
                except Exception:
                    pass
            for x in cont_parties:
                k = (x.get("side_code_raw"), x.get("entity_type_raw"), x.get("name_raw"))
                if k not in seen:
                    pr.append(x)
                    seen.add(k)

        m = last_ev.get("meta")
        if not isinstance(m, dict):
            m = {}
            last_ev["meta"] = m
        m["pagebreak_continuation"] = True
        m["pagebreak_from_page"] = p
        m["pagebreak_into_page"] = next_p
        m["pagebreak_captured_lines"] = captured[:120]

        counts["stitched"] += 1
        if len(samples) < 20:
            samples.append({
                "from_page": p,
                "into_page": next_p,
                "inst": ((last_ev.get("recording") or {}).get("inst_raw")),
                "stitched_addr": addrs[0],
                "captured_preview": captured[:12],
                "n_addrs": len(addrs),
                "n_cont_parties": len(cont_parties) if cont_parties else 0,
            })
    os.makedirs(os.path.dirname(args.qa), exist_ok=True)
    with open(args.qa, "w", encoding="utf-8") as f:
        json.dump({
            "engine": "stitch_townblocks_pagebreak_continuations_v1_2",
            "inputs": {"in": args.infile, "raw_lines_ndjson": args.raw_lines_ndjson},
            "counts": counts,
            "samples": samples,
            "note": "Stitches top-of-next-page Town/Addr continuation into last event of prior page ONLY when prior event has blank address. Conservative; no guessing without explicit Town/Addr."
        }, f, ensure_ascii=False, indent=2)

    write_ndjson(args.out, events)
    print(f"[done] stitched={counts['stitched']} candidates={counts['candidate_pagebreaks']} out={args.out} qa={args.qa}")


if __name__ == "__main__":
    main()