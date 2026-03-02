#!/usr/bin/env python3
# stitch_townblocks_pagebreak_continuations_v1_5_7.py
#
# Universal stitcher:
# - PAGEBREAK stitching from top-of-next-page raw OCR lines (refs + parties; parties-only supported)
# - SAME_PAGE_REPAIR for missing refs, but STRICTLY record-scoped using structured TB events (no raw-page scanning)
#
# This prevents "page vacuum" contamination like pg=53 rec=6 absorbing refs from rec=1..5.

import argparse
import json
import os
import re
from typing import Dict, List, Optional, Tuple, Any

# -------------------------
# Patch A: raw-line record segmentation boundaries
# -------------------------

# Some earlier patches referred to RE_TX_BOUNDARY; define it here to match your Pylance expectation.
# Hampden headers show "FILE SIMPLIFILE E-RECORDING" as the record boundary marker.
RE_TX_BOUNDARY = re.compile(r"\bFILE\s+SIMPLIFILE\b", re.IGNORECASE)
# Town/Addr lines inside raw OCR blocks
RE_RAW_TOWN_ADDR = re.compile(r"Town\s*:\s*(.+?)\s+Addr\s*:\s*(.+)$", re.IGNORECASE)

def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _normalize_town_ocr(town: str) -> str:
    """
    Tiny OCR cleanup only (NOT full normalization).
    Fix the known recurring error: 'BAST LONGMEADOW' -> 'EAST LONGMEADOW'
    Also strip '*' and collapse whitespace.
    """
    t = _normalize_ws(town).upper().replace("*", "")
    if t == "BAST LONGMEADOW":
        return "EAST LONGMEADOW"
    if t == "HAMPDE":
        return "HAMPDEN"
    return t

def _split_lines_into_record_segments(lines_raw: List[str]) -> Dict[int, List[str]]:
    """
    Returns dict {record_index:int -> [lines]} based on FILE SIMPLIFILE boundaries.
    Segment numbering starts at 1 to match TownBlocks record_index.
    """
    segs: Dict[int, List[str]] = {}
    cur: List[str] = []
    seg_idx = 0

    for ln in (lines_raw or []):
        s = str(ln).strip()
        if not s:
            continue

        if RE_TX_BOUNDARY.search(s):
            # start a new segment
            if cur and seg_idx > 0:
                segs[seg_idx] = cur
            seg_idx += 1
            cur = [s]
        else:
            if seg_idx == 0:
                # ignore page header/preamble before first FILE SIMPLIFILE
                continue
            cur.append(s)

    if cur and seg_idx > 0:
        segs[seg_idx] = cur

    return segs

def _extract_property_refs_from_lines(lines: List[str]) -> List[dict]:
    """
    Parse Town/Addr lines only from the provided segment lines.
    """
    refs: List[dict] = []
    for ln in (lines or []):
        m = RE_RAW_TOWN_ADDR.search(ln)
        if not m:
            continue

        town = _normalize_town_ocr(m.group(1))
        addr = _normalize_ws(m.group(2))

        if not town and not addr:
            continue

        refs.append({
            "ref_index": len(refs),
            "town": town,
            "address_raw": addr,
            "unit_hint": None,
            "ref_role": "PRIMARY" if len(refs) == 0 else "ADDITIONAL",
        })

    return dedupe_refs(refs)

# -------------------------
# NDJSON helpers
# -------------------------

def read_ndjson(path: str) -> List[dict]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out

def write_ndjson(path: str, rows: List[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# -------------------------
# TB event accessors
# -------------------------

def get_meta(ev: dict) -> dict:
    return ev.get("meta") or {}

def get_page_index(ev: dict) -> int:
    return int((get_meta(ev).get("page_index") or 0))

def get_record_index(ev: dict) -> int:
    return int((get_meta(ev).get("record_index") or 0))

def get_inst_raw(ev: dict) -> Optional[str]:
    """
    Best-effort instrument number accessor.
    If TownBlocks event already contains inst in meta/recording/document, we use it.
    Returns digits-only string or None.
    """
    inst = None

    rec = ev.get("recording") or {}
    doc = ev.get("document") or {}
    m = ev.get("meta") or {}

    for cand in [
        rec.get("inst_raw"),
        rec.get("instrument_raw"),
        rec.get("instrument_number"),
        doc.get("inst_raw"),
        doc.get("instrument_number"),
        m.get("inst_raw"),
        m.get("instrument_number"),
    ]:
        if cand is not None and str(cand).strip():
            inst = str(cand).strip()
            break

    if not inst:
        return None

    digits = re.sub(r"\D+", "", inst)
    return digits if digits else None


def get_property_refs(ev: dict) -> List[dict]:
    refs = ev.get("property_refs")
    if refs is None:
        return []
    if isinstance(refs, list):
        return refs
    return []

def get_parties_raw_list(ev: dict) -> List[str]:
    parties = ev.get("parties") or {}
    # expected: {"parties_raw":[...]} OR {"parties_raw": "..."} OR {"parties":[...]}
    pr = parties.get("parties_raw")
    if isinstance(pr, list):
        return [str(x) for x in pr if str(x).strip()]
    if isinstance(pr, str) and pr.strip():
        return [pr.strip()]
    # fallback
    pr2 = parties.get("parties")
    if isinstance(pr2, list):
        return [str(x) for x in pr2 if str(x).strip()]
    return []

def ensure_stitch_meta(ev: dict) -> dict:
    m = ev.setdefault("meta", {})
    stitch = m.setdefault("stitch", {})
    return stitch

def dedupe_refs(refs: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for r in refs:
        if not isinstance(r, dict):
            continue
        town = (r.get("town") or "").strip().upper()
        addr = (r.get("address_raw") or "").strip().upper()
        key = (town, addr)
        if not town and not addr:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def looks_like_ref_rollup(refs: List[dict], max_refs_ok: int = 3) -> bool:
    """
    Heuristic: if a row contains "too many" refs it is likely a bad SAME_PAGE_REPAIR rollup
    (vacuum row) and should never be used as a donor.
    """
    if not refs:
        return False

    towns = set()
    n = 0

    for r in refs:
        if not isinstance(r, dict):
            continue
        town = (r.get("town") or "").strip()
        addr = (r.get("address_raw") or "").strip()

        if town or addr:
            n += 1
        if town:
            towns.add(town)

    # Too many refs OR many towns = unsafe donor
    return (n > max_refs_ok) or (n >= (max_refs_ok + 2) and len(towns) >= 2)



    # many refs + multiple towns = very likely a vacuum/rollup row
    return (len(refs) >= (max_refs_ok + 2)) and (len(towns) >= 2)


def event_missing_refs(ev: dict) -> bool:
    refs = get_property_refs(ev)
    if not refs:
        return True
    # also treat "all empty" refs as missing
    ok = False
    for r in refs:
        if not isinstance(r, dict):
            continue
        if (r.get("town") or "").strip() or (r.get("address_raw") or "").strip():
            ok = True
            break
    return not ok

def event_missing_parties(ev: dict) -> bool:
    return len(get_parties_raw_list(ev)) == 0

# -------------------------
# PAGEBREAK continuation extraction (raw OCR lines)
# -------------------------

RE_TOWN_ADDR = re.compile(r"(?i)\bTown\s*:\s*([A-Z\*\- ]{2,})\s+Addr\s*:\s*(.+?)\s*(?:\bY\b)?\s*$")
RE_ADDR_ONLY = re.compile(r"(?i)\bAddr\s*:\s*(.+?)\s*$")



def normalize_town(town_raw: str) -> str:
    t = (town_raw or "").strip().upper()
    t = re.sub(r"\s+", " ", t)
    t = t.replace("*", "")
    # NOTE: real town normalization belongs upstream; this is just safety.
    return t

def extract_top_continuation(lines_raw: List[str], max_scan: int = 80) -> Tuple[List[Tuple[Optional[str], str]], List[str], List[str]]:
    """
    Returns:
      refs_found: list of (town, address_raw) where town may be None if only Addr: lines exist
      parties_found: list of party lines
      captured: captured lines scanned (for QA)
    """
    captured: List[str] = []
    refs_found: List[Tuple[Optional[str], str]] = []
    parties_found: List[str] = []
    started = False
    
    # Scan top of page until we find at least one meaningful signal.
    for i, raw in enumerate(lines_raw[:max_scan]):
        s = str(raw).strip()
        if not s:
            continue

        # boundary marker (used when inst is missing)
        if RE_TX_BOUNDARY.search(s):
            if started:
                # next transaction begins -> STOP (prevents page vacuum)
                break
            started = True
            captured.append(s)
            continue


        # ignore page headers until we hit the first tx boundary
        if not started:
            continue

        captured.append(s)

        # refs
        m = RE_TOWN_ADDR.search(s)
        if m:
            town = normalize_town(m.group(1))
            addr = (m.group(2) or "").strip()
            if addr:
                refs_found.append((town, addr))
            continue

        m2 = RE_ADDR_ONLY.search(s)
        if m2:
            addr = (m2.group(1) or "").strip()
            if addr:
                refs_found.append((None, addr))
            continue

        # parties
        if re.search(r"(?i)\b[12]\s*[PC]\b", s) or re.search(r"(?i)\b(TR\b|\bTRUST\b|\bLLC\b|\bINC\b)", s):
            if "RECORDED LAND" in s.upper():
                continue
            if "INQUIRY" in s.upper() or "PRINTED" in s.upper() or "RG" in s.upper():
                continue
            parties_found.append(s)

    # Post-process refs: if we have Addr-only lines, try to inherit most recent Town from earlier Town/Addr
    last_town: Optional[str] = None
    final_refs: List[Tuple[Optional[str], str]] = []
    for town, addr in refs_found:
        if town:
            last_town = town
            final_refs.append((town, addr))
        else:
            final_refs.append((last_town, addr))

    # Convert to TB-like ref dicts later
    return final_refs, parties_found, captured

def refs_to_dicts(ref_pairs: List[Tuple[Optional[str], str]]) -> List[dict]:
    out = []
    for idx, (town, addr) in enumerate(ref_pairs):
        out.append({
            "ref_index": idx,
            "town": town,
            "address_raw": addr,
            "unit_hint": None,
            "ref_role": "PRIMARY" if idx == 0 else "ADDITIONAL",
        })
    return dedupe_refs(out)

# -------------------------
# SAME_PAGE_REPAIR using structured TB events only
# -------------------------

def party_tokens(parties_raw: List[str]) -> set:
    toks = set()
    for p in parties_raw:
        s = re.sub(r"[^A-Z0-9 ]+", " ", (p or "").upper())
        s = re.sub(r"\s+", " ", s).strip()
        # keep longer tokens; drop noise
        for t in s.split(" "):
            if len(t) >= 4 and t not in {"TRUST", "LLC", "INC", "BANK", "ASSOC", "THE"}:
                toks.add(t)
    return toks

def party_overlap_score(p1: List[str], p2: List[str]) -> float:
    a = party_tokens(p1)
    b = party_tokens(p2)
    if not a or not b:
        return 0.0
    inter = len(a.intersection(b))
    denom = max(1, min(len(a), len(b)))
    return inter / denom

def same_page_repair(page_events: List[dict], max_record_lookahead: int, min_overlap: float) -> Tuple[int, List[dict]]:
    """
    Attempts to repair missing refs for events on a page by borrowing from nearby TB events
    ONLY when highly likely they are continuations of the same transaction.
    Returns (repairs_count, samples)
    """
    repairs = 0
    samples: List[dict] = []
    consumed = set()  # record_indexes we treat as continuation donors (optional; we do NOT delete rows)

    for i, ev in enumerate(page_events):
        if not event_missing_refs(ev):
            continue

        pg = get_page_index(ev)
        rec = get_record_index(ev)
        inst1 = get_inst_raw(ev)
        p1 = get_parties_raw_list(ev)


        # search candidates in next few TB records
        best_j = None
        best_score = 0.0

        for j in range(i + 1, min(len(page_events), i + 1 + max_record_lookahead)):
            ev2 = page_events[j]
            rec2 = get_record_index(ev2)
            inst2 = get_inst_raw(ev2)
            # PATCH C: If we do NOT have an instrument number on the target row,
            # we refuse multi-record lookahead. Only allow borrowing from the immediate next record.
            # This prevents "vacuum / rollup" contamination from sweeping the page.
            if not inst1:
                if rec2 != rec + 1:
                    continue

            # HARD boundary: if both instrument numbers exist and do not match, NEVER borrow
            inst1n = (inst1 or "").strip()
            inst2n = (inst2 or "").strip()

            if inst1n and inst2n and inst1n != inst2n:
                continue

            if rec2 in consumed:
                continue

            refs2 = get_property_refs(ev2)
            if not refs2:
                continue

            # never borrow from an obvious vacuum/rollup donor
            if looks_like_ref_rollup(refs2, max_refs_ok=3):
                continue

            

            p2 = get_parties_raw_list(ev2)

            score = party_overlap_score(p1, p2)

            # Special-case: continuation row may have refs but missing parties (common)
            if score == 0.0 and p1 and not p2:
                # require it be immediately next record to be safe
                if rec2 == rec + 1:
                    score = 0.51  # barely above threshold

            if score > best_score:
                best_score = score
                best_j = j

        if best_j is None or best_score < min_overlap:
            continue

        donor = page_events[best_j]
        donor_rec = get_record_index(donor)

        # merge refs
        merged = dedupe_refs(get_property_refs(ev) + get_property_refs(donor))
        if not merged:
            continue

        ev["property_refs"] = merged

        # if parties missing, optionally merge parties (but do NOT expand if already present)
        if event_missing_parties(ev) and not event_missing_parties(donor):
            ev.setdefault("parties", {})
            ev["parties"]["parties_raw"] = get_parties_raw_list(donor)

        stitch = ensure_stitch_meta(ev)
        stitch["did_stitch"] = True
        stitch["continuation_type"] = "SAME_PAGE_REPAIR"
        stitch["into_page"] = pg
        stitch["from_page"] = pg
        stitch["source"] = {
            "method": "STRUCTURED_TB_LOOKAHEAD",
            "donor_record_index": donor_rec,
            "party_overlap_score": round(best_score, 3),
        }

        consumed.add(donor_rec)
        repairs += 1

        if len(samples) < 20:
            samples.append({
                "page_index": pg,
                "record_index": rec,
                "ctype": "SAME_PAGE_REPAIR",
                "donor_record_index": donor_rec,
                "party_overlap_score": round(best_score, 3),
                "refs_after": len(get_property_refs(ev)),
                "parties_after": len(get_parties_raw_list(ev)),
            })

    return repairs, samples

# -------------------------
# PAGEBREAK stitching (raw OCR lines)
# -------------------------

def pagebreak_stitch(events_by_page: Dict[int, List[dict]],
                    page_lines: Dict[int, List[str]],
                    max_scan: int,
                    counts: dict,
                    samples: List[dict]) -> None:
    pages = sorted(events_by_page.keys())
    for p in pages:
        next_p = p + 1
        last_ev = events_by_page[p][-1]

        # candidate if last event missing refs
        if not event_missing_refs(last_ev):
            continue

        counts["candidate_pagebreaks"] += 1

        if next_p not in page_lines:
            counts["missing_raw_lines_for_next_page"] += 1
            continue

                # PATCH B: record-scoped PAGEBREAK stitching.
        # Only use raw lines belonging to record_index=1 on the next page (top record segment),
        # delimited by FILE SIMPLIFILE.
        next_lines_all = page_lines.get(next_p) or []
        segs = _split_lines_into_record_segments(next_lines_all)

        # Continuation from prior page should land in the first record block on the next page.
        seg_lines = segs.get(1) or []

        # Fallback: if we can't segment, fall back to old behavior (still bounded by max_scan),
        # but segmentation is the preferred safe path.
        lines_for_scan = seg_lines if seg_lines else next_lines_all

        refs_pairs, parties_found, captured = extract_top_continuation(lines_for_scan, max_scan=max_scan)

        # If segmentation exists, we can additionally extract Town/Addr refs directly from that segment
        # (more reliable than regex inheritance).
        seg_refs = _extract_property_refs_from_lines(seg_lines) if seg_lines else []


        refs_found = seg_refs if seg_refs else refs_to_dicts(refs_pairs)
        did_refs = len(refs_found) > 0
        did_parties = len(parties_found) > 0

        # allow parties-only stitch (your v1_5_5_1 behavior)
        if not did_refs and not did_parties:
            counts["no_continuation_found"] += 1
            continue

        if not did_refs and did_parties:
            counts["stitched_parties_only"] += 1

        # merge refs and parties
        if did_refs:
            last_ev["property_refs"] = dedupe_refs(get_property_refs(last_ev) + refs_found)

        if did_parties:
            if not isinstance(last_ev.get("parties"), dict):
                last_ev["parties"] = {}
            existing = get_parties_raw_list(last_ev)
            if not existing:
                last_ev["parties"]["parties_raw"] = parties_found

        stitch = ensure_stitch_meta(last_ev)
        stitch["did_stitch"] = True
        stitch["continuation_type"] = "PAGEBREAK" if did_refs else "PAGEBREAK_PARTIES_ONLY"
        stitch["from_page"] = p
        stitch["into_page"] = next_p
        stitch["captured_top_lines"] = captured[:min(len(captured), 50)]

        counts["stitched"] += 1

        if len(samples) < 20:
            samples.append({
                "page_index": p,
                "record_index": get_record_index(last_ev),
                "ctype": stitch["continuation_type"],
                "into_page": next_p,
                "refs_after": len(get_property_refs(last_ev)),
                "parties_after": len(get_parties_raw_list(last_ev)),
            })

# -------------------------
# main
# -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="TownBlocks events NDJSON")
    ap.add_argument("--out", dest="out_path", required=True, help="Output stitched events NDJSON")
    ap.add_argument("--qa", dest="qa_path", required=True, help="QA json output")
    ap.add_argument("--raw_lines_ndjson", dest="raw_lines_ndjson", default=None,
                    help="NDJSON with page_index + lines_raw (raw OCR lines); used for PAGEBREAK stitching only")
    ap.add_argument("--max_scan", type=int, default=80)
    ap.add_argument("--same_page_lookahead", type=int, default=0)
    ap.add_argument("--same_page_min_overlap", type=float, default=0.60)  # conservative
    args = ap.parse_args()

    in_path = args.in_path
    out_path = args.out_path
    qa_path = args.qa_path

    events = read_ndjson(in_path)

    # group by page + sort by record
    by_page: Dict[int, List[dict]] = {}
    for ev in events:
        pg = get_page_index(ev)
        by_page.setdefault(pg, []).append(ev)

    for pg in by_page:
        by_page[pg].sort(key=lambda e: get_record_index(e))

    # load raw lines (page-scoped) if provided
    page_lines: Dict[int, List[str]] = {}
    raw_path = args.raw_lines_ndjson
    if raw_path:
        raw_rows = read_ndjson(raw_path)
        for r in raw_rows:
            pg = int(r.get("page_index") or 0)
            lines = r.get("lines_raw") or r.get("lines") or []
            if isinstance(lines, list):
                page_lines[pg] = [str(x) for x in lines]

    counts = {
        "pages_seen": len(by_page),
        "candidate_pagebreaks": 0,
        "stitched": 0,
        "stitched_parties_only": 0,
        "same_page_repair": 0,
        "no_continuation_found": 0,
        "missing_raw_lines_for_next_page": 0,
        "notes": (
            "v1_5_7: PAGEBREAK stitch uses raw page lines; SAME_PAGE_REPAIR is record-scoped via structured TB lookahead "
            "(party overlap / parties-missing continuation heuristic). No raw-page scanning for same-page repair."
        ),
    }
    samples: List[dict] = []

    # 1) pagebreak stitch (only needs page-scoped raw lines)
    if raw_path:
        pagebreak_stitch(by_page, page_lines, max_scan=args.max_scan, counts=counts, samples=samples)

        # 2) SAME_PAGE_REPAIR DISABLED (belt + suspenders)
    # Policy: Never perform same-page repair in this pipeline.
    # If someone tries to enable it later, hard-fail.
    if int(args.same_page_lookahead or 0) > 0:
        raise SystemExit("[fatal] same-page repair is disabled by policy. Set --same_page_lookahead 0.")


    # write
    write_ndjson(out_path, events)
    os.makedirs(os.path.dirname(qa_path), exist_ok=True)
    with open(qa_path, "w", encoding="utf-8") as f:
        json.dump({
            "engine": "stitch_townblocks_pagebreak_continuations_v1_5_7",
            "inputs": {
                "in": in_path,
                "raw_lines_ndjson": raw_path,
                "same_page_lookahead": args.same_page_lookahead,
                "same_page_min_overlap": args.same_page_min_overlap,
                "max_scan": args.max_scan,
            },
            "counts": counts,
            "samples": samples,
        }, f, indent=2)

    # console summary (PS-friendly)
    print(f"[done] pages_seen={counts['pages_seen']} candidate_pagebreaks={counts['candidate_pagebreaks']} "
          f"stitched={counts['stitched']} stitched_parties_only={counts['stitched_parties_only']} "
          f"same_page_repair={counts['same_page_repair']} no_continuation_found={counts['no_continuation_found']} "
          f"missing_raw_lines_for_next_page={counts['missing_raw_lines_for_next_page']}")
    print(f"[done] out={out_path} qa={qa_path}")

if __name__ == "__main__":
    main()
# --- cross-chunk finalizer compatibility wrappers ---
# These wrappers must exist at module scope for finalize_crosschunk_stitches_v1_0.py

def event_missing_addr(ev: dict) -> bool:
    """Candidate test: true if this event is missing addr/refs."""
    refs = ev.get("property_refs") or []
    # Treat empty refs as missing (your pipeline's candidate definition)
    return len(refs) == 0

def extract_top_continuation(lines_raw: list[str], max_scan: int = 80):
    """
    Extract continuation refs/parties from the top of a page (bounded).
    Return: (refs_found, parties_found, captured_lines)
    """
    # Reuse your existing internal logic if you already have it; otherwise implement a strict parser:
    refs_found = []
    parties_found = []
    captured = []

    n = min(max_scan, len(lines_raw))
    for i in range(n):
        line = lines_raw[i]
        captured.append(line)

        # Town/Addr lines (TownBlocks style)
        # Examples: "Town: SPRINGFIELD Addr:185 KING ST"
        if "Town:" in line and "Addr:" in line:
            try:
                town = line.split("Town:", 1)[1].split("Addr:", 1)[0].strip()
                addr = line.split("Addr:", 1)[1].strip()
                if town or addr:
                    refs_found.append({"ref_index": len(refs_found), "town": town, "address_raw": addr, "unit_hint": None})
            except Exception:
                pass

        # Parties lines (very light; keep strict)
        # Examples: "1 P LAST FIRST" / "2 C ENTITY"
        if re.match(r"^\s*\d+\s+[PC]\s+", line):
            parties_found.append(line.strip())

        # Stop early if we hit a clear next transaction boundary
        # (strict and deterministic: any "FILE " marker)
        if line.strip().upper().startswith("FILE "):
            # if we already captured something meaningful, we can stop
            if refs_found or parties_found:
                break

    return refs_found, parties_found, captured

def stitch_event(
    last_ev: dict,
    from_page: int,
    into_page: int,
    refs_found: list,
    parties_found: list,
    captured: list[str],
    counts: dict,
    samples: list,
    continuation_type: str = "CROSS_CHUNK",
):
    """
    Apply continuation to an existing stitched event in-place (mutates last_ev).
    This mirrors the stitcher’s PAGEBREAK behavior: fill missing refs/parties only.
    """
    # Fill refs if missing
    if (not (last_ev.get("property_refs") or [])) and refs_found:
        last_ev["property_refs"] = refs_found

    # Fill parties if missing (your TB events may use structured parties; keep safe)
    if (last_ev.get("parties") is None) and parties_found:
        last_ev["parties"] = parties_found

    # Attach stitch meta
    meta = last_ev.setdefault("meta", {})
    stitch = meta.setdefault("stitch", {})
    stitch.update({
        "continuation_type": continuation_type,
        "from_page": from_page,
        "into_page": into_page,
        "captured_top_lines": captured[:20],  # keep small for QA
    })

    # Counts/samples (minimal)
    counts["stitched"] = int(counts.get("stitched", 0)) + 1
    if len(samples) < 20:
        samples.append({
            "page_index": from_page,
            "record_index": int(meta.get("record_index", -1)),
            "ctype": continuation_type,
            "into_page": into_page,
            "refs_after": len(last_ev.get("property_refs") or []),
            "parties_after": len(last_ev.get("parties") or []) if isinstance(last_ev.get("parties"), list) else (0 if last_ev.get("parties") is None else 1),
        })
# --- end wrappers ---
