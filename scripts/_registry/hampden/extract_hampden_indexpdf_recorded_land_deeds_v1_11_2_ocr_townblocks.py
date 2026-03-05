def is_tx_boundary_line(s: str) -> bool:
    """
    Legacy helper (optional). Returns True if the line looks like a vendor header /
    transaction boundary marker.
    NOTE: Main code uses vendor_hdr_anywhere_re + is_vendor_header_line() below.
    """
    if not s:
        return False
    u = s.strip().upper()

    if u.startswith("FILE "):
        return True
    if u.startswith("ENV "):
        return True
    if u.startswith("INGEO"):
        return True
    if u.startswith("ERECORDING PARTNERS NETWORK"):
        return True

    return False



import argparse
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
import pytesseract
from pytesseract import Output


# Money like 147,500.00
MONEY_RE = re.compile(r"\b\d{1,3}(?:,\d{3})*(?:\.\d{2})\b")

# Header date like: "jh 01-16-2026 TRANSACTION #:"
HEADER_DATE_RE = re.compile(r"\b(\d{2}-\d{2}-\d{4})\b")

# Town/Addr line: "Town: SPRINGFIELD Addr:37 DORSET ST Y"
TOWN_ADDR_RE = re.compile(r"\bTown:\s*([A-Z][A-Z\s\-']+?)\s+Addr:\s*([0-9A-Z].*)$", re.IGNORECASE)

# Record "descr" line often like: "1 PUTNAM ST DEED 147,500.00 Y"
# or: "1 DORSET ST DEED Y"
DESCR_LINE_RE = re.compile(r"^\s*(\d+)\s+(.*?)\s+\bDEE[D]?\b(.*)$", re.IGNORECASE)

# Party line like: "1 P NGHIEM VINH CAM Y" or "2 C PLATA O PLOMO INC"
PARTY_LINE_RE = re.compile(r"^\s*([12])\s+([PC])\s+(.*)$", re.IGNORECASE)

bp_inline_re = re.compile(r"\bDEED\b.*?\b(\d{3,6})\s+(\d{1,4})\b", re.IGNORECASE)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_ndjson_line(f, obj: Dict[str, Any]) -> None:
    f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    
   

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def strip_trailing_yes(s: str) -> str:
    """
    Removes trailing right-side 'yes' column artifacts without touching real content.
    We only strip from the END, never middle.
    Handles common OCR variants: Y, V, YES, and stray symbols like Â©.
    """
    s = normalize_ws(s)

    # strip trailing symbols that often show up after corporate names
    s = re.sub(r"(?:\s*[Â©©®™]+)\s*$", "", s).strip()

    # repeatedly strip trailing tokens that act like the far-right 'yes' column
    while True:
        before = s
        s = re.sub(r"\s+(?:Y|V|YES)\s*$", "", s, flags=re.IGNORECASE).strip()
        if s == before:
            break

    return s


def ocr_page_lines(
    pdf_path: str,
    page_index: int,
    crop_top: float,
    crop_right: float,
    dpi: int,
    tesseract_config: str,
) -> List[str]:
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_index]
        left = 0
        top = crop_top
        right = max(0, page.width - crop_right)
        bottom = page.height

        crop = page.crop((left, top, right, bottom))
        img = crop.to_image(resolution=dpi).original

        data = pytesseract.image_to_data(img, config=(tesseract_config or ""), output_type=Output.DICT)

        # Rebuild lines using layout geometry so ordering is deterministic top->bottom
        # Keys: block_num, par_num, line_num, word_num, left, top, width, height, text
        n = len(data.get("text", []))

        line_map = {}  # (block, par, line) -> {"top": y, "words": [(left, text), ...]}
        for i in range(n):
            txt = (data["text"][i] or "").strip()
            if not txt:
                continue
            b = data.get("block_num", [0]*n)[i]
            p = data.get("par_num",   [0]*n)[i]
            l = data.get("line_num",  [0]*n)[i]
            left = data.get("left",   [0]*n)[i]
            top  = data.get("top",    [0]*n)[i]

            key = (b, p, l)
            slot = line_map.get(key)
            if slot is None:
                slot = {"top": int(top), "words": []}
                line_map[key] = slot
            else:
                # keep the minimum top y for the line
                if int(top) < slot["top"]:
                    slot["top"] = int(top)

            slot["words"].append((int(left), txt))

        # Sort lines by y then by block/par/line; words by x
        ordered = sorted(line_map.items(), key=lambda kv: (kv[1]["top"], kv[0][0], kv[0][1], kv[0][2]))

        lines = []
        for _, slot in ordered:
            words = sorted(slot["words"], key=lambda t: t[0])
            line = normalize_ws(" ".join(w for _, w in words))
            if line:
                lines.append(line)

        return lines

def detect_recording_date(lines: List[str]) -> Optional[str]:
    for ln in lines[:80]:
        m = HEADER_DATE_RE.search(ln)
        if m:
            return m.group(1)
    return None

def parse_descr_line(ln: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (descr_loc, consideration_raw)
    """
    m = DESCR_LINE_RE.match(ln)
    if not m:
        return None, None

    descr_part = strip_trailing_yes(m.group(2))
    tail = m.group(3) or ""

    # consideration usually appears in the tail OR sometimes embedded near end
    cons = None
    money = MONEY_RE.findall(ln)
    if money:
        # pick the LAST money token on the line (most likely consideration column)
        cons = money[-1]

    return normalize_ws(descr_part), cons

def parse_town_addr_line(ln: str) -> Tuple[Optional[str], Optional[str]]:
    if not ln:
        return None, None

    # Strip trailing status marker BEFORE parsing
    ln2 = strip_trailing_yes(normalize_ws(ln))

    m = TOWN_ADDR_RE.search(ln2)
    if not m:
        return None, None

    town = normalize_town(m.group(1))
    addr = normalize_ws(m.group(2)).strip()

    # Belt+suspenders: kill trailing Y if it still survived
    addr = re.sub(r"\s+Y\s*$", "", addr, flags=re.IGNORECASE).strip()

    return town, addr

def normalize_town(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = normalize_ws(s).strip().upper()
    s = re.sub(r"[^A-Z0-9\s\-\&]", "", s)
    s = normalize_ws(s)
    return s or None

def parse_party_line(ln: str) -> Optional[Dict[str, Any]]:
    m = PARTY_LINE_RE.match(ln or "")
    if not m:
        return None

    side = m.group(1)              # "1" or "2"
    ent_type = m.group(2).upper()  # "P" or "C"

    # Clean party name once, in the right order
    name = normalize_ws(strip_trailing_yes(m.group(3))).strip()
    name = re.sub(r"\s+Y\s*$", "", name, flags=re.IGNORECASE).strip()

    if not name:
        return None

    return {
        "side_code_raw": side,
        "entity_type_raw": ent_type,
        "name_raw": name,
    }


def extract_events_from_lines(lines: List[str], run_id: str, page_index: int) -> List[Dict[str, Any]]:
    """
    OCR text does not include per-row book/page/inst reliably.

    We build "records" (transactions) from the OCR stream using a belt+suspenders boundary strategy:

    Strong record-start anchors (in priority order):
      A) LEFT-COLUMN row header line: 01-28-2021  3:31:29p  23679  95  6478
      B) SEQ/GRP + DOC-TYPE line:     1   1   <descr...>  DEE[D]?
      C) Town/Addr line:             Town: <TOWN> Addr:<ADDRESS>   (fallback when A/B are missing)
      D) Vendor header line:         FILE <...> / ENV <...> / INGEO / ERECORDING PARTNERS NETWORK ... (fallback)

    Notes:
      - We DO NOT use "FILE SIMPLIFILE ..." as the only delimiter. Vendor headers vary (ENV, INGEO, etc.).
      - We do NOT flush on a second Town/Addr; transactions can legitimately include multiple addresses.
      - If we see a DESCR line *before* a record start anchor (OCR truncation/crop), we keep it as "pending"
        and attach it to the next record-start anchor. Once a record has started, DESCR belongs to the current record.
    """
    recording_date = detect_recording_date(lines)

    events: List[Dict[str, Any]] = []

    def new_cur() -> Dict[str, Any]:
        return {
            "descr_loc": None,
            "consideration_raw": None,
            "property_refs": [],   # list of {ref_index,town,address_raw,unit_hint,ref_role}
            "parties_raw": [],
            "parties_pending": [],
            "evidence_lines_raw": [],
            "evidence_lines_clean": [],

            # row-context fields (left-side columns)
            "recorded_at_raw": None,
            "book_page_raw": None,
            "inst_raw": None,
            "grp_seq_raw": None,
            "ref_book_page_raw": None,  # may stay None until we can reliably parse it
        }

    cur = new_cur()
    in_record = False  # we have "started" a current record

    # Page-prefix orphan parties (often bleed from prior page/record)
    page_prefix_orphan_parties: List[Dict[str, Any]] = []

    # Pending descr/cons that appeared BEFORE we had a record-start anchor.
    pending_descr_loc: Optional[str] = None
    pending_cons_raw: Optional[str] = None


    # Pending left-column row context to attach to the NEXT record-start anchor.
    pending_row_ctx: Dict[str, Any] = {
        "recorded_at_raw": None,
        "book_page_raw": None,
        "inst_raw": None,
        "grp_seq_raw": None,
        "ref_book_page_raw": None,
        "evidence_prefix": [],  # context lines to prepend into evidence for next record
    }

    # --- row-context capture ---
    row_dt_re = re.compile(r"\b(\d{2}-\d{2}-\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*[ap]?)\b", re.IGNORECASE)
    grp_seq_re = re.compile(r"^\s*(\d+)\s+(\d+)\s*$")

    # Inline BOOK-PAGE sometimes appears in OCR as: "1 DEED 23649 187"
    bp_inline_re_local = re.compile(r"\bDEED\b.*?\b(\d{3,6})\s+(\d{1,4})\b", re.IGNORECASE)

    # Consideration tokens (allow down to "1", also commas, optional .00)
    money_token_re = re.compile(r"\b\d{1,3}(?:,\d{3})*(?:\.\d{2})?\b")

    # Vendor header / transaction delimiter lines (NOT just SIMPLIFILE)
    # Covers: "FILE SIMPLIFILE ...", "FILE INGEO", "ENV ...", "ERECORDING PARTNERS NETWORK ..."
    vendor_hdr_anywhere_re = re.compile(
        r"\bFILE\b|\bENV\b|\bINGEO\b|\bERECORDING\s+PARTNERS\s+NETWORK\b",
        re.IGNORECASE,
    )

    def is_vendor_header_line(ln: str) -> bool:
        return bool(vendor_hdr_anywhere_re.search(ln or ""))

    # --- strong anchors for transaction boundaries ---
    # A) Left-column row header: "01-28-2021  3:31:29p  23679  95  6478"
    row_header_re = re.compile(
        r"^\s*(\d{2}-\d{2}-\d{4})\s+(\d{1,2}:\d{2}:\d{2}\s*[ap]?)\s+(\d{3,6})\s+(\d{1,4})\s+(\d{3,7})\b",
        re.IGNORECASE,
    )

    # B) SEQ/GRP + DESCR + DEE[D]? line: "1 1 UNIT D DEED" or "1 1 WASHINGTON RD DEED 1.00"
    seq_grp_doctype_re = re.compile(
        r"^\s*(\d+)\s+(\d+)\s+(.*?)\s+\bDEE[D]?\b(.*)$",
        re.IGNORECASE,
    )

    def is_row_header_line(ln: str):
        return row_header_re.match(ln or "")

    def is_seq_grp_doctype_line(ln: str):
        return seq_grp_doctype_re.match(ln or "")


    def has_any_record_content(d: Dict[str, Any]) -> bool:
        return bool(d.get("descr_loc") or d.get("property_refs") or d.get("parties_raw"))

    def reset_pending_row_ctx() -> None:
        pending_row_ctx["recorded_at_raw"] = None
        pending_row_ctx["book_page_raw"] = None
        pending_row_ctx["inst_raw"] = None
        pending_row_ctx["grp_seq_raw"] = None
        pending_row_ctx["ref_book_page_raw"] = None
        pending_row_ctx["evidence_prefix"] = []

    def attach_pending_into_cur() -> None:
        # only attach if cur doesn't already have them (future-proof)
        if pending_row_ctx["recorded_at_raw"] and not cur.get("recorded_at_raw"):
            cur["recorded_at_raw"] = pending_row_ctx["recorded_at_raw"]
        if pending_row_ctx["book_page_raw"] and not cur.get("book_page_raw"):
            cur["book_page_raw"] = pending_row_ctx["book_page_raw"]
        if pending_row_ctx["inst_raw"] and not cur.get("inst_raw"):
            cur["inst_raw"] = pending_row_ctx["inst_raw"]
        if pending_row_ctx["grp_seq_raw"] and not cur.get("grp_seq_raw"):
            cur["grp_seq_raw"] = pending_row_ctx["grp_seq_raw"]
        if pending_row_ctx["ref_book_page_raw"] and not cur.get("ref_book_page_raw"):
            cur["ref_book_page_raw"] = pending_row_ctx["ref_book_page_raw"]

        # prepend evidence context so it appears before vendor header in evidence
        if pending_row_ctx["evidence_prefix"]:
            cur["evidence_lines_raw"][0:0] = pending_row_ctx["evidence_prefix"]
            cur["evidence_lines_clean"][0:0] = [strip_trailing_yes(x) for x in pending_row_ctx["evidence_prefix"]]

        reset_pending_row_ctx()

    def attach_pending_descr_into_cur() -> None:
        nonlocal pending_descr_loc, pending_cons_raw
        if pending_descr_loc and not cur.get("descr_loc"):
            cur["descr_loc"] = pending_descr_loc
        if pending_cons_raw and not cur.get("consideration_raw"):
            cur["consideration_raw"] = pending_cons_raw
            
        pending_descr_loc = None
        pending_cons_raw = None

    def flush() -> None:
        nonlocal cur, in_record, pending_descr_loc, pending_cons_raw
        if not has_any_record_content(cur):
            cur = new_cur()
            in_record = False
            # do NOT nuke pending_* here; they belong to the next anchor if we never started
            return

        evt = {
            "event_type": "DEED",
            "document": {
                "doc_type_code_raw": "DEED",
                "doc_type_code": "DEED",
                "registry_office": "Hampden Registry of Deeds",
                "land_type": "RECORDED_LAND",
            },
            "recording": {
                "recording_date_raw": recording_date,
                "recording_date": None,
                "recorded_at_raw": cur.get("recorded_at_raw"),
                "book_page_raw": cur.get("book_page_raw"),
                "book_page_norm": None,
                "inst_raw": cur.get("inst_raw"),
                "grp_seq_raw": cur.get("grp_seq_raw"),
                "ref_book_page_raw": cur.get("ref_book_page_raw"),
            },
            "consideration": {"amount_raw": cur.get("consideration_raw"), "amount": None},
            "property_refs": (cur.get("property_refs") or []),
            "parties": ({"parties_raw": cur["parties_raw"]} if cur["parties_raw"] else None),
            "descr_loc_raw": cur.get("descr_loc"),
            "meta": {"run_id": run_id, "page_index": page_index},
            "evidence": {"lines_clean": cur["evidence_lines_clean"][:200]},
        }

        events.append(evt)
        cur = new_cur()
        in_record = False

    def maybe_capture_row_context(line: str) -> bool:
        """
        Capture DATE/TIME RECORDED + book + page + inst from a line that contains them.
        Example OCR: "01-19-2021 11:11:56a  23656  355  3786"
        """
        m = row_dt_re.search(line)
        if not m:
            return False

        recorded_at = m.group(1).strip()

        # extract numeric tokens AFTER the datetime token
        tail = line[m.end():]
        nums = [x for x in re.findall(r"\d+", tail)]

        if len(nums) >= 3:
            book = nums[0]
            page = nums[1]
            inst = nums[2]
            pending_row_ctx["recorded_at_raw"] = recorded_at
            pending_row_ctx["book_page_raw"] = f"{book}-{page}"
            pending_row_ctx["inst_raw"] = inst
            pending_row_ctx["evidence_prefix"].append(line)
            return True

        return False

    def maybe_capture_grp_seq(line: str) -> bool:
        """
        GRP-SEQ often appears as its own small line like: "1 1"
        right after the row context line and before the vendor header.
        """
        if pending_row_ctx["inst_raw"] is None:
            return False
        m = grp_seq_re.match(line)
        if not m:
            return False
        pending_row_ctx["grp_seq_raw"] = f"{m.group(1)}-{m.group(2)}"
        pending_row_ctx["evidence_prefix"].append(line)
        return True

    def pick_consideration_after_deed(line: str) -> Optional[str]:
        """
        Consideration down to $1.00.
        Only look AFTER 'DEED' to avoid left-column numbers.
        Accept:
          - has comma, or
          - has decimals (e.g., 1.00, 10.00), or
          - 3+ digits (e.g., 500)
        Reject 1–2 digit integers like "20" that are almost always noise.
        """
        if not re.search(r"\bDEED\b", line, re.IGNORECASE):
            return None

        parts = re.split(r"\bDEED\b", line, flags=re.IGNORECASE, maxsplit=1)
        if len(parts) < 2:
            return None

        tail = parts[1]
        toks = [m.group(0) for m in money_token_re.finditer(tail)]
        if not toks:
            return None

        for tok in reversed(toks):
            t = (tok or "").strip()

            # allow decimals, commas, or 3+ digits
            if ("," in t) or re.fullmatch(r"\d+\.\d{2}", t) or re.fullmatch(r"\d{3,}", t):
                return t

            # reject tiny integers (noise)
            if re.fullmatch(r"\d{1,2}", t):
                continue

        return None

    def start_record(anchor_line: Optional[str] = None) -> None:
        nonlocal cur, in_record
        cur = new_cur()
        attach_pending_into_cur()
        attach_pending_descr_into_cur()
        in_record = True
        if anchor_line:
            cur["evidence_lines_raw"].append(anchor_line)
            cur["evidence_lines_clean"].append(strip_trailing_yes(anchor_line))


    # ---- main parse loop ----
    for ln in lines:
        ln = normalize_ws(ln)
        if not ln:
            continue
        up = ln.upper()

        # Harvest left-column row-context lines whenever they appear (even before we start records)
        if maybe_capture_row_context(ln):
            continue
        if maybe_capture_grp_seq(ln):
            continue

        # Detect anchors
        m_hdr = is_row_header_line(ln)
        m_seq = is_seq_grp_doctype_line(ln)
        is_town_addr = ("TOWN:" in up and "ADDR:" in up)
        is_vendor_hdr = is_vendor_header_line(ln)

        # Vendor header lines (FILE/ENV/INGEO/ERECORDING PARTNERS...) are reliable
        # transaction boundaries in this PDF family, BUT the vendor name varies.
        #
        # Belt + suspenders:
        #   - Treat vendor header as an anchor (start of a new record)
        #   - Keep it as evidence for the NEW record
        #
        # This protects us when OCR drops the left-column row header and/or the SEQ/GRP line.
        if is_vendor_hdr:
            if in_record and has_any_record_content(cur):
                flush()
            if not in_record:
                start_record(anchor_line=None)
            cur["evidence_lines_raw"].append(ln)
            cur["evidence_lines_clean"].append(strip_trailing_yes(ln))
            continue

        # True record anchors:
        #  1) Left-column row header (best)
        #  2) SEQ/GRP + DEE[D]? line (fallback)
        #  3) Town/Addr line only if we are not already inside a record (last-resort)
        is_anchor = bool(m_hdr or m_seq or (is_town_addr and not in_record))

        if is_anchor:
            if in_record and has_any_record_content(cur):
                flush()

            if not in_record:
                start_record(anchor_line=ln if m_hdr else None)

            # Keep SEQ/GRP anchor lines as evidence too (helps QA/debug)
            if m_seq:
                cur["evidence_lines_raw"].append(ln)
                cur["evidence_lines_clean"].append(strip_trailing_yes(ln))

            # If this line is a row header, we've already captured the row context in start_record().
            if m_hdr:
                continue
            # If this is SEQ/GRP or Town/Addr, let normal parsing below handle it (fall through).

        # DESCR line
        descr_loc, cons = parse_descr_line(ln)
        if descr_loc is not None:
            # Inline BOOK-PAGE sometimes appears as: "1 DEED 23649 187"
            m_bp = bp_inline_re_local.search(ln)
            if m_bp and not cur.get("ref_book_page_raw"):
                cur["ref_book_page_raw"] = f"{m_bp.group(1)}-{m_bp.group(2)}"

            if not cons:
                cons = pick_consideration_after_deed(ln)

            if in_record:
                if descr_loc and not cur.get("descr_loc"):
                    cur["descr_loc"] = descr_loc

                    # ✅ PROMOTE pending parties NOW that we have an anchor (DESCR)
                    if cur.get("parties_pending"):
                        cur["parties_raw"][0:0] = cur["parties_pending"]
                        cur["parties_pending"] = []

                if cons and not cur.get("consideration_raw"):
                    cur["consideration_raw"] = cons

            else:
                # Not yet in a record.
                # Only stash descr if this line ALSO looks like a SEQ/GRP + DEED anchor.
                if descr_loc and m_seq:
                    pending_descr_loc = descr_loc
                if cons and m_seq:
                    pending_cons_raw = cons

            # evidence
            if in_record:
                cur["evidence_lines_raw"].append(ln)
                cur["evidence_lines_clean"].append(strip_trailing_yes(ln))
            continue

        # Town/Addr line(s) — can repeat 5–10 times per transaction
        if is_town_addr:
            if not in_record:
                start_record()

            town, addr = parse_town_addr_line(ln)

            def extract_unit_hint_from_descr(descr: Optional[str]) -> Optional[str]:
                if not descr:
                    return None
                u = descr.strip().upper()
                m = re.search(r"\b(?:UNIT|APT)\s+([A-Z0-9\-]+)\b", u)
                if m:
                    return m.group(1)
                m = re.search(r"#\s*([A-Z0-9\-]+)\b", u)
                if m:
                    return m.group(1)
                return None

            def ends_with_unit_marker(a: str) -> Optional[str]:
                au = (a or "").strip().upper()
                if au.endswith(" UNIT"):
                    return "UNIT"
                if au.endswith(" APT"):
                    return "APT"
                if au.endswith("#") or au.endswith(" #"):
                    return "#"
                return None

            if town or addr:
                unit_hint = extract_unit_hint_from_descr(cur.get("descr_loc"))

                # Heal truncated addr like "... UNIT" using descr_loc "UNIT D"
                if addr and unit_hint:
                    marker = ends_with_unit_marker(addr)
                    if marker == "UNIT":
                        addr = f"{addr} {unit_hint}"
                    elif marker == "APT":
                        addr = f"{addr} {unit_hint}"
                    elif marker == "#":
                        addr = f"{addr}{unit_hint}"

                refs = cur.setdefault("property_refs", [])
                ref_index = len(refs)
                refs.append({
                    "ref_index": ref_index,
                    "town": town,
                    "address_raw": addr,
                    "unit_hint": unit_hint if unit_hint else None,
                    "ref_role": "PRIMARY" if ref_index == 0 else "ADDITIONAL",
                })

                # ✅ PROMOTE pending parties NOW that we have an anchor (Town/Addr)
                if cur.get("parties_pending"):
                    cur["parties_raw"][0:0] = cur["parties_pending"]
                    cur["parties_pending"] = []

            # evidence
            cur["evidence_lines_raw"].append(ln)
            cur["evidence_lines_clean"].append(strip_trailing_yes(ln))
            continue

        # Party line
        p = parse_party_line(ln)
        if p:
            if not in_record:
                start_record()

            # If we have an identity anchor, accept immediately.
            if cur.get("descr_loc") or cur.get("property_refs"):
                cur["parties_raw"].append(p)

                # Also promote anything pending now that we're anchored.
                if cur.get("parties_pending"):
                    cur["parties_raw"][0:0] = cur["parties_pending"]
                    cur["parties_pending"] = []
            else:
                # Hold until we get a Town/Addr or DESCR anchor in this same record
                cur.setdefault("parties_pending", []).append(p)

            # evidence always (only for lines that are within a record)
            cur["evidence_lines_raw"].append(ln)
            cur["evidence_lines_clean"].append(strip_trailing_yes(ln))
            continue

        # Default: keep evidence only once we're inside a record
        if in_record:
            cur["evidence_lines_raw"].append(ln)
            cur["evidence_lines_clean"].append(strip_trailing_yes(ln))


    flush()
    return events



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--quarantine_dir", required=True)
    ap.add_argument("--start_page", type=int, default=0)
    ap.add_argument("--max_pages", type=int, default=1)
    ap.add_argument("--progress_every", type=int, default=50)
    ap.add_argument("--force", action="store_true")

    # OCR controls (default = V1.6 behavior)
    ap.add_argument("--crop_top", type=float, default=240.0)
    ap.add_argument("--crop_right", type=float, default=0.0)
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--tess_config", default="")  # keep empty by default; V1.6 produced good text this way
    args = ap.parse_args()

    ensure_dir(os.path.dirname(args.out))
    ensure_dir(os.path.dirname(args.audit))
    ensure_dir(args.quarantine_dir)

    run_id = f"{now_utc_iso().replace(':','').replace('-','')}".replace("Z","Z") + "__hampden__recorded_land__deed_ocr_v1_11_2"

    counts = {
        "pages_seen": 0,
        "events_out": 0,
        "quarantined": 0,
    }

    audit = {
        "engine": "extract_hampden_indexpdf_recorded_land_deeds_v1_11_ocr_townblocks",
        "run_id": run_id,
        "pdf": args.pdf,
        "out": args.out,
        "quarantine_dir": args.quarantine_dir,
        "start_page": args.start_page,
        "max_pages": args.max_pages,
        "crop_top": args.crop_top,
        "crop_right": args.crop_right,
        "dpi": args.dpi,
        "tess_config": args.tess_config,
        "started_at_utc": now_utc_iso(),
        "counts": counts,
    }

    # overwrite guard (simple)
    if os.path.exists(args.out) and not args.force:
        raise SystemExit(f"[ERR] out exists; pass --force: {args.out}")

    out_f = open(args.out, "w", encoding="utf-8")

    try:
        with pdfplumber.open(args.pdf) as pdf:
            total_pages = len(pdf.pages)
            end_page = min(total_pages, args.start_page + args.max_pages)

        for pno in range(args.start_page, end_page):
            counts["pages_seen"] += 1

            lines = ocr_page_lines(
                pdf_path=args.pdf,
                page_index=pno,
                crop_top=args.crop_top,
                crop_right=args.crop_right,
                dpi=args.dpi,
                tesseract_config=args.tess_config,
            )

            # A5: write full raw OCR lines for this page (audit-only)
            raw_lines_ndjson = os.path.join(args.quarantine_dir, "raw_ocr_lines__ALLPAGES.ndjson")
            with open(raw_lines_ndjson, "a", encoding="utf-8") as rf:
                write_ndjson_line(rf, {
                    "run_id": run_id,
                    "page_index": pno,
                    "crop_top": args.crop_top,
                    "crop_right": args.crop_right,
                    "dpi": args.dpi,
                    "tess_config": args.tess_config,
                    "lines_raw": lines,
                })



            # always write preview quarantine for page 0 of a test run, so you can see text
            preview_path = os.path.join(args.quarantine_dir, "quarantine__pow_ocr_text_preview.ndjson")
            with open(preview_path, "a", encoding="utf-8") as qf:
                write_ndjson_line(qf, {
                    "run_id": run_id,
                    "page_index": pno,
                    "reason": "pow_ocr_text_preview",
                    "crop_top": args.crop_top,
                    "crop_right": args.crop_right,
                    "dpi": args.dpi,
                    "lines_preview": lines[:120],
                })

            evts = extract_events_from_lines(lines, run_id=run_id, page_index=pno)

            for ridx, e in enumerate(evts, start=1):  # or start=0, but be consistent with rowctx
                e.setdefault("meta", {})
                e["meta"]["record_index"] = ridx
                write_ndjson_line(out_f, e)
                counts["events_out"] += 1


            if args.progress_every and (counts["pages_seen"] % args.progress_every == 0):
                print(f"[progress] pages_seen: {counts['pages_seen']} events_out: {counts['events_out']} quarantined: {counts['quarantined']}")

    finally:
        out_f.close()
        audit["finished_at_utc"] = now_utc_iso()
        audit["counts"] = counts
        with open(args.audit, "w", encoding="utf-8") as af:
            json.dump(audit, af, ensure_ascii=False, indent=2)

    print(f"[done] pages_seen: {counts['pages_seen']} events_out: {counts['events_out']} quarantined: {counts['quarantined']} audit: {args.audit}")

if __name__ == "__main__":
    main()

# BOUNDARY_ANCHOR_V1: record boundaries accept any '^FILE ' vendor line + ERECORDING PARTNERS (non-FILE).

