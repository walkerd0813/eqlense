# otr_extract_hampden_v2.py
# Clean v2: schema-safe, whitelist doctype, normalize recording_date to YYYY-MM-DD,
# detect registry_office, keep LAND_COURT book/page nullable but flagged.

import os, re, json, argparse
from datetime import datetime, timezone
from typing import List, Dict
import fitz  # PyMuPDF

SCHEMA_NAME = "equitylens.registry_event"
SCHEMA_VERSION = "mim_v1_0"

def nowz():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
VERIFY_TOKEN_RE = re.compile(r'\s+Y\s*$')

def strip_trailing_verify_token(s: str):
    if s is None:
        return None
    s2 = s.strip()
    s2 = VERIFY_TOKEN_RE.sub('', s2).strip()
    return s2



def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def safe_upper(s: str) -> str:
    return (s or "").upper()

def parse_money(raw: str):
    if raw is None:
        return None
    t = raw.strip().replace(",", "")
    if not t:
        return None
    try:
        return float(t)
    except:
        return None

def normalize_date_to_iso(raw: str):
    """
    Accepts: 01-26-2021, 01/26/2021, 2021-01-26
    Returns: 2021-01-26 or None
    """
    if not raw:
        return None
    s = raw.strip()
    s = s.replace(".", "").replace(" ", "")
    # sometimes raw is like "01-26-2021" or "01/26/2021"
    for fmt in ("%m-%d-%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except:
            pass
    # sometimes "01-26-2021 9:43:04a" in recorded_at_raw; pull first token
    m = re.match(r"^(\d{1,2}[-/]\d{1,2}[-/]\d{4})", s)
    if m:
        return normalize_date_to_iso(m.group(1))
    return None

def detect_registry_office(lines: List[str]) -> str:
    u = safe_upper(" ".join(lines[:30]))
    # Hampden observed variants
    if "RECORDED LAND" in u:
        return "RECORDED_LAND"
    if "LAND COURT" in u or "LAN CORT" in u or "LANCOURT" in u:
        return "LAND_COURT"
    return "UNKNOWN"

# ---- Doc type whitelist + canonicalization ----
# User locked list additions: DM (Discharge of Mortgage), DIS (Discharge), FTL (Federal Tax Lien),
# FDD (Foreclosure Deed), LP (Lis Pendens)
DOC_WHITELIST = {
    "DEED","MTG","ASG","ASSIGN","REL","RELEASE",
    "DM","DIS","FTL","FDD","LP",
    "ESMT"
}
DOC_CANON = {
    "ASSIGN":"ASG",
    "RELEASE":"REL",
}

def canon_doctype(dt: str):
    if not dt:
        return None, None, ["MISSING_DOCTYPE"]
    raw = dt.strip().upper()
    c = DOC_CANON.get(raw, raw)
    if c not in DOC_WHITELIST:
        return None, raw, ["DOCTYPE_NOT_WHITELISTED"]
    return c, raw, []

UNIT_RE = re.compile(r"\b(?:UNIT|APT|APARTMENT|#)\s*([A-Z0-9\-]+)\b", re.I)

def extract_unit(text: str):
    if not text:
        return None
    m = UNIT_RE.search(text)
    if m:
        return m.group(1).strip()
    return None

def make_event_id(county: str, inst: str, seq: str, dt: str, rd_iso: str, book: str, page: str):
    # deterministic, stable; keep nulls as empty slots like existing v1 behavior
    county = county.lower()
    inst = (inst or "").strip()
    seq = (seq or "").strip()
    dt = (dt or "").strip()
    rd = (rd_iso or "").strip()
    bp = "-"
    if book and page:
        bp = f"{book}-{page}"
    return f"MA|registry|otr|{county}|INST|{inst}|SEQ|{seq}|DT|{dt}|RD|{rd}|BP|{bp}"

# ----------------------------
# Template A (Recorded Land)
# ----------------------------
ANCHOR_RE_A = re.compile(r"^\s*\d{2}-\d{2}-\d{4}\s+\d{1,2}:\d{2}:\d{2}[ap]\s+\d+")

TOWN_ADDR_RE = re.compile(r"Town:\s*(?P<town>[A-Z \-']+)\s+Addr:\s*(?P<addr>.+)$", re.I)

def parse_template_a(lines: List[str], pdf_name: str, page_index: int, county: str, registry_office: str):
    events = []
    current = None
    mode = "SEARCH"

    def flush():
        nonlocal current
        if not current:
            return

        dt_code, dt_raw, dt_flags = canon_doctype(current.get("doc_type_raw"))
        inst = (current.get("instrument_number_raw") or "").strip()
        seq = str(current.get("seq") or "1")
        rd_iso = normalize_date_to_iso(current.get("recording_date_raw"))
        book = current.get("book")
        page = current.get("page")

        eid = make_event_id(county, inst, seq, (dt_code or (dt_raw or "UNKNOWN")), rd_iso or "", book or "", page or "")

        unit = current.get("unit_raw") or extract_unit(current.get("address_raw")) or extract_unit(current.get("description_raw"))

        # consideration flags
        cons_amt = current.get("consideration_amount")
        cons_flags = []
        if cons_amt in (0.0, 1.0, 100.0):
            cons_flags.append("ZERO_OR_NOMINAL")

        evt = {
            "schema": {"name": SCHEMA_NAME, "version": SCHEMA_VERSION},
            "event_id": eid,
            "event_type": (dt_code or (dt_raw or "UNKNOWN")),
            "county": county,
            "registry_system": "otr_pdf_index",
            "registry_office": registry_office,
            "doc_type_code": dt_code,
            "doc_type_desc": (dt_code or dt_raw),
            "source": {"pdf": pdf_name, "page": page_index + 1, "extracted_at": nowz()},
            "recording": {
                "recorded_at_raw": current.get("recorded_at_raw"),
                "recording_date": rd_iso,
                "recording_time": current.get("recording_time_raw"),
                "book": book,
                "page": page,
                "instrument_number_raw": inst,
                "seq": seq
            },
            "property_ref": {
                "town_raw": current.get("town_raw"),
                "address_raw": current.get("address_raw"),
                "unit_raw": unit,
                "state": "MA",
                "zip_raw": None,
                "legal_desc_raw": current.get("description_raw")
            },
            "consideration": {
                "raw_text": current.get("consideration_raw"),
                "amount": cons_amt,
                "parse_status": "PARSED" if cons_amt is not None else "MISSING",
                "flags": cons_flags,
                "source": "INDEX"
            },
            "parties_raw": current.get("parties_raw", []),
            "parties": [],
            "attach": {"status": "UNKNOWN"},
            "meta": {
                "doctype_raw": dt_raw,
                "doctype_flags": dt_flags
            }
        }
        prop_refs = current.get("property_refs") or []
        if prop_refs:
            for j, pr in enumerate(prop_refs, start=1):
                evt2 = dict(evt)
                evt2["recording"] = dict(evt["recording"])
                evt2["property_ref"] = dict(evt["property_ref"])
                if pr.get("town_raw"): evt2["property_ref"]["town_raw"] = pr.get("town_raw")
                if pr.get("address_raw"): evt2["property_ref"]["address_raw"] = pr.get("address_raw")
                if pr.get("unit_raw"): evt2["property_ref"]["unit_raw"] = pr.get("unit_raw")
                evt2["event_id"] = evt["event_id"] + f"|ADDR|{j}"
                events.append(evt2)
        else:
            events.append(evt)
        current = None

    for ln in lines:
        s = norm_ws(ln)
        if not s:
            continue

        if ANCHOR_RE_A.match(s):
            flush()
            # split: "01-21-2021 12:33:55p  23662  581   4570"
            parts = s.split()
            rec_date_raw = parts[0]
            rec_time_raw = parts[1]
            book = parts[2] if len(parts) > 2 else None
            page = parts[3] if len(parts) > 3 else None
            inst = parts[4] if len(parts) > 4 else None
            current = {
                "recorded_at_raw": f"{rec_date_raw} {rec_time_raw}",
                "recording_date_raw": rec_date_raw,
                "recording_time_raw": rec_time_raw,
                "book": book,
                "page": page,
                "instrument_number_raw": inst,
                "seq": None,
                "doc_type_raw": None,
                "description_raw": None,
                "consideration_raw": None,
                "consideration_amount": None,
                "town_raw": None,
                "address_raw": None,
                "unit_raw": None,
                "parties_raw": []
            }
            mode = "IN_HEADER"
            continue

        if not current:
            continue

        # Entry line (grp/seq + desc + doctype + consideration tail)
        # Example: "1   1        LAUREL ST                  MTG                    212,000.00               Y"
        m = re.match(r"^\s*(\d+)\s+(\d+)\s+(.+?)\s+([A-Z]{2,6})\s+(.+)$", s)
        if m:
            grp = m.group(1); seq = m.group(2); desc = norm_ws(m.group(3)); dt = m.group(4)
            current["seq"] = seq
            current["doc_type_raw"] = dt
            current["description_raw"] = desc
            # consideration is last money-like token in tail
            tail = m.group(5).strip().split()
            cons_raw = None; cons_amt = None
            for tk in reversed(tail):
                tk2 = tk.strip().replace(",", "")
                if re.match(r"^\d+(\.\d{1,2})?$", tk2):
                    cons_raw = tk.strip()
                    cons_amt = parse_money(tk2)
                    break
            current["consideration_raw"] = cons_raw
            current["consideration_amount"] = cons_amt

            # unit from desc
            u = extract_unit(desc)
            if u:
                current["unit_raw"] = current.get("unit_raw") or u

            mode = "IN_ENTRY"
            continue

        # Town/Addr line
        tm = TOWN_ADDR_RE.search(s)
        if tm:
            current["town_raw"] = norm_ws(tm.group("town"))
            addr = norm_ws(tm.group("addr"))
            current["address_raw"] = strip_trailing_verify_token(addr)
            u = extract_unit(addr)
            if u:
                current["unit_raw"] = current.get("unit_raw") or u
            mode = "AFTER_TOWN_ADDR"
            continue

        # party-ish lines (keep raw)
        if mode in ("AFTER_TOWN_ADDR","IN_ENTRY"):
            t = s.strip()
            if re.match(r"^\d+\s+[PC]\s+.+$", t):
                current["parties_raw"].append(t)
                continue

    flush()
    return events

# ----------------------------
# Template B (Land Court-ish)
# ----------------------------
LANDCOURT_ANCHOR_RE = re.compile(r"^\s*(?P<docnum>[\d,]+)\s+(?P<seq>\d+)\s+(?P<doctype>[A-Z]{2,6})\s+(?P<towncode>[A-Z]{2,3})\s+(?P<time>\d{1,2}:\d{2})\b")

def parse_template_b(lines: List[str], pdf_name: str, page_index: int, county: str, registry_office: str):
    events = []
    current = None

    def flush():
        nonlocal current
        if not current:
            return

        dt_code, dt_raw, dt_flags = canon_doctype(current.get("doc_type_raw"))
        inst = (current.get("instrument_number_raw") or "").replace(",", "").strip()
        seq = str(current.get("seq") or "1")

        # Land Court pages often do NOT provide book/page; keep nullable but flag
        rd_iso = normalize_date_to_iso(current.get("recording_date_raw"))

        eid = make_event_id(county, inst, seq, (dt_code or (dt_raw or "UNKNOWN")), rd_iso or "", "", "")

        unit = current.get("unit_raw") or extract_unit(current.get("address_raw"))

        cons_amt = current.get("consideration_amount")
        cons_flags = []
        if cons_amt in (0.0, 1.0, 100.0):
            cons_flags.append("ZERO_OR_NOMINAL")

        meta_flags = []
        if registry_office == "LAND_COURT" and (not current.get("book")) and (not current.get("page")):
            meta_flags.append("LAND_COURT_NO_BOOK_PAGE_EXPECTED")

        evt = {
            "schema": {"name": SCHEMA_NAME, "version": SCHEMA_VERSION},
            "event_id": eid,
            "event_type": (dt_code or (dt_raw or "UNKNOWN")),
            "county": county,
            "registry_system": "otr_pdf_index",
            "registry_office": registry_office,
            "doc_type_code": dt_code,
            "doc_type_desc": (dt_code or dt_raw),
            "source": {"pdf": pdf_name, "page": page_index + 1, "extracted_at": nowz()},
            "recording": {
                "recorded_at_raw": current.get("recorded_at_raw"),
                "recording_date": rd_iso,
                "recording_time": current.get("recording_time_raw"),
                "book": None,
                "page": None,
                "instrument_number_raw": inst,
                "seq": seq
            },
            "property_ref": {
                "town_raw": current.get("town_raw"),
                "address_raw": current.get("address_raw"),
                "unit_raw": unit,
                "state": "MA",
                "zip_raw": None,
                "legal_desc_raw": current.get("description_raw")
            },
            "consideration": {
                "raw_text": current.get("consideration_raw"),
                "amount": cons_amt,
                "parse_status": "PARSED" if cons_amt is not None else "MISSING",
                "flags": cons_flags,
                "source": "INDEX"
            },
            "parties_raw": current.get("parties_raw", []),
            "parties": [],
            "attach": {"status": "UNKNOWN"},
            "meta": {
                "doctype_raw": dt_raw,
                "doctype_flags": dt_flags,
                "flags": meta_flags
            }
        }
        events.append(evt)
        current = None

    for ln in lines:
        s = norm_ws(ln)
        if not s:
            continue

        m = LANDCOURT_ANCHOR_RE.match(s)
        if m:
            flush()
            current = {
                "instrument_number_raw": m.group("docnum").replace(",", "").strip(),
                "seq": m.group("seq"),
                "doc_type_raw": m.group("doctype"),
                "town_raw": None,
                "address_raw": None,
                "unit_raw": None,
                "description_raw": None,
                "consideration_raw": None,
                "consideration_amount": None,
                "parties_raw": []
            }
            continue

        if not current:
            continue

        # address line sometimes has money at end
        if current.get("address_raw") is None:
            mm = re.match(r"^(?P<addr>.+?)\s+(?P<amt>\d[\d,]*\.\d{2})$", s)
            if mm:
                current["address_raw"] = strip_trailing_verify_token(mm.group("addr"))
                current["consideration_raw"] = mm.group("amt").strip()
                current["consideration_amount"] = parse_money(mm.group("amt").replace(",", ""))
                u = extract_unit(current["address_raw"])
                if u:
                    current["unit_raw"] = u
                continue

        # parties raw blocks
        if "GRANTORS:" in safe_upper(s) or "GRANTEES:" in safe_upper(s):
            current["parties_raw"].append(s)
            continue
        if re.match(r"^\d+\s+[PC]\s+.+$", s):
            current["parties_raw"].append(s)
            continue

    flush()
    return events

def extract_pdf(pdf_path: str, county: str, out_events, out_quarantine, counters: Dict[str, int]):
    pdf_name = os.path.basename(pdf_path)
    doc = fitz.open(pdf_path)
    counters["pdfs"] += 1
    counters["pages_total"] += doc.page_count

    for i in range(doc.page_count):
        try:
            text = doc[i].get_text("text") or ""
        except Exception as e:
            counters["pages_quarantined"] += 1
            out_quarantine.write(json.dumps({"pdf": pdf_name, "page": i + 1, "error": str(e)}) + "\n")
            continue

        if not text.strip():
            continue

        lines = [ln.rstrip("\n") for ln in text.splitlines() if ln.strip()]
        one = safe_upper(" ".join(lines))
        office = detect_registry_office(lines)

        page_events = []
        if ("DATE/TIME" in one and "INST" in one and "BOOK-PAGE" in one) or (lines and ANCHOR_RE_A.match(norm_ws(lines[0]))) or any(ANCHOR_RE_A.match(norm_ws(x)) for x in lines[:30]):
            page_events = parse_template_a(lines, pdf_name, i, county, office)
        elif ("DOCUMENT NUMBER" in one) or any(LANDCOURT_ANCHOR_RE.match(norm_ws(x)) for x in lines[:30]):
            # if we detected LAND_COURT in header keep it, else UNKNOWN
            if office == "UNKNOWN":
                office = "LAND_COURT"
            page_events = parse_template_b(lines, pdf_name, i, county, office)

        for ev in page_events:
            out_events.write(json.dumps(ev, ensure_ascii=False) + "\n")
            counters["events"] += 1

    doc.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_dir", required=True)
    ap.add_argument("--out_events", required=True)
    ap.add_argument("--out_quarantine", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--county", required=True)
    ap.add_argument("--glob", default="*.pdf")
    args = ap.parse_args()

    in_dir = args.in_dir
    out_events_path = args.out_events
    out_quar_path = args.out_quarantine
    audit_path = args.audit
    county = args.county.lower()
    glob = args.glob

    pdfs = []
    # simple glob
    if glob == "*.pdf":
        for fn in os.listdir(in_dir):
            if fn.lower().endswith(".pdf"):
                pdfs.append(os.path.join(in_dir, fn))
    else:
        # fallback: include all pdfs
        for fn in os.listdir(in_dir):
            if fn.lower().endswith(".pdf"):
                pdfs.append(os.path.join(in_dir, fn))

    counters = {"pdfs": 0, "pages_total": 0, "pages_quarantined": 0, "events": 0}
    with open(out_events_path, "w", encoding="utf-8") as oe, open(out_quar_path, "w", encoding="utf-8") as oq:
        for p in sorted(pdfs):
            extract_pdf(p, county, oe, oq, counters)

    audit = {
        "engine_id": "registry.otr_extract_hampden_v2",
        "schema": {"name": SCHEMA_NAME, "version": SCHEMA_VERSION},
        "county": county,
        "in_dir": in_dir,
        "glob": glob,
        "out_events": out_events_path,
        "out_quarantine": out_quar_path,
        "counters": counters,
        "extracted_at": nowz(),
        "doctype_whitelist": sorted(list(DOC_WHITELIST))
    }
    with open(audit_path, "w", encoding="utf-8") as a:
        json.dump(audit, a, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()