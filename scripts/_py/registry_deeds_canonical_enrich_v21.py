import argparse
import json
import re
from pathlib import Path
from datetime import datetime, timezone

BOSTON_SUBCITY = {
    "JAMAICA PLAIN": "Jamaica Plain",
    "ROXBURY": "Roxbury",
    "ROXBURY CROSSING": "Roxbury Crossing",
    "DORCHESTER": "Dorchester",
    "SOUTH BOSTON": "South Boston",
    "EAST BOSTON": "East Boston",
    "CHARLESTOWN": "Charlestown",
    "BRIGHTON": "Brighton",
    "ALLSTON": "Allston",
    "HYDE PARK": "Hyde Park",
    "MATTAPAN": "Mattapan",
    "WEST ROXBURY": "West Roxbury",
    "BACK BAY": "Back Bay",
    "NORTH END": "North End",
    "SOUTH END": "South End",
    "BEACON HILL": "Beacon Hill",
    "FENWAY": "Fenway",
    "MISSION HILL": "Mission Hill",
    "ROSINDALE": "Roslindale",
}

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")

def safe_str(x):
    return x if isinstance(x, str) else ""

def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def parse_date_any(s: str):
    s = norm_spaces(s)
    if not s:
        return None
    m = re.match(r"^(?P<mm>\d{1,2})/(?P<dd>\d{1,2})/(?P<yy>\d{4})$", s)
    if m:
        mm = int(m.group("mm")); dd = int(m.group("dd")); yy = int(m.group("yy"))
        return f"{yy:04d}-{mm:02d}-{dd:02d}"
    try:
        dt = datetime.strptime(s, "%B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def parse_time_any(s: str):
    s = norm_spaces(s)
    if not s:
        return None
    for fmt in ("%I:%M:%S %p", "%I:%M %p", "%H:%M:%S", "%H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt in ("%I:%M:%S %p", "%H:%M:%S"):
                return dt.strftime("%H:%M:%S")
            return dt.strftime("%H:%M")
        except Exception:
            pass
    return None

def get_all_text_blobs(rec: dict) -> str:
    blobs = []
    cands = rec.get("address_candidates") or []
    for c in cands:
        ctx = c.get("context")
        if isinstance(ctx, str) and ctx.strip():
            blobs.append(ctx)
    src = rec.get("source") or {}
    extracted = rec.get("extracted") or {}
    for k in ("margin_text", "body_text", "text"):
        v = extracted.get(k) or src.get(k)
        if isinstance(v, str) and v.strip():
            blobs.append(v)
    return "\n".join(blobs)

# ===== Recording blocks patterns =====
RE_DOCNUM_A = re.compile(r"\bDocument\s+Number\s*[:\-]?\s*(\d{3,})\b", re.IGNORECASE)
RE_DOCTYPE_A = re.compile(r"\bDocument\s+Type\s*[:\-]?\s*([A-Z]{2,6})\b", re.IGNORECASE)
RE_RECDATE_A = re.compile(r"\bRecorded\s+Date\s*[:\-]?\s*([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})\b", re.IGNORECASE)
RE_RECTIME_A = re.compile(r"\bRecorded\s+Time\s*[:\-]?\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?\s*(?:AM|PM))\b", re.IGNORECASE)
RE_BOOKPAGE_A = re.compile(r"\bRecorded\s+Book\s+and\s+Page\s*[:\-]?\s*(\d{3,})\s*[/\-]\s*(\d{1,6})\b", re.IGNORECASE)
RE_PAGECOUNT_A = re.compile(r"\bNumber\s+of\s+Pages.*?[:\-]?\s*(\d{1,3})\b", re.IGNORECASE)

# Barcode box style
RE_BOOKPG_B = re.compile(r"\bBk[:\s]*([0-9]{3,})\s+Pg[:\s]*([0-9]{1,6})\b", re.IGNORECASE)
RE_PAGEOF_B = re.compile(r"\bPage[:\s]*([0-9]{1,3})\s+of\s+([0-9]{1,3})\b", re.IGNORECASE)
RE_RECORDED_B = re.compile(r"\bRecorded[:\s]*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\s+([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?\s*(?:AM|PM))\b", re.IGNORECASE)

# Registry header style (THIS is what you have a lot)
RE_DATE_LINE = re.compile(r"\bDate[:\s]*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM))\b", re.IGNORECASE)
RE_DOCNUM_LINE = re.compile(r"\bDoc#\s*0*([0-9]{5,})\b", re.IGNORECASE)
RE_CTRL_LINE = re.compile(r"\bCtrl#\s*([0-9]{3,})\b", re.IGNORECASE)

# Lot/Map patterns
RE_LOT = re.compile(r"\bLot\s*(?:No\.?|Number)?\s*[:#]?\s*([A-Za-z0-9\-]+)\b", re.IGNORECASE)
RE_BLOCK = re.compile(r"\bBlock\s*(?:No\.?|Number)?\s*[:#]?\s*([A-Za-z0-9\-]+)\b", re.IGNORECASE)
RE_MAP = re.compile(r"\bMap\s*(?:No\.?|Number)?\s*[:#]?\s*([A-Za-z0-9\-]+)\b", re.IGNORECASE)
RE_MAPBLOCKLOT = re.compile(r"\bMap\s*([A-Za-z0-9\-]+)\s*[,/ ]+\s*Block\s*([A-Za-z0-9\-]+)\s*[,/ ]+\s*Lot\s*([A-Za-z0-9\-]+)\b", re.IGNORECASE)

RE_BUILDINGS_THEREON = re.compile(r"\bwith\s+the\s+buildings\s+thereon\b|\bbuildings\s+thereon\b", re.IGNORECASE)

# Parties + mailing
RE_GRANT_TO = re.compile(r"\b(?:GRANT(?:S)?\s+TO|GRANTS\s+TO|GRANT\s+TO)\s+(.+?)(?:,|\n)\s*", re.IGNORECASE)
RE_I_GRANTOR = re.compile(r"\bI,\s*([A-Z][A-Za-z\.\-'\s]+?),\s+of\b", re.IGNORECASE)
RE_MAILING_CUE = re.compile(r"\b(?:now\s+of|residing\s+at|having\s+an\s+address\s+of|with\s+an\s+address\s+of|After\s+Recording\s+Return\s+To)\b", re.IGNORECASE)
RE_ADDRESS_LINE = re.compile(r"\b(\d{1,6}\s+[A-Za-z0-9\.\-'\s]+?\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Place|Pl|Way)\b[^\n,]{0,40})", re.IGNORECASE)
RE_CITYSTATEZIP = re.compile(r"\b([A-Za-z][A-Za-z\.\-'\s]+),\s*(MA|Massachusetts)\s*(\d{5})?\b", re.IGNORECASE)

def guess_entity_type(name: str):
    u = name.upper()
    if any(x in u for x in ("LLC","INC","CORP","COMPANY","TRUST","BANK","LP","LLP")):
        return "Entity"
    return "Individual"

def enrich_one(rec: dict) -> dict:
    doc = rec.get("document") or {}
    prop = rec.get("property") or {}
    parties = rec.get("parties") or {}
    t = get_all_text_blobs(rec)

    # --- recording fields ---
    m = RE_DOCNUM_A.search(t)
    if m and not doc.get("document_number"):
        doc["document_number"] = m.group(1)

    m = RE_DOCTYPE_A.search(t)
    if m:
        dt = m.group(1).strip().upper()
        doc.setdefault("document_type", dt)
        doc.setdefault("instrument_type", dt)

    m = RE_RECDATE_A.search(t)
    if m and not doc.get("recording_date"):
        doc["recording_date"] = parse_date_any(m.group(1))

    m = RE_RECTIME_A.search(t)
    if m and not doc.get("recording_time"):
        doc["recording_time"] = parse_time_any(m.group(1))

    m = RE_BOOKPAGE_A.search(t)
    if m:
        doc.setdefault("book", m.group(1))
        doc.setdefault("page", m.group(2))

    m = RE_PAGECOUNT_A.search(t)
    if m and not doc.get("page_count"):
        try: doc["page_count"] = int(m.group(1))
        except Exception: pass

    m = RE_BOOKPG_B.search(t)
    if m:
        doc.setdefault("book", m.group(1))
        doc.setdefault("page", m.group(2))

    m = RE_PAGEOF_B.search(t)
    if m and not doc.get("page_count"):
        try: doc["page_count"] = int(m.group(2))
        except Exception: pass

    m = RE_RECORDED_B.search(t)
    if m:
        doc.setdefault("recording_date", parse_date_any(m.group(1)))
        doc.setdefault("recording_time", parse_time_any(m.group(2)))

    # NEW: Date: 12/26/2024 10:36 AM (registry header)
    m = RE_DATE_LINE.search(t)
    if m:
        doc.setdefault("recording_date", parse_date_any(m.group(1)))
        doc.setdefault("recording_time", parse_time_any(m.group(2)))

    m = RE_DOCNUM_LINE.search(t)
    if m and not doc.get("document_number"):
        doc["document_number"] = m.group(1)

    # --- safer Boston subcity: ONLY from property address text ---
    if safe_str(prop.get("city")).strip().upper() == "BOSTON" and not prop.get("subcity"):
        addr_u = (safe_str(prop.get("address_raw")) + " " + safe_str(prop.get("address_norm"))).upper()
        for k, v in BOSTON_SUBCITY.items():
            if k in addr_u:
                prop["subcity"] = v
                break

    # --- property_type upgrade: "buildings thereon" => BUILDING ---
    if RE_BUILDINGS_THEREON.search(t):
        if safe_str(prop.get("property_type")).upper() in ("LAND", "", "UNKNOWN") or prop.get("property_type") is None:
            prop["property_type"] = "BUILDING"

    # --- lot/map ---
    m = RE_MAPBLOCKLOT.search(t)
    if m:
        prop.setdefault("map_reference", f"Map {m.group(1)} / Block {m.group(2)} / Lot {m.group(3)}")
        prop.setdefault("block_number", m.group(2))
        prop.setdefault("lot_number", m.group(3))

    if not prop.get("lot_number"):
        m = RE_LOT.search(t)
        if m: prop["lot_number"] = m.group(1)

    if not prop.get("block_number"):
        m = RE_BLOCK.search(t)
        if m: prop["block_number"] = m.group(1)

    if not prop.get("map_reference"):
        m = RE_MAP.search(t)
        if m: prop["map_reference"] = f"Map {m.group(1)}"
        elif prop.get("block_number") and prop.get("lot_number"):
            prop["map_reference"] = f"Block {prop['block_number']} / Lot {prop['lot_number']}"

    # --- parties (light) ---
    parties.setdefault("grantors", parties.get("grantors") if isinstance(parties.get("grantors"), list) else [])
    parties.setdefault("grantees", parties.get("grantees") if isinstance(parties.get("grantees"), list) else [])

    if not parties["grantors"]:
        m = RE_I_GRANTOR.search(t)
        if m:
            name = norm_spaces(m.group(1))
            if name:
                parties["grantors"].append({"name_raw": name, "party_type": guess_entity_type(name)})

    if not parties["grantees"]:
        m = RE_GRANT_TO.search(t)
        if m:
            name = norm_spaces(m.group(1))
            if name:
                parties["grantees"].append({"name_raw": name, "party_type": guess_entity_type(name)})

    def find_mailing_address(text_block: str):
        if not RE_MAILING_CUE.search(text_block):
            return None
        m1 = RE_ADDRESS_LINE.search(text_block)
        if not m1:
            return None
        line = norm_spaces(m1.group(1))
        m2 = RE_CITYSTATEZIP.search(text_block)
        if m2:
            city2 = norm_spaces(m2.group(1))
            zip2 = m2.group(3)
            if zip2:
                return f"{line}, {city2}, MA {zip2}"
            return f"{line}, {city2}, MA"
        return line

    mailing = find_mailing_address(t)
    if mailing:
        if parties["grantees"]:
            parties["grantees"][0].setdefault("mailing_address_raw", mailing)
        elif parties["grantors"]:
            parties["grantors"][0].setdefault("mailing_address_raw", mailing)

    rec["document"] = doc
    rec["property"] = prop
    rec["parties"] = parties
    return rec

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inV2", required=True)
    ap.add_argument("--outV21", required=True)
    ap.add_argument("--outAudit", required=True)
    ap.add_argument("--maxRecords", type=int, default=0)
    args = ap.parse_args()

    inp = Path(args.inV2)
    out = Path(args.outV21)
    out_audit = Path(args.outAudit)
    out.parent.mkdir(parents=True, exist_ok=True)
    out_audit.parent.mkdir(parents=True, exist_ok=True)

    counts = {
        "rows_seen": 0,
        "rows_written": 0,
        "recording_fields_filled": 0,
        "lot_fields_filled": 0,
        "property_type_upgraded_to_building": 0,
        "mailing_address_filled": 0,
    }

    with inp.open("r", encoding="utf-8") as f_in, out.open("w", encoding="utf-8") as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            counts["rows_seen"] += 1

            before_doc = (rec.get("document") or {}).copy()
            before_prop = (rec.get("property") or {}).copy()
            before_parties = json.dumps(rec.get("parties") or {}, sort_keys=True)

            rec2 = enrich_one(rec)

            after_doc = rec2.get("document") or {}
            after_prop = rec2.get("property") or {}
            after_parties = json.dumps(rec2.get("parties") or {}, sort_keys=True)

            if (
                (not before_doc.get("recording_date") and after_doc.get("recording_date")) or
                (not before_doc.get("recording_time") and after_doc.get("recording_time")) or
                (not before_doc.get("document_number") and after_doc.get("document_number")) or
                (not before_doc.get("book") and after_doc.get("book")) or
                (not before_doc.get("page") and after_doc.get("page")) or
                (not before_doc.get("page_count") and after_doc.get("page_count"))
            ):
                counts["recording_fields_filled"] += 1

            if (
                (not before_prop.get("lot_number") and after_prop.get("lot_number")) or
                (not before_prop.get("block_number") and after_prop.get("block_number")) or
                (not before_prop.get("map_reference") and after_prop.get("map_reference"))
            ):
                counts["lot_fields_filled"] += 1

            if safe_str(before_prop.get("property_type")).upper() != "BUILDING" and safe_str(after_prop.get("property_type")).upper() == "BUILDING":
                counts["property_type_upgraded_to_building"] += 1

            if before_parties != after_parties:
                p = rec2.get("parties") or {}
                g = (p.get("grantees") or []) + (p.get("grantors") or [])
                if any(isinstance(x, dict) and x.get("mailing_address_raw") for x in g):
                    counts["mailing_address_filled"] += 1

            f_out.write(json.dumps(rec2, ensure_ascii=False) + "\n")
            counts["rows_written"] += 1

            if args.maxRecords and counts["rows_written"] >= args.maxRecords:
                break

    audit = {
        "created_at": utc_now_iso(),
        "in": str(inp),
        "out": str(out),
        "counts": counts
    }
    out_audit.write_text(json.dumps(audit, indent=2), encoding="utf-8")

    print("[done] wrote v2.1:", str(out))
    print("[done] wrote audit:", str(out_audit))
    print(json.dumps(counts, indent=2))

if __name__ == "__main__":
    main()
