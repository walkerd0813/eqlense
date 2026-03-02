import os, json, re, hashlib
from datetime import datetime, timezone

def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def ndjson_iter(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            yield json.loads(line)

def stable_id(*parts):
    s = "|".join([p if p is not None else "" for p in parts])
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()[:24]
    return h

# Hampden town-code hints (fallback only)
TOWN_CODE_MAP = {
  "AGA":"AGAWAM",
  "BLAN":"BLANDFORD",
  "BRIM":"BRIMFIELD",
  "CHES":"CHESTER",
  "CHIC":"CHICOPEE",
  "ELO":"EAST LONGMEADOW",
  "EAST":"EAST LONGMEADOW",
  "GRAN":"GRANVILLE",
  "HAMP":"HAMPDEN",
  "HOLL":"HOLLAND",
  "HOL":"HOLYOKE",
  "HOLY":"HOLYOKE",
  "LONG":"LONGMEADOW",
  "LUD":"LUDLOW",
  "MON":"MONSON",
  "MONT":"MONTGOMERY",
  "PALM":"PALMER",
  "RUSS":"RUSSELL",
  "SOU":"SOUTHWICK",
  "SWIC":"SOUTHWICK",
  "SPD":"SPRINGFIELD",
  "SPR":"SPRINGFIELD",
  "TOLL":"TOLLAND",
  "WAL":"WALES",
  "WFD":"WESTFIELD",
  "WSFD":"WEST SPRINGFIELD",
  "WSPR":"WEST SPRINGFIELD",
  "WILB":"WILBRAHAM",
}

def clean_ws(s):
    if s is None: return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def strip_y_addr_artifacts(s):
    s = clean_ws(s)
    # common artifacts from your dumps
    s = re.sub(r"\bAddr\b", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s+\bY\b\s*$", "", s).strip()   # trailing Y marker
    return clean_ws(s)

SUFFIX = [
  (r"\bAVENUE\b","AVE"),
  (r"\bSTREET\b","ST"),
  (r"\bROAD\b","RD"),
  (r"\bDRIVE\b","DR"),
  (r"\bLANE\b","LN"),
  (r"\bCOURT\b","CT"),
  (r"\bPLACE\b","PL"),
  (r"\bTERRACE\b","TER"),
  (r"\bCIRCLE\b","CIR"),
]

def normalize_address(a):
    a = strip_y_addr_artifacts(a).upper()
    for pat, rep in SUFFIX:
        a = re.sub(pat, rep, a)
    a = re.sub(r"\s+", " ", a).strip()
    return a

def normalize_town(t):
    t = strip_y_addr_artifacts(t).upper()
    t = re.sub(r"\s+", " ", t).strip()
    return t

def derive_town_raw(row):
    # prefer explicit town name if present
    t = row.get("town_raw") or row.get("town") or row.get("Town") or ""
    t = strip_y_addr_artifacts(t)
    if t: return t.upper()

    code = clean_ws(row.get("town_code") or row.get("townCode") or row.get("TOWN") or "").upper()
    if code:
        return TOWN_CODE_MAP.get(code, code)
    return ""

def derive_address_raw(row):
    # prefer structured property_address if present
    addr = row.get("property_address") or row.get("address") or row.get("Addr") or row.get("ADDRESS") or ""
    addr = strip_y_addr_artifacts(addr)
    if addr: return addr.upper()

    # parse from raw_block if needed
    rb = row.get("raw_block") or ""
    rb = rb.replace("\r","")
    # patterns seen in screenshots: "Town: XXX  Addr:YYY"
    m = re.search(r"Addr:\s*([^\n]+)", rb, flags=re.IGNORECASE)
    if m:
        return strip_y_addr_artifacts(m.group(1)).upper()

    # mortgages-style second line: indentation + address
    lines = [ln.rstrip() for ln in rb.split("\n") if ln.strip()]
    if len(lines) >= 2:
        cand = strip_y_addr_artifacts(lines[1])
        # remove trailing money if present
        cand = re.sub(r"\s+\d[\d,]*\.\d{2}\s*$", "", cand).strip()
        if cand:
            return cand.upper()

    return ""

def derive_amount(row):
    # most raw index rows already have numeric amount
    a = row.get("amount")
    if isinstance(a, (int, float)):
        return float(a)
    # sometimes consideration shows up differently
    for k in ["consideration", "CONSID", "consideration_amount"]:
        v = row.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    return None

def main():
    in_path  = os.path.join("publicData","registry","hampden","_raw_from_index_v1","deed_index_raw_v1_2.ndjson")
    out_dir  = os.path.join("publicData","registry","hampden","_events_v1_4")
    out_path = os.path.join(out_dir, "deed_events.ndjson")
    os.makedirs(out_dir, exist_ok=True)

    if not os.path.exists(in_path):
        raise SystemExit(f"Input not found: {in_path}")

    total = 0
    locator_present = 0
    locator_missing = 0
    samples_missing = []

    with open(out_path, "w", encoding="utf-8") as out:
        for row in ndjson_iter(in_path):
            total += 1

            docnum_raw = clean_ws(row.get("document_number_raw") or row.get("document_number") or row.get("doc_number_raw") or "")
            docnum     = clean_ws(row.get("document_number") or re.sub(r"[^\d]", "", docnum_raw) or "")
            seq        = row.get("seq") or row.get("SEQ") or 1
            entry_date_raw = clean_ws(row.get("entry_date_raw") or row.get("entry_date") or row.get("ENTRY_DATE") or "")
            recorded_time_raw = clean_ws(row.get("recorded_time_raw") or row.get("time_raw") or row.get("TIME") or "")

            town_raw    = derive_town_raw(row)
            address_raw = derive_address_raw(row)

            if town_raw and address_raw:
                locator_present += 1
            else:
                locator_missing += 1
                if len(samples_missing) < 10:
                    samples_missing.append({
                        "document_number_raw": docnum_raw,
                        "town_raw": town_raw,
                        "address_raw": address_raw,
                        "has_raw_block": bool(row.get("raw_block")),
                    })

            # normalize for downstream keying (attach script will do its own too, but we store clean values)
            town_norm = normalize_town(town_raw)
            addr_norm = normalize_address(address_raw)

            event_id = "MA|registry|deed|hampden|" + stable_id("hampden","DEED",docnum,str(seq),entry_date_raw,recorded_time_raw)

            event = {
                "event_id": event_id,
                "event_type": "DEED",
                "county": "hampden",

                "recording": {
                    "document_number_raw": docnum_raw or None,
                    "document_number": docnum or None,
                    "seq": seq,
                    "entry_date_raw": entry_date_raw or None,
                    "recorded_time_raw": recorded_time_raw or None,
                    "book": row.get("book") if "book" in row else None,
                    "page": row.get("page") if "page" in row else None,
                },

                "document": {
                    "doc_type": row.get("doc_type") or row.get("DOC_TYPE") or "DEED",
                    "doc_date_raw": row.get("doc_date_raw") or None,
                    "description_raw": row.get("description_raw") or None,
                    "raw_block": row.get("raw_block") or None,
                    # deed-specific parties if present in raw index
                    "grantors": row.get("grantors") or None,
                    "grantees": row.get("grantees") or None,
                },

                "property_ref": {
                    "town_raw": town_raw or "",
                    "town_code": clean_ws(row.get("town_code") or "").upper() or "",
                    "address_raw": address_raw or "",
                    "town_norm": town_norm or "",
                    "address_norm": addr_norm or "",
                },

                "consideration": {
                    "text_raw": None,
                    "amount": derive_amount(row),
                    "nominal_flag": False,
                },

                "transaction_semantics": {
                    "tx_class": "deed",
                    "confidence_score": 0.9 if (town_raw and address_raw) else 0.6,
                    "rules_fired": ["rebuilt_from_step0_raw_index_deed_v1"],
                },

                "source": row.get("source") or {},
                "meta": {"rebuilt_from_raw_index": True, "rebuilt_utc": now_utc()},
            }

            out.write(json.dumps(event, ensure_ascii=False) + "\n")

    print({
        "in": os.path.abspath(in_path),
        "out": os.path.abspath(out_path),
        "raw_total": total,
        "locator_present": locator_present,
        "locator_missing": locator_missing,
        "locator_pct": (locator_present / total * 100.0) if total else 0.0,
        "missing_samples": samples_missing
    })

if __name__ == "__main__":
    main()
