import json, re, os, hashlib
from datetime import datetime

INP  = r"C:\seller-app\backend\publicData\registry\hampden\_raw_from_index_v1\deed_index_raw_v1_2.ndjson"
OUT  = r"C:\seller-app\backend\publicData\registry\hampden\_events_v1_4\deed_events.ndjson"

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def strip_trailing_y(s: str) -> str:
    s = s or ""
    # remove trailing single-letter Y tokens at end (often padded)
    s = re.sub(r"\s+Y\s*$", "", s)
    return s

def clean_town(town_field: str) -> str:
    t = norm_space(town_field)
    t = strip_trailing_y(t)
    # examples: "SPRINGFIELD           Addr" -> "SPRINGFIELD"
    t = re.sub(r"\bAddr\b.*$", "", t, flags=re.IGNORECASE).strip()
    t = norm_space(t)
    return t

def clean_addr(addr_field: str) -> str:
    a = norm_space(addr_field)
    a = strip_trailing_y(a)
    return norm_space(a)

def stable_id(county: str, docno: str, book: str, page: str, rdate: str, rtime: str, town: str, addr: str) -> str:
    seed = "|".join([county or "", docno or "", book or "", page or "", rdate or "", rtime or "", town or "", addr or ""])
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]
    return f"MA|registry|deed|{county}|{h}"

os.makedirs(os.path.dirname(OUT), exist_ok=True)

total = 0
written = 0
locator_present = 0
missing_samples = []

with open(INP, "r", encoding="utf-8") as f, open(OUT, "w", encoding="utf-8") as w:
    for line in f:
        line = line.strip()
        if not line:
            continue
        total += 1
        obj = json.loads(line)

        county = (obj.get("county") or "hampden").lower()

        rec = obj.get("recording") or {}
        docno = rec.get("document_number") or ""
        rdate = rec.get("recording_date") or ""
        rtime = rec.get("recording_time") or ""
        book  = rec.get("book") or ""
        page  = rec.get("page") or ""

        pref = obj.get("property_ref") or {}
        town_raw = clean_town(pref.get("town") or "")
        addr_raw = clean_addr(pref.get("address") or "")

        raw_lines = obj.get("raw_lines") or []
        raw_block = "\n".join(raw_lines).strip() if raw_lines else None

        has_locator = bool(town_raw and addr_raw)
        if has_locator:
            locator_present += 1
        else:
            if len(missing_samples) < 10:
                missing_samples.append({
                    "document_number": docno,
                    "town": pref.get("town"),
                    "address": pref.get("address"),
                    "has_raw_lines": bool(raw_lines)
                })

        event = {
            "event_id": stable_id(county, docno, book, page, rdate, rtime, town_raw, addr_raw),
            "event_type": "DEED",
            "county": county,
            "recording": {
                "document_number_raw": docno if docno else None,
                "document_number": docno if docno else None,
                "seq": 1,
                "entry_date_raw": None,
                "recorded_time_raw": rtime if rtime else None,
                "book": book if book else None,
                "page": page if page else None,
                "recording_date_raw": rdate if rdate else None
            },
            "document": {
                "doc_type": "DEED",
                "doc_date_raw": None,
                "description_raw": None,
                "raw_block": raw_block,
                "grantors": None,
                "grantees": None
            },
            "property_ref": {
                "town_raw": town_raw,
                "town_code": None,
                "address_raw": addr_raw,
                "legal_desc_raw": raw_block
            },
            "consideration": {"text_raw": None, "amount": None, "nominal_flag": False},
            "transaction_semantics": {"tx_class": "deed", "confidence_score": 0.7, "rules_fired": ["rebuilt_from_deed_index_raw_v1_2"]},
            "source": obj.get("source") or {},
            "meta": {"rebuilt_from_raw_index": True, "rebuilt_utc": datetime.utcnow().isoformat() + "Z"}
        }

        w.write(json.dumps(event, ensure_ascii=False) + "\n")
        written += 1

print({
    "in": INP,
    "out": OUT,
    "raw_total": total,
    "deed_events_written": written,
    "locator_present": locator_present,
    "locator_missing": (total - locator_present),
    "locator_pct": (locator_present / total * 100.0 if total else 0.0),
    "missing_samples": missing_samples
})
