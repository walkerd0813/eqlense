import json, re, hashlib
from datetime import datetime, timezone

RAW = r"C:\seller-app\backend\publicData\registry\hampden\_raw_from_index_v1\mortgage_index_raw_v1_7.ndjson"
OUT = r"C:\seller-app\backend\publicData\registry\hampden\_events_v1_4\mortgage_events.ndjson"

TOWN_CODE_MAP = {
  "SPD":"SPRINGFIELD",
  "WFD":"WESTFIELD",
  "WSP":"WEST SPRINGFIELD",
  "CHIC":"CHICOPEE",
  "HLND":"HOLLAND",
  "HOLY":"HOLYOKE",
  "HLYK":"HOLYOKE",
  "MONS":"MONSON",
  "PALM":"PALMER",
  "LUDL":"LUDLOW",
  "WILB":"WILBRAHAM",
  "SOUT":"SOUTHWICK",
  "LONG":"LONGMEADOW",
  "ELOM":"EAST LONGMEADOW",
  "ELMD":"EAST LONGMEADOW",
  "AGAW":"AGAWAM",
  "HAMP":"HAMPDEN",
  "BRIM":"BRIMFIELD",
  "GRAN":"GRANVILLE",
  "TOLL":"TOLLAND",
  "WALE":"WALES",
  "MONT":"MONTGOMERY",
  "RUSS":"RUSSELL",
  "BLAN":"BLANDFORD",
  "CHEST":"CHESTER",
}

def sha1(s: str) -> str:
  return hashlib.sha1(s.encode("utf-8")).hexdigest()

def norm_space(s: str) -> str:
  return re.sub(r"\s+", " ", (s or "").strip())

def event_id_from(parts):
  key = "|".join([str(p) for p in parts if p is not None])
  return "MA|registry|mortgage|hampden|" + sha1(key)[:24]

def address_from_raw_block(raw_block: str) -> str:
  # In these reports, the address is usually on the next line after line0, indented, before the amount
  # Example: "                    95 EDDY ST                             135,205.00"
  if not raw_block:
    return ""
  lines = raw_block.splitlines()
  for ln in lines[1:6]:
    t = ln.rstrip()
    if not t.strip():
      continue
    t = re.sub(r"\s+Y\s*$", "", t)
    # remove trailing amount if present
    t2 = re.split(r"\s{2,}[\d,]+\.\d{2}\s*$", t.strip())[0].strip()
    # heuristics: starts with digit or contains LOT/UNIT/ST/RD/etc
    if re.search(r"^\d", t2) or re.search(r"\b(LOT|UNIT|ST|AVE|RD|DR|LA|LN|CT|PL|BLVD|HWY|PKWY)\b", t2.upper()):
      return norm_space(t2)
  return ""

total=0
locator_present=0
locator_missing=0
missing_samples=[]

with open(RAW,"r",encoding="utf-8") as f_in, open(OUT,"w",encoding="utf-8") as f_out:
  for line in f_in:
    if not line.strip():
      continue
    total += 1
    r = json.loads(line)

    src = r.get("source") or {}
    page = src.get("page")
    line0 = src.get("line0") or r.get("line0") or ""

    town_code = norm_space(r.get("town_code") or "")
    town_raw = TOWN_CODE_MAP.get(town_code, "")

    # This field exists in your raw row
    addr_raw = norm_space(r.get("property_address") or "")

    # fallback: parse from raw_block if missing
    if not addr_raw:
      addr_raw = address_from_raw_block(r.get("raw_block") or "")

    eid = event_id_from([page, line0, r.get("document_number"), r.get("seq"), town_code, addr_raw])

    out = {
      "event_id": eid,
      "event_type": "MORTGAGE",
      "county": "hampden",
      "recording": {
        "document_number_raw": r.get("document_number_raw"),
        "document_number": r.get("document_number"),
        "seq": r.get("seq"),
        "entry_date_raw": r.get("entry_date_raw"),
        "recorded_time_raw": r.get("recorded_time_raw"),
      },
      "document": {
        "doc_type": r.get("doc_type"),
        "doc_date_raw": r.get("doc_date_raw"),
        "favor_of": r.get("favor_of"),
      },
      "property_ref": {
        "town_raw": town_raw,
        "town_code": town_code,
        "address_raw": addr_raw,
        "raw_block": r.get("raw_block")  # keep for audit/explainability
      },
      "source": {
        "county": "hampden",
        "pdf": src.get("pdf"),
        "page": page,
        "line0": line0,
      },
      "consideration": {
        "text_raw": None,
        "amount": r.get("amount"),
        "nominal_flag": False
      },
      "transaction_semantics": {
        "tx_class": "mortgage",
        "confidence_score": 0.9,
        "rules_fired": ["rebuilt_from_step0_raw_index"]
      },
      "meta": {
        "rebuilt_from_raw_index": True,
        "rebuilt_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat()
      }
    }

    if town_raw and addr_raw:
      locator_present += 1
    else:
      locator_missing += 1
      if len(missing_samples) < 10:
        missing_samples.append({"event_id": eid, "page": page, "line0": line0, "town_code": town_code, "town_raw": town_raw, "address_raw": addr_raw})

    f_out.write(json.dumps(out, ensure_ascii=False) + "\n")

print({
  "raw_total": total,
  "mortgage_events_written": total,
  "locator_present": locator_present,
  "locator_missing": locator_missing,
  "missing_samples": missing_samples
})
