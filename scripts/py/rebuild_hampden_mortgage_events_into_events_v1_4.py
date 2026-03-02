import json, hashlib
from datetime import datetime, timezone

INP = r"C:\seller-app\backend\publicData\registry\hampden\_raw_from_index_v1\mortgage_index_raw_v1_7.ndjson"
OUT = r"C:\seller-app\backend\publicData\registry\hampden\_events_v1_4\mortgage_events.ndjson"

def hid(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:24]

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

raw_total = 0
written = 0
locator_present = 0

with open(INP, "r", encoding="utf-8") as f_in, open(OUT, "w", encoding="utf-8") as f_out:
    for line in f_in:
        line = line.strip()
        if not line:
            continue
        raw_total += 1
        r = json.loads(line)

        # Pull locator from raw index (these fields exist in your sample)
        town_code = (r.get("town_code") or "").strip()
        town_raw = ""  # we'll keep both: code + inferred name later if needed; for now attach can work with town_raw if we map it
        addr_raw = (r.get("property_address") or "").strip()

        # IMPORTANT: we *can* derive town_raw from the printed block too, but your raw index doesn't store "Town: ___".
        # For Hampden index PDFs, the 3-letter code is the town key: SPD/WILB/CHIC/etc.
        # Step2 v1.7.4 is town-name based. So we store BOTH:
        # - town_code (for later mapping)
        # - town_raw blank for now (or we can map via a code->town table if you have it)
        #
        # HOWEVER: Your earlier ATTACHED_A results for deeds/assignments show "SPRINGFIELD Addr" etc,
        # meaning those event files already carried a town name. Mortgages need the same.
        #
        # For now, we attempt to infer town_name from common Hampden codes:
        code_map = {
            "SPD":"SPRINGFIELD",
            "CHIC":"CHICOPEE",
            "HOL":"HOLYOKE",
            "HLND":"HOLLAND",
            "LUD":"LUDLOW",
            "WFD":"WESTFIELD",
            "WILB":"WILBRAHAM",
            "LONG":"LONGMEADOW",
            "WSF":"WEST SPRINGFIELD",
            "AGA":"AGAWAM",
            "MONS":"MONSON",
            "PAL":"PALMER",
            "SOU":"SOUTHWICK",
            "ELO":"EAST LONGMEADOW",
            "BRIM":"BRIMFIELD",
            "TOLL":"TOLLAND",
            "GRAN":"GRANVILLE",
            "MONT":"MONTGOMERY",
            "RUSS":"RUSSELL"
        }
        town_raw = code_map.get(town_code, "")

        # Event ID: stable from doc_number + seq + county
        docnum = (r.get("document_number") or r.get("document_number_raw") or "").replace(",","").strip()
        seq = r.get("seq") or 1
        base = f"hampden|mortgage|{docnum}|{seq}|{addr_raw}|{town_raw}|{town_code}"
        event_id = f"MA|registry|mortgage|hampden|{hid(base)}"

        out = {
            "event_id": event_id,
            "event_type": "MORTGAGE",
            "county": "hampden",
            "recording": {
                "document_number_raw": r.get("document_number_raw"),
                "document_number": docnum or r.get("document_number"),
                "seq": seq,
                "entry_date_raw": r.get("entry_date_raw"),
                "recorded_time_raw": r.get("recorded_time_raw"),
                "book": (r.get("book") or None),
                "page": (r.get("page") or None),
            },
            "document": {
                "doc_type": r.get("doc_type") or "MTG",
                "doc_date_raw": r.get("doc_date_raw"),
                "favor_of": r.get("favor_of"),
                "description_raw": r.get("description_raw"),
                "raw_block": r.get("raw_block"),
            },
            "property_ref": {
                "town_raw": town_raw,
                "town_code": town_code,
                "address_raw": addr_raw,
                "legal_desc_raw": (r.get("raw_block") or None),
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
            "source": r.get("source") or {},
            "meta": {
                "rebuilt_from_raw_index": True,
                "rebuilt_utc": utc_now()
            }
        }

        if out["property_ref"]["town_raw"] and out["property_ref"]["address_raw"]:
            locator_present += 1

        f_out.write(json.dumps(out, ensure_ascii=False) + "\n")
        written += 1

print({
    "raw_total": raw_total,
    "mortgage_events_written": written,
    "locator_present": locator_present,
    "locator_missing": written - locator_present,
    "out": OUT
})
