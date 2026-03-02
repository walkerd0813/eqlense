import argparse, os, json, hashlib, datetime, re
from typing import Any, Dict, Optional, List, Tuple

def now_utc_iso():
    return datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00","Z")

def stable_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:24]

def read_ndjson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def write_ndjson_line(path: str, obj: Dict[str, Any]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def norm_money(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", str(s))
    if not digits:
        return None
    try:
        return int(digits)
    except:
        return None

ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
STATE_RE = re.compile(r"\bMA\b", re.IGNORECASE)

def parse_city_state_zip(addr: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not addr:
        return (None, None, None)
    z = None
    m = ZIP_RE.search(addr)
    if m:
        z = m.group(1)
    st = "MA" if STATE_RE.search(addr) else None

    # naive city grab: "... , CITY, MA" or "CITY MA"
    city = None
    # prefer comma pattern
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if len(parts) >= 2:
        # if last part contains MA/zip, take the second-to-last as city
        tail = parts[-1]
        if ("MA" in tail.upper()) or ZIP_RE.search(tail):
            city = parts[-2].upper()
    return (city, st, z)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inExtracted", required=True)
    ap.add_argument("--county", default="suffolk")
    ap.add_argument("--outEvents", required=True)
    ap.add_argument("--outAudit", required=True)
    args = ap.parse_args()

    # overwrite output if exists (idempotent)
    if os.path.exists(args.outEvents):
        os.remove(args.outEvents)

    seen = 0
    wrote = 0
    missing = {
        "NO_RECORDED_DATE": 0,
        "NO_BOOK_PAGE": 0,
        "NO_PROPERTY_ADDRESS": 0,
        "AMBIGUOUS_ADDRESS": 0
    }

    for row in read_ndjson(args.inExtracted):
        seen += 1
        src = row.get("source", {})
        ext = row.get("extracted", {})
        flags = ext.get("flags", []) or []

        recorded_at = ext.get("recorded_date")
        book = ext.get("book")
        page = ext.get("page")

        prop_raw = ext.get("property_address_raw")
        prop_norm = ext.get("property_address_norm")
        cand = ext.get("address_candidates", []) or []

        # track missing
        for k in list(missing.keys()):
            if k in flags:
                missing[k] += 1
        if not recorded_at:
            missing["NO_RECORDED_DATE"] += 1
        if (not book) or (not page):
            missing["NO_BOOK_PAGE"] += 1
        if not prop_raw:
            missing["NO_PROPERTY_ADDRESS"] += 1

        city, st, zipc = parse_city_state_zip(prop_norm or prop_raw)

        key_parts = [
            args.county.lower(),
            "DEED",
            str(recorded_at or ""),
            str(book or ""),
            str(page or ""),
            str(src.get("file") or ""),
            str(src.get("ocr_text_fingerprint") or "")
        ]
        event_id = stable_hash("|".join(key_parts))

        evt = {
            "event_id": event_id,
            "county": args.county.lower(),
            "event_type": "DEED",
            "recorded_at": recorded_at,     # YYYY-MM-DD (V1)
            "book": book,
            "page": page,
            "instrument_number": None,
            "instrument_type": ext.get("instrument_type"),
            "consideration_amount": norm_money(ext.get("consideration")),
            "grantors": ext.get("grantors") or [],
            "grantees": ext.get("grantees") or [],
            "property_locator": {
                "raw_address": prop_raw,
                "normalized_address": prop_norm,
                "city": city,
                "state": st,
                "zip": zipc
            },
            "address_candidates": cand,  # keep evidence; do NOT throw away
            "source": {
                "source_file": src.get("file"),
                "rel_path": src.get("rel_path"),
                "sha256": src.get("sha256"),
                "extract_method": src.get("extract_method"),
                "ocr_text_fingerprint": src.get("ocr_text_fingerprint"),
                "ocr_page_limit": src.get("ocr_page_limit"),
                "ocr_dpi": src.get("ocr_dpi"),
            },
            "qa": {
                "flags": flags,
                "address_status": ("OK" if prop_raw else ("AMBIGUOUS" if "AMBIGUOUS_ADDRESS" in flags else "MISSING"))
            }
        }

        write_ndjson_line(args.outEvents, evt)
        wrote += 1
        if wrote % 1000 == 0:
            print(f"[progress] normalized {wrote}/{seen}")

    audit = {
        "created_at": now_utc_iso(),
        "in": args.inExtracted,
        "out": args.outEvents,
        "seen": seen,
        "wrote": wrote,
        "missing_rates": {k: (round(v/seen, 4) if seen else 0.0) for k, v in missing.items()}
    }
    os.makedirs(os.path.dirname(args.outAudit), exist_ok=True)
    with open(args.outAudit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote events:", args.outEvents)
    print("[done] audit:", args.outAudit)

if __name__ == "__main__":
    main()
