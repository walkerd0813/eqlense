import argparse, json, re, hashlib
from datetime import datetime, timezone
from pathlib import Path

BOSTON_SUBCITIES = [
  "Jamaica Plain","Roslindale","Dorchester","Roxbury","Mattapan","East Boston",
  "South Boston","South Boston Waterfront","Charlestown","Brighton","Allston",
  "West Roxbury","Hyde Park","Mission Hill","Fenway","Back Bay","North End",
  "Beacon Hill","South End","Chinatown","Downtown","Seaport","Longwood"
]

SUBCITY_RE = re.compile(r"\b(" + "|".join(re.escape(x) for x in sorted(BOSTON_SUBCITIES, key=len, reverse=True)) + r")\b", re.I)

RECORDING_INFO_BLOCK = re.compile(
  r"(Recording Information|Document Number|Recorded Date|Recorded Time|Number of Pages|Recorded Book and Page)",
  re.I
)

DOCNUM_RE = re.compile(r"\b(Document\s*Number|Doc#|Doc\s*#)\s*[:#]?\s*0*([0-9]{4,})\b", re.I)
DOCTYPE_RE = re.compile(r"\b(Document\s*Type|Doc\s*Type)\s*[:#]?\s*([A-Z]{2,5})\b", re.I)
REC_DATE_RE = re.compile(r"\b(Recorded\s*Date)\s*[:#]?\s*([A-Za-z]+)\s+([0-9]{1,2}),\s*([0-9]{4})\b", re.I)
REC_TIME_RE = re.compile(r"\b(Recorded\s*Time)\s*[:#]?\s*([0-9]{1,2}:[0-9]{2}:[0-9]{2}\s*(AM|PM))\b", re.I)
PAGES_RE = re.compile(r"\b(Number\s*of\s*Pages(?:\s*\(including\s*cover\s*sheet\))?)\s*[:#]?\s*([0-9]{1,3})\b", re.I)
BOOKPAGE_RE = re.compile(r"\b(Recorded\s*Book\s*and\s*Page|Bk:\s*|Book)\s*[:#]?\s*([0-9]{3,})\s*(?:/|Pg:\s*|Page)\s*([0-9]{1,5})\b", re.I)

# Parties (lightweight heuristics; we’ll improve later)
GRANT_TO_RE = re.compile(r"\bgrant\s+to\s+(.+?)(?:,?\s+with\s+QUITCLAIM|\s+WITH\s+QUITCLAIM|\s+with\s+quitclaim|\n)\b", re.I | re.S)
NOW_OF_ADDR_RE = re.compile(r"\bnow\s+of\s+(.+?)(?:\n|,?\s+WITH|\s+with\s+QUITCLAIM|\.)", re.I | re.S)
HAVING_ADDR_RE = re.compile(r"\bhaving\s+an\s+address\s+of\s+(.+?)(?:\n|,?\s+WITH|\.)", re.I | re.S)
SEND_TAX_RE = re.compile(r"\bSend\s+Tax\s+Notices\s+to\s*[:/]\s*(.+)$", re.I | re.M)

# Property semantics
BUILDINGS_CUE_RE = re.compile(r"\b(buildings\s+thereon|with\s+the\s+buildings\s+thereon|the\s+buildings\s+thereon)\b", re.I)
CONDO_CUE_RE = re.compile(r"\b(condominium|condo\s+unit|unit\s+designation|master\s+deed|declaration\s+of\s+trust)\b", re.I)
UNIT_RE = re.compile(r"\b(?:Unit|Apt\.?|Apartment|#)\s*([A-Za-z0-9\-]+)\b", re.I)

def utc_now():
  return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00","Z")

def norm_ws(s: str) -> str:
  return re.sub(r"\s+", " ", (s or "")).strip()

def month_to_num(m):
  months = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
  return months.get(m.lower())

def parse_recording_fields(text):
  if not text:
    return {}
  t = text
  out = {}
  m = DOCNUM_RE.search(t)
  if m: out["document_number"] = m.group(2)
  m = DOCTYPE_RE.search(t)
  if m: out["document_type_code"] = m.group(2).upper()
  m = REC_DATE_RE.search(t)
  if m:
    mm = month_to_num(m.group(2))
    if mm:
      dd = int(m.group(3)); yy = int(m.group(4))
      out["recording_date"] = f"{yy:04d}-{mm:02d}-{dd:02d}"
  m = REC_TIME_RE.search(t)
  if m:
    out["recording_time"] = m.group(2).upper().replace(" ", "")
  m = PAGES_RE.search(t)
  if m:
    try: out["page_count"] = int(m.group(2))
    except: pass
  m = BOOKPAGE_RE.search(t)
  if m:
    out["book"] = m.group(2)
    out["page"] = m.group(3)
  return out

def pick_subcity(city, address_text, body_text):
  if (city or "").lower() != "boston":
    return None
  blob = f"{address_text or ''} {body_text or ''}"
  m = SUBCITY_RE.search(blob)
  if m:
    val = m.group(1)
    # normalize to canonical capitalization
    for x in BOSTON_SUBCITIES:
      if x.lower() == val.lower():
        return x
    return val
  return None

def improved_property_type(existing_type, body_text):
  t = existing_type or None
  bt = body_text or ""
  if CONDO_CUE_RE.search(bt):
    return "CONDO"
  if BUILDINGS_CUE_RE.search(bt):
    # If it explicitly says buildings, it’s not “LAND-only” semantics.
    return "BUILDING"
  return t or "UNKNOWN"

def extract_unit_designation(text):
  if not text:
    return None
  m = UNIT_RE.search(text)
  return m.group(1) if m else None

def best_mailing_address(body_text):
  if not body_text:
    return None
  m = SEND_TAX_RE.search(body_text)
  if m:
    return norm_ws(m.group(1))
  # fallback: “having an address of …”
  m = HAVING_ADDR_RE.search(body_text)
  if m:
    return norm_ws(m.group(1))
  # fallback: “now of …”
  m = NOW_OF_ADDR_RE.search(body_text)
  if m:
    return norm_ws(m.group(1))
  return None

def safe_hash(s: str) -> str:
  return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:24]

def build_event_id(sha256, book, page, docnum):
  base = f"{sha256}|{book or ''}|{page or ''}|{docnum or ''}"
  return hashlib.sha256(base.encode("utf-8")).hexdigest()[:24]

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--inCanonicalV1", required=True)
  ap.add_argument("--outCanonicalV2", required=True)
  ap.add_argument("--outAudit", required=True)
  ap.add_argument("--maxRecords", type=int, default=0)
  args = ap.parse_args()

  inp = Path(args.inCanonicalV1)
  out = Path(args.outCanonicalV2)
  aud = Path(args.outAudit)
  out.parent.mkdir(parents=True, exist_ok=True)
  aud.parent.mkdir(parents=True, exist_ok=True)

  counts = {
    "rows_seen": 0,
    "rows_written": 0,
    "subcity_filled": 0,
    "property_type_changed": 0,
    "unit_designation_filled": 0,
    "mailing_address_filled": 0,
    "recording_fields_filled": 0,
  }
  address_source_location_counts = {}

  with inp.open("r", encoding="utf-8") as f_in, out.open("w", encoding="utf-8") as f_out:
    for line in f_in:
      line = line.strip()
      if not line:
        continue
      counts["rows_seen"] += 1
      if args.maxRecords and counts["rows_seen"] > args.maxRecords:
        break

      o = json.loads(line)

      # v1 layout has property_locator + address_candidates + source + (maybe) body/margin context inside candidates
      prop_loc = o.get("property_locator") or {}
      src = o.get("source") or {}
      addr_raw = prop_loc.get("raw_address") or o.get("property", {}).get("address_raw")
      addr_norm = prop_loc.get("normalized_address") or o.get("property", {}).get("address_norm")
      city = prop_loc.get("city") or o.get("property", {}).get("city")
      state = prop_loc.get("state") or o.get("property", {}).get("state")
      zipc = prop_loc.get("zip") or o.get("property", {}).get("zip")

      addr_src_loc = prop_loc.get("address_source_location") or "NONE"
      address_source_location_counts[addr_src_loc] = address_source_location_counts.get(addr_src_loc, 0) + 1

      # Collect “body text” from best available place:
      # - many v1 records contain OCR context in address_candidates[].context
      # We'll stitch contexts for enrichment only (not for property-address selection).
      body_blob = ""
      cands = o.get("address_candidates") or []
      # prefer non-margin contexts (they tend to include deed body + recording info)
      for c in cands:
        ctx = c.get("context") if isinstance(c, dict) else None
        if ctx:
          body_blob += "\n" + ctx
      body_blob = body_blob[:50000]  # cap

      # Recording fields: try to parse from stitched contexts (often includes header block)
      rec = parse_recording_fields(body_blob)
      recording_filled = any(k in rec for k in ["document_number","recording_date","recording_time","page_count","book","page"])
      if recording_filled:
        counts["recording_fields_filled"] += 1

      # Property semantics
      existing_pt = o.get("property_type") or (o.get("property") or {}).get("property_type")
      new_pt = improved_property_type(existing_pt, body_blob)
      if (existing_pt or "") != (new_pt or ""):
        counts["property_type_changed"] += 1

      unit_designation = extract_unit_designation(body_blob)
      if unit_designation:
        counts["unit_designation_filled"] += 1

      subcity = pick_subcity(city, addr_norm or addr_raw, body_blob)
      if subcity:
        counts["subcity_filled"] += 1

      mailing_addr = best_mailing_address(body_blob)
      if mailing_addr:
        counts["mailing_address_filled"] += 1

      # Build v2 record (matches your required schema sections)
      sha = src.get("sha256")
      book = rec.get("book") or o.get("book") or (o.get("document") or {}).get("book") or o.get("page")  # keep fallback safe
      page = rec.get("page") or o.get("page") or (o.get("document") or {}).get("page")
      docnum = rec.get("document_number") or (o.get("document") or {}).get("document_number")

      event_id = build_event_id(sha or "", book, page, docnum)

      # Confidence: margin hit is “A” and high score by policy
      if addr_src_loc == "LEFT_MARGIN_ROTATED":
        conf_grade = "A"
        conf_score = 0.97
      elif addr_src_loc == "BODY_OR_OTHER":
        conf_grade = "B"
        conf_score = 0.75
      else:
        conf_grade = "C"
        conf_score = 0.40

      v2 = {
        "event_id": event_id,
        "event_type": o.get("doc_type") or o.get("event_type") or "DEED",
        "registry": {
          "state": "MA",
          "county": (o.get("registry_county") or o.get("registry", {}).get("county") or None),
          "district": (o.get("registry_district") or o.get("registry", {}).get("district") or None)
        },
        "document": {
          "document_type": (o.get("document_type") or o.get("document", {}).get("document_type") or "DEED"),
          "instrument_type": (o.get("instrument_type") or o.get("document", {}).get("instrument_type") or "DEED"),
          "recording_date": (rec.get("recording_date") or o.get("recording_date") or o.get("document", {}).get("recording_date")),
          "recording_time": (rec.get("recording_time") or o.get("recording_time") or o.get("document", {}).get("recording_time")),
          "book": (rec.get("book") or o.get("book") or o.get("document", {}).get("book")),
          "page": (rec.get("page") or o.get("page") or o.get("document", {}).get("page")),
          "instrument_number": (o.get("instrument_number") or o.get("document", {}).get("instrument_number")),
          "document_number": (rec.get("document_number") or o.get("document_number") or o.get("document", {}).get("document_number")),
          "page_count": (rec.get("page_count") or o.get("page_count") or o.get("document", {}).get("page_count"))
        },
        "source": {
          "rel_path": src.get("rel_path"),
          "file": src.get("file"),
          "sha256": sha,
          "local_path": (src.get("local_path") or None),
          "image_source_url": (src.get("image_source_url") or None)
        },
        "property": {
          "address_raw": addr_raw,
          "address_norm": addr_norm,
          "city": city,
          "subcity": subcity,
          "state": state,
          "zip": zipc,
          "town_name_raw": None,
          "property_type": new_pt,
          "legal_description_raw": None,
          "map_reference": None,
          "lot_number": None,
          "block_number": None,
          "unit_designation": unit_designation,
          "condo_name": None,
          "land_court_flag": None
        },
        "transaction": {
          "transaction_type": None,
          "arms_length_flag": None,
          "consideration_text_raw": (o.get("consideration_text_raw") or o.get("consideration") or None),
          "consideration_numeric": None,
          "nominal_consideration_flag": None,
          "transfer_tax_amount": None,
          "tax_stamp_value": None
        },
        "parties": {
          "grantors": (o.get("grantors") or []),
          "grantees": (o.get("grantees") or []),
          "signatories": [],
          "trust": {
            "trust_name": None,
            "trust_date": None,
            "executor_or_trustee_name": None
          },
          "mailing_address_party": mailing_addr
        },
        "references": {
          "prior_deed_refs": [],
          "mortgage_refs": [],
          "assignment_refs": [],
          "release_refs": [],
          "subject_to_mortgage_flag": None,
          "assumption_flag": None
        },
        "timing": {
          "execution_date": None,
          "recording_delay_days": None,
          "same_day_group_id": None,
          "related_document_count_same_day": None
        },
        "ocr_evidence": {
          "address_source_location": addr_src_loc,
          "page_number_address_found": (o.get("ocr_evidence", {}).get("page_number_address_found") or 1),
          "bounding_box": None,
          "ocr_method": src.get("extract_method"),
          "ocr_page_limit": src.get("ocr_page_limit"),
          "ocr_dpi": src.get("ocr_dpi"),
          "tesseract_config": src.get("tesseract_config"),
          "ocr_timeout_sec": src.get("ocr_timeout_sec"),
          "margin_text_len": src.get("margin_text_len"),
          "body_text_len": src.get("body_text_len"),
          "address_candidates": cands
        },
        "derived": {
          "property_match_candidates": [],
          "property_id_final": None,
          "match_confidence": None,
          "ambiguity_flag": None,
          "manual_review_required_flag": None
        },
        "qa": {
          "flags": (o.get("flags") or []),
          "confidence": conf_score,
          "notes": None
        }
      }

      f_out.write(json.dumps(v2, ensure_ascii=False) + "\n")
      counts["rows_written"] += 1

  audit = {
    "created_at": utc_now(),
    "in": str(inp),
    "out": str(out),
    "counts": counts,
    "address_source_location_counts": address_source_location_counts
  }
  aud.write_text(json.dumps(audit, indent=2), encoding="utf-8")

  print("[done] wrote v2:", str(out))
  print("[done] wrote audit:", str(aud))
  print(json.dumps(counts, indent=2))
  print("[verify] address_source_location_counts:", json.dumps(address_source_location_counts, indent=2))

if __name__ == "__main__":
  main()
