#!/usr/bin/env python3
"""
EquityLens Registry Postfix: normalize doc types + QA report

Goal:
- Detect "doc_type_code" values that are clearly street suffixes/directions or address bleed-through.
- For those rows: set doc_type_code/doc_type_desc to "UNKNOWN" (do not invent a replacement).
- Preserve originals into doc_type_code_raw/doc_type_desc_raw (your evolved schema allows extras).
- Emit a QA report JSON with counts, top offenders, per-pdf breakdown, and samples.

This is designed to run AFTER extraction for:
- Hampden OTR PDF index events (otr_pdf_index)
- Suffolk CSV index events (registry_index_csv)
"""

import argparse
import collections
import json
import os
import re
from datetime import datetime, timezone

STREET_SUFFIX = {
    "ST","RD","AVE","DR","LN","CT","PL","BLVD","WAY","PATH","SQ","TER","TERR","CIR","PKWY","HWY","EXPY","TRL","ROW",
    "PLZ","PK","PARK","RUN"
}
DIRECTIONS = {"N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST"}
# Extra tokens you flagged as common false doc types in your Hampden run:
SUSPICIOUS_SHORT = {
    "ST","RD","AVE","DR","LA","CIR","TERR","PL","CT","WAY","RUN","SQ","PATH",
    "NORTH","SOUTH","EAST","WEST",
    # A few that are often address bleed-through in Hampden
    "MAIN","PARK","MAPLE","OAK","PINE","ELM","HILL","BAY","UNION","MILL","STATE","HIGH","STONY","BOSTON"
}

# These are legitimate DOC TYPE codes we want to protect even though they’re short.
# Expand this list over time; do NOT include street tokens here.
PROTECTED_DOC_TYPES = {
    "DEED","DM","ASN","REL","LIEN","MTL","ESMT","MORT","MSDD","LIS","FTL"
}

def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def norm_token(x: str) -> str:
    x = (x or "").strip().upper()
    x = re.sub(r"[^A-Z0-9]+", "", x)
    return x

def looks_like_address_suffix(doc_type_code: str, address_raw: str) -> bool:
    """
    If doc_type is ST/RD/AVE etc and address ends with that token (or contains it as suffix),
    it’s almost certainly address bleed-through.
    """
    dt = (doc_type_code or "").strip().upper()
    if not dt:
        return False
    if dt in PROTECTED_DOC_TYPES:
        return False

    addr = (address_raw or "").upper()
    # Common pattern in your OTR extracts: "... ST Y" / "... RD Y"
    if dt in STREET_SUFFIX or dt in DIRECTIONS:
        if re.search(rf"\b{re.escape(dt)}\b\s*Y\b", addr):
            return True
        # Also catch "... <DT>" at end (even without Y)
        if re.search(rf"\b{re.escape(dt)}\b\s*$", addr.strip()):
            return True
        return True

    # Wider Hampden offenders (MAIN, PARK, MAPLE...) – only flag if it appears in the address
    if dt in SUSPICIOUS_SHORT and re.search(rf"\b{re.escape(dt)}\b", addr):
        return True

    return False

def classify_doc_type(doc_type_code: str, address_raw: str) -> bool:
    """
    Returns True if doc_type should be considered invalid/suspicious.
    """
    dt = (doc_type_code or "").strip().upper()
    if not dt:
        return False
    if dt in PROTECTED_DOC_TYPES:
        return False
    if dt in STREET_SUFFIX or dt in DIRECTIONS:
        return True
    if looks_like_address_suffix(dt, address_raw):
        return True

    # If it's super short (<=2) and not protected, usually not a real doc type in your feeds
    if len(dt) <= 2 and dt not in PROTECTED_DOC_TYPES:
        return True

    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_events", required=True, help="Input NDJSON events")
    ap.add_argument("--out_events", required=True, help="Output NDJSON (normalized)")
    ap.add_argument("--out_report", required=True, help="Output QA report JSON")
    ap.add_argument("--max_samples", type=int, default=50)
    args = ap.parse_args()

    in_path = args.in_events
    out_path = args.out_events
    rep_path = args.out_report

    total = 0
    changed = 0
    missing_doc_type = 0

    by_doc_type = collections.Counter()
    suspicious_by_doc_type = collections.Counter()
    suspicious_by_pdf = collections.Counter()
    totals_by_pdf = collections.Counter()

    samples = []  # small sample of changed rows

    with open(in_path, "r", encoding="utf-8") as f_in, open(out_path, "w", encoding="utf-8") as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            total += 1
            r = json.loads(line)

            dt = r.get("doc_type_code")
            if not dt:
                missing_doc_type += 1

            by_doc_type[(dt or "NULL")] += 1

            src = r.get("source") or {}
            pdf = src.get("pdf") or src.get("uri") or "UNKNOWN_SOURCE"
            totals_by_pdf[pdf] += 1

            pr = r.get("property_ref") or {}
            addr = pr.get("address_raw") or ""

            if dt and classify_doc_type(dt, addr):
                suspicious_by_doc_type[dt] += 1
                suspicious_by_pdf[pdf] += 1

                # normalize: do NOT invent a new doc type; just mark UNKNOWN and preserve raw
                r["doc_type_code_raw"] = r.get("doc_type_code")
                r["doc_type_desc_raw"] = r.get("doc_type_desc")

                r["doc_type_code"] = "UNKNOWN"
                r["doc_type_desc"] = "UNKNOWN"

                # add QA flags (your evolved schema allows extras; if not, you’ll see it in validation)
                flags = (r.get("qa_flags") or [])
                if "DOC_TYPE_BLEED_FROM_ADDRESS" not in flags:
                    flags.append("DOC_TYPE_BLEED_FROM_ADDRESS")
                r["qa_flags"] = flags

                changed += 1
                if len(samples) < args.max_samples:
                    rec = r.get("recording") or {}
                    samples.append({
                        "pdf": pdf,
                        "page": src.get("page"),
                        "instrument_number_raw": rec.get("instrument_number_raw"),
                        "seq": rec.get("seq"),
                        "doc_type_code_raw": r.get("doc_type_code_raw"),
                        "address_raw": addr,
                        "town_raw": pr.get("town_raw"),
                    })

            f_out.write(json.dumps(r, ensure_ascii=False) + "\n")

    report = {
        "ok": True,
        "ran_at": utc_now_iso(),
        "in_events": in_path,
        "out_events": out_path,
        "total_events": total,
        "missing_doc_type": missing_doc_type,
        "normalized_doc_type_to_unknown": changed,
        "pct_doc_type_normalized": round((changed * 100.0 / total), 4) if total else 0.0,
        "top_doc_types": by_doc_type.most_common(50),
        "top_suspicious_doc_types": suspicious_by_doc_type.most_common(50),
        "top_pdfs_by_suspicious": [
            {"pdf": k, "suspicious": v, "total": totals_by_pdf.get(k, 0),
             "pct": round((v * 100.0 / totals_by_pdf.get(k, 1)), 2)}
            for k, v in suspicious_by_pdf.most_common(50)
        ],
        "samples": samples,
        "notes": [
            "This postfix only fixes provable doc_type contamination. It does not guess the true doc type.",
            "If pct_doc_type_normalized is high, extractor regex is pulling from address column; fix extractor next.",
            "We preserve original values into doc_type_code_raw/doc_type_desc_raw and set doc_type_code/doc_type_desc to UNKNOWN."
        ]
    }

    os.makedirs(os.path.dirname(rep_path), exist_ok=True)
    with open(rep_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(json.dumps({
        "ok": True,
        "total_events": total,
        "normalized_doc_type_to_unknown": changed,
        "pct_doc_type_normalized": report["pct_doc_type_normalized"],
        "out_events": out_path,
        "out_report": rep_path
    }))

if __name__ == "__main__":
    main()
