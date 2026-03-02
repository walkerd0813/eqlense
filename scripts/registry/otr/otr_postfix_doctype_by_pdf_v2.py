import os, re, json, datetime

def utc_ts():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def ts_compact():
    return datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

ALLOWED_DOC_CODES = {
    "DEED","MTG","ASN","REL","DIS","DM","FTL","FDD","LP","LIEN","MTL","STL","ESMT","MDEED"
}

def doc_code_from_pdf_name(pdf_name: str):
    n = (pdf_name or "").lower()

    if "forclosure_deeds" in n or "foreclosure_deeds" in n:
        return "FDD"
    if "lispenden" in n or "lis_penden" in n or "lispendens" in n:
        return "LP"
    if "fed_taxliens" in n or "federal_tax" in n:
        return "FTL"
    if "mass_taxliens" in n:
        return "STL"
    if "manicipal_liens" in n or "municipal_liens" in n:
        return "MTL"
    if "liens_" in n or "hamden_liens" in n:
        return "LIEN"
    if "discharge_mortgage" in n:
        return "DM"
    if "discharge-generic" in n or "discharge_generic" in n:
        return "DIS"
    if "release_" in n:
        return "REL"
    if "assignments" in n:
        return "ASN"
    if "master_deeds" in n:
        return "MDEED"
    if "easement" in n:
        return "ESMT"
    if "hamden_deeds" in n or "deeds(landcourt)" in n:
        return "DEED"
    if "mortgage" in n:
        return "MTG"
    return None

def registry_office_from_pdf_name(pdf_name: str):
    n = (pdf_name or "").lower()
    if "landcourt" in n:
        return "LAND_COURT"
    if "registered" in n or "registered_land" in n:
        return "REGISTERED_LAND"
    return "RECORDED_LAND"

def event_type_from_doc_code(code: str):
    if code in ("DEED","MDEED"):
        return "DEED"
    if code == "FDD":
        return "FORECLOSURE_DEED"
    if code == "MTG":
        return "MORTGAGE"
    if code == "ASN":
        return "ASSIGNMENT"
    if code == "REL":
        return "RELEASE"
    if code in ("DIS","DM"):
        return "DISCHARGE"
    if code == "LP":
        return "LIS_PENDENS"
    if code == "FTL":
        return "LIEN_FED"
    if code == "STL":
        return "LIEN_STATE"
    if code == "MTL":
        return "LIEN_MUNI"
    if code in ("LIEN","ESMT"):
        return "LIEN_OTHER"
    return None

UNIT_PATS = [
    re.compile(r"\\bUNIT\\s+([A-Z0-9\\-]+)\\b", re.I),
    re.compile(r"\\bAPT\\s+([A-Z0-9\\-]+)\\b", re.I),
    re.compile(r"\\b#\\s*([A-Z0-9\\-]+)\\b", re.I),
    re.compile(r"\\bSUITE\\s+([A-Z0-9\\-]+)\\b", re.I),
    re.compile(r"\\bSTE\\s+([A-Z0-9\\-]+)\\b", re.I),
]

def extract_unit(addr: str):
    if not addr:
        return (addr, None)
    a = addr.strip()
    a = re.sub(r"\\s+Y\\s*$", "", a).strip()
    unit = None
    for pat in UNIT_PATS:
        m = pat.search(a)
        if m:
            unit = m.group(1).strip()
            a = (a[:m.start()] + a[m.end():]).strip(" ,;-")
            break
    return (a if a else None, unit if unit else None)

def run(in_events, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    out_events = os.path.join(out_dir, f"events__POSTFIX_DOCTYPE_BY_PDF_V2__{ts_compact()}.ndjson")
    out_report = os.path.join(out_dir, f"qa__POSTFIX_DOCTYPE_BY_PDF_V2__{ts_compact()}.json")

    total = 0
    overridden = 0
    unknown_pdf = 0
    blocked_not_allowed = 0
    by_pdf = {}

    with open(in_events, "r", encoding="utf-8") as fin, open(out_events, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                r = json.loads(line)
            except Exception:
                continue

            src = (r.get("source") or {})
            pdf = src.get("pdf") or "UNKNOWN_PDF"

            doc_code = doc_code_from_pdf_name(pdf)
            st = by_pdf.setdefault(pdf, {"total":0,"overridden":0,"doc_code":None,"blocked":0})
            st["total"] += 1

            if not doc_code:
                unknown_pdf += 1
                fout.write(json.dumps(r, ensure_ascii=False) + "\\n")
                continue

            st["doc_code"] = doc_code

            if doc_code not in ALLOWED_DOC_CODES:
                blocked_not_allowed += 1
                st["blocked"] += 1
                r["doc_type_code"] = None
                r["doc_type_desc"] = None
                fout.write(json.dumps(r, ensure_ascii=False) + "\\n")
                continue

            r["registry_office"] = registry_office_from_pdf_name(pdf)

            old = r.get("doc_type_code")
            if old != doc_code:
                overridden += 1
                st["overridden"] += 1

            r["doc_type_code"] = doc_code
            r["doc_type_desc"] = doc_code

            ev = event_type_from_doc_code(doc_code)
            if ev:
                r["event_type"] = ev

            pref = r.get("property_ref") or {}
            addr = pref.get("address_raw")
            cleaned, unit = extract_unit(addr)
            if cleaned != addr:
                pref["address_raw"] = cleaned
            if not pref.get("unit_raw") and unit:
                pref["unit_raw"] = unit
            r["property_ref"] = pref

            fout.write(json.dumps(r, ensure_ascii=False) + "\\n")

    pdf_rows = []
    for pdf, st in sorted(by_pdf.items(), key=lambda kv: kv[1]["total"], reverse=True):
        pdf_rows.append({
            "pdf": pdf,
            "doc_code": st.get("doc_code"),
            "total": st.get("total", 0),
            "overridden": st.get("overridden", 0),
            "blocked": st.get("blocked", 0),
            "pct_overridden": (100.0 * st.get("overridden", 0) / st.get("total", 1)) if st.get("total", 0) else 0.0
        })

    report = {
        "ok": True,
        "engine": "events.otr_postfix_doctype_by_pdf_v2",
        "ran_at": utc_ts(),
        "inputs": {"in_events": in_events},
        "outputs": {"out_events": out_events, "out_report": out_report},
        "total_events": total,
        "events_overridden": overridden,
        "pct_overridden": (100.0 * overridden / total) if total else 0.0,
        "unknown_pdf_doc_code": unknown_pdf,
        "blocked_not_allowed": blocked_not_allowed,
        "by_pdf_top": pdf_rows[:30],
        "allowed_doc_codes": sorted(list(ALLOWED_DOC_CODES)),
    }

    with open(out_report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report))

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-events", dest="in_events", required=True)
    ap.add_argument("--out-dir", dest="out_dir", required=True)
    args = ap.parse_args()
    run(args.in_events, args.out_dir)