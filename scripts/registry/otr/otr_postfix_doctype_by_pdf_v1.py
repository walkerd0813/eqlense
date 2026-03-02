import json, os, datetime, argparse
from collections import Counter, defaultdict

def utc_ts_compact():
    return datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

BAD_TOKENS = {
  'ST','RD','AVE','DR','CIR','TERR','NORTH','SOUTH','EAST','WEST','PL','BLVD','WAY','CT','RUN','PATH','SQ','TER','LN',
  'UNIT','SEE','MAIN','OLD','HILL','PINE','PARK','PARKER','MAPLE','OAK','ELM','HIGH','BAY','STATE','UNION','MILL','BOSTON','STONY'
}

def infer_doctype_from_pdf(pdf_name: str):
    if not pdf_name:
        return None
    s = pdf_name.lower().strip()

    # NOTE: treat LandCourt and recorded land the same for doc_type purposes
    # map by file family
    if "mortgage" in s and "discharge" not in s:
        return ("MTG","MORTGAGE")
    if "discharge_mortgage" in s or "discharge-mortgage" in s or ("discharge" in s and "mortgage" in s):
        return ("DM","DISCHARGE OF MORTGAGE")
    if "deeds" in s and "master" not in s and "forclosure" not in s and "foreclosure" not in s:
        return ("DEED","DEED")
    if "assignments" in s or "assignment" in s:
        return ("ASN","ASSIGNMENT")
    if "release_" in s or "release(" in s or ("release" in s and "tax" not in s):
        return ("REL","RELEASE")
    if "liens" in s and "tax" not in s and "municip" not in s and "manicip" not in s:
        return ("LIEN","LIEN")
    if "mass_taxliens" in s or ("taxliens" in s and "fed" not in s):
        return ("MTL","MASS TAX LIEN")
    if "fed_taxliens" in s or "fed taxliens" in s:
        return ("FTL","FEDERAL TAX LIEN")
    if "easement" in s:
        return ("ESMT","EASEMENT")
    if "lispenden" in s or "lis pend" in s or "lis_penden" in s:
        return ("LIS","LIS PENDENS")
    if "master_deeds" in s or ("master" in s and "deed" in s):
        return ("MSDD","MASTER DEED")
    if "forclosure_deeds" in s or "foreclosure_deeds" in s or ("foreclosure" in s and "deed" in s):
        return ("FCD","FORECLOSURE DEED")
    if "discharge-generic" in s or ("discharge" in s and "generic" in s):
        return ("DIS","DISCHARGE")

    # municipal liens misspelled in your filenames
    if "manicipal_liens" in s or "municipal_liens" in s or ("municip" in s and "lien" in s):
        return ("ML","MUNICIPAL LIEN")

    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_events", required=True)
    ap.add_argument("--out_dir", required=True)
    args = ap.parse_args()

    in_path = args.in_events
    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    ts = utc_ts_compact()
    out_events = os.path.join(out_dir, f"events__POSTFIX_DOCTYPE_BY_PDF__{ts}.ndjson")
    out_report = os.path.join(out_dir, f"qa__POSTFIX_DOCTYPE_BY_PDF__{ts}.json")

    total = 0
    overridden = 0
    unknown_pdf_map = 0
    by_pdf = defaultdict(lambda: {"total":0, "overridden":0, "target":None})
    remaining_bad = Counter()
    before_bad = Counter()

    with open(in_path, "r", encoding="utf-8") as fin, open(out_events, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            total += 1

            src = r.get("source") or {}
            pdf = src.get("pdf") or "UNKNOWN_PDF"
            dt = (r.get("doc_type_code") or "").strip().upper()

            if dt in BAD_TOKENS:
                before_bad[dt] += 1

            inferred = infer_doctype_from_pdf(pdf)
            if inferred is None:
                unknown_pdf_map += 1
                # keep as-is
                by_pdf[pdf]["total"] += 1
                if by_pdf[pdf]["target"] is None:
                    by_pdf[pdf]["target"] = None
            else:
                target_code, target_desc = inferred
                by_pdf[pdf]["total"] += 1
                by_pdf[pdf]["target"] = target_code

                # apply to every row in this PDF (not just the ones that look wrong)
                if dt != target_code:
                    old = dt
                    # preserve old
                    meta = r.get("meta") or {}
                    meta["doc_type_code_raw"] = old
                    meta["doc_type_source_fix"] = "PDF_FILENAME_FAMILY"
                    r["meta"] = meta

                    r["doc_type_code"] = target_code
                    r["doc_type_desc"] = target_desc
                    # keep event_type aligned to doc_type_code (your downstream expects this)
                    r["event_type"] = target_code

                    overridden += 1
                    by_pdf[pdf]["overridden"] += 1

            # track remaining bad after fix
            final_dt = (r.get("doc_type_code") or "").strip().upper()
            if final_dt in BAD_TOKENS:
                remaining_bad[final_dt] += 1

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    # compact report
    by_pdf_list = []
    for pdf, info in sorted(by_pdf.items(), key=lambda kv: (-kv[1]["overridden"], -kv[1]["total"], kv[0])):
        by_pdf_list.append({
            "pdf": pdf,
            "total": info["total"],
            "target_doc_type": info["target"],
            "overridden": info["overridden"],
            "pct_overridden": (0.0 if info["total"]==0 else round(100.0*info["overridden"]/info["total"], 2))
        })

    report = {
        "ok": True,
        "engine": "events.otr_postfix_doctype_by_pdf_v1",
        "in_events": in_path,
        "out_events": out_events,
        "out_report": out_report,
        "total_events": total,
        "events_overridden": overridden,
        "pct_overridden": (0.0 if total==0 else round(100.0*overridden/total, 4)),
        "unknown_pdf_map_rows": unknown_pdf_map,
        "bad_tokens_before_top20": before_bad.most_common(20),
        "bad_tokens_after_top20": remaining_bad.most_common(20),
        "by_pdf_top": by_pdf_list[:25]
    }

    with open(out_report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps({
        "ok": True,
        "engine": report["engine"],
        "total_events": total,
        "events_overridden": overridden,
        "pct_overridden": report["pct_overridden"],
        "out_events": out_events,
        "out_report": out_report
    }))

if __name__ == "__main__":
    main()