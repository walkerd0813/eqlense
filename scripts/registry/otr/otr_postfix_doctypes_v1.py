#!/usr/bin/env python3
import argparse
import collections
import datetime
import json

# Strongly suspicious tokens (street suffixes / directions) that should never be doc types.
SUSPICIOUS = set([
    "ST","RD","AVE","DR","LA","CIR","TERR","TER","PL","BLVD","WAY","CT","PATH","SQ","LN","RUN",
    "NORTH","SOUTH","EAST","WEST",
])

def utc_now_z():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def norm_token(x):
    if x is None:
        return None
    x = str(x).strip()
    if not x:
        return None
    return x.upper()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_events", required=True, help="Input NDJSON events")
    ap.add_argument("--out_events", required=True, help="Output NDJSON events (postfixed)")
    ap.add_argument("--out_report", required=True, help="Output QA JSON report")
    args = ap.parse_args()

    total = 0
    changed = 0

    by_token = collections.Counter()
    by_pdf_total = collections.Counter()
    by_pdf_susp = collections.Counter()

    with open(args.in_events, "r", encoding="utf-8") as fin, open(args.out_events, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            r = json.loads(line)

            src = r.get("source") or {}
            pdf = src.get("pdf") or "?"

            rec = r.get("recording") or {}
            dt = r.get("doc_type_code")
            dtN = norm_token(dt)

            by_pdf_total[pdf] += 1

            if dtN in SUSPICIOUS:
                # normalize to UNKNOWN
                r["doc_type_code"] = "UNKNOWN"
                r["doc_type_desc"] = "UNKNOWN"
                changed += 1
                by_token[dtN] += 1
                by_pdf_susp[pdf] += 1

                # optional: leave an evidence flag (safe + auditable)
                meta = r.get("meta") or {}
                flags = meta.get("qa_flags") or []
                if "DOC_TYPE_FROM_ADDRESS_TOKEN" not in flags:
                    flags.append("DOC_TYPE_FROM_ADDRESS_TOKEN")
                meta["qa_flags"] = flags
                meta["postfix_doctypes_v1_at"] = utc_now_z()
                r["meta"] = meta

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    # build report
    top_tokens = [{"token": k, "count": int(v)} for k, v in by_token.most_common(100)]
    pdf_rows = []
    for pdf, n in by_pdf_total.most_common():
        s = int(by_pdf_susp.get(pdf, 0))
        pdf_rows.append({
            "pdf": pdf,
            "suspicious_doc_types": s,
            "total": int(n),
            "pct": (float(s) * 100.0 / float(n)) if n else 0.0
        })

    report = {
        "ok": True,
        "engine": "events.otr_postfix_doctypes_v1",
        "ran_at": utc_now_z(),
        "total_events": int(total),
        "normalized_doc_type_to_unknown": int(changed),
        "pct_doc_type_normalized": (float(changed) * 100.0 / float(total)) if total else 0.0,
        "top_suspicious_tokens": top_tokens,
        "by_pdf": pdf_rows,
        "inputs": {
            "in_events": args.in_events
        },
        "outputs": {
            "out_events": args.out_events,
            "out_report": args.out_report
        }
    }

    with open(args.out_report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report))

if __name__ == "__main__":
    main()
