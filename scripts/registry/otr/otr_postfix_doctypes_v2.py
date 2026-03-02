import json, os, datetime, collections, re

def utcnow_z():
  return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# Street/direction tokens (keep your original list)
SUSPICIOUS = {
  "ST","RD","AVE","DR","LA","CIR","TERR","NORTH","SOUTH","EAST","WEST","PL","BLVD","WAY","CT","RUN","PATH","SQ","TER","LN",
  "MAIN","HILL","PINE","PARK","MAPLE","OAK","ELM","BAY","STATE","BOSTON","UNION","MILL","HIGH","STONY",
  "SEE","UNIT","OLD","NEW","MT","VAN"
}

DIRS = {"N","S","E","W","NE","NW","SE","SW"}

def clean(tok: str) -> str:
  tok = (tok or "").strip().upper()
  tok = re.sub(r"[^A-Z0-9\-\_]", "", tok)
  return tok[:32]

def is_suspicious_doc_type(tok: str) -> bool:
  t = clean(tok)
  if not t:
    return True

  # If it looks like a street-name word, treat as suspicious.
  # This is the key new rule that catches HONEY, MAPLE, STONY, PARKER, etc.
  if len(t) > 4:
    return True

  if t in DIRS:
    return True

  if t in SUSPICIOUS:
    return True

  # numeric-only tokens are never a doc type
  if t.isdigit():
    return True

  return False

def main():
  import argparse
  ap = argparse.ArgumentParser()
  ap.add_argument("--in_events", required=True)
  ap.add_argument("--out_events", required=True)
  ap.add_argument("--out_report", required=True)
  args = ap.parse_args()

  total = 0
  normed = 0
  top = collections.Counter()
  by_pdf = collections.Counter()
  by_pdf_susp = collections.Counter()

  with open(args.in_events, "r", encoding="utf-8") as f_in, \
       open(args.out_events, "w", encoding="utf-8") as f_out:

    for line in f_in:
      line = line.strip()
      if not line:
        continue
      total += 1
      r = json.loads(line)

      pdf = (r.get("source") or {}).get("pdf") or "?"
      dt  = r.get("doc_type_code")

      by_pdf[pdf] += 1

      if is_suspicious_doc_type(dt):
        by_pdf_susp[pdf] += 1
        top[clean(dt) or "NULL"] += 1
        # Normalize to UNKNOWN (do NOT guess)
        r["doc_type_code"] = "UNKNOWN"
        r["doc_type_desc"] = "UNKNOWN"
        # keep original token for audit
        r.setdefault("meta", {})
        r["meta"]["doc_type_original"] = dt
        r["meta"]["doc_type_normalized_by"] = "postfix_doctypes_v2"
        normed += 1

      f_out.write(json.dumps(r, ensure_ascii=False) + "\n")

  report = {
    "ok": True,
    "engine": "events.otr_postfix_doctypes_v2",
    "ran_at": utcnow_z(),
    "total_events": total,
    "normalized_doc_type_to_unknown": normed,
    "pct_doc_type_normalized": (normed * 100.0 / total) if total else 0.0,
    "top_suspicious_tokens": [{"token": k, "count": v} for k, v in top.most_common(30)],
    "by_pdf": [
      {"pdf": pdf, "suspicious_doc_types": int(by_pdf_susp[pdf]), "total": int(by_pdf[pdf]),
       "pct": (by_pdf_susp[pdf] * 100.0 / by_pdf[pdf]) if by_pdf[pdf] else 0.0}
      for pdf in sorted(by_pdf.keys(), key=lambda p: (-by_pdf_susp[p], -by_pdf[p]))
    ],
    "inputs": {"in_events": os.path.abspath(args.in_events)},
    "outputs": {"out_events": os.path.abspath(args.out_events), "out_report": os.path.abspath(args.out_report)},
  }

  with open(args.out_report, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

  print(json.dumps(report))

if __name__ == "__main__":
  main()