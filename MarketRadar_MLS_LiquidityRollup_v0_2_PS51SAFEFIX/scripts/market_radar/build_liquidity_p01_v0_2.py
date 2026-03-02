#!/usr/bin/env python3
import argparse, json, os, re, datetime, hashlib
from collections import defaultdict

ZIP_RE = re.compile(r"^\d{5}$")

def ndjson_iter(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def is_valid_zip(z):
    return bool(z) and isinstance(z, str) and ZIP_RE.match(z) and z != "00000"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--as_of", required=True)
    args = ap.parse_args()

    audit = {
        "built_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of_date": args.as_of,
        "inputs": {"infile": args.infile},
        "mls_scan": {"rows_in": 0, "rows_written": 0, "skipped_bad_zip": 0, "skipped_bad_bucket": 0, "skipped_bad_window": 0, "key_dupes_seen": 0,
                     "missing_dom": 0, "missing_pending": 0, "missing_active": 0, "missing_withdrawn": 0, "missing_off_market": 0}
    }

    seen = set()
    wrote = 0
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as out:
        for r in ndjson_iter(args.infile):
            audit["mls_scan"]["rows_in"] += 1
            z = r.get("zip")
            b = r.get("asset_bucket")
            w = r.get("window_days")
            if not is_valid_zip(z):
                audit["mls_scan"]["skipped_bad_zip"] += 1
                continue
            if not b:
                audit["mls_scan"]["skipped_bad_bucket"] += 1
                continue
            if not isinstance(w, int) or w <= 0:
                audit["mls_scan"]["skipped_bad_window"] += 1
                continue

            key = (z, b, w)
            if key in seen:
                audit["mls_scan"]["key_dupes_seen"] += 1
                continue
            seen.add(key)

            m = r.get("metrics") if isinstance(r.get("metrics"), dict) else {}

            active = m.get("active_count")
            pending = m.get("pending_count")
            withdrawn = m.get("withdrawn_count")
            canceled = m.get("canceled_count")
            offm = m.get("off_market_count")
            closed = m.get("closed_count")

            if m.get("dom_median") is None: audit["mls_scan"]["missing_dom"] += 1
            if pending is None: audit["mls_scan"]["missing_pending"] += 1
            if active is None: audit["mls_scan"]["missing_active"] += 1
            if withdrawn is None and canceled is None: audit["mls_scan"]["missing_withdrawn"] += 1
            if offm is None: audit["mls_scan"]["missing_off_market"] += 1

            denom_ap = (active or 0) + (pending or 0)
            pending_ratio = (float(pending) / denom_ap) if denom_ap > 0 and pending is not None else None

            denom_total = (active or 0) + (pending or 0) + (closed or 0) + (withdrawn or 0) + (canceled or 0) + (offm or 0)
            churn_num = (withdrawn or 0) + (canceled or 0) + (offm or 0)
            churn_ratio = (float(churn_num) / denom_total) if denom_total > 0 else None

            doc = {
                "layer": "liquidity_p01",
                "as_of_date": args.as_of,
                "window_days": w,
                "zip": z,
                "asset_bucket": b,
                "metrics": {
                    "active_count": active,
                    "pending_count": pending,
                    "closed_count": closed,
                    "withdrawn_count": withdrawn,
                    "canceled_count": canceled,
                    "off_market_count": offm,
                    "pending_ratio": pending_ratio,
                    "churn_ratio": churn_ratio,
                    "dom_median": m.get("dom_median"),
                    "dom_p25": m.get("dom_p25"),
                    "dom_p75": m.get("dom_p75"),
                    "dom_mean": m.get("dom_mean"),
                    "dom_samples": m.get("dom_samples")
                },
                "inputs": {"source_rollup": args.infile}
            }
            out.write(json.dumps(doc, ensure_ascii=False) + "\n")
            wrote += 1

    audit["mls_scan"]["rows_written"] = wrote
    audit["output"] = {"out": args.out, "rows_written": wrote, "sha256": sha256_file(args.out)}
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] Liquidity P01 v0_2 complete.")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
