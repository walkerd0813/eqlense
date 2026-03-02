from __future__ import annotations

import argparse, json, time, hashlib

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--canon", required=True)
    ap.add_argument("--upgrades", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    t0 = time.time()

    upgrades = {}
    upgrades_rows = 0
    json_err = 0

    with open(args.upgrades, "r", encoding="utf-8") as f:
        for ln in f:
            if not ln.strip():
                continue
            try:
                r = json.loads(ln)
            except Exception:
                json_err += 1
                continue
            eid = r.get("event_id")
            if not eid:
                continue
            upgrades[eid] = r
            upgrades_rows += 1

    applied = 0
    canon_rows = 0
    canon_json_err = 0

    with open(args.canon, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for ln in fin:
            if not ln.strip():
                continue
            try:
                r = json.loads(ln)
            except Exception:
                canon_json_err += 1
                continue

            canon_rows += 1
            eid = r.get("event_id")
            u = upgrades.get(eid)
            if u:
                r2 = dict(r)
                r2["attach"] = u.get("attach")
                fout.write(json.dumps(r2, ensure_ascii=False) + "\n")
                applied += 1
            else:
                fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    audit = {
        "engine": "merge_upgrades_by_eventid_v1",
        "inputs": {"canon": args.canon, "upgrades": args.upgrades},
        "counts": {
            "canon_rows_seen": canon_rows,
            "canon_json_errors_skipped": canon_json_err,
            "upgrades_rows_loaded": upgrades_rows,
            "upgrades_json_errors_skipped": json_err,
            "applied": applied,
        },
        "sha256_out": sha256_file(args.out),
        "seconds": round(time.time() - t0, 2),
        "out": args.out,
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[ok]", json.dumps(audit, ensure_ascii=False))

if __name__ == "__main__":
    main()
