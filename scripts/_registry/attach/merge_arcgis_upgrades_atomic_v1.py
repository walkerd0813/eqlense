import argparse, json, os, hashlib, time

def is_unknown(a: dict) -> bool:
    st = (a.get("status") or a.get("attach_status") or "").upper()
    return st == "UNKNOWN"

def is_attached(a: dict) -> bool:
    st = (a.get("status") or a.get("attach_status") or "").upper()
    return st.startswith("ATTACHED")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--canon", required=True)
    ap.add_argument("--partial", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--method_prefix", default="parcel_id_lookup")  # matches parcel_id_lookup_arcgis
    args = ap.parse_args()

    t0 = time.time()

    # 1) Read PARTIAL and build upgrade map: event_id -> attach block
    upgrades = {}
    partial_rows = 0
    partial_json_errors = 0

    with open(args.partial, "r", encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if not s:
                continue
            partial_rows += 1
            try:
                r = json.loads(s)
            except Exception:
                partial_json_errors += 1
                continue

            eid = r.get("event_id")
            a = r.get("attach") or {}

            method = (a.get("method") or "").strip()
            pid = a.get("property_id")
            if (not eid) or (not pid):
                continue

            # accept either exact parcel_id_lookup_arcgis or any parcel_id_lookup*
            if not method.startswith(args.method_prefix):
                continue

            # only store if this row is attached (upgrade evidence)
            if not is_attached(a):
                continue

            upgrades[eid] = a

    # 2) Stream CANON, apply upgrades only when canon is UNKNOWN
    out_tmp = args.out + ".tmp"
    sha = hashlib.sha256()
    canon_rows = 0
    canon_json_errors = 0
    upgraded = 0
    kept = 0

    with open(args.canon, "r", encoding="utf-8") as fi, open(out_tmp, "w", encoding="utf-8") as fo:
        for ln in fi:
            s = ln.strip()
            if not s:
                continue
            canon_rows += 1
            try:
                r = json.loads(s)
            except Exception:
                canon_json_errors += 1
                continue

            eid = r.get("event_id")
            a = r.get("attach") or {}

            if eid in upgrades and is_unknown(a):
                # Replace attach with upgraded attach (keeps deterministic evidence)
                r["attach"] = upgrades[eid]
                upgraded += 1

            line = json.dumps(r, ensure_ascii=False)
            fo.write(line + "\n")
            sha.update((line + "\n").encode("utf-8"))
            kept += 1

        fo.flush()
        os.fsync(fo.fileno())

    # Atomic replace
    if os.path.exists(args.out):
        os.replace(args.out, args.out + ".bak")
    os.replace(out_tmp, args.out)

    audit = {
        "canon_rows_seen": canon_rows,
        "canon_json_errors_skipped": canon_json_errors,
        "partial_rows_seen": partial_rows,
        "partial_json_errors_skipped": partial_json_errors,
        "upgrade_candidates_loaded": len(upgrades),
        "upgraded_into_canon": upgraded,
        "rows_written": kept,
        "sha256_out": sha.hexdigest(),
        "out": args.out,
        "inputs": {"canon": args.canon, "partial": args.partial},
        "seconds": round(time.time() - t0, 2),
    }

    with open(args.audit, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, indent=2, ensure_ascii=False)

    print("[ok]", audit)

if __name__ == "__main__":
    main()