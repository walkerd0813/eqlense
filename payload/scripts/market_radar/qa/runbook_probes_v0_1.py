import argparse, json, os, sys

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            yield json.loads(line)

def find_indicator_row(indicators_path, zip_code, asset_bucket):
    # Accept both "zip" and "zip_code" fields, and both "asset_bucket" and "bucket"
    for r in iter_ndjson(indicators_path):
        z = str(r.get("zip") or r.get("zip_code") or "")
        b = r.get("asset_bucket") or r.get("bucket") or r.get("assetBucket")
        if z == str(zip_code) and str(b) == str(asset_bucket):
            return r
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--zip", required=True)
    ap.add_argument("--assetBucket", required=True)
    ap.add_argument("--windowDays", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    root = args.root
    zip_code = args.zip
    asset_bucket = args.assetBucket

    # Primary artifacts we probe
    explain_cur = os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson")
    ind_cur = os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT", "CURRENT_MARKET_RADAR_INDICATORS_P01_MASS.ndjson")

    # Probe 1: explainability CURRENT exists
    if not os.path.exists(explain_cur):
        print(f"[fail] missing explainability CURRENT: {explain_cur}")
        return 2

    # Probe 2: indicators CURRENT exists
    if not os.path.exists(ind_cur):
        print(f"[fail] missing indicators CURRENT: {ind_cur}")
        return 2

    # Probe 3: MF_5_PLUS safe behavior
    # We treat MF_5_PLUS as unsupported until upstream pillars/indicators truly support it.
    # Safe behavior means: state=UNKNOWN and reason in allowed UNSUPPORTED* enum.
    if asset_bucket == "MF_5_PLUS":
        row = find_indicator_row(ind_cur, zip_code, asset_bucket)
        if row is None:
            print(f"[fail] MF_5_PLUS row missing for zip {zip_code} in indicators CURRENT (expected explicit UNKNOWN+UNSUPPORTED_BUCKET)")
            if args.debug:
                # Print some nearby rows for the zip to help diagnosis
                n=0
                for r in iter_ndjson(ind_cur):
                    z = str(r.get("zip") or r.get("zip_code") or "")
                    if z == str(zip_code):
                        if n < 20:
                            print("[debug] row:", json.dumps(r)[:500])
                        n += 1
                print(f"[debug] total rows for zip={zip_code}: {n}")
            return 2

        state = (row.get("state") or "").upper()
        reason = (row.get("reason") or row.get("unknown_reason") or "").upper()

        allowed_reasons = {
            "UNSUPPORTED_BUCKET",
            "UNSUPPORTED_ASSET_BUCKET",
            "UNSUPPORTED_BUCKET_POLICY",
            "UNSUPPORTED",
        }

        if args.debug:
            print("[debug] indicator row:", json.dumps(row, indent=2)[:2000])

        if state != "UNKNOWN" or reason not in allowed_reasons:
            print(f"[fail] MF_5_PLUS does not emit UNKNOWN+UNSUPPORTED_BUCKET for zip {zip_code} (expected safe behavior)")
            print(f"       got state={state!r} reason={reason!r}")
            return 2

    print("[pass] runbook probes OK")
    return 0

if __name__ == "__main__":
    sys.exit(main())
