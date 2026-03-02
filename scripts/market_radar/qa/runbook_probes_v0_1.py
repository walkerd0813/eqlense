import argparse, json, os

EXPECTED_KEYS = [
  "tbi_transaction_breadth",
  "divergence_deeds_mls",
  "momentum_absorption_accel",
  "volatility_liquidity_stability",
  "rotation_capital_pressure",
  "off_market_participation",
]

OK_REASONS = set([
  "UNSUPPORTED_BUCKET",
  "UNSUPPORTED_ASSET_BUCKET",
  "UNSUPPORTED",
  "BUCKET_UNSUPPORTED",
  "NOT_SUPPORTED_BUCKET",
])

def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def find_row(indicators_path, zip_code, asset_bucket, window_days):
    for r in read_ndjson(indicators_path):
        z = str(r.get("zip") or r.get("zip_code") or "")
        b = str(r.get("asset_bucket") or r.get("assetBucket") or "")
        w = r.get("window_days") or r.get("windowDays")
        try:
            w = int(w)
        except Exception:
            w = None
        if z == str(zip_code) and b == str(asset_bucket) and w == int(window_days):
            return r
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--zip", required=True)
    ap.add_argument("--assetBucket", required=True)
    ap.add_argument("--windowDays", required=True, type=int)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    indicators_current = os.path.join(
        args.root,
        "publicData","marketRadar","indicators","CURRENT",
        "CURRENT_MARKET_RADAR_INDICATORS_P01_MASS.ndjson"
    )
    if not os.path.exists(indicators_current):
        print(f"[fail] missing indicators CURRENT: {indicators_current}")
        raise SystemExit(2)

    row = find_row(indicators_current, args.zip, args.assetBucket, args.windowDays)

    if args.debug:
        print("[debug] indicators_current:", indicators_current)
        print("[debug] query:", {"zip": args.zip, "assetBucket": args.assetBucket, "windowDays": args.windowDays})
        print("[debug] found:", (row is not None))
        if row is not None:
            inds = row.get("indicators") or {}
            # print a compact view of the 6 expected keys
            for k in EXPECTED_KEYS:
                v = inds.get(k) or {}
                print(f"[debug] {k}: state={v.get('state')} reason={v.get('reason')}")
            print("[debug] row_json:", json.dumps(row, ensure_ascii=False))

    if row is None:
        print(f"[fail] no indicator row for zip={args.zip} bucket={args.assetBucket} windowDays={args.windowDays}")
        raise SystemExit(2)

    # MF_5_PLUS safe behavior: every indicator must be UNKNOWN + UNSUPPORTED_BUCKET-like
    if args.assetBucket.upper() == "MF_5_PLUS":
        inds = row.get("indicators") or {}
        missing = [k for k in EXPECTED_KEYS if k not in inds]
        if missing:
            print(f"[fail] MF_5_PLUS row missing indicators keys: {missing}")
            raise SystemExit(2)

        bad = []
        for k in EXPECTED_KEYS:
            v = inds.get(k) or {}
            st = str(v.get("state") or "").upper()
            rs = str(v.get("reason") or "").upper()
            if not (st == "UNKNOWN" and rs in OK_REASONS):
                bad.append({"key": k, "state": v.get("state"), "reason": v.get("reason")})

        if bad:
            print(f"[fail] MF_5_PLUS does not emit UNKNOWN+UNSUPPORTED_BUCKET for all indicators (expected safe behavior)")
            print("       bad:", bad)
            raise SystemExit(2)

        print("[ok] MF_5_PLUS safe placeholder row verified (all 6 indicators UNKNOWN+UNSUPPORTED_BUCKET)")
        raise SystemExit(0)

    # For other buckets, we just require row exists (we can harden later)
    print("[ok] runbook probes passed (row exists)")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
