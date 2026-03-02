import argparse, json, os, sys, re
from typing import Any, Dict, Optional

def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def iter_ndjson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)

def find_row_by_zip(path: str, zip5: str) -> Optional[Dict[str, Any]]:
    for r in iter_ndjson(path):
        if str(r.get("zip") or "") == zip5:
            return r
    return None

def find_row_by_zip_asset_window(path: str, zip5: str, asset_bucket: str, window_days: int) -> Optional[Dict[str, Any]]:
    for r in iter_ndjson(path):
        if str(r.get("zip") or "") != zip5:
            continue
        if asset_bucket is not None and (r.get("asset_bucket") != asset_bucket):
            continue
        if window_days is not None and int(r.get("window_days") or -1) != int(window_days):
            continue
        return r
    return None

def safe_zip(z: str) -> str:
    z = (z or "").strip()
    if not re.match(r"^\d{5}$", z):
        raise SystemExit(f"[error] zip must be 5 digits. got: {z!r}")
    return z

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--zip", required=True)
    ap.add_argument("--as_of", required=False, default=None)  # informative only right now
    ap.add_argument("--asset_bucket", default="SFR")
    ap.add_argument("--window_days", type=int, default=90)
    ap.add_argument("--out", required=False, default=None)
    args = ap.parse_args()

    root = args.root
    zip5 = safe_zip(args.zip)

    ptr_path = os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_POINTERS.json")
    ind_ptr_path = os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT", "CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")

    ptr = load_json(ptr_path)
    mr = (ptr.get("market_radar") or {})

    # Pillar CURRENTs
    velocity_path = (mr.get("velocity_zip") or {}).get("ndjson")
    absorption_path = (mr.get("absorption_zip") or {}).get("ndjson")
    liquidity_path = (mr.get("liquidity_p01_zip") or {}).get("ndjson")
    price_disc_path = (mr.get("price_discovery_p01_zip") or {}).get("ndjson")
    explain_path = (mr.get("explainability_zip") or {}).get("ndjson")

    if not explain_path:
        raise SystemExit("[error] explainability_zip pointer missing in CURRENT_MARKET_RADAR_POINTERS.json")

    # Indicators CURRENT (state = MASS)
    indicators_path = None
    if os.path.exists(ind_ptr_path):
        indptr = load_json(ind_ptr_path)
        cur = indptr.get("current") or {}
        mass = cur.get("MASS") or cur.get("MA") or cur.get("MASSACHUSETTS")  # defensive
        if isinstance(mass, dict):
            indicators_path = mass.get("ndjson")

    # Pull rows
    out = {
        "meta": {
            "zip": zip5,
            "asset_bucket": args.asset_bucket,
            "window_days": args.window_days,
            "as_of_arg": args.as_of,
            "pointers": {
                "market_radar": ptr_path,
                "indicators": ind_ptr_path if os.path.exists(ind_ptr_path) else None
            }
        },
        "paths": {
            "velocity_zip": velocity_path,
            "absorption_zip": absorption_path,
            "liquidity_p01_zip": liquidity_path,
            "price_discovery_p01_zip": price_disc_path,
            "explainability_zip": explain_path,
            "indicators_p01_zip_state": indicators_path
        },
        "rows": {}
    }

    # explainability is always zip-wide (no bucket/window in contract output)
    out["rows"]["explainability"] = find_row_by_zip(explain_path, zip5)

    # pillars tend to be zip+bucket+window; we try to locate with those fields
    if velocity_path: out["rows"]["velocity"] = find_row_by_zip_asset_window(velocity_path, zip5, args.asset_bucket, args.window_days)
    if absorption_path: out["rows"]["absorption"] = find_row_by_zip_asset_window(absorption_path, zip5, args.asset_bucket, args.window_days)
    if liquidity_path: out["rows"]["liquidity"] = find_row_by_zip_asset_window(liquidity_path, zip5, args.asset_bucket, args.window_days)
    if price_disc_path: out["rows"]["price_discovery"] = find_row_by_zip_asset_window(price_disc_path, zip5, args.asset_bucket, args.window_days)

    # indicators output is zip-wide (usually keyed by zip + window + bucket; we’ll attempt bucket/window first then fallback)
    if indicators_path:
        r = find_row_by_zip_asset_window(indicators_path, zip5, args.asset_bucket, args.window_days)
        if r is None:
            r = find_row_by_zip(indicators_path, zip5)
        out["rows"]["indicators"] = r

    # light diagnostics (founder-grade)
    diag = {"notes": []}

    if out["rows"]["explainability"] is None:
        diag["notes"].append("No explainability row found for zip (check explainability build).")

    # missing pillars expected depending on coverage
    for k in ["velocity", "absorption", "liquidity", "price_discovery"]:
        if out["paths"].get(f"{k}_zip") and out["rows"].get(k) is None:
            diag["notes"].append(f"Missing pillar row: {k} for zip/bucket/window (zip={zip5}, bucket={args.asset_bucket}, window={args.window_days}).")

    if out["rows"].get("indicators") is None:
        diag["notes"].append("No indicators row found for this zip (state pointers or build may be missing).")

    out["diagnostics"] = diag

    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"[ok] wrote {args.out}")
    else:
        print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
