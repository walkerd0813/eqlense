param([Parameter(Mandatory=$true)][string]$Root)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$target = Join-Path $Root "scripts\market_radar\qa\runbook_probes_v0_1.py"
if(-not (Test-Path $target)){
  throw "[error] target missing: $target"
}

$bak = $target + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -Path $target -Destination $bak -Force

$src = @"
import argparse, json, os

def jload(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def find_indicator_row(indicators_path, zip_code, asset_bucket, window_days):
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

    # Canonical CURRENT indicators location (as discussed)
    indicators_current = os.path.join(
        args.root,
        "publicData", "marketRadar", "indicators", "CURRENT",
        "CURRENT_MARKET_RADAR_INDICATORS_P01_MASS.ndjson"
    )
    if not os.path.exists(indicators_current):
        print(f"[fail] missing indicators CURRENT: {indicators_current}")
        raise SystemExit(2)

    row = find_indicator_row(indicators_current, args.zip, args.assetBucket, args.windowDays)

    if args.debug:
        print("[debug] indicators_current:", indicators_current)
        print("[debug] query:", {"zip": args.zip, "assetBucket": args.assetBucket, "windowDays": args.windowDays})
        print("[debug] found:", (row is not None))
        if row is not None:
            # print a compact view
            print("[debug] row_state:", row.get("state"))
            print("[debug] row_reason:", row.get("reason"))
            # safe full dump (single line)
            print("[debug] row_json:", json.dumps(row, ensure_ascii=False))

    # Probe 1: row must exist for supported combos.
    # For MF_5_PLUS today, we accept that it may be UNKNOWN/unsupported (safe behavior)
    if row is None:
        print(f"[fail] no indicator row for zip={args.zip} bucket={args.assetBucket} windowDays={args.windowDays}")
        raise SystemExit(2)

    state = str(row.get("state") or "")
    reason = str(row.get("reason") or "")

    # Probe 2: MF_5_PLUS must be safely UNKNOWN with an "unsupported bucket" reason.
    # Allow drift in reason tokens so we don't false-block due to naming.
    if args.assetBucket.upper() == "MF_5_PLUS":
        ok_reasons = set([
            "UNSUPPORTED_BUCKET",
            "UNSUPPORTED_ASSET_BUCKET",
            "UNSUPPORTED",
            "BUCKET_UNSUPPORTED",
            "NOT_SUPPORTED_BUCKET",
        ])
        if not (state.upper() == "UNKNOWN" and reason.upper() in ok_reasons):
            print(f"[fail] MF_5_PLUS does not emit UNKNOWN+UNSUPPORTED_BUCKET for zip {args.zip} (expected safe behavior)")
            print("       got:", {"state": state, "reason": reason})
            raise SystemExit(2)

    print("[ok] runbook probes passed")
    raise SystemExit(0)

if __name__ == "__main__":
    main()
"@

Set-Content -Path $target -Value $src -Encoding UTF8

Write-Host "[backup] $bak"
Write-Host "[ok] patched $target (added --debug + tolerant MF_5_PLUS reason set)"
