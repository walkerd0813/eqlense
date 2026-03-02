#!/usr/bin/env python3
import argparse, json, os, datetime, hashlib

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    contract = {
        "schema_version": "market_radar_indicator_contract_v1",
        "engine_version": "v0_2",
        "built_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of_date": args.as_of,
        "indicator_set": "P01",
        "defaults": {
            "asset_buckets": ["SINGLE_FAMILY","CONDO","MF_2_4","MF_5_PLUS","LAND"],
            "bucket_case": "UPPER"
        },
        "bucket_aliases": {
            "SFR": "SINGLE_FAMILY",
            "SINGLE": "SINGLE_FAMILY",
            "SINGLE_FAMILY": "SINGLE_FAMILY",
            "CONDO": "CONDO",
            "MF": "MF_2_4",
            "MULTI": "MF_2_4",
            "MULTI_2_4": "MF_2_4",
            "MF_2_4": "MF_2_4",
            "MULTI_5_PLUS": "MF_5_PLUS",
            "MF_5_PLUS": "MF_5_PLUS",
            "LAND": "LAND"
        },
        "unknown_state": {
            "state": "UNKNOWN",
            "reasons": ["INSUFFICIENT_SAMPLES","MISSING_METRIC","UNSUPPORTED_BUCKET","MISSING_UPSTREAM_ROW","BAD_INPUT"]
        },
        "messaging_layer": {
            "technical_terms": True,
            "layman_terms": True,
            "hover_supported": True
        }
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(contract, f, indent=2)

    sha = sha256_file(args.out)
    sha_path = args.out + ".sha256.json"
    with open(sha_path, "w", encoding="utf-8") as f:
        json.dump({"path": os.path.abspath(args.out), "sha256": sha}, f, indent=2)

    print(json.dumps({"ok": True, "out": os.path.abspath(args.out), "sha256_json": os.path.abspath(sha_path)}, indent=2))

if __name__ == "__main__":
    main()
