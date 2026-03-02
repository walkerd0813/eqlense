#!/usr/bin/env python3
# freeze_market_radar_liquidity_currents_v0_1.py
# PowerShell-safe, deterministic CURRENT pointer freezer for Market Radar liquidity artifacts.
# Writes:
#   publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson (+ sha)
#   publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_LIQUIDITY_ZIP.ndjson (+ sha)
#   updates publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_POINTERS.json (backup first)
#
# Notes:
# - Does not rewrite source files; only copies + hashes + pointer JSON.
# - Idempotent: re-run safely; backups created with timestamp suffix.

import argparse, os, shutil, json, hashlib, datetime

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def write_sha_json(target_path: str, sha_hex: str):
    obj = {
        "path": os.path.abspath(target_path),
        "sha256": sha_hex,
        "computed_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    with open(target_path + ".sha256.json", "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def backup_if_exists(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    bak = f"{path}.bak_{ts}"
    shutil.copy2(path, bak)
    return bak

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mls_rollup", required=True, help="MLS liquidity rollup NDJSON (zip,bucket,window)")
    ap.add_argument("--liquidity", required=True, help="Liquidity P01 NDJSON")
    ap.add_argument("--as_of", required=True, help="As-of date YYYY-MM-DD")
    ap.add_argument("--root", default=".", help="Repo root (default .)")
    args = ap.parse_args()

    root = os.path.abspath(args.root)

    cur_dir = os.path.join(root, "publicData", "marketRadar", "CURRENT")
    ensure_dir(cur_dir)

    # Targets
    mls_cur = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_MLS_LIQUIDITY_ZIP.ndjson")
    liq_cur = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_LIQUIDITY_ZIP.ndjson")
    ptr_path = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_POINTERS.json")

    # Backups
    bak_ptr = backup_if_exists(ptr_path)
    bak_mls = backup_if_exists(mls_cur)
    bak_liq = backup_if_exists(liq_cur)
    bak_mls_sha = backup_if_exists(mls_cur + ".sha256.json")
    bak_liq_sha = backup_if_exists(liq_cur + ".sha256.json")

    # Copy sources into CURRENT
    shutil.copy2(os.path.join(root, args.mls_rollup), mls_cur) if not os.path.isabs(args.mls_rollup) else shutil.copy2(args.mls_rollup, mls_cur)
    shutil.copy2(os.path.join(root, args.liquidity), liq_cur) if not os.path.isabs(args.liquidity) else shutil.copy2(args.liquidity, liq_cur)

    # Hash + sidecars
    mls_sha = sha256_file(mls_cur)
    liq_sha = sha256_file(liq_cur)
    write_sha_json(mls_cur, mls_sha)
    write_sha_json(liq_cur, liq_sha)

    # Pointers JSON (merge/update)
    ptr_obj = {}
    if os.path.exists(ptr_path):
        try:
            with open(ptr_path, "r", encoding="utf-8") as f:
                ptr_obj = json.load(f) or {}
        except Exception:
            ptr_obj = {}

    ptr_obj.setdefault("schema_version", "market_radar_pointers_v0_1")
    ptr_obj["updated_at_utc"] = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    ptr_obj.setdefault("market_radar", {})
    mr = ptr_obj["market_radar"]
    mr.setdefault("current_dir", os.path.abspath(cur_dir))
    mr.setdefault("as_of_date", args.as_of)

    mr["mls_liquidity_rollup"] = {
        "path": os.path.abspath(mls_cur),
        "source_path": os.path.abspath(args.mls_rollup) if os.path.isabs(args.mls_rollup) else os.path.abspath(os.path.join(root, args.mls_rollup)),
        "sha256": mls_sha,
        "as_of_date": args.as_of,
    }
    mr["liquidity_p01"] = {
        "path": os.path.abspath(liq_cur),
        "source_path": os.path.abspath(args.liquidity) if os.path.isabs(args.liquidity) else os.path.abspath(os.path.join(root, args.liquidity)),
        "sha256": liq_sha,
        "as_of_date": args.as_of,
    }

    with open(ptr_path, "w", encoding="utf-8") as f:
        json.dump(ptr_obj, f, indent=2)

    print("[done] froze Market Radar LIQUIDITY CURRENT")
    print(json.dumps({
        "ok": True,
        "mls_liquidity_current": os.path.abspath(mls_cur),
        "mls_liquidity_sha": os.path.abspath(mls_cur + ".sha256.json"),
        "liquidity_current": os.path.abspath(liq_cur),
        "liquidity_sha": os.path.abspath(liq_cur + ".sha256.json"),
        "pointers": os.path.abspath(ptr_path),
        "backups": {
            "pointers": bak_ptr,
            "mls_liquidity": bak_mls,
            "liquidity": bak_liq,
            "mls_liquidity_sha": bak_mls_sha,
            "liquidity_sha": bak_liq_sha
        }
    }, indent=2))
    print("[done] freeze liquidity CURRENT complete.")

if __name__ == "__main__":
    main()
