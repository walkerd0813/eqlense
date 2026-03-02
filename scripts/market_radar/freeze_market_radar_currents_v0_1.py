#!/usr/bin/env python3
"""
Freeze Market Radar CURRENT pointers (institution-grade, reproducible).

Writes:
  publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_DEEDS_ZIP.ndjson
  publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_DEEDS_ZIP.sha256.json
  publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_UNIFIED_ZIP.ndjson
  publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_UNIFIED_ZIP.sha256.json
  publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_POINTERS.json

Notes:
- Uses copy (not symlink) for Windows friendliness.
- Creates timestamped backups if CURRENT files already exist.
"""
import argparse, os, json, hashlib, datetime, shutil

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def backup_if_exists(dst: str):
    if os.path.exists(dst):
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        bak = dst + f".bak_{ts}"
        shutil.copy2(dst, bak)
        return bak
    return None

def copy_to_current(src: str, dst: str):
    bak = backup_if_exists(dst)
    ensure_dir(os.path.dirname(dst))
    shutil.copy2(src, dst)
    return bak

def write_sha_json(target_path: str, src_path: str, as_of: str):
    sha = sha256_file(target_path)
    meta = {
        "sha256": sha,
        "computed_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of_date": as_of,
        "current_file": target_path,
        "source_file": src_path,
        "bytes": os.path.getsize(target_path),
    }
    out = target_path + ".sha256.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return out, sha

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deeds", required=True, help="Layer B rollup NDJSON (e.g. zip_rollup__deeds_v0_5_ASOF....ndjson)")
    ap.add_argument("--unified", required=True, help="Layer D unified NDJSON (e.g. zip_unified__v0_7.ndjson)")
    ap.add_argument("--as_of", required=True, help="YYYY-MM-DD")
    ap.add_argument("--root", default=r"C:\seller-app\backend", help="Repo root (default: C:\\seller-app\\backend)")
    args = ap.parse_args()

    root = args.root
    curr_dir = os.path.join(root, "publicData", "marketRadar", "CURRENT")
    ensure_dir(curr_dir)

    deeds_curr = os.path.join(curr_dir, "CURRENT_MARKET_RADAR_DEEDS_ZIP.ndjson")
    unified_curr = os.path.join(curr_dir, "CURRENT_MARKET_RADAR_UNIFIED_ZIP.ndjson")

    deeds_src = args.deeds
    unified_src = args.unified
    if not os.path.isabs(deeds_src):
        deeds_src = os.path.join(root, deeds_src)
    if not os.path.isabs(unified_src):
        unified_src = os.path.join(root, unified_src)

    if not os.path.exists(deeds_src):
        raise FileNotFoundError(deeds_src)
    if not os.path.exists(unified_src):
        raise FileNotFoundError(unified_src)

    bak1 = copy_to_current(deeds_src, deeds_curr)
    bak2 = copy_to_current(unified_src, unified_curr)

    deeds_sha_file, deeds_sha = write_sha_json(deeds_curr, deeds_src, args.as_of)
    unified_sha_file, unified_sha = write_sha_json(unified_curr, unified_src, args.as_of)

    pointers = {
        "updated_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of_date": args.as_of,
        "layerB_deeds_zip": {
            "current": deeds_curr,
            "sha256": deeds_sha,
            "sha_file": deeds_sha_file,
            "source": deeds_src,
            "backup": bak1
        },
        "layerD_unified_zip": {
            "current": unified_curr,
            "sha256": unified_sha,
            "sha_file": unified_sha_file,
            "source": unified_src,
            "backup": bak2
        }
    }

    out_ptr = os.path.join(curr_dir, "CURRENT_MARKET_RADAR_POINTERS.json")
    bak_ptr = backup_if_exists(out_ptr)
    with open(out_ptr, "w", encoding="utf-8") as f:
        json.dump(pointers, f, indent=2)

    print("[done] froze Market Radar CURRENTs")
    print("  deeds_current:  ", deeds_curr)
    print("  unified_current:", unified_curr)
    print("  pointers:       ", out_ptr)
    if bak_ptr:
        print("  pointers_backup:", bak_ptr)

if __name__ == "__main__":
    main()
