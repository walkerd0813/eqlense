#!/usr/bin/env python3
"""Freeze Market Radar REGIME CURRENT pointers (v0_1)

Copies a regime NDJSON into:
  publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_REGIME_ZIP.ndjson
and updates:
  publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_POINTERS.json

BOM-safe JSON reads (utf-8-sig) to avoid the "Unexpected UTF-8 BOM" crash.
"""

from __future__ import annotations
import argparse, datetime, hashlib, json, os, shutil

def utc_now_z() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def write_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--regime", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--root", required=True)
    args = ap.parse_args()

    root = args.root
    cur_dir = os.path.join(root, "publicData", "marketRadar", "CURRENT")
    ensure_dir(cur_dir)

    dst = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_REGIME_ZIP.ndjson")
    dst_sha = dst + ".sha256.json"
    ptr = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_POINTERS.json")

    # copy + sha
    shutil.copyfile(os.path.join(root, args.regime) if not os.path.isabs(args.regime) else args.regime, dst)
    write_json(dst_sha, {
        "sha256": sha256_file(dst),
        "computed_at_utc": utc_now_z(),
        "ndjson": dst
    })

    # update pointers
    ptr_obj = load_json(ptr) or {}
    ptr_obj.setdefault("market_radar", {})
    ptr_obj["market_radar"].setdefault("as_of_date", args.as_of)
    ptr_obj["market_radar"].setdefault("updated_at_utc", utc_now_z())

    ptr_obj["market_radar"]["regime_zip"] = {
        "as_of_date": args.as_of,
        "updated_at_utc": utc_now_z(),
        "ndjson": dst,
        "sha256_json": dst_sha
    }

    # backup pointers
    if os.path.exists(ptr):
        ts = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")
        shutil.copyfile(ptr, ptr + f".bak_{ts}")

    write_json(ptr, ptr_obj)

    print("[done] froze REGIME CURRENT + updated pointers")
    print(json.dumps({
        "ok": True,
        "regime_current": dst,
        "regime_sha": dst_sha,
        "pointers": ptr
    }, indent=2))

if __name__ == "__main__":
    main()
