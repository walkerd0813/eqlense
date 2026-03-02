#!/usr/bin/env python3
import argparse, os, shutil, hashlib, json, datetime

def now_utc_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listings", required=True, help="Absolute path to mls/normalized/listings.ndjson")
    ap.add_argument("--root", required=True, help="C:\\seller-app\\backend")
    args = ap.parse_args()

    src = args.listings
    if not os.path.exists(src):
        raise SystemExit(f"[error] listings file not found: {src}")

    cur_dir = os.path.join(args.root, "publicData", "mls", "CURRENT")
    ensure_dir(cur_dir)

    dst = os.path.join(cur_dir, "CURRENT_MLS_NORMALIZED_LISTINGS.ndjson")
    shutil.copy2(src, dst)

    sha = sha256_file(dst)
    sha_path = dst + ".sha256.json"
    with open(sha_path, "w", encoding="utf-8") as f:
        json.dump({
            "sha256": sha,
            "ndjson": dst,
            "computed_at_utc": now_utc_iso(),
            "source": src,
        }, f, indent=2)

    print("[done] ensured CURRENT MLS normalized listings")
    print(json.dumps({"ok": True, "current": dst, "sha256_json": sha_path}, indent=2))

if __name__ == "__main__":
    main()
