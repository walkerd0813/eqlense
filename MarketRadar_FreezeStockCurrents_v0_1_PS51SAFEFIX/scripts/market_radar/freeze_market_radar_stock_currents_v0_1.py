#!/usr/bin/env python3
import argparse, os, json, hashlib, datetime, shutil

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def atomic_copy(src: str, dst: str):
    # copy to temp then replace for atomic-ish behavior on Windows
    tmp = dst + ".tmp"
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)

def backup_if_exists(path: str) -> str | None:
    if os.path.exists(path):
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        bak = f"{path}.bak_{ts}"
        shutil.copyfile(path, bak)
        return bak
    return None

def write_json(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def main():
    ap = argparse.ArgumentParser(description="Freeze Market Radar STOCK (ZIP) CURRENT pointers v0_1")
    ap.add_argument("--stock", required=True, help="Stock NDJSON (zip_stock__spine_v0_1.ndjson)")
    ap.add_argument("--as_of", required=True, help="As-of date YYYY-MM-DD")
    ap.add_argument("--root", default=os.getcwd(), help="Backend root (default: cwd)")
    args = ap.parse_args()

    backend_root = os.path.abspath(args.root)
    stock_src = args.stock
    # allow relative paths from root
    if not os.path.isabs(stock_src):
        stock_src = os.path.join(backend_root, stock_src)
    stock_src = os.path.abspath(stock_src)
    if not os.path.exists(stock_src):
        raise FileNotFoundError(stock_src)

    cur_dir = os.path.join(backend_root, "publicData", "marketRadar", "CURRENT")
    os.makedirs(cur_dir, exist_ok=True)

    stock_cur = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_STOCK_ZIP.ndjson")
    stock_sha = stock_cur + ".sha256.json"
    pointers = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_POINTERS.json")

    # Backup existing CURRENTs
    bak_stock = backup_if_exists(stock_cur)
    bak_sha = backup_if_exists(stock_sha)
    bak_ptr = backup_if_exists(pointers)

    # Copy stock into CURRENT
    atomic_copy(stock_src, stock_cur)

    # Write sha json
    digest = sha256_file(stock_cur)
    sha_obj = {
        "sha256": digest,
        "computed_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source_file": stock_src.replace("\\", "/"),
        "as_of_date": args.as_of,
        "schema": "market_radar_stock_zip_current_v0_1"
    }
    write_json(stock_sha, sha_obj)

    # Update pointers json (preserve any existing keys)
    ptr_obj = {}
    if os.path.exists(pointers):
        try:
            with open(pointers, "r", encoding="utf-8") as f:
                ptr_obj = json.load(f) or {}
        except Exception:
            ptr_obj = {}

    ptr_obj.setdefault("market_radar", {})
    mr = ptr_obj["market_radar"]

    mr["stock_zip_current"] = {
        "as_of_date": args.as_of,
        "ndjson": stock_cur.replace("\\", "/"),
        "sha256_json": stock_sha.replace("\\", "/"),
        "source_stock_ndjson": stock_src.replace("\\", "/"),
        "updated_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "schema": "market_radar_stock_zip_pointer_v0_1"
    }

    ptr_obj["updated_at_utc"] = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    ptr_obj.setdefault("note", "AUTO: Market Radar CURRENT pointers (Layer A/B/D + stock denominator).")

    write_json(pointers, ptr_obj)

    out = {
        "ok": True,
        "stock_current": stock_cur,
        "stock_sha": stock_sha,
        "pointers": pointers,
        "backups": {
            "stock": bak_stock,
            "stock_sha": bak_sha,
            "pointers": bak_ptr
        }
    }
    print("[done] froze Market Radar STOCK CURRENT")
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
