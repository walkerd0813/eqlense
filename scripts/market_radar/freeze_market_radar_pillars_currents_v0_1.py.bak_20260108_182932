#!/usr/bin/env python3
import argparse, os, json, shutil, hashlib, datetime

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def now_utc_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, obj):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def backup_if_exists(path: str):
    if not os.path.exists(path):
        return None
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    bak = f"{path}.bak_{ts}"
    shutil.copy2(path, bak)
    return bak

def copy_current(root: str, src: str, current_name: str):
    cur_dir = os.path.join(root, "publicData", "marketRadar", "CURRENT")
    ensure_dir(cur_dir)
    dst = os.path.join(cur_dir, current_name)
    shutil.copy2(src, dst)
    sha = sha256_file(dst)
    sha_path = dst + ".sha256.json"
    write_json(sha_path, {
        "sha256": sha,
        "ndjson": dst,
        "computed_at_utc": now_utc_iso(),
        "source": src,
    })
    return dst, sha_path

def set_pointer(ptr_obj: dict, key: str, ndjson_path: str, sha_json_path: str):
    if "market_radar" not in ptr_obj or not isinstance(ptr_obj.get("market_radar"), dict):
        ptr_obj["market_radar"] = {}
    mr = ptr_obj["market_radar"]
    mr[key] = {
        "updated_at_utc": now_utc_iso(),
        "ndjson": ndjson_path,
        "sha256_json": sha_json_path,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--velocity")
    ap.add_argument("--absorption")
    ap.add_argument("--liquidity")
    ap.add_argument("--price_discovery")
    args = ap.parse_args()

    root = args.root
    ptr_path = os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_POINTERS.json")
    ptr_obj = load_json(ptr_path) or {}
    bak = backup_if_exists(ptr_path)

    report = {
        "ok": True,
        "as_of_date": args.as_of,
        "computed_at_utc": now_utc_iso(),
        "pointers": ptr_path,
        "pointers_backup": bak,
        "frozen": {}
    }

    if args.velocity:
        nd, sh = copy_current(root, args.velocity, "CURRENT_MARKET_RADAR_VELOCITY_ZIP.ndjson")
        set_pointer(ptr_obj, "velocity_zip", nd, sh)
        report["frozen"]["velocity_zip"] = {"ndjson": nd, "sha256_json": sh}

    if args.absorption:
        nd, sh = copy_current(root, args.absorption, "CURRENT_MARKET_RADAR_ABSORPTION_ZIP.ndjson")
        set_pointer(ptr_obj, "absorption_zip", nd, sh)
        report["frozen"]["absorption_zip"] = {"ndjson": nd, "sha256_json": sh}

    if args.liquidity:
        nd, sh = copy_current(root, args.liquidity, "CURRENT_MARKET_RADAR_LIQUIDITY_P01_ZIP.ndjson")
        set_pointer(ptr_obj, "liquidity_p01_zip", nd, sh)
        report["frozen"]["liquidity_p01_zip"] = {"ndjson": nd, "sha256_json": sh}

    if args.price_discovery:
        nd, sh = copy_current(root, args.price_discovery, "CURRENT_MARKET_RADAR_PRICE_DISCOVERY_P01_ZIP.ndjson")
        set_pointer(ptr_obj, "price_discovery_p01_zip", nd, sh)
        report["frozen"]["price_discovery_p01_zip"] = {"ndjson": nd, "sha256_json": sh}

    ptr_obj["updated_at_utc"] = now_utc_iso()
    ptr_obj["as_of_date"] = args.as_of
    write_json(ptr_path, ptr_obj)

    print("[done] froze pillar CURRENTs + updated pointers")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
