#!/usr/bin/env python3
import argparse, json, os, shutil, hashlib, datetime

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

def write_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def now_utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--explainability", required=True)
    ap.add_argument("--founder_contract", required=False, default=None)
    args = ap.parse_args()

    root = args.root
    cur_dir = os.path.join(root, "publicData", "marketRadar", "CURRENT")
    ptr_path = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_POINTERS.json")

    ptr = load_json(ptr_path) or {}
    ptr.setdefault("schema_version", "market_radar_pointers_v1")
    ptr["updated_at_utc"] = now_utc()
    ptr["as_of_date"] = args.as_of
    ptr["current_dir"] = os.path.abspath(cur_dir)

    # copy explainability to CURRENT
    src = os.path.join(root, args.explainability) if not os.path.isabs(args.explainability) else args.explainability
    dst = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson")
    shutil.copyfile(src, dst)
    sha = sha256_file(dst)
    sha_json = dst + ".sha256.json"
    write_json(sha_json, {"path": dst, "sha256": sha, "updated_at_utc": now_utc(), "as_of_date": args.as_of})

    ptr.setdefault("market_radar", {})
    ptr["market_radar"].setdefault("explainability_zip", {})
    ptr["market_radar"]["explainability_zip"].update({
        "as_of_date": args.as_of,
        "ndjson": os.path.abspath(dst),
        "sha256_json": os.path.abspath(sha_json),
        "schemaVersion": "market_radar_explainability_v1b",
        "updated_at_utc": now_utc()
    })

    if args.founder_contract:
        fcp = os.path.join(root, args.founder_contract) if not os.path.isabs(args.founder_contract) else args.founder_contract
        if os.path.exists(fcp):
            ptr["market_radar"]["founder_guidance_contract_b1"] = {
                "as_of_date": args.as_of,
                "json": os.path.abspath(fcp),
                "sha256_json": os.path.abspath(fcp + ".sha256.json"),
                "updated_at_utc": now_utc()
            }

    # backup pointers
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    if os.path.exists(ptr_path):
        shutil.copyfile(ptr_path, ptr_path + f".bak_{ts}")

    write_json(ptr_path, ptr)

    print(json.dumps({
        "ok": True,
        "explainability_current": os.path.abspath(dst),
        "explainability_sha": os.path.abspath(sha_json),
        "pointers": os.path.abspath(ptr_path),
        "pointers_backup": os.path.abspath(ptr_path + f".bak_{ts}") if os.path.exists(ptr_path + f".bak_{ts}") else None
    }, indent=2))

if __name__ == "__main__":
    main()
