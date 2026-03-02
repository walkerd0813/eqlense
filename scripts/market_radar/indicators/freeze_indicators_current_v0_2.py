#!/usr/bin/env python3
import argparse, json, os, shutil, hashlib, datetime

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--ndjson", required=True)
    args = ap.parse_args()

    root = args.root
    state = args.state.upper()
    ndjson_in = args.ndjson
    if not os.path.isabs(ndjson_in):
        ndjson_in = os.path.join(root, ndjson_in)
    ndjson_in = os.path.abspath(ndjson_in)

    cur_dir = os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT")
    os.makedirs(cur_dir, exist_ok=True)

    cur_nd = os.path.join(cur_dir, f"CURRENT_MARKET_RADAR_INDICATORS_P01_{state}.ndjson")
    shutil.copyfile(ndjson_in, cur_nd)

    sha = sha256_file(cur_nd)
    sha_json = cur_nd + ".sha256.json"
    with open(sha_json, "w", encoding="utf-8") as f:
        json.dump({"path": os.path.abspath(cur_nd), "sha256": sha}, f, indent=2)

    ptr_path = os.path.join(cur_dir, "CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")
    if os.path.exists(ptr_path):
        with open(ptr_path, "r", encoding="utf-8-sig") as f:
            ptr = json.load(f)
    else:
        ptr = {"states": {}}

    ptr.setdefault("states", {})
    ptr["states"][state] = {
        "as_of_date": args.as_of,
        "ndjson": os.path.abspath(cur_nd),
        "sha256_json": os.path.abspath(sha_json),
        "updated_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }

    with open(ptr_path, "w", encoding="utf-8") as f:
        json.dump(ptr, f, indent=2)

    print(json.dumps({
        "ok": True,
        "state": state,
        "indicators_current": os.path.abspath(cur_nd),
        "sha256_json": os.path.abspath(sha_json),
        "pointers": os.path.abspath(ptr_path)
    }, indent=2))

if __name__ == "__main__":
    main()
