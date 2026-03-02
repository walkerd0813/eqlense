#!/usr/bin/env python3
import argparse, datetime, json, os, shutil, hashlib

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()

def write_json(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2, sort_keys=True)

def load_json_utf8sig(path: str):
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8-sig') as f:
        return json.load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--explainability', required=True)
    ap.add_argument('--as_of', required=True)
    ap.add_argument('--root', required=True)
    args = ap.parse_args()

    root = args.root
    cur_dir = os.path.join(root, 'publicData', 'marketRadar', 'CURRENT')
    os.makedirs(cur_dir, exist_ok=True)

    src = os.path.normpath(os.path.join(root, args.explainability)) if not os.path.isabs(args.explainability) else os.path.normpath(args.explainability)
    dst = os.path.join(cur_dir, 'CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson')

    if not os.path.exists(src):
        raise SystemExit(f"[error] explainability file not found: {src}")

    # Copy
    shutil.copyfile(src, dst)
    sha = sha256_file(dst)
    sha_path = dst + '.sha256.json'
    write_json(sha_path, {
        'sha256': sha,
        'computed_at_utc': datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        'ndjson': os.path.abspath(dst),
        'source': os.path.abspath(src),
        'as_of_date': args.as_of,
    })

    # Update pointers
    ptr_path = os.path.join(cur_dir, 'CURRENT_MARKET_RADAR_POINTERS.json')
    ptr_bak = None
    ptr_obj = load_json_utf8sig(ptr_path) or {}

    # ensure tree
    if 'market_radar' not in ptr_obj or not isinstance(ptr_obj.get('market_radar'), dict):
        ptr_obj['market_radar'] = {}

    mr = ptr_obj['market_radar']
    mr['explainability_zip'] = {
        'as_of_date': args.as_of,
        'updated_at_utc': datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        'ndjson': os.path.abspath(dst),
        'sha256_json': os.path.abspath(sha_path),
    }

    if os.path.exists(ptr_path):
        ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        ptr_bak = ptr_path + f".bak_{ts}"
        shutil.copyfile(ptr_path, ptr_bak)

    write_json(ptr_path, ptr_obj)

    print('[done] froze Market Radar EXPLAINABILITY CURRENT + updated pointers')
    print(json.dumps({
        'ok': True,
        'explainability_current': os.path.abspath(dst),
        'explainability_sha': os.path.abspath(sha_path),
        'pointers': os.path.abspath(ptr_path),
        'pointers_backup': os.path.abspath(ptr_bak) if ptr_bak else None,
    }, indent=2))

if __name__ == '__main__':
    main()
