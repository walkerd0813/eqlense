import argparse, json, os, sys, re
from typing import Any, Dict, List, Tuple

def jload(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def exists(root: str, rel: str) -> str:
    p = os.path.join(root, rel.replace("/", os.sep))
    return p

def die(msg: str, code: int = 2):
    print(msg)
    sys.exit(code)

def warn(msg: str):
    print("[warn]", msg)

def ok(msg: str):
    print("[ok]", msg)

def get_all_strings(obj: Any) -> List[str]:
    out = []
    if isinstance(obj, str):
        return [obj]
    if isinstance(obj, list):
        for x in obj: out.extend(get_all_strings(x))
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.extend(get_all_strings(k))
            out.extend(get_all_strings(v))
    return out

def has_key_anywhere(obj: Any, key: str) -> bool:
    if isinstance(obj, dict):
        if key in obj:
            return True
        return any(has_key_anywhere(v, key) for v in obj.values())
    if isinstance(obj, list):
        return any(has_key_anywhere(v, key) for v in obj)
    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    root = args.root
    cfg_path = exists(root, args.config)
    if not os.path.exists(cfg_path):
        die(f"[error] config not found: {cfg_path}")

    cfg = jload(cfg_path)

    failures = 0

    # 1) required files exist
    for rel in cfg.get("required_files_exist", []):
        p = exists(root, rel)
        if not os.path.exists(p):
            print(f"[error] missing required file: {rel}")
            failures += 1
    if failures:
        die(f"[fail] missing {failures} required files")

    ok("required files exist")

    # 2) track placeholder rules
    for rule in cfg.get("track_placeholder_rules", []):
        rel = rule["path"]
        must = rule.get("must_have", {})
        p = exists(root, rel)
        data = jload(p)
        for k, v in must.items():
            if data.get(k) != v:
                print(f"[error] placeholder rule failed for {rel}: expected {k}={v}, got {data.get(k)}")
                failures += 1
    if failures:
        die(f"[fail] placeholder rules failed ({failures})")
    ok("track placeholder rules OK")

    # 3) pointer artifact header requirements (lightweight)
    req = cfg.get("pointer_artifact_header_requirements", {})
    must_have_one_of = req.get("must_have_one_of", [])
    min_keys = req.get("min_keys_anywhere", [])

    pointer_files = [
        "publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_POINTERS.json",
        "publicData/marketRadar/indicators/CURRENT/CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json",
        "publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_POINTERS__RES_1_4.json",
        "publicData/marketRadar/indicators/CURRENT/CURRENT_MARKET_RADAR_INDICATORS_POINTERS__RES_1_4.json",
    ]
    for rel in pointer_files:
        p = exists(root, rel)
        data = jload(p)
        for k in min_keys:
            if not has_key_anywhere(data, k):
                print(f"[error] pointer file missing key anywhere '{k}': {rel}")
                failures += 1
        for group in must_have_one_of:
            if not any(has_key_anywhere(data, k) for k in group):
                print(f"[error] pointer file missing at least one of {group}: {rel}")
                failures += 1

    if failures:
        die(f"[fail] pointer header requirements failed ({failures})")
    ok("pointer header requirements OK")

    # 4) forbidden coupling checks
    for chk in cfg.get("forbidden_coupling_checks", []):
        name = chk.get("name","check")
        forbidden = chk.get("forbidden_substrings", [])
        for rel in chk.get("paths", []):
            p = exists(root, rel)
            data = jload(p)
            strings = get_all_strings(data)
            hit = [s for s in strings for f in forbidden if f.lower() in s.lower()]
            if hit:
                print(f"[error] forbidden coupling '{name}' found in {rel}: sample='{hit[0]}'")
                failures += 1

    if failures:
        die(f"[fail] forbidden coupling checks failed ({failures})")

    ok("forbidden coupling checks OK")
    print("[done] contracts validator gate passed")
    sys.exit(0)

if __name__ == "__main__":
    main()
