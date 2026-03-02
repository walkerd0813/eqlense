#!/usr/bin/env python
import argparse, json, os
def fail(msg, code=2):
    print("[error] " + msg)
    return code

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--track", required=True, choices=["RES_1_4","MF_5_PLUS","LAND"])
    args = ap.parse_args()

    root = args.root
    candidates = [
        os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_POINTERS.json"),
        os.path.join(root, "publicData", "marketRadar", "CURRENT", f"CURRENT_MARKET_RADAR_POINTERS_{args.track}.json"),
    ]
    path = next((p for p in candidates if os.path.exists(p)), None)
    if not path:
        return fail(f"Missing Market Radar pointers CURRENT file. Looked for: {candidates}", 3)

    with open(path, "r", encoding="utf-8-sig") as f:
        j = json.load(f)

    track = args.track
    if isinstance(j, dict) and j.get("radar_track") == track:
        obj = j
    elif isinstance(j, dict) and track in j:
        obj = j[track]
    else:
        return fail(f"Pointers JSON not in expected dict format for track {track}: {path}", 4)

    if obj.get("state") not in ("OK","WARN"):
        return fail(f"Track {track} state is not OK/WARN (got {obj.get('state')}): {path}", 5)

    pointers = obj.get("pointers") or {}
    if not pointers:
        return fail(f"Track {track} has no pointers object: {path}", 6)

    if track == "RES_1_4":
        required_any = ["market_radar", "layerB_deeds_zip", "stock_zip_current"]
        if not any(k in pointers for k in required_any):
            return fail(f"Track {track} pointers missing expected keys (any of {required_any}). Keys present: {list(pointers.keys())}", 7)

    print(f"[ok] Market Radar pointers look sane for {track}: {path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
