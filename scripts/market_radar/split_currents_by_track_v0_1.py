import argparse
import json
import os
from datetime import datetime, timezone

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def build_placeholder(track, kind, reason):
    # kind: "market_radar" | "indicators"
    return {
        "schema": "equity_lens.market_radar.current_pointers.v0_1",
        "kind": kind,
        "radar_track": track,
        "state": "UNKNOWN",
        "reason": reason,
        "generated_at": now_iso(),
        "pointers": {}
    }

def wrap_existing(existing, track, kind):
    # preserve existing structure exactly, but wrap with metadata so your loader can be track-aware later
    return {
        "schema": "equity_lens.market_radar.current_pointers.v0_1",
        "kind": kind,
        "radar_track": track,
        "state": "OK",
        "generated_at": now_iso(),
        "pointers": existing
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Backend root, e.g. C:\\seller-app\\backend")
    args = ap.parse_args()

    root = args.root

    # Existing pointers we already have
    mr_current = os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_POINTERS.json")
    ind_current = os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT", "CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")

    if not os.path.exists(mr_current):
        raise SystemExit(f"[error] missing: {mr_current}")
    if not os.path.exists(ind_current):
        raise SystemExit(f"[error] missing: {ind_current}")

    mr = read_json(mr_current)
    ind = read_json(ind_current)

    out_mr_dir = os.path.join(root, "publicData", "marketRadar", "CURRENT")
    out_ind_dir = os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT")

    # 1) RES_1_4 points to existing CURRENT pointers (safe, zero changes)
    res_mr = wrap_existing(mr, "RES_1_4", "market_radar")
    res_ind = wrap_existing(ind, "RES_1_4", "indicators")

    write_json(os.path.join(out_mr_dir, "CURRENT_MARKET_RADAR_POINTERS__RES_1_4.json"), res_mr)
    write_json(os.path.join(out_ind_dir, "CURRENT_MARKET_RADAR_INDICATORS_POINTERS__RES_1_4.json"), res_ind)

    # 2) MF_5_PLUS placeholder
    mf_mr = build_placeholder("MF_5_PLUS", "market_radar", "UNSUPPORTED_TRACK_NOT_BUILT_YET")
    mf_ind = build_placeholder("MF_5_PLUS", "indicators", "UNSUPPORTED_TRACK_NOT_BUILT_YET")
    write_json(os.path.join(out_mr_dir, "CURRENT_MARKET_RADAR_POINTERS__MF_5_PLUS.json"), mf_mr)
    write_json(os.path.join(out_ind_dir, "CURRENT_MARKET_RADAR_INDICATORS_POINTERS__MF_5_PLUS.json"), mf_ind)

    # 3) LAND placeholder
    land_mr = build_placeholder("LAND", "market_radar", "UNSUPPORTED_TRACK_NOT_BUILT_YET")
    land_ind = build_placeholder("LAND", "indicators", "UNSUPPORTED_TRACK_NOT_BUILT_YET")
    write_json(os.path.join(out_mr_dir, "CURRENT_MARKET_RADAR_POINTERS__LAND.json"), land_mr)
    write_json(os.path.join(out_ind_dir, "CURRENT_MARKET_RADAR_INDICATORS_POINTERS__LAND.json"), land_ind)

    print("[ok] wrote track-scoped CURRENT pointers")
    print("  - RES_1_4 -> existing pointers")
    print("  - MF_5_PLUS + LAND -> placeholders (unsupported for now)")
    print("[done] split_currents_by_track_v0_1 complete")

if __name__ == "__main__":
    main()
