# market_radar_explainability_debug_v0_2.py
# Purpose: founder/debug extraction for a single ZIP, with optional glossary expansion,
#          and indicators resolution via indicators CURRENT pointers.
# PowerShell-safe file (ASCII/UTF-8), no f-strings that confuse PS here-docs.

import argparse
import datetime
import json
import os
import sys

def _read_json(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def _safe_join(root, rel):
    rel2 = rel.replace("/", os.sep).replace("\\\\", os.sep).replace("\\", os.sep)
    return os.path.join(root, rel2)

def _load_pointers(root):
    mr_ptr = _safe_join(root, "publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_POINTERS.json")
    ind_ptr = _safe_join(root, "publicData/marketRadar/indicators/CURRENT/CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")
    out = {
        "market_radar": mr_ptr if os.path.exists(mr_ptr) else None,
        "indicators": ind_ptr if os.path.exists(ind_ptr) else None,
    }
    return out

def _scan_ndjson_for_match(path, want):
    # want: dict of field->value; all must match
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            ok = True
            for k, v in want.items():
                if v is None:
                    continue
                if obj.get(k) != v:
                    ok = False
                    break
            if ok:
                return obj
    return None

def _first_existing(*paths):
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None

def _resolve_market_radar_paths(root, mr_ptr_obj):
    # tolerate different pointer layouts
    mr = mr_ptr_obj.get("market_radar", mr_ptr_obj)

    def _get(*keys):
        cur = mr
        for k in keys:
            if not isinstance(cur, dict) or k not in cur:
                return None
            cur = cur[k]
        return cur

    explainability = _get("explainability_zip", "ndjson")
    if not explainability:
        # some builds may store under "explainability_zip" directly
        explainability = _get("explainability_zip", "path") or _get("explainability_zip", "ndjson")

    # Normalize to absolute path if needed
    if explainability and not os.path.isabs(explainability):
        explainability = _safe_join(root, explainability)

    return {
        "explainability_zip": explainability
    }

def _resolve_indicators_ndjson(root, ind_ptr_obj, state):
    # Expected: { "states": { "MASS": { "ndjson": "...CURRENT...P01...ndjson", ... } } }
    states = ind_ptr_obj.get("states", {})
    st = states.get(state)
    if not isinstance(st, dict):
        return None
    nd = st.get("ndjson")
    if not nd:
        return None
    if not os.path.isabs(nd):
        nd = _safe_join(root, nd)
    return nd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--state", required=False, default="MASS")
    ap.add_argument("--zip", required=True)
    ap.add_argument("--asset_bucket", required=False, default=None)
    ap.add_argument("--window_days", required=False, type=int, default=None)
    ap.add_argument("--as_of", required=False, default=None)
    ap.add_argument("--expand_glossary", action="store_true")
    ap.add_argument("--out", required=False, default=None)
    args = ap.parse_args()

    root = args.root
    state = args.state
    zip5 = str(args.zip).strip()
    asset_bucket = args.asset_bucket
    window_days = args.window_days
    as_of = args.as_of

    pointers = _load_pointers(root)
    resp = {
        "meta": {
            "zip": zip5,
            "asset_bucket": asset_bucket,
            "window_days": window_days,
            "as_of_arg": as_of,
            "pointers": pointers
        },
        "paths": {},
        "rows": {},
        "diagnostics": { "notes": [] }
    }

    # Load Market Radar pointers
    if not pointers["market_radar"]:
        resp["diagnostics"]["notes"].append("Missing market_radar pointers file.")
    else:
        mr_ptr_obj = _read_json(pointers["market_radar"])
        mr_paths = _resolve_market_radar_paths(root, mr_ptr_obj)
        resp["paths"].update(mr_paths)

    # Load Explainability row
    exp_path = resp["paths"].get("explainability_zip")
    if not exp_path or not os.path.exists(exp_path):
        resp["diagnostics"]["notes"].append("Explainability CURRENT not found.")
        resp["rows"]["explainability"] = None
    else:
        # Explainability rows are keyed by zip + asset_bucket + window_days (in our contract)
        # If asset_bucket/window_days not provided, we fallback to zip only.
        want = {"zip": zip5}
        if asset_bucket:
            want["asset_bucket"] = asset_bucket
        if window_days is not None:
            want["window_days"] = window_days
        row = _scan_ndjson_for_match(exp_path, want)
        if not row and (asset_bucket or window_days is not None):
            # fallback to zip-only match
            row = _scan_ndjson_for_match(exp_path, {"zip": zip5})
        resp["rows"]["explainability"] = row
        if not row:
            resp["diagnostics"]["notes"].append("No explainability row found for this ZIP (and optional bucket/window).")

    # Indicators resolution (CURRENT pointers format you actually have)
    ind_nd = None
    if not pointers["indicators"]:
        resp["diagnostics"]["notes"].append("Missing indicators pointers file.")
    else:
        ind_ptr_obj = _read_json(pointers["indicators"])
        ind_nd = _resolve_indicators_ndjson(root, ind_ptr_obj, state)
        resp["paths"]["indicators_p01_zip_state"] = ind_nd

        if not ind_nd or not os.path.exists(ind_nd):
            resp["diagnostics"]["notes"].append("Indicators CURRENT not found for state={0}.".format(state))
            resp["rows"]["indicators"] = None
        else:
            # Indicators rows are usually zip-keyed; some builds also include asset_bucket/window_days.
            want2 = {"zip": zip5}
            if asset_bucket:
                want2["asset_bucket"] = asset_bucket
            if window_days is not None:
                want2["window_days"] = window_days
            ind_row = _scan_ndjson_for_match(ind_nd, want2)
            if not ind_row and (asset_bucket or window_days is not None):
                ind_row = _scan_ndjson_for_match(ind_nd, {"zip": zip5})
            resp["rows"]["indicators"] = ind_row
            if not ind_row:
                resp["diagnostics"]["notes"].append("No indicators row found for this ZIP (state={0}).".format(state))

    # Optional glossary expansion:
    # If explainability row includes glossary_expanded already, we surface it.
    # If it includes only glossary_refs, we just return refs (contract-driven) for now.
    if args.expand_glossary and resp["rows"].get("explainability"):
        row = resp["rows"]["explainability"]
        if "glossary_expanded" in row:
            resp["glossary_expanded"] = row.get("glossary_expanded")
        else:
            resp["glossary_expanded"] = None
            if "glossary_refs" in row:
                resp["diagnostics"]["notes"].append("glossary_refs present but glossary_expanded missing in explainability row (did you build V1B rows?).")

    # Write output
    out_path = args.out
    if out_path:
        if not os.path.isabs(out_path):
            out_path = _safe_join(root, out_path)
        out_dir = os.path.dirname(out_path)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(resp, f, ensure_ascii=False, indent=2)
        print("[ok] wrote {0}".format(out_path))

    print(json.dumps(resp, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    sys.exit(main())
