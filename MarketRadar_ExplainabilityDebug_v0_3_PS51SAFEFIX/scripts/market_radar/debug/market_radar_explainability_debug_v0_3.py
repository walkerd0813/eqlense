#!/usr/bin/env python3
import argparse, json, os, datetime

def read_json(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def read_ndjson_find(path, predicate):
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            try:
                if predicate(obj):
                    return obj
            except Exception:
                continue
    return None

def norm_bucket(b):
    if b is None:
        return None
    s = str(b).strip()
    if not s:
        return None
    # Normalize common variants
    up = s.upper()
    if up in ("SINGLE", "SFR", "SFH"):
        return "SFR"
    if up in ("CONDO", "CONDOS"):
        return "CONDO"
    if up in ("MULTI", "MULTIFAMILY", "MF", "2-4"):
        return "MF"
    if up in ("LAND", "LOT"):
        return "LAND"
    return up

def try_load_contract(root, rel_or_abs):
    if not rel_or_abs:
        return None
    p = rel_or_abs
    if not os.path.isabs(p):
        p = os.path.join(root, p)
    if os.path.exists(p):
        try:
            return read_json(p)
        except Exception:
            return None
    return None

def built_in_glossary():
    return {
        "absorption": {
            "technical": "Absorption measures how quickly inventory clears (sales relative to active supply) over a time window.",
            "layman": "How fast homes are getting bought compared to how many are for sale.",
            "hover": "High absorption = homes are selling fast. Low absorption = homes are sitting longer."
        },
        "liquidity": {
            "technical": "Liquidity reflects the reliability and stability of transaction flow and listing lifecycle conversion.",
            "layman": "How easy it is to sell here without weird dry spells.",
            "hover": "Stable liquidity = consistent activity. Unstable liquidity = stop-and-go."
        },
        "price_discovery": {
            "technical": "Price discovery summarizes observed prices (MLS and/or deeds) when sample counts are sufficient.",
            "layman": "Whether we have enough real sales to confidently summarize prices.",
            "hover": "If samples are thin, we avoid claiming a price story."
        },
        "velocity": {
            "technical": "Velocity summarizes turnover intensity relative to parcel stock (proxy).",
            "layman": "How much the market is actually moving (turnover proxy).",
            "hover": "Higher = more churn; lower = quieter."
        },
        "suppressed": {
            "technical": "Suppressed means the metric is intentionally withheld due to missing fields or insufficient sample size.",
            "layman": "We’re choosing not to show it because the data isn’t strong enough.",
            "hover": "Suppressed = not guessed."
        },
        "unknown": {
            "technical": "Unknown indicates a first-class missing state; the system did not infer or guess.",
            "layman": "We don’t know yet, and we won’t pretend we do.",
            "hover": "Unknown is safer than wrong."
        },
        "confidence": {
            "technical": "Confidence is a qualitative grade (A/B/C) derived from data sufficiency and coverage integrity.",
            "layman": "How much you should trust this number for this ZIP right now.",
            "hover": "A = strong evidence, B = usable, C = thin/partial."
        }
    }

def resolve_indicators_current(ind_ptrs, state):
    # Pointers schema: { "states": { "MASS": { "ndjson": "...", ... } } }
    st = (ind_ptrs.get("states") or {}).get(state) or {}
    return st.get("ndjson")

def resolve_market_radar_currents(ptrs):
    # Market radar pointers include keys like "velocity_zip", "absorption_zip", "liquidity_p01_zip", "price_discovery_p01_zip", "explainability_zip"
    # Some older files used "ndjson" vs "path" — support both.
    def get_path(obj, fallback_key=None):
        if not obj:
            return None
        if isinstance(obj, str):
            return obj
        return obj.get("ndjson") or obj.get("path") or (obj.get(fallback_key) if fallback_key else None)

    return {
        "velocity_zip": get_path(ptrs.get("velocity_zip")),
        "absorption_zip": get_path(ptrs.get("absorption_zip")),
        "liquidity_p01_zip": get_path(ptrs.get("liquidity_p01_zip")),
        "price_discovery_p01_zip": get_path(ptrs.get("price_discovery_p01_zip")),
        "explainability_zip": get_path(ptrs.get("explainability_zip")),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--state", default="MASS")
    ap.add_argument("--zip", required=True)
    ap.add_argument("--asset_bucket", default="CONDO")
    ap.add_argument("--window_days", type=int, default=30)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--expand_glossary", action="store_true")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    root = args.root
    state = args.state.upper().strip()
    zipc = str(args.zip).strip()
    bucket = norm_bucket(args.asset_bucket)
    window_days = int(args.window_days)

    pointers_path = os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_POINTERS.json")
    ind_ptrs_path = os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT", "CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")

    ptrs = read_json(pointers_path)
    ind_ptrs = read_json(ind_ptrs_path) if os.path.exists(ind_ptrs_path) else {"states": {}}

    paths = resolve_market_radar_currents(ptrs)
    indicators_ndjson = resolve_indicators_current(ind_ptrs, state)

    # Explainability row
    exp_path = paths.get("explainability_zip")
    explainability_row = read_ndjson_find(
        exp_path,
        lambda r: str(r.get("zip")) == zipc and norm_bucket(r.get("asset_bucket")) == bucket and int(r.get("window_days", -1)) == window_days
    )

    # Indicators row
    indicators_row = read_ndjson_find(
        indicators_ndjson,
        lambda r: str(r.get("zip")) == zipc and norm_bucket(r.get("asset_bucket")) == bucket and int(r.get("window_days", -1)) == window_days
    )
    if indicators_row and "asset_bucket" in indicators_row:
        indicators_row["asset_bucket"] = norm_bucket(indicators_row.get("asset_bucket"))

    out = {
        "meta": {
            "zip": zipc,
            "asset_bucket": bucket,
            "window_days": window_days,
            "as_of_arg": args.as_of,
            "pointers": {
                "market_radar": pointers_path,
                "indicators": ind_ptrs_path
            }
        },
        "paths": {
            "explainability_zip": exp_path,
            "indicators_p01_zip_state": indicators_ndjson
        },
        "rows": {
            "explainability": explainability_row,
            "indicators": indicators_row
        },
        "diagnostics": {
            "notes": []
        }
    }

    if explainability_row is None:
        out["diagnostics"]["notes"].append(
            f"Missing explainability row for zip/bucket/window (zip={zipc}, bucket={bucket}, window={window_days})."
        )

    if indicators_row is None:
        out["diagnostics"]["notes"].append(
            "No indicators row found for this zip (state pointers/build may be missing, or bucket/window mismatch)."
        )

    # Expand glossary (explainability + indicators) using built-in + contracts
    if args.expand_glossary:
        g = built_in_glossary()

        # Explainability contract path (B1) may be embedded inside the row
        if explainability_row:
            cpath = None
            try:
                cpath = (((explainability_row.get("contracts") or {}).get("founder_guidance_b1") or {}).get("path"))
            except Exception:
                cpath = None
            contract_b1 = try_load_contract(root, cpath)
            if contract_b1 and isinstance(contract_b1, dict):
                # contract_b1 may include glossary
                cg = contract_b1.get("glossary")
                if isinstance(cg, dict):
                    for k,v in cg.items():
                        if isinstance(v, dict):
                            g.setdefault(k, v)

        # Indicator founder guidance contract (IB1): load from conventional location by as_of
        ib1_path = os.path.join(root, "publicData", "marketRadar", "indicators", "contracts",
                                f"indicator_founder_guidance_contract__ib1__v0_1_ASOF{args.as_of}.json")
        contract_ib1 = try_load_contract(root, ib1_path)
        if contract_ib1 and isinstance(contract_ib1, dict):
            cg = contract_ib1.get("glossary")
            if isinstance(cg, dict):
                for k,v in cg.items():
                    if isinstance(v, dict):
                        g.setdefault(k, v)
            # attach founder guidance to indicators row in debug output (internal only)
            if indicators_row is not None:
                indicators_row.setdefault("founder_guidance", {})
                indicators_row["founder_guidance"]["contract_ref"] = {
                    "contract_id": contract_ib1.get("contract_id", "IB1"),
                    "version": contract_ib1.get("engine_version", "v0_1"),
                    "path": ib1_path,
                    "schemaVersion": contract_ib1.get("schemaVersion", "")
                }
                indicators_row["founder_guidance"]["notes"] = contract_ib1.get("notes", [])
                indicators_row["founder_guidance"]["role_lenses"] = contract_ib1.get("role_lenses", {})
                indicators_row["founder_guidance"]["investigation_prompts"] = contract_ib1.get("investigation_prompts", {})

        # Which terms to expand?
        refs = []
        if explainability_row and isinstance(explainability_row.get("glossary_refs"), list):
            refs += [str(x) for x in explainability_row.get("glossary_refs") if x]
        # Add indicator keys as refs
        if indicators_row and isinstance(indicators_row.get("indicators"), dict):
            refs += list(indicators_row["indicators"].keys())
        refs = sorted(set(refs))

        out["glossary_expanded"] = {k: g.get(k) for k in refs if g.get(k)}

        # If explainability row had refs but none expanded, add a hint
        if explainability_row and explainability_row.get("glossary_refs") and not out["glossary_expanded"]:
            out["diagnostics"]["notes"].append("glossary_refs present but glossary expansion returned empty (check contracts or built-in glossary).")

    if args.out:
        os.makedirs(os.path.dirname(os.path.join(root, args.out)), exist_ok=True)
        out_path = os.path.join(root, args.out) if not os.path.isabs(args.out) else args.out
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"[ok] wrote {out_path}")

    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
