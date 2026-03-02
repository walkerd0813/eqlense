#!/usr/bin/env python3
"""Market Radar Explainability Debug v0_3

Founder/internal debug helper.

Reads CURRENT Market Radar artifacts plus CURRENT Indicators artifacts and emits a focused
JSON bundle for a specific ZIP + asset bucket + window.

Key behaviors:
- Uses Market Radar pointers: publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_POINTERS.json
- Uses Indicators pointers: publicData/marketRadar/indicators/CURRENT/CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json
- Streams NDJSON to find the single matching row for the requested key(s)
- Optionally expands glossary refs (Explainability V1B emits glossary_refs)
- Optionally injects Indicator Founder Guidance (IB1) contract if present (for founder-only UX)

This is intentionally conservative: if something is missing, it reports it and does NOT guess.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional, Tuple


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def _ndjson_find_row(path: str, predicate) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except Exception:
                continue
            try:
                if predicate(o):
                    return o
            except Exception:
                continue
    return None


def _norm_bucket(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip().upper()


def _norm_zip(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if len(s) == 4 and s.isdigit():
        s = "0" + s
    return s


def _resolve_market_radar_paths(root: str) -> Tuple[str, Dict[str, Any]]:
    p = os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_POINTERS.json")
    pointers = _read_json(p)
    paths = {
        "market_radar_pointers": p,
        "explainability_zip": (pointers.get("explainability_zip") or {}).get("ndjson")
        or (pointers.get("explainability_zip") or {}).get("path"),
    }
    return p, paths


def _resolve_indicators_path(root: str, state: str) -> Tuple[str, Optional[str]]:
    p = os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT", "CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")
    obj = _read_json(p)
    st = (obj.get("states") or {}).get(state)
    if not st:
        return p, None
    return p, st.get("ndjson")


def _expand_glossary(glossary_refs) -> Dict[str, Any]:
    base = {
        "absorption": {
            "technical": "Absorption measures how quickly inventory clears (sales relative to active supply) over a time window.",
            "layman": "How fast homes are getting bought compared to how many are for sale.",
            "hover": "High absorption = homes are selling fast. Low absorption = homes are sitting longer.",
        },
        "liquidity": {
            "technical": "Liquidity reflects the reliability and stability of transaction flow and listing lifecycle conversion.",
            "layman": "How easy it is to sell here without weird dry spells.",
            "hover": "Stable liquidity = consistent sales activity. Unstable liquidity = stop‑and‑go market.",
        },
        "price_discovery": {
            "technical": "Price discovery summarizes observed prices (MLS and/or deeds) when sample counts are sufficient.",
            "layman": "Whether we have enough real sales to confidently summarize prices.",
            "hover": "If samples are thin, we avoid claiming a price story.",
        },
        "confidence": {
            "technical": "Confidence is a qualitative grade (A/B/C) derived from data sufficiency and coverage integrity.",
            "layman": "How much you should trust this number for this ZIP right now.",
            "hover": "A = strong evidence, B = usable, C = thin/partial.",
        },
        "suppressed": {
            "technical": "Suppressed means the metric is intentionally withheld due to missing fields or insufficient sample size.",
            "layman": "We’re choosing not to show it because the data isn’t strong enough.",
            "hover": "Suppressed = not guessed.",
        },
        "unknown": {
            "technical": "Unknown indicates a first‑class missing state; the system did not infer or guess.",
            "layman": "We don’t know yet, and we won’t pretend we do.",
            "hover": "Unknown is safer than wrong.",
        },
        "indicator_unknown": {
            "technical": "Indicator UNKNOWN means the indicator is gated by required samples/inputs and was not computed.",
            "layman": "We didn’t have enough usable data to calculate this indicator.",
            "hover": "UNKNOWN ≠ zero. It’s missing by design.",
        },
        "off_market_proxy": {
            "technical": "Off‑market participation is a proxy comparing deed volume to MLS closed volume; it is not a perfect matcher.",
            "layman": "A rough estimate of how much activity might be happening outside the MLS.",
            "hover": "Proxy only; validate with raw comps + MLS coverage.",
        },
    }
    expanded: Dict[str, Any] = {}
    for k in glossary_refs or []:
        kk = str(k).strip().lower()
        if kk in base:
            expanded[kk] = base[kk]
    return expanded


def _load_indicator_founder_guidance_contract(root: str, as_of: str) -> Optional[Dict[str, Any]]:
    fn = f"indicator_founder_guidance_contract__ib1__v0_1_ASOF{as_of}.json"
    p = os.path.join(root, "publicData", "marketRadar", "indicators", "contracts", fn)
    if not os.path.exists(p):
        return None
    try:
        return _read_json(p)
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--state", default="MASS")
    ap.add_argument("--zip", required=True)
    ap.add_argument("--asset_bucket", default="SFR")
    ap.add_argument("--window_days", type=int, default=30)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--expand_glossary", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    root = args.root
    state = str(args.state).strip().upper()
    z = _norm_zip(args.zip)
    bucket = _norm_bucket(args.asset_bucket)
    window_days = int(args.window_days)
    as_of = str(args.as_of).strip()

    market_ptr_path, mr_paths = _resolve_market_radar_paths(root)
    ind_ptr_path, ind_ndjson = _resolve_indicators_path(root, state)

    explainability_path = mr_paths.get("explainability_zip")
    explain_row = _ndjson_find_row(
        explainability_path,
        lambda r: _norm_zip(r.get("zip")) == z
        and _norm_bucket(r.get("asset_bucket")) == bucket
        and int(r.get("window_days") or -1) == window_days,
    )

    ind_row = None
    if ind_ndjson:
        ind_row = _ndjson_find_row(
            ind_ndjson,
            lambda r: _norm_zip(r.get("zip")) == z
            and _norm_bucket(r.get("asset_bucket")) == bucket
            and int(r.get("window_days") or -1) == window_days,
        )

    notes = []
    if not explain_row:
        notes.append(f"Missing explainability row for zip/bucket/window (zip={z}, bucket={bucket}, window={window_days}).")
    if not ind_ndjson:
        notes.append(f"No indicators state pointer found for state={state}.")
    elif not ind_row:
        notes.append(f"No indicators row found for zip/bucket/window (zip={z}, bucket={bucket}, window={window_days}).")

    out: Dict[str, Any] = {
        "meta": {
            "zip": z,
            "asset_bucket": bucket,
            "window_days": window_days,
            "as_of_arg": as_of,
            "pointers": {"market_radar": market_ptr_path, "indicators": ind_ptr_path},
        },
        "paths": {"explainability_zip": explainability_path, "indicators_p01_zip_state": ind_ndjson},
        "rows": {"explainability": explain_row, "indicators": ind_row},
        "diagnostics": {"notes": notes},
    }

    if args.expand_glossary and explain_row:
        out["glossary_expanded"] = _expand_glossary(explain_row.get("glossary_refs") or [])

    ig = _load_indicator_founder_guidance_contract(root, as_of)
    if ig:
        out.setdefault("contracts", {})["indicator_founder_guidance_ib1"] = {
            "contract_id": ig.get("contract_id", "IB1"),
            "version": ig.get("version"),
            "schemaVersion": ig.get("schemaVersion"),
            "path": ig.get("path"),
        }
        # Keep it small: provide the role lenses + prompts only (no UI copy)
        out.setdefault("founder", {})["indicator_guidance"] = ig.get("guidance")

    if args.out:
        out_path = os.path.join(root, args.out) if not os.path.isabs(args.out) else args.out
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, sort_keys=False)
        print(f"[ok] wrote {out_path}")

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
