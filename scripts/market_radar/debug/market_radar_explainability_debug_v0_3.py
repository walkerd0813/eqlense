# -*- coding: utf-8 -*-
"""
Market Radar Explainability Debug v0_3 (known-good rewrite)

Guarantees:
- argparse works and --help prints usage
- --out writes JSON to disk (or exits non-zero)
- resolves CURRENT explainability + CURRENT indicators by state
- can expand glossary terms used by explainability
- includes indicator founder guidance contract IB1 if present
"""

from __future__ import annotations
import argparse
import datetime
import json
import os
import sys
from typing import Any, Dict, Optional, Tuple

def _utc_now_iso() -> str:
    # use timezone-aware UTC to avoid utcnow deprecation
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def _safe_exists(path: Optional[str]) -> bool:
    return bool(path) and os.path.exists(path)

def _norm_bucket(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    return s.upper()

def _norm_bucket_lo(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    return s.lower()

def _scan_ndjson_for_row(path: str, zip5: str, bucket: Optional[str], window_days: Optional[int]) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None

    zip5 = str(zip5).strip()
    b_up = _norm_bucket(bucket)
    b_lo = _norm_bucket_lo(bucket)

    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue

            if str(obj.get("zip","")).strip() != zip5:
                continue

            # bucket may be stored as "CONDO" or "condo"
            ob = obj.get("asset_bucket")
            if bucket is not None:
                if ob is None:
                    continue
                ob_s = str(ob).strip()
                if ob_s.upper() != b_up and ob_s.lower() != b_lo:
                    continue

            # window may not exist for some datasets; if provided, require match
            if window_days is not None:
                ow = obj.get("window_days")
                try:
                    ow_i = int(ow) if ow is not None else None
                except Exception:
                    ow_i = None
                if ow_i != int(window_days):
                    continue

            return obj

    return None

def _glossary() -> Dict[str, Dict[str, str]]:
    # Expand as you want later; keep minimal + stable now
    return {
        "absorption": {
            "technical": "Absorption measures how quickly inventory clears (sales relative to active supply) over a time window.",
            "layman": "How fast homes are getting bought compared to how many are for sale.",
            "hover": "High absorption = homes are selling fast. Low absorption = homes are sitting longer."
        },
        "liquidity": {
            "technical": "Liquidity reflects the reliability and stability of transaction flow and listing lifecycle conversion.",
            "layman": "How easy it is to sell here without weird dry spells.",
            "hover": "Stable liquidity = consistent sales activity. Unstable liquidity = stop-and-go market."
        },
        "price_discovery": {
            "technical": "Price discovery summarizes observed prices (MLS and/or deeds) when sample counts are sufficient.",
            "layman": "Whether we have enough real sales to confidently summarize prices.",
            "hover": "If samples are thin, we avoid claiming a price story."
        },
        "confidence": {
            "technical": "Confidence is a qualitative grade (A/B/C) derived from data sufficiency and coverage integrity.",
            "layman": "How much you should trust this number for this ZIP right now.",
            "hover": "A = strong evidence, B = usable, C = thin/partial."
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
    }

def _expand_glossary_refs(explainability_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    refs = explainability_row.get("glossary_refs")
    if not refs or not isinstance(refs, list):
        return None
    g = _glossary()
    out: Dict[str, Any] = {}
    for k in refs:
        if not isinstance(k, str):
            continue
        kk = k.strip()
        if not kk:
            continue
        if kk in g:
            out[kk] = g[kk]
    return out or None

def _resolve_explainability_path(root: str) -> str:
    # Always trust CURRENT file for now (we already froze it)
    return os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson")

def _resolve_indicators_state_path(root: str, state: str) -> Optional[str]:
    ptr = os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT", "CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")
    if not os.path.exists(ptr):
        return None
    o = _read_json(ptr)
    st = (o.get("states") or {}).get(state)
    if not isinstance(st, dict):
        return None
    p = st.get("ndjson")
    if isinstance(p, str) and p.strip():
        return p
    return None

def _resolve_ib1_contract(root: str, as_of: str) -> Optional[str]:
    # expected file name from your build
    p = os.path.join(
        root, "publicData", "marketRadar", "indicators", "contracts",
        f"indicator_founder_guidance_contract__ib1__v0_1_ASOF{as_of}.json"
    )
    return p if os.path.exists(p) else None

def main() -> int:
    ap = argparse.ArgumentParser(prog="market_radar_explainability_debug_v0_3.py")
    ap.add_argument("--root", required=True)
    ap.add_argument("--state", required=True, help="State key, e.g. MASS")
    ap.add_argument("--zip", required=True)
    ap.add_argument("--asset_bucket", default=None)
    ap.add_argument("--window_days", type=int, default=None)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--expand_glossary", action="store_true")
    ap.add_argument("--out", default=None, help="If provided, write JSON to this file path")
    args = ap.parse_args()

    root = args.root
    state = str(args.state).strip().upper()
    zip5 = str(args.zip).strip()
    bucket = args.asset_bucket
    window_days = args.window_days
    as_of = str(args.as_of).strip()

    ex_path = _resolve_explainability_path(root)
    ind_path = _resolve_indicators_state_path(root, state)

    out_obj: Dict[str, Any] = {
        "meta": {
            "zip": zip5,
            "asset_bucket": bucket,
            "window_days": window_days,
            "as_of_arg": as_of,
            "built_at_utc": _utc_now_iso(),
            "pointers": {
                "market_radar": os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_POINTERS.json"),
                "indicators": os.path.join(root, "publicData", "marketRadar", "indicators", "CURRENT", "CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json"),
            }
        },
        "paths": {
            "explainability_zip": ex_path if os.path.exists(ex_path) else None,
            "indicators_p01_zip_state": ind_path if (ind_path and os.path.exists(ind_path)) else None
        },
        "rows": {
            "explainability": None,
            "indicators": None
        },
        "diagnostics": {
            "notes": []
        }
    }

    # Explainability row
    if os.path.exists(ex_path):
        ex_row = _scan_ndjson_for_row(ex_path, zip5, bucket, window_days)
        if ex_row is None:
            out_obj["diagnostics"]["notes"].append(
                f"Missing explainability row for zip/bucket/window (zip={zip5}, bucket={bucket}, window={window_days})."
            )
        else:
            out_obj["rows"]["explainability"] = ex_row
            if args.expand_glossary:
                out_obj["glossary_expanded"] = _expand_glossary_refs(ex_row)

    else:
        out_obj["diagnostics"]["notes"].append(f"Explainability CURRENT not found at: {ex_path}")

    # Indicators row
    if ind_path and os.path.exists(ind_path):
        ind_row = _scan_ndjson_for_row(ind_path, zip5, bucket, window_days)
        if ind_row is None:
            out_obj["diagnostics"]["notes"].append(
                f"Missing indicators row for zip/bucket/window (zip={zip5}, bucket={bucket}, window={window_days})."
            )
        else:
            out_obj["rows"]["indicators"] = ind_row
    else:
        out_obj["diagnostics"]["notes"].append("Indicators state ndjson path missing (pointers may be missing or invalid).")

    # Contracts
    contracts: Dict[str, Any] = {}
    ib1 = _resolve_ib1_contract(root, as_of)
    if ib1:
        contracts["indicator_founder_guidance_ib1"] = {
            "contract_id": "IB1",
            "version": "v0_1",
            "schemaVersion": "market_radar_indicator_founder_guidance_contract_ib1",
            "path": ib1
        }
    if contracts:
        out_obj["contracts"] = contracts

    # Write or print
    if args.out:
        out_path = args.out
        out_dir = os.path.dirname(out_path)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(out_obj, f, ensure_ascii=False, indent=2)
            print(f"[ok] wrote {out_path}")
        except Exception as e:
            print(f"[error] failed to write --out file: {out_path} :: {e}", file=sys.stderr)
            return 2
    else:
        print(json.dumps(out_obj, ensure_ascii=False, indent=2))

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
