#!/usr/bin/env python3
"""Market Radar - Regime State (P02) v0_1

Consumes frozen pillar outputs and emits a structural regime classification per (zip, asset_bucket, window_days).

NOTES
- This is NOT forecasting and NOT advice.
- It is a structural classifier based on observed rollups.
- Includes confidence + short explainability text.
- Schema-tolerant: searches multiple candidate keys.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
from typing import Any, Dict, Iterable, Optional, Tuple

ZIP_RE = re.compile(r"^\d{5}$")


def utc_now_z() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ndjson_iter(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def norm_bucket(b: Any) -> Optional[str]:
    if b is None:
        return None
    s = str(b).strip().upper()
    if s in {"SF", "SINGLE", "SINGLE_FAMILY", "SINGLE FAMILY"}:
        return "SF"
    if s in {"MF", "MULTI", "MULTI_FAMILY", "MULTI FAMILY", "2F", "3F", "4F"}:
        return "MF"
    if s in {"CONDO", "CC", "CONDOMINIUM"}:
        return "CONDO"
    if s in {"LAND", "VACANT", "VACANT LAND"}:
        return "LAND"
    if s in {"OTHER"}:
        return "OTHER"
    if s in {"UNKNOWN", ""}:
        return "UNKNOWN"
    return s


def key_of(r: Dict[str, Any]) -> Optional[Tuple[str, str, int]]:
    z = r.get("zip")
    if not z or not isinstance(z, str) or not ZIP_RE.match(z):
        return None
    b = norm_bucket(r.get("asset_bucket") or r.get("bucket"))
    if not b:
        return None
    w = r.get("window_days") or r.get("window") or r.get("windowDays")
    try:
        w = int(w)
    except Exception:
        return None
    return (z, b, w)


def pick_metric(d: Dict[str, Any], candidates: Iterable[str]) -> Optional[float]:
    metrics = d.get("metrics") if isinstance(d.get("metrics"), dict) else {}
    for k in candidates:
        for src in (d, metrics):
            if k in src:
                v = src.get(k)
                if isinstance(v, dict) and "value" in v:
                    v = v.get("value")
                if isinstance(v, (int, float)):
                    return float(v)
    return None


def classify(bundle: Dict[str, Any]) -> Tuple[str, float, list[str], str]:
    velocity = bundle.get("velocity")
    months_supply = bundle.get("months_supply")
    dom_med = bundle.get("dom_median")
    pending_rate = bundle.get("pending_rate")
    off_market_rate = bundle.get("off_market_rate")
    divergence_abs = bundle.get("pd_divergence_abs")

    drivers: list[str] = []

    score_hot = 0.0
    score_cold = 0.0
    score_chaos = 0.0

    if velocity is not None:
        if velocity >= 0.06:
            score_hot += 1.0
            drivers.append("high turnover")
        elif velocity <= 0.02:
            score_cold += 1.0
            drivers.append("low turnover")

    if months_supply is not None:
        if months_supply <= 2.5:
            score_hot += 1.0
            drivers.append("tight supply")
        elif months_supply >= 6.5:
            score_cold += 1.0
            drivers.append("loose supply")

    if dom_med is not None:
        if dom_med <= 25:
            score_hot += 0.7
            drivers.append("fast DOM")
        elif dom_med >= 60:
            score_cold += 0.7
            drivers.append("slow DOM")

    if pending_rate is not None:
        if pending_rate >= 0.35:
            score_hot += 0.6
            drivers.append("high pending share")

    if off_market_rate is not None and off_market_rate >= 0.25:
        score_chaos += 0.5
        drivers.append("elevated off-market churn")

    if divergence_abs is not None:
        if divergence_abs >= 0.12:
            score_chaos += 1.0
            drivers.append("MLS↔deeds divergence")
        elif divergence_abs <= 0.04:
            score_hot += 0.2

    if score_chaos >= 1.0 and max(score_hot, score_cold) <= 1.2:
        state = "DISLOCATION"
    elif score_hot >= 1.8 and score_cold <= 0.6:
        state = "DISTRIBUTION" if (divergence_abs is not None and divergence_abs >= 0.10) else "EXPANSION"
    elif score_cold >= 1.6 and score_hot <= 0.6:
        state = "COMPRESSION"
    else:
        state = "STAGNATION"

    signals = [velocity, months_supply, dom_med, pending_rate, divergence_abs]
    have = sum(1 for x in signals if x is not None)
    base_conf = min(1.0, have / 5.0)
    sep = abs(score_hot - score_cold)
    conf = min(1.0, 0.15 + 0.65 * base_conf + 0.10 * min(1.0, sep / 2.0) + 0.10 * min(1.0, score_chaos))
    conf = max(0.0, min(1.0, conf))

    if not drivers:
        expl = "Regime based on available market rollups; limited inputs for this ZIP/window."
    else:
        expl = f"{state.title()} regime driven by " + ", ".join(drivers[:3]) + "."

    return state, conf, drivers[:6], expl


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--velocity", required=True)
    ap.add_argument("--absorption", required=True)
    ap.add_argument("--liquidity", required=True)
    ap.add_argument("--price_discovery", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--min_samples", type=int, default=10)
    args = ap.parse_args()

    def load_map(path: str) -> Dict[Tuple[str, str, int], Dict[str, Any]]:
        m: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
        for r in ndjson_iter(path):
            k = key_of(r)
            if k:
                m[k] = r
        return m

    vel = load_map(args.velocity)
    absr = load_map(args.absorption)
    liq = load_map(args.liquidity)
    pd = load_map(args.price_discovery)

    keys = set(vel.keys()) | set(absr.keys()) | set(liq.keys()) | set(pd.keys())

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    scan = {
        "keys_total": len(keys),
        "have_velocity": 0,
        "have_absorption": 0,
        "have_liquidity": 0,
        "have_price_discovery": 0,
        "pd_divergence_strong": 0,
    }

    VEL_RATE_KEYS = ["velocity_rate_annual","velocity_rate","turnover_rate","turnover_rate_annual","sales_per_stock","sales_per_stock_annual"]
    VEL_SALES_KEYS = ["deeds_arms_length","sales_count","deeds_sales"]
    STOCK_KEYS = ["stock_total","stock","stock_units","stock_count"]

    MOS_KEYS = ["months_of_supply","mos","months_supply"]
    INV_KEYS = ["inventory","active_inventory","active_count"]

    DOM_KEYS = ["dom_median","median_dom","median_days_on_market","days_on_market_median"]
    ACTIVE_KEYS = ["active","active_count"]
    PENDING_KEYS = ["pending","pending_count","under_agreement","ua_count"]
    OFFMARKET_KEYS = ["off_market","off_market_count"]
    WITHDRAWN_KEYS = ["withdrawn","withdrawn_count","canceled","canceled_count"]

    MLS_MED_KEYS = ["mls_median_price","median_list_price","median_sale_price_mls"]
    DEEDS_MED_KEYS = ["deeds_median_price","consideration_median","median_sale_price_deeds"]
    DEEDS_SAMPLES_KEYS = ["deeds_samples","consideration_median_samples","deeds_arms_length"]

    def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None or b is None or b == 0:
            return None
        return a / b

    rows_written = 0
    with open(args.out, "w", encoding="utf-8") as outf:
        for k in sorted(keys):
            z, b, w = k
            vrow = vel.get(k)
            arow = absr.get(k)
            lrow = liq.get(k)
            prow = pd.get(k)

            scan["have_velocity"] += 1 if vrow else 0
            scan["have_absorption"] += 1 if arow else 0
            scan["have_liquidity"] += 1 if lrow else 0
            scan["have_price_discovery"] += 1 if prow else 0

            bundle: Dict[str, Any] = {}

            velocity_rate = pick_metric(vrow or {}, VEL_RATE_KEYS)
            if velocity_rate is None:
                sales = pick_metric(vrow or {}, VEL_SALES_KEYS)
                stock = pick_metric(vrow or {}, STOCK_KEYS)
                if sales is not None and stock is not None and stock > 0 and w > 0:
                    velocity_rate = (sales / stock) * (365.0 / float(w))
            bundle["velocity"] = velocity_rate

            months_supply = pick_metric(arow or {}, MOS_KEYS)
            if months_supply is None:
                inv = pick_metric(arow or {}, INV_KEYS)
                sold = pick_metric(arow or {}, ["closed_sales","sold","sold_count","closed_count"])
                if inv is not None and sold is not None and sold > 0 and w > 0:
                    sold_per_month = sold / (float(w) / 30.0)
                    if sold_per_month > 0:
                        months_supply = inv / sold_per_month
            bundle["months_supply"] = months_supply

            dom_med = pick_metric(lrow or {}, DOM_KEYS)
            bundle["dom_median"] = dom_med

            active = pick_metric(lrow or {}, ACTIVE_KEYS)
            pending = pick_metric(lrow or {}, PENDING_KEYS)
            offm = pick_metric(lrow or {}, OFFMARKET_KEYS)
            withdrawn = pick_metric(lrow or {}, WITHDRAWN_KEYS)

            denom = None
            if active is not None or pending is not None or offm is not None or withdrawn is not None:
                denom = float((active or 0) + (pending or 0) + (offm or 0) + (withdrawn or 0))
                if denom <= 0:
                    denom = None

            bundle["pending_rate"] = safe_div(pending, denom) if denom else None
            bundle["off_market_rate"] = safe_div(offm, denom) if denom else None

            mls_med = pick_metric(prow or {}, MLS_MED_KEYS)
            deeds_med = pick_metric(prow or {}, DEEDS_MED_KEYS)
            dabs = None
            if mls_med is not None and deeds_med is not None and mls_med > 0:
                dabs = abs(deeds_med - mls_med) / mls_med
            bundle["pd_divergence_abs"] = dabs

            deeds_samples = pick_metric(prow or {}, DEEDS_SAMPLES_KEYS)
            bundle["deeds_samples"] = deeds_samples

            if dabs is not None and deeds_samples is not None and deeds_samples >= args.min_samples and dabs >= 0.12:
                scan["pd_divergence_strong"] += 1

            state, conf, drivers, expl = classify(bundle)

            out_row = {
                "schemaVersion": "market_radar_regime_p02_v0_1",
                "as_of_date": args.as_of,
                "zip": z,
                "asset_bucket": b,
                "window_days": w,
                "regime_state": state,
                "regime_confidence": round(conf, 4),
                "drivers": drivers,
                "explainability": {
                    "short": expl,
                    "signals": {
                        "velocity": bundle.get("velocity"),
                        "months_supply": bundle.get("months_supply"),
                        "dom_median": bundle.get("dom_median"),
                        "pending_rate": bundle.get("pending_rate"),
                        "off_market_rate": bundle.get("off_market_rate"),
                        "pd_divergence_abs": bundle.get("pd_divergence_abs"),
                        "deeds_samples": bundle.get("deeds_samples"),
                    },
                },
                "inputs_present": {
                    "velocity": bool(vrow),
                    "absorption": bool(arow),
                    "liquidity": bool(lrow),
                    "price_discovery": bool(prow),
                },
            }
            outf.write(json.dumps(out_row, ensure_ascii=False) + "\n")
            rows_written += 1

    audit = {
        "built_at": utc_now_z(),
        "as_of_date": args.as_of,
        "inputs": {
            "velocity": args.velocity,
            "absorption": args.absorption,
            "liquidity": args.liquidity,
            "price_discovery": args.price_discovery,
        },
        "config": {"min_samples": args.min_samples},
        "scan": scan,
        "output": {"out": args.out, "rows_written": rows_written, "sha256": sha256_file(args.out)},
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    print("[done] Regime P02 complete.")
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
