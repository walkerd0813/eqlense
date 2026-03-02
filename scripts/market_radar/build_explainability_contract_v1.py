#!/usr/bin/env python3
"""Market Radar Explainability Contract V1

Goal
- Produce a *non-advisory*, evidence-first explanation packet for each ZIP x asset_bucket x window_days key.
- Outputs are safe for: founder/internal, pro UI "Why?" popovers, and downstream engines.

Design rules (locked)
- Explainability is observed behavior + time window + comparison (when available).
- Always cite counts/values when present.
- Never predict, recommend, or give strategy.
- Every packet carries data_sufficiency + confidence flags.

Inputs
- P01 pillar CURRENT ndjsons (velocity, absorption, liquidity, price_discovery)

Output
- NDJSON with one row per key.

This script is intentionally tolerant to schema drift: it looks for a small set of common metric names,
falls back to generic "data unavailable" packets, and never crashes on missing fields.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
from typing import Any, Dict, Iterable, Optional, Tuple


def utc_now_iso() -> str:
    # Python 3.14 deprecates utcnow(); use timezone-aware
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def ndjson_iter(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def write_ndjson(path: str, rows: Iterable[Dict[str, Any]]) -> int:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            n += 1
    return n


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def is_zip(z: Any) -> bool:
    return isinstance(z, str) and len(z) == 5 and z.isdigit() and z != "00000"


def key_of(r: Dict[str, Any]) -> Optional[Tuple[str, str, int]]:
    z = r.get("zip")
    b = r.get("asset_bucket") or r.get("bucket")
    w = r.get("window_days")
    if not is_zip(z):
        return None
    if not isinstance(b, str) or not b:
        return None
    if not isinstance(w, int):
        return None
    return (z, b, w)


def pick(metrics: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
    for k in keys:
        if k in metrics and metrics[k] is not None:
            return metrics[k]
    return None


def num(v: Any) -> Optional[float]:
    if isinstance(v, (int, float)):
        return float(v)
    return None


def make_packet(
    *,
    pillar: str,
    as_of: str,
    window_days: int,
    zip_code: str,
    asset_bucket: str,
    facts: Dict[str, Any],
    headline: str,
    why_pro: str,
    why_founder: str,
    data_sufficient: bool,
    confidence: str,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    pkt = {
        "pillar": pillar,
        "as_of": as_of,
        "window_days": window_days,
        "zip": zip_code,
        "asset_bucket": asset_bucket,
        "headline": headline,
        "why": {
            "pro": why_pro,
            "founder": why_founder,
        },
        "facts": facts,
        "flags": {
            "data_sufficient": bool(data_sufficient),
            "confidence": confidence,  # A/B/C (A=high)
        },
    }
    if notes:
        pkt["notes"] = notes
    return pkt


def velocity_packet(row: Dict[str, Any], as_of: str, min_stock: int = 30) -> Dict[str, Any]:
    m = (row.get("metrics") or {})
    z, b, w = row["zip"], row.get("asset_bucket") or row.get("bucket"), row["window_days"]

    deeds_al = pick(m, ["deeds_arms_length", "arms_length_deeds", "arms_length_count", "deeds_al"])
    stock = pick(m, ["stock", "stock_count", "parcel_stock", "universe_stock"])
    vel = pick(m, ["velocity_p01", "velocity", "turnover_rate", "turnover_annualized"])

    deeds_al_n = int(deeds_al) if isinstance(deeds_al, int) else None
    stock_n = int(stock) if isinstance(stock, int) else None
    vel_n = num(vel)

    data_ok = (deeds_al_n is not None) and (stock_n is not None) and (stock_n >= min_stock)

    facts = {
        "deeds_arms_length": deeds_al_n,
        "stock": stock_n,
        "velocity_value": vel_n,
        "min_stock": min_stock,
    }

    if not data_ok:
        return make_packet(
            pillar="velocity",
            as_of=as_of,
            window_days=w,
            zip_code=z,
            asset_bucket=b,
            facts=facts,
            headline="Velocity unavailable (insufficient stock or counts)",
            why_pro=f"Not enough parcel stock in the last {w} days to compute a reliable velocity signal.",
            why_founder=f"Velocity suppressed due to data sufficiency gates (stock >= {min_stock} required).",
            data_sufficient=False,
            confidence="C",
        )

    # Non-advisory explanation: observed turnover pace.
    headline = "Velocity computed"
    why_pro = (
        f"In the last {w} days, {deeds_al_n} arms-length transfers occurred out of a stock of {stock_n} parcels."
    )
    why_founder = (
        f"Velocity uses arms-length deed count normalized by parcel stock (stock gate={min_stock})."
    )

    return make_packet(
        pillar="velocity",
        as_of=as_of,
        window_days=w,
        zip_code=z,
        asset_bucket=b,
        facts=facts,
        headline=headline,
        why_pro=why_pro,
        why_founder=why_founder,
        data_sufficient=True,
        confidence="B" if deeds_al_n >= 10 else "C",
    )


def absorption_packet(row: Dict[str, Any], as_of: str) -> Dict[str, Any]:
    m = (row.get("metrics") or {})
    z, b, w = row["zip"], row.get("asset_bucket") or row.get("bucket"), row["window_days"]

    closed = pick(m, ["closed_sales", "closed", "sold", "mls_closed"])
    inv = pick(m, ["inventory_active", "inventory", "active_listings"])
    months = pick(m, ["months_of_supply", "mos", "months_supply"])
    pend = pick(m, ["pending", "pending_listings", "pendings"])

    closed_n = int(closed) if isinstance(closed, int) else None
    inv_n = int(inv) if isinstance(inv, int) else None
    pend_n = int(pend) if isinstance(pend, int) else None
    months_n = num(months)

    data_ok = closed_n is not None and inv_n is not None

    facts = {
        "closed_sales": closed_n,
        "active_inventory": inv_n,
        "pending": pend_n,
        "months_of_supply": months_n,
    }

    if not data_ok:
        return make_packet(
            pillar="absorption",
            as_of=as_of,
            window_days=w,
            zip_code=z,
            asset_bucket=b,
            facts=facts,
            headline="Absorption unavailable (missing MLS counts)",
            why_pro=f"Absorption needs closed-sales and active-inventory counts for the last {w} days.",
            why_founder="Absorption suppressed due to missing required MLS rollup fields.",
            data_sufficient=False,
            confidence="C",
        )

    headline = "Absorption computed"
    why_pro = f"In the last {w} days, {closed_n} listings closed against {inv_n} active listings."
    if pend_n is not None:
        why_pro += f" Pending listings: {pend_n}."
    why_founder = "Absorption is MLS-based clearing speed using closed vs active inventory (and optional pendings)."

    return make_packet(
        pillar="absorption",
        as_of=as_of,
        window_days=w,
        zip_code=z,
        asset_bucket=b,
        facts=facts,
        headline=headline,
        why_pro=why_pro,
        why_founder=why_founder,
        data_sufficient=True,
        confidence="B" if closed_n >= 10 else "C",
    )


def liquidity_packet(row: Dict[str, Any], as_of: str) -> Dict[str, Any]:
    m = (row.get("metrics") or {})
    z, b, w = row["zip"], row.get("asset_bucket") or row.get("bucket"), row["window_days"]

    # counts
    active = pick(m, ["active", "active_count", "mls_active"])
    pending = pick(m, ["pending", "pending_count", "mls_pending"])
    withdrawn = pick(m, ["withdrawn", "withdrawn_count", "mls_withdrawn"])
    off_market = pick(m, ["off_market", "off_market_count", "mls_off_market"])
    dom_med = pick(m, ["dom_median", "days_on_market_median", "median_dom"])

    a_n = int(active) if isinstance(active, int) else None
    p_n = int(pending) if isinstance(pending, int) else None
    w_n = int(withdrawn) if isinstance(withdrawn, int) else None
    o_n = int(off_market) if isinstance(off_market, int) else None
    dom_n = num(dom_med)

    # Liquidity is usually broader than absorption; allow partials
    data_ok = (a_n is not None) or (p_n is not None) or (dom_n is not None)

    facts = {
        "active": a_n,
        "pending": p_n,
        "withdrawn": w_n,
        "off_market": o_n,
        "dom_median": dom_n,
    }

    if not data_ok:
        return make_packet(
            pillar="liquidity",
            as_of=as_of,
            window_days=w,
            zip_code=z,
            asset_bucket=b,
            facts=facts,
            headline="Liquidity unavailable (missing lifecycle/DOM fields)",
            why_pro=f"Liquidity needs listing lifecycle counts and/or days-on-market for the last {w} days.",
            why_founder="Liquidity suppressed due to missing required fields.",
            data_sufficient=False,
            confidence="C",
        )

    headline = "Liquidity computed"
    parts = []
    if a_n is not None:
        parts.append(f"active: {a_n}")
    if p_n is not None:
        parts.append(f"pending: {p_n}")
    if w_n is not None:
        parts.append(f"withdrawn: {w_n}")
    if o_n is not None:
        parts.append(f"off-market: {o_n}")
    if dom_n is not None:
        parts.append(f"median DOM: {int(dom_n)}")

    why_pro = f"Listing lifecycle over the last {w} days — " + ", ".join(parts) + "."
    why_founder = "Liquidity summarizes lifecycle counts (active/pending/withdrawn/off-market) and DOM distribution (if present)."

    # confidence based on completeness
    conf = "B" if (a_n is not None and p_n is not None and dom_n is not None) else "C"

    return make_packet(
        pillar="liquidity",
        as_of=as_of,
        window_days=w,
        zip_code=z,
        asset_bucket=b,
        facts=facts,
        headline=headline,
        why_pro=why_pro,
        why_founder=why_founder,
        data_sufficient=True,
        confidence=conf,
    )


def price_discovery_packet(row: Dict[str, Any], as_of: str, min_samples: int = 10) -> Dict[str, Any]:
    m = (row.get("metrics") or {})
    z, b, w = row["zip"], row.get("asset_bucket") or row.get("bucket"), row["window_days"]

    # These names are intentionally flexible; your unified carries MLS+deeds fields.
    mls_med = pick(m, ["mls_price_median", "mls_median_price", "mls_median"])
    deeds_med = pick(m, ["deeds_price_median", "consideration_median", "deeds_median_price"])
    mls_n = pick(m, ["mls_samples", "mls_count", "closed_sales"])
    deeds_n = pick(m, ["deeds_samples", "consideration_median_samples", "deeds_arms_length"])

    mls_med_n = num(mls_med)
    deeds_med_n = num(deeds_med)
    mls_n_i = int(mls_n) if isinstance(mls_n, int) else None
    deeds_n_i = int(deeds_n) if isinstance(deeds_n, int) else None

    data_ok = (mls_med_n is not None and mls_n_i is not None and mls_n_i >= min_samples) or (
        deeds_med_n is not None and deeds_n_i is not None and deeds_n_i >= min_samples
    )

    # Divergence (diagnostic only): deeds vs MLS
    divergence = None
    if mls_med_n and deeds_med_n and mls_med_n > 0:
        divergence = (deeds_med_n / mls_med_n) - 1.0

    facts = {
        "mls_median_price": mls_med_n,
        "mls_samples": mls_n_i,
        "deeds_median_price": deeds_med_n,
        "deeds_samples": deeds_n_i,
        "divergence_ratio_minus1": divergence,
        "min_samples": min_samples,
    }

    if not data_ok:
        return make_packet(
            pillar="price_discovery",
            as_of=as_of,
            window_days=w,
            zip_code=z,
            asset_bucket=b,
            facts=facts,
            headline="Price discovery unavailable (insufficient samples)",
            why_pro=f"Not enough observations in the last {w} days to summarize prices with confidence.",
            why_founder=f"Suppressed by min_samples={min_samples}. Deeds may be thin until statewide deed coverage is ingested.",
            data_sufficient=False,
            confidence="C",
        )

    headline = "Price discovery computed"
    why_parts = []
    if mls_med_n is not None and mls_n_i is not None:
        why_parts.append(f"MLS median: ${int(mls_med_n):,} (n={mls_n_i})")
    if deeds_med_n is not None and deeds_n_i is not None:
        why_parts.append(f"Deeds median: ${int(deeds_med_n):,} (n={deeds_n_i})")

    why_pro = f"Observed prices over the last {w} days — " + "; ".join(why_parts) + "."
    why_founder = "Price discovery is descriptive: medians + sample counts; deeds-vs-MLS divergence is diagnostic only (not surfaced by default)."

    conf = "B" if ((mls_n_i or 0) >= 25 or (deeds_n_i or 0) >= 25) else "C"

    return make_packet(
        pillar="price_discovery",
        as_of=as_of,
        window_days=w,
        zip_code=z,
        asset_bucket=b,
        facts=facts,
        headline=headline,
        why_pro=why_pro,
        why_founder=why_founder,
        data_sufficient=True,
        confidence=conf,
    )


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
    ap.add_argument("--min_stock", type=int, default=30)
    args = ap.parse_args()

    built_at = utc_now_iso()

    # Load each pillar into key->row maps
    maps: Dict[str, Dict[Tuple[str, str, int], Dict[str, Any]]] = {
        "velocity": {},
        "absorption": {},
        "liquidity": {},
        "price_discovery": {},
    }

    def load_into(pillar: str, path: str) -> int:
        n = 0
        for r in ndjson_iter(path):
            k = key_of(r)
            if not k:
                continue
            maps[pillar][k] = r
            n += 1
        return n

    rows_vel = load_into("velocity", args.velocity)
    rows_abs = load_into("absorption", args.absorption)
    rows_liq = load_into("liquidity", args.liquidity)
    rows_pd = load_into("price_discovery", args.price_discovery)

    # Union of keys
    keys = set(maps["velocity"].keys()) | set(maps["absorption"].keys()) | set(maps["liquidity"].keys()) | set(
        maps["price_discovery"].keys()
    )

    out_rows = []
    for (z, b, w) in sorted(keys):
        packets = []
        if (z, b, w) in maps["velocity"]:
            packets.append(velocity_packet(maps["velocity"][(z, b, w)], args.as_of, min_stock=args.min_stock))
        if (z, b, w) in maps["absorption"]:
            packets.append(absorption_packet(maps["absorption"][(z, b, w)], args.as_of))
        if (z, b, w) in maps["liquidity"]:
            packets.append(liquidity_packet(maps["liquidity"][(z, b, w)], args.as_of))
        if (z, b, w) in maps["price_discovery"]:
            packets.append(price_discovery_packet(maps["price_discovery"][(z, b, w)], args.as_of, min_samples=args.min_samples))

        # Provide a compact "why" surface for pro UI later: one-liners per pillar
        pro_why = {p["pillar"]: p["why"]["pro"] for p in packets}
        founder_why = {p["pillar"]: p["why"]["founder"] for p in packets}

        out_rows.append(
            {
                "schemaVersion": "market_radar_explainability_v1",
                "as_of_date": args.as_of,
                "built_at": built_at,
                "zip": z,
                "asset_bucket": b,
                "window_days": w,
                "why": {
                    "pro": pro_why,
                    "founder": founder_why,
                },
                "packets": packets,
            }
        )

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    wrote = write_ndjson(args.out, out_rows)

    audit_obj = {
        "built_at": built_at,
        "as_of_date": args.as_of,
        "inputs": {
            "velocity": args.velocity,
            "absorption": args.absorption,
            "liquidity": args.liquidity,
            "price_discovery": args.price_discovery,
        },
        "config": {"min_samples": args.min_samples, "min_stock": args.min_stock},
        "rows_in": {
            "velocity": rows_vel,
            "absorption": rows_abs,
            "liquidity": rows_liq,
            "price_discovery": rows_pd,
        },
        "keys_out": len(out_rows),
        "sha256": sha256_file(args.out),
        "out": args.out,
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit_obj, f, indent=2)

    print("[done] Explainability Contract V1 built")
    print(json.dumps(audit_obj, indent=2))


if __name__ == "__main__":
    main()
