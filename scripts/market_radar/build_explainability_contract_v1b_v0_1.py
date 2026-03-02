#!/usr/bin/env python3
import argparse, json, os, hashlib, datetime
from typing import Dict, Tuple, Any, Optional, Iterable

def ndjson_iter(path: str) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                # tolerate bad lines but don't crash
                continue

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def key_of(r: dict) -> Tuple[str,str,int]:
    z = str(r.get("zip") or r.get("ZIP") or "").zfill(5)
    b = (r.get("asset_bucket") or r.get("bucket") or r.get("property_bucket") or "ALL")
    try:
        w = int(r.get("window_days") or r.get("window") or 30)
    except Exception:
        w = 30
    return (z, str(b), w)

def now_utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def confidence_grade(data_sufficient: bool, sample_n: Optional[int]=None) -> str:
    if not data_sufficient:
        return "C"
    if sample_n is None:
        return "B"
    if sample_n >= 30:
        return "A"
    if sample_n >= 10:
        return "B"
    return "C"

def make_glossary_refs(packets: list) -> list:
    refs = set(["confidence","suppressed","unknown"])
    for p in packets:
        refs.add(p["pillar"])
    return sorted(refs)

def founder_guidance(packets: list, window_days: int) -> dict:
    """
    Internal-only: 'direction' = investigation prompts, not advice.
    """
    # Collect issue codes
    issues = []
    for p in packets:
        if not p["flags"].get("data_sufficient", False):
            issues.append(f"{p['pillar']}_suppressed")
    # Heuristics for checks (NOT recommendations)
    checks = []
    if "absorption_suppressed" in issues:
        checks += [
            "Verify MLS rollup contains closed_sales and active_inventory for this ZIP/bucket/window.",
            "Confirm MLS normalized listings CURRENT is up to date for the as_of date."
        ]
    if "price_discovery_suppressed" in issues:
        checks += [
            "Check sample counts (mls_samples, deeds_samples) vs min_samples for this window.",
            "If deeds are thin statewide, validate deed coverage ingestion for this county/period."
        ]
    # If liquidity computed but actives/pending are zero, it may be bucket mismatch or window mismatch.
    liq = next((p for p in packets if p["pillar"]=="liquidity"), None)
    if liq and liq["flags"].get("data_sufficient") and (liq["facts"].get("active")==0 and liq["facts"].get("pending")==0):
        checks += [
            "Possible bucket mismatch: try CONDO vs SFR vs MF buckets for this ZIP.",
            "Try alternate windows (30 vs 90 days) to ensure the rollup window aligns with your debug call."
        ]

    # Role lens messages: technical + layman + prompts
    def lens(title: str, technical: str, layman: str):
        return {"title": title, "technical": technical, "layman": layman}

    lenses = {
        "broker": lens(
            "Broker lens",
            f"Interpret signals as observed microstructure over {window_days}d; contradictions imply coverage or cohort shifts, not 'market magic'.",
            "Use this to sanity-check if listings are actually moving. If a card is suppressed, it usually means 'not enough recent data'."
        ),
        "investor": lens(
            "Investor lens",
            f"Prioritize exit liquidity stability; treat suppressed price discovery as insufficient evidence for pricing narratives in a {window_days}d window.",
            "First ask: can you sell here reliably? If we don’t have enough real sales, we won’t pretend we do."
        ),
        "homeowner": lens(
            "Homeowner lens",
            "Homeowner-facing copy must remain neutral; no prescriptive language.",
            "This is a snapshot of activity, not a recommendation."
        )
    }

    return {
        "notes": [
            "Founder guidance is internal-only and framed as investigation prompts, not advice.",
            "Always validate anomalies against raw rollups before acting on them."
        ],
        "role_lenses": lenses,
        "investigation_prompts": checks[:10]  # keep it tight
    }

def main():
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
    ap.add_argument("--founder_contract", default=None, help="Optional path to founder guidance contract JSON")
    args = ap.parse_args()

    built_at = now_utc()

    # Index source rows by (zip, bucket, window)
    idx: Dict[Tuple[str,str,int], Dict[str, Any]] = {}
    for pillar_name, path in [("velocity", args.velocity), ("absorption", args.absorption), ("liquidity", args.liquidity), ("price_discovery", args.price_discovery)]:
        for r in ndjson_iter(path):
            k = key_of(r)
            obj = idx.setdefault(k, {})
            obj[pillar_name] = r

    # Load founder contract (optional)
    founder_contract = None
    if args.founder_contract and os.path.exists(args.founder_contract):
        try:
            with open(args.founder_contract, "r", encoding="utf-8-sig") as f:
                founder_contract = json.load(f)
        except Exception:
            founder_contract = None

    rows_out = 0
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fout:
        for (zip5, bucket, window_days), obj in sorted(idx.items()):
            # Build packets
            packets = []

            # Absorption packet
            absr = obj.get("absorption") or {}
            closed_sales = absr.get("closed_sales")
            active_inventory = absr.get("active_inventory")
            pending = absr.get("pending")
            months_of_supply = absr.get("months_of_supply")
            abs_ok = (closed_sales is not None) and (active_inventory is not None)
            abs_conf = confidence_grade(abs_ok, sample_n=closed_sales if isinstance(closed_sales, int) else None)
            packets.append({
                "pillar": "absorption",
                "as_of": args.as_of,
                "window_days": window_days,
                "zip": zip5,
                "asset_bucket": bucket,
                "headline": "Absorption computed" if abs_ok else "Absorption unavailable (missing MLS counts)",
                "why": {
                    "pro": f"Absorption needs closed-sales and active-inventory counts for the last {window_days} days.",
                    "founder": "Absorption suppressed due to missing required MLS rollup fields." if not abs_ok else "Absorption computed from MLS closed_sales and active_inventory."
                },
                "facts": {
                    "closed_sales": closed_sales,
                    "active_inventory": active_inventory,
                    "pending": pending,
                    "months_of_supply": months_of_supply
                },
                "flags": {"data_sufficient": bool(abs_ok), "confidence": abs_conf}
            })

            # Liquidity packet
            liq = obj.get("liquidity") or {}
            active = liq.get("active")
            pend = liq.get("pending")
            withdrawn = liq.get("withdrawn")
            off_market = liq.get("off_market")
            dom_median = liq.get("dom_median") if liq.get("dom_median") is not None else liq.get("median_dom")
            liq_ok = dom_median is not None
            liq_conf = confidence_grade(liq_ok, sample_n=None)
            packets.append({
                "pillar": "liquidity",
                "as_of": args.as_of,
                "window_days": window_days,
                "zip": zip5,
                "asset_bucket": bucket,
                "headline": "Liquidity computed" if liq_ok else "Liquidity unavailable",
                "why": {
                    "pro": f"Listing lifecycle over the last {window_days} days — active: {active or 0}, pending: {pend or 0}, withdrawn: {withdrawn or 0}, off-market: {off_market or 0}, median DOM: {dom_median if dom_median is not None else 'n/a'}.",
                    "founder": "Liquidity summarizes lifecycle counts (active/pending/withdrawn/off-market) and DOM distribution (if present)."
                },
                "facts": {
                    "active": active,
                    "pending": pend,
                    "withdrawn": withdrawn,
                    "off_market": off_market,
                    "dom_median": dom_median
                },
                "flags": {"data_sufficient": bool(liq_ok), "confidence": liq_conf}
            })

            # Price discovery packet
            pd = obj.get("price_discovery") or {}
            mls_median_price = pd.get("mls_median_price")
            mls_samples = pd.get("mls_samples")
            deeds_median_price = pd.get("deeds_median_price")
            deeds_samples = pd.get("deeds_samples")
            divergence = pd.get("divergence_ratio_minus1")
            have_samples = (isinstance(mls_samples, int) and mls_samples >= args.min_samples) or (isinstance(deeds_samples, int) and deeds_samples >= args.min_samples)
            pd_conf = confidence_grade(have_samples, sample_n=max([s for s in [mls_samples, deeds_samples] if isinstance(s,int)] or [None]) if have_samples else None)
            packets.append({
                "pillar": "price_discovery",
                "as_of": args.as_of,
                "window_days": window_days,
                "zip": zip5,
                "asset_bucket": bucket,
                "headline": "Price discovery computed" if have_samples else "Price discovery unavailable (insufficient samples)",
                "why": {
                    "pro": f"Not enough observations in the last {window_days} days to summarize prices with confidence." if not have_samples else f"Price summaries computed from observed sales/listings over the last {window_days} days.",
                    "founder": f"Suppressed by min_samples={args.min_samples}. Deeds may be thin until statewide deed coverage is ingested." if not have_samples else "Price discovery computed when sample counts meet thresholds."
                },
                "facts": {
                    "mls_median_price": mls_median_price,
                    "mls_samples": mls_samples,
                    "deeds_median_price": deeds_median_price,
                    "deeds_samples": deeds_samples,
                    "divergence_ratio_minus1": divergence,
                    "min_samples": args.min_samples
                },
                "flags": {"data_sufficient": bool(have_samples), "confidence": pd_conf}
            })

            explain = {
                "schemaVersion": "market_radar_explainability_v1b",
                "as_of_date": args.as_of,
                "built_at": built_at,
                "zip": zip5,
                "asset_bucket": bucket,
                "window_days": window_days,
                "why": {
                    "pro": {
                        "absorption": packets[0]["why"]["pro"],
                        "liquidity": packets[1]["why"]["pro"],
                        "price_discovery": packets[2]["why"]["pro"]
                    },
                    "founder": {
                        "absorption": packets[0]["why"]["founder"],
                        "liquidity": packets[1]["why"]["founder"],
                        "price_discovery": packets[2]["why"]["founder"]
                    }
                },
                "packets": packets,
                "glossary_refs": make_glossary_refs(packets),
                "founder_guidance": founder_guidance(packets, window_days),
                "contracts": {
                    "founder_guidance_b1": {
                        "contract_id": "B1",
                        "version": "v0_1",
                        "path": args.founder_contract if args.founder_contract else None,
                        "schemaVersion": founder_contract.get("schemaVersion") if isinstance(founder_contract, dict) else None
                    }
                }
            }

            fout.write(json.dumps(explain, ensure_ascii=False) + "\n")
            rows_out += 1

    sha = sha256_file(args.out)
    sha_json_path = args.out + ".sha256.json"
    with open(sha_json_path, "w", encoding="utf-8") as f:
        json.dump({"path": args.out, "sha256": sha, "built_at_utc": built_at}, f, indent=2)

    audit = {
        "built_at": built_at,
        "as_of_date": args.as_of,
        "schemaVersion": "market_radar_explainability_v1b",
        "inputs": {
            "velocity": args.velocity,
            "absorption": args.absorption,
            "liquidity": args.liquidity,
            "price_discovery": args.price_discovery,
            "founder_contract": args.founder_contract
        },
        "config": {"min_samples": args.min_samples, "min_stock": args.min_stock},
        "rows_written": rows_out,
        "sha256": sha,
        "out": args.out,
        "sha256_json": sha_json_path
    }
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps({"ok": True, "rows_written": rows_out, "out": os.path.abspath(args.out), "sha256": sha, "sha256_json": os.path.abspath(sha_json_path)}, indent=2))

if __name__ == "__main__":
    main()
