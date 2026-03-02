#!/usr/bin/env python3
import argparse, json, os, datetime, hashlib

SCHEMA_VERSION = "market_radar_indicator_founder_guidance_contract_ib1"
CONTRACT_ID = "IB1"
ENGINE_VERSION = "v0_1"

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def utc_now_iso() -> str:
    # Python 3.14 deprecates utcnow; use timezone-aware UTC.
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00","Z")

def build_contract(as_of: str) -> dict:
    # This is INTERNAL ONLY: investigation prompts and vocabulary to help the founder debug & learn.
    # Not consumer advice.
    glossary = {
        "tbi_transaction_breadth": {
            "technical": "Transaction Breadth Index (TBI): arms-length deed transfers normalized by parcel stock over the window.",
            "layman": "How much of the ZIP actually changed hands (normalized).",
            "hover": "High = lots of different properties sold. Low = very little turnover."
        },
        "divergence_deeds_mls": {
            "technical": "Deed–MLS Divergence: bounded difference between deed transfers and MLS closed volume (proxy).",
            "layman": "How much sale activity might be happening off-MLS vs showing up in MLS closes (proxy).",
            "hover": "High divergence can mean private sales, coverage gaps, or timing differences."
        },
        "momentum_absorption_accel": {
            "technical": "Absorption Acceleration: change in absorption rate between two horizons (e.g., 90 vs 180 days), clipped to [-1,1].",
            "layman": "Are homes starting to sell faster or slower than before?",
            "hover": "Positive = speeding up. Negative = slowing down."
        },
        "volatility_liquidity_stability": {
            "technical": "Liquidity Stability: stability of DOM / lifecycle conversion across windows (30/90/180), scaled 0–1.",
            "layman": "Is the market behaving consistently, or whipping around week to week?",
            "hover": "High stability = predictable sell times. Low = stop-and-go."
        },
        "rotation_capital_pressure": {
            "technical": "Rotation Capital Pressure (proxy): within-ZIP deed acceleration over time, clipped [-1,1].",
            "layman": "Is transaction activity heating up compared to the recent past?",
            "hover": "Proxy only (true rotation needs adjacency/neighbor graph)."
        },
        "off_market_participation": {
            "technical": "Off-Market Participation (proxy): share of deed transfers not mirrored by MLS closed volume (bounded 0–1).",
            "layman": "How much selling might be happening outside MLS (proxy).",
            "hover": "Proxy; can also reflect MLS definition mismatches or lag."
        },
        "unknown": {
            "technical": "UNKNOWN is a first-class state: missing/insufficient evidence; the system did not infer.",
            "layman": "We don't know yet, and we won’t pretend we do.",
            "hover": "Unknown is safer than wrong."
        },
        "suppressed": {
            "technical": "Suppressed means intentionally withheld because fields/samples fail the contract gate.",
            "layman": "We chose not to show it because the data isn't strong enough.",
            "hover": "Suppressed = not guessed."
        },
        "confidence": {
            "technical": "Confidence grade (A/B/C) derived from data sufficiency and coverage integrity.",
            "layman": "How much you should trust this indicator right now.",
            "hover": "A = strong evidence, B = usable, C = thin/partial."
        }
    }

    role_lenses = {
        "broker": {
            "title": "Broker lens",
            "technical": "Treat indicators as microstructure proxies. UNKNOWN states usually mean cohort coverage gaps or window mismatch — validate pipelines before interpreting market narratives.",
            "layman": "If an indicator is unknown, it usually means we don’t have enough recent data — check the input counts first."
        },
        "investor": {
            "title": "Investor lens",
            "technical": "Prioritize exit liquidity. Use stability + absorption accel to assess timing risk; treat divergence/off-market proxies as hypothesis generators only until deed/MLS alignment improves.",
            "layman": "First ask: can you sell reliably here? If not enough evidence, don’t build a story."
        },
        "homeowner": {
            "title": "Homeowner lens",
            "technical": "Homeowner-facing messaging must remain neutral; do not convert indicators into prescriptive claims.",
            "layman": "This is activity context, not a recommendation."
        }
    }

    investigation_prompts = [
        "Check indicator inputs_snapshot: deeds_arms_length, stock_parcels, mls_closed. If any are zero or null, trace back to CURRENT rollups.",
        "Confirm bucket mapping (SFR/CONDO/MF/LAND) is consistent across pillars and indicators.",
        "If divergence/off-market are UNKNOWN, validate deeds coverage (county/time) and MLS closed definition used in rollups.",
        "If absorption acceleration is missing, verify months_of_supply or closed/inventory fields exist for both horizons.",
        "If stability is UNKNOWN, verify DOM metrics exist and window selection is correct."
    ]

    return {
        "schemaVersion": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "engine_version": ENGINE_VERSION,
        "as_of_date": as_of,
        "built_at": utc_now_iso(),
        "notes": [
            "Internal-only founder guidance. Investigation prompts, not advice.",
            "Indicators are proxies; treat them as hypotheses until corroborated by rollups."
        ],
        "role_lenses": role_lenses,
        "glossary": glossary,
        "indicators": {
            "tbi_transaction_breadth": {"label":"Transaction Breadth Index (TBI)", "range":"0..1", "unknown_gates":["min_deeds","min_stock"]},
            "divergence_deeds_mls": {"label":"Deed–MLS Divergence (bounded)", "range":"-1..1", "unknown_gates":["min_deeds","min_mls_closed"]},
            "momentum_absorption_accel": {"label":"Absorption Acceleration", "range":"-1..1", "unknown_gates":["missing_horizon_metrics"]},
            "volatility_liquidity_stability": {"label":"Liquidity Stability", "range":"0..1", "unknown_gates":["dom_missing","min_samples"]},
            "rotation_capital_pressure": {"label":"Rotation Capital Pressure (proxy)", "range":"-1..1", "unknown_gates":["min_deeds","baseline_missing"]},
            "off_market_participation": {"label":"Off-Market Participation (proxy)", "range":"0..1", "unknown_gates":["min_deeds"]}
        },
        "investigation_prompts": investigation_prompts
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    root = args.root
    as_of = args.as_of

    out = args.out
    if not out:
        outdir = os.path.join(root, "publicData", "marketRadar", "indicators", "contracts")
        os.makedirs(outdir, exist_ok=True)
        out = os.path.join(outdir, f"indicator_founder_guidance_contract__ib1__v0_1_ASOF{as_of}.json")

    obj = build_contract(as_of)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

    sha_path = out + ".sha256.json"
    sha_obj = {"path": out, "sha256": sha256_file(out), "built_at": utc_now_iso(), "schemaVersion": "sha256_manifest_v1"}
    with open(sha_path, "w", encoding="utf-8") as f:
        json.dump(sha_obj, f, ensure_ascii=False, indent=2)

    print(json.dumps({"ok": True, "out": out, "sha256_json": sha_path}))

if __name__ == "__main__":
    main()
