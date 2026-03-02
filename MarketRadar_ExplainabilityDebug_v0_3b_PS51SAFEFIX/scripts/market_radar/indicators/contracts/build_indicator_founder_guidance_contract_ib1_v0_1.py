#!/usr/bin/env python3
"""Build Indicator Founder Guidance Contract IB1 v0_1

This produces an internal-only contract that the founder/debug tools can read to provide:
- role-lens interpretations (broker/investor/homeowner/operator)
- investigation prompts when indicators are UNKNOWN or contradictory
- plain-English + technical phrasing guidance
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
from typing import Any, Dict


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    root = args.root
    as_of = str(args.as_of).strip()
    if not args.out:
        out = os.path.join(
            root,
            "publicData",
            "marketRadar",
            "indicators",
            "contracts",
            f"indicator_founder_guidance_contract__ib1__v0_1_ASOF{as_of}.json",
        )
    else:
        out = os.path.join(root, args.out) if not os.path.isabs(args.out) else args.out

    os.makedirs(os.path.dirname(out), exist_ok=True)

    contract: Dict[str, Any] = {
        "schemaVersion": "market_radar_indicator_founder_guidance_contract_ib1",
        "contract_id": "IB1",
        "version": "v0_1",
        "as_of_date": as_of,
        "built_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "path": out,
        "guidance": {
            "notes": [
                "Internal-only. Framed as investigation prompts, not advice.",
                "UNKNOWN indicators are first-class missing states (not zero).",
                "Always validate anomalies against raw rollups (deeds/MLS/stock) before acting."
            ],
            "hover_terms": {
                "UNKNOWN": "We didn't compute this because the evidence is thin or required fields are missing.",
                "INSUFFICIENT_SAMPLES": "Not enough recent observations for this ZIP/bucket/window to compute safely.",
                "MISSING_METRIC": "A required upstream metric (pillar) is missing for this ZIP/bucket/window."
            },
            "role_lenses": {
                "broker": {
                    "technical": "Treat indicators as microstructure diagnostics; focus on where coverage is strong and contradictions are explainable.",
                    "layman": "Use indicators to spot ZIPs that are heating up or acting weird, then verify with raw listings and comps."
                },
                "investor": {
                    "technical": "Prioritize exit liquidity and price confidence; suppress narratives when discovery is thin.",
                    "layman": "If we can't confidently see sales/prices, don't underwrite a tight exit—use bigger margins or skip."
                },
                "homeowner": {
                    "technical": "Homeowner-facing copy remains neutral and non-prescriptive.",
                    "layman": "This is a market snapshot, not a recommendation."
                },
                "operator": {
                    "technical": "Use UNKNOWN/suppressed patterns as QA signals for pipeline coverage gaps.",
                    "layman": "If a lot of ZIPs are UNKNOWN, it's usually a data pipeline issue—not the market."
                }
            },
            "investigation_prompts": [
                "Confirm stock_parcels > 0 for this ZIP and bucket mapping is correct.",
                "Check deeds_arms_length counts for the requested window and ensure arms-length classification is running for the period.",
                "Verify MLS closed counts exist and that listing statuses are normalized correctly for the window.",
                "If divergence/off-market proxy is high, sample raw deeds vs MLS closes and check timing (recording lag).",
                "If absorption acceleration is UNKNOWN, verify months_of_supply or closed/inventory rollups exist for both windows.",
                "If liquidity stability is UNKNOWN, verify DOM metrics exist and that outliers are handled as expected."
            ],
            "copy_templates": {
                "tbi_transaction_breadth": {
                    "technical": "Transaction Breadth Index (TBI) approximates turnover normalized by parcel stock.",
                    "layman": "How much of the ZIP is actually changing hands recently (scaled)."
                },
                "divergence_deeds_mls": {
                    "technical": "Deed–MLS Divergence contrasts recorded transfers vs MLS-closed activity over the same window.",
                    "layman": "Are we seeing more transfers in deeds than the MLS shows (or vice versa)?"
                },
                "momentum_absorption_accel": {
                    "technical": "Absorption Acceleration compares clearing speed between two windows (e.g., 90d vs 180d).",
                    "layman": "Is the market clearing faster or slower than it was recently?"
                },
                "volatility_liquidity_stability": {
                    "technical": "Liquidity Stability summarizes how consistent time-to-sell dynamics are across windows.",
                    "layman": "Is selling speed stable, or does it whipsaw?"
                },
                "rotation_capital_pressure": {
                    "technical": "Rotation Capital Pressure is a within-ZIP acceleration proxy; adjacency flows come later.",
                    "layman": "Is activity speeding up inside this ZIP?"
                },
                "off_market_participation": {
                    "technical": "Off-Market Participation is a proxy comparing deeds to MLS closes; not a perfect matcher.",
                    "layman": "Roughly how much activity might be happening outside MLS closes."
                }
            }
        }
    }

    with open(out, "w", encoding="utf-8") as f:
        json.dump(contract, f, indent=2, sort_keys=False)

    sha_path = out + ".sha256.json"
    import hashlib

    with open(out, "rb") as rf:
        h = hashlib.sha256(rf.read()).hexdigest()
    with open(sha_path, "w", encoding="utf-8") as sf:
        json.dump({"path": out, "sha256": h, "as_of_date": as_of, "built_at": contract["built_at"]}, sf, indent=2)

    print(json.dumps({"ok": True, "out": out, "sha256_json": sha_path}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
