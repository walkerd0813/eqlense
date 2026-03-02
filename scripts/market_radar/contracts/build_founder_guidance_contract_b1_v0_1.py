#!/usr/bin/env python3
import argparse, json, os, hashlib, datetime

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output JSON path")
    ap.add_argument("--as_of", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    built_at = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

    contract = {
        "schemaVersion": "market_radar_founder_guidance_contract_b1",
        "contract_id": "B1",
        "version": "v0_1",
        "as_of_date": args.as_of,
        "built_at_utc": built_at,
        "glossary": {
            "absorption": {
                "technical": "Absorption measures how quickly inventory clears (sales relative to active supply) over a time window.",
                "layman": "How fast homes are getting bought compared to how many are for sale.",
                "hover": "High absorption = homes are selling fast. Low absorption = homes are sitting longer."
            },
            "liquidity": {
                "technical": "Liquidity reflects the reliability and stability of transaction flow and listing lifecycle conversion.",
                "layman": "How easy it is to sell here without weird dry spells.",
                "hover": "Stable liquidity = consistent sales activity. Unstable liquidity = stop‑and‑go market."
            },
            "price_discovery": {
                "technical": "Price discovery summarizes observed prices (MLS and/or deeds) when sample counts are sufficient.",
                "layman": "Whether we have enough real sales to confidently summarize prices.",
                "hover": "If samples are thin, we avoid claiming a price story."
            },
            "suppressed": {
                "technical": "Suppressed means the metric is intentionally withheld due to missing fields or insufficient sample size.",
                "layman": "We’re choosing not to show it because the data isn’t strong enough.",
                "hover": "Suppressed = not guessed."
            },
            "confidence": {
                "technical": "Confidence is a qualitative grade (A/B/C) derived from data sufficiency and coverage integrity.",
                "layman": "How much you should trust this number for this ZIP right now.",
                "hover": "A = strong evidence, B = usable, C = thin/partial."
            },
            "unknown": {
                "technical": "Unknown indicates a first‑class missing state; the system did not infer or guess.",
                "layman": "We don’t know yet, and we won’t pretend we do.",
                "hover": "Unknown is safer than wrong."
            }
        },
        "role_lenses": {
            "broker": {
                "technical": [
                    "Treat these as observed microstructure signals over the stated window.",
                    "Use contradictions (e.g., high velocity but suppressed price discovery) as prompts to inspect data coverage and cohort mix."
                ],
                "layman": [
                    "This helps you tell if buyers are active and whether listings are really moving.",
                    "If a signal looks weird, it may be because the ZIP doesn’t have enough recent transactions in our data yet."
                ]
            },
            "investor": {
                "technical": [
                    "Focus on exit liquidity stability and off‑market share (when present) before trusting any single price summary.",
                    "Use data sufficiency flags to avoid over‑weighting thin windows."
                ],
                "layman": [
                    "First question: can you reliably sell here? That’s liquidity.",
                    "If we say 'suppressed', it means there aren’t enough real sales in the data to be confident."
                ]
            },
            "homeowner": {
                "technical": [
                    "Homeowner‑facing messaging must remain neutral and non‑prescriptive."
                ],
                "layman": [
                    "This is a snapshot of market activity — not a recommendation."
                ]
            }
        },
        "guardrails": {
            "no_predictions": True,
            "no_recommendations": True,
            "observed_behavior_only": True,
            "always_include_window": True,
            "always_include_counts_when_available": True
        }
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(contract, f, ensure_ascii=False, indent=2)

    sha_path = args.out + ".sha256.json"
    sha = sha256_file(args.out)
    with open(sha_path, "w", encoding="utf-8") as f:
        json.dump({"path": args.out, "sha256": sha, "built_at_utc": built_at}, f, indent=2)

    print(json.dumps({"ok": True, "out": os.path.abspath(args.out), "sha256_json": os.path.abspath(sha_path)}, indent=2))

if __name__ == "__main__":
    main()
