#!/usr/bin/env python
import argparse, json, os, sys, datetime

def jload(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def jdump(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    args = ap.parse_args()
    root = args.root

    reg_path = os.path.join(root, "governance", "engine_registry", "ENGINE_REGISTRY.json")
    tests_path = os.path.join(root, "governance", "engine_registry", "tests", "ACCEPTANCE_TESTS.json")

    if not os.path.exists(reg_path):
        print("[error] missing ENGINE_REGISTRY.json:", reg_path)
        return 2
    if not os.path.exists(tests_path):
        print("[error] missing ACCEPTANCE_TESTS.json:", tests_path)
        return 2

    reg = jload(reg_path)
    tests = jload(tests_path)

    reg.setdefault("engines", [])
    tests.setdefault("tests", [])

    # Add / upsert engine
    engine_id = "market_radar.runbook_probes_v0_1"
    engine = {
        "engine_id": engine_id,
        "name": "Market Radar Runbook Probes (v0_1)",
        "status": "active",
        "owner": "founder",
        "inputs": {
            "required_artifacts": [
                "publicData/marketRadar/indicators/CURRENT/CURRENT_MARKET_RADAR_INDICATORS_P01_MASS.ndjson",
                "publicData/marketRadar/CURRENT/CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson"
            ],
            "optional_artifacts": []
        },
        "gates": { "required": [] },
        "outputs": [],
        "acceptance_tests": [
            "test.market_radar.runbook.bucket_presence",
            "test.market_radar.runbook.mf5_safe_unknown",
            "test.market_radar.runbook.explainability_row_exists"
        ],
        "promotion_policy": {
            "allow_candidate_outputs": True,
            "promote_only_if_tests_pass": True
        },
        "meta": { "installed_at": datetime.datetime.now().isoformat(timespec="seconds") }
    }

    # upsert
    reg["engines"] = [e for e in reg["engines"] if e.get("engine_id") != engine_id]
    reg["engines"].append(engine)

    # Add / upsert tests (placeholders referencing the script)
    def upsert_test(tid, desc):
        tests["tests"] = [t for t in tests["tests"] if t.get("test_id") != tid]
        tests["tests"].append({
            "test_id": tid,
            "engine_id": engine_id,
            "type": "probe",
            "description": desc,
            "how_to_run": f"python scripts/market_radar/qa/runbook_probes_v0_1.py --root {root} --zip 01104 --assetBucket RES_1_4 --windowDays 30",
            "pass_conditions": []
        })

    upsert_test("test.market_radar.runbook.bucket_presence", "Runbook probe: indicator buckets exist for ZIP.")
    upsert_test("test.market_radar.runbook.mf5_safe_unknown", "Runbook probe: MF_5_PLUS emits UNKNOWN+UNSUPPORTED_BUCKET.")
    upsert_test("test.market_radar.runbook.explainability_row_exists", "Runbook probe: explainability row exists for zip/bucket/window.")

    jdump(reg_path, reg)
    jdump(tests_path, tests)
    print("[ok] registry updated:", engine_id)
    return 0

if __name__ == "__main__":
    sys.exit(main())
