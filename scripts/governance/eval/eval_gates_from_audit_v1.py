import json, argparse, datetime, os, sys

def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def read_json(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def append_ndjson(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gates", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--gate_outcomes", required=True)
    ap.add_argument("--engine_id", required=True)
    ap.add_argument("--run_id", required=True)
    args = ap.parse_args()

    gates = read_json(args.gates)
    audit = read_json(args.audit)

    ts = utc_now()
    gate_set_id = gates.get("gate_set_id") or "UNKNOWN_GATESET"
    failures = 0

    # Minimal evaluator for the ArcGIS attach gates you defined:
    # ARC_HIT_NO_SPINE_MATCH_LOW: (arc_hit_no_spine_match / arc_hit) <= 0.02 when arc_hit>=50
    arc_hit = audit.get("arc_hit")
    arc_no_spine = audit.get("arc_hit_no_spine_match")
    outcome = "SKIP"
    detail = "missing_metrics"
    rate = None
    if isinstance(arc_hit, (int,float)) and isinstance(arc_no_spine, (int,float)) and arc_hit >= 50:
        rate = (arc_no_spine / arc_hit) if arc_hit else 0.0
        outcome = "PASS" if rate <= 0.02 else "FAIL"
        detail = f"rate={rate:.6f} no_spine={arc_no_spine} arc_hit={arc_hit} max=0.02"
        if outcome == "FAIL":
            failures += 1

    append_ndjson(args.gate_outcomes, {
        "ts_utc": ts,
        "gate_set_id": gate_set_id,
        "gate_id": "ARC_HIT_NO_SPINE_MATCH_LOW",
        "run_id": args.run_id,
        "engine_id": args.engine_id,
        "outcome": outcome,
        "detail": detail,
        "metrics": {"arc_hit": arc_hit, "arc_hit_no_spine_match": arc_no_spine, "rate": rate}
    })

    # For now, we record the other two as informational (enforced inside engine):
    for gid in ("SCOPE_ONLY_UNKNOWN", "EVIDENCE_REQUIRED"):
        append_ndjson(args.gate_outcomes, {
            "ts_utc": ts,
            "gate_set_id": gate_set_id,
            "gate_id": gid,
            "run_id": args.run_id,
            "engine_id": args.engine_id,
            "outcome": "INFO",
            "detail": "Enforced inside engine; not evaluated from audit-only gate evaluator.",
            "metrics": {}
        })

    if failures:
        sys.exit(2)

if __name__ == "__main__":
    main()
