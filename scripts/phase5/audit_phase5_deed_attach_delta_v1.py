import argparse, json, os, sys
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            yield json.loads(line)

def pick_event_id(row):
    for k in ("event_id","id","registry_event_id","eventId"):
        if k in row and row[k]:
            return row[k]
    return None

def pick_property_id(row):
    for k in ("property_id","attached_property_id","pid"):
        if k in row and row[k]:
            return row[k]
    return None

def pick_status(row):
    for k in ("attach_status","status","attached_status"):
        if k in row and row[k]:
            return row[k]
    return None

def pick_method(row):
    for k in ("match_method","attach_method","method"):
        if k in row and row[k]:
            return row[k]
    return None

def load_map(path):
    m = {}
    dup = 0
    for row in iter_ndjson(path):
        eid = pick_event_id(row)
        if not eid:
            continue
        if eid in m:
            dup += 1
            continue
        m[eid] = {
            "status": pick_status(row),
            "pid": pick_property_id(row),
            "method": pick_method(row),
        }
    return m, dup

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--outDir", required=True)
    args = ap.parse_args()

    os.makedirs(args.outDir, exist_ok=True)

    base, base_dup = load_map(args.baseline)
    cand, cand_dup = load_map(args.candidate)

    only_in_base = []
    only_in_cand = []
    status_changes = []
    pid_changes = []
    regressions = []

    counts = Counter()

    all_ids = set(base.keys()) | set(cand.keys())
    for eid in all_ids:
        b = base.get(eid)
        c = cand.get(eid)

        if b and not c:
            only_in_base.append(eid)
            counts["missing_in_candidate"] += 1
            continue
        if c and not b:
            only_in_cand.append(eid)
            counts["new_in_candidate"] += 1
            continue

        bs, cs = b["status"], c["status"]
        bp, cp = b["pid"], c["pid"]

        if bs != cs:
            status_changes.append({"event_id": eid, "baseline": b, "candidate": c})
            counts["status_changed"] += 1

            # regression rule: baseline ATTACHED_A must not degrade
            if bs == "ATTACHED_A" and cs != "ATTACHED_A":
                regressions.append({"event_id": eid, "type": "ATTACHED_A_regressed", "baseline": b, "candidate": c})
                counts["regression_attachedA_to_non"] += 1

        # property_id flip rule (only meaningful when attached)
        if (bp or cp) and bp != cp:
            pid_changes.append({"event_id": eid, "baseline": b, "candidate": c})
            counts["pid_changed"] += 1

            # strict: if both attached and pid differs, that's a hard stop
            if bs == "ATTACHED_A" and cs == "ATTACHED_A" and bp and cp and bp != cp:
                regressions.append({"event_id": eid, "type": "ATTACHED_A_pid_flip", "baseline": b, "candidate": c})
                counts["regression_pid_flip_attachedA"] += 1

        # improvements
        if bs != "ATTACHED_A" and cs == "ATTACHED_A":
            counts["improved_to_attachedA"] += 1

    summary = {
        "baseline_path": args.baseline,
        "candidate_path": args.candidate,
        "baseline_events": len(base),
        "candidate_events": len(cand),
        "baseline_dupe_event_ids_seen": base_dup,
        "candidate_dupe_event_ids_seen": cand_dup,
        "counts": dict(counts),
        "regression_rules": {
            "no_attachedA_degrade": True,
            "no_pid_flip_when_attachedA": True
        }
    }

    def write(name, obj):
        p = os.path.join(args.outDir, name)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)
        return p

    p_sum = write("delta_summary.json", summary)
    p_status = write("status_changes.json", status_changes)
    p_pid = write("pid_changes.json", pid_changes)
    p_reg = write("regressions.json", regressions)
    p_onlyb = write("only_in_baseline.json", only_in_base)
    p_onlyc = write("only_in_candidate.json", only_in_cand)

    print("[done] wrote:")
    print("  " + p_sum)
    print("  " + p_reg)
    print("  " + p_status)
    print("  " + p_pid)
    print("  " + p_onlyb)
    print("  " + p_onlyc)

    # Exit code: 0 if no regressions, 2 if regressions
    if len(regressions) > 0:
        print(f"[fail] regressions found: {len(regressions)} (see regressions.json)")
        sys.exit(2)

    print("[ok] no regressions found")

if __name__ == "__main__":
    main()
