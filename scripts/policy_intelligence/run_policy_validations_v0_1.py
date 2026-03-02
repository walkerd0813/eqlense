import argparse, json, os
from datetime import datetime, timezone

def read_events(path):
    if path.lower().endswith(".ndjson"):
        out = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                out.append(json.loads(line))
        return out
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy_events", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--as_of", default="")
    args = ap.parse_args()

    events = read_events(args.policy_events)
    run_date = args.as_of.strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    runs = []
    for e in events:
        pid = e.get("policy_id")
        status = e.get("status","")
        runs.append({
            "policy_id": pid,
            "run_date": run_date,
            "status": "insufficient",
            "metrics_snapshot": {
                "note": "No signal calculators wired yet. This is a placeholder run.",
                "policy_status": status
            },
            "notes_internal": "wire signal calculators (deeds/mls/permits/capital) before enabling real validation"
        })

    write_ndjson(args.out, runs)
    print("[ok] wrote policy_validation_runs")
    print("  events:", len(events))
    print("  out:", args.out)

if __name__ == "__main__":
    main()
