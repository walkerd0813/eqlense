import json
import argparse
from collections import Counter

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="Current Axis2 ndjson")
    ap.add_argument("--out", required=True, help="Projection summary json")
    args = ap.parse_args()

    totals = Counter()
    unknown_buckets = Counter()
    rows = 0

    with open(args.infile, "r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            rows += 1

            status = r.get("attach_status", "UNKNOWN")
            totals[status] += 1

            if status == "UNKNOWN":
                town = r.get("town")
                addr = r.get("addr")
                why  = (r.get("why") or "").upper()

                if not town and not addr:
                    unknown_buckets["STRUCTURAL_NO_TOWN_NO_ADDR"] += 1
                elif not addr:
                    unknown_buckets["NO_ADDR_ONLY"] += 1
                elif "COLLISION" in why:
                    unknown_buckets["COLLISION"] += 1
                elif "NO_NUM" in why:
                    unknown_buckets["NO_HOUSE_NUMBER"] += 1
                else:
                    unknown_buckets["OTHER_UNKNOWN"] += 1

    projection = {
        "rows_total": rows,
        "attached_A": totals.get("ATTACHED_A", 0),
        "attached_B": totals.get("ATTACHED_B", 0),
        "attached_total": totals.get("ATTACHED_A", 0) + totals.get("ATTACHED_B", 0),
        "unknown_total": totals.get("UNKNOWN", 0),
        "unknown_breakdown": dict(unknown_buckets),
        "notes": {
            "STRUCTURAL_NO_TOWN_NO_ADDR": "Impossible without upstream registry recovery",
            "NO_ADDR_ONLY": "Impossible without address text recovery",
            "NO_HOUSE_NUMBER": "Requires assessor authority",
            "COLLISION": "Requires human or parcel split logic"
        }
    }

    print("\nAXIS2 PROGRESSION PROJECTION")
    for k, v in projection.items():
        print(f"{k}: {v}")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(projection, f, indent=2)

    print(f"\n[ok] wrote projection → {args.out}")

if __name__ == "__main__":
    main()
