import json, argparse, datetime
from collections import Counter

def utc_now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00","Z")

def grade_rank(g):
    return {"A": 3, "B": 2, "C": 1}.get((g or "").upper(), 0)

def get_sha(o):
    return ((o.get("source") or {}).get("sha256") or "").strip()

def get_margin(o):
    pl = o.get("property_locator") or {}
    return 1 if (pl.get("address_source_location") == "LEFT_MARGIN_ROTATED") else 0

def get_grade(o):
    pl = o.get("property_locator") or {}
    return pl.get("address_confidence_grade")

def get_raw_score(o):
    pl = o.get("property_locator") or {}
    v = pl.get("address_candidate_score_raw")
    try:
        return int(v) if v is not None else 0
    except Exception:
        return 0

def rank_tuple(o):
    return (get_margin(o), grade_rank(get_grade(o)), get_raw_score(o))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input NDJSON (canonical with dups)")
    ap.add_argument("--out", required=True, help="Output NDJSON (deduped)")
    ap.add_argument("--outAudit", required=True, help="Output audit JSON")
    args = ap.parse_args()

    best = {}          # sha -> (rank, obj)
    order = []         # preserve first-seen order for deterministic output
    counts = Counter()
    addr_src_counts = Counter()
    grade_counts = Counter()
    prop_type_counts = Counter()

    with open(args.inp, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            counts["rows_seen"] += 1
            o = json.loads(line)

            sha = get_sha(o)
            if not sha:
                counts["missing_sha"] += 1
                # keep it, but treat it as unique by synthetic key
                sha = f"__NO_SHA__::{counts['rows_seen']}"

            r = rank_tuple(o)

            if sha not in best:
                best[sha] = (r, o)
                order.append(sha)
            else:
                counts["dup_seen"] += 1
                if r > best[sha][0]:
                    best[sha] = (r, o)
                    counts["dup_replaced_with_better"] += 1
                else:
                    counts["dup_kept_existing"] += 1

    # write out
    with open(args.out, "w", encoding="utf-8") as out:
        for sha in order:
            o = best[sha][1]
            out.write(json.dumps(o, ensure_ascii=False) + "\n")
            counts["rows_written"] += 1

            pl = o.get("property_locator") or {}
            addr_src_counts[pl.get("address_source_location") or "UNKNOWN"] += 1
            grade_counts[pl.get("address_confidence_grade") or "UNKNOWN"] += 1
            prop_type_counts[(o.get("property_type") or "UNKNOWN")] += 1

    audit = {
        "created_at": utc_now_iso(),
        "in": args.inp,
        "out": args.out,
        "rows_seen": counts["rows_seen"],
        "rows_written": counts["rows_written"],
        "dup_seen": counts.get("dup_seen", 0),
        "dup_replaced_with_better": counts.get("dup_replaced_with_better", 0),
        "dup_kept_existing": counts.get("dup_kept_existing", 0),
        "address_source_location_counts": dict(addr_src_counts),
        "address_confidence_grade_counts": dict(grade_counts),
        "property_type_counts": dict(prop_type_counts),
    }

    with open(args.outAudit, "w", encoding="utf-8") as a:
        json.dump(audit, a, indent=2)

    print("[done] wrote deduped:", args.out)
    print("[done] wrote audit:", args.outAudit)
    print(json.dumps({k:audit[k] for k in ["rows_seen","rows_written","dup_seen","dup_replaced_with_better"]}, indent=2))
    print("[verify] address_source_location_counts:", json.dumps(audit["address_source_location_counts"], indent=2))

if __name__ == "__main__":
    main()
