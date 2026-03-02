import argparse, json, re
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            yield json.loads(line)

def g(obj, *paths):
    """Get first non-empty value from dotted paths, supports dict nesting."""
    for p in paths:
        cur = obj
        ok = True
        for part in p.split("."):
            if not isinstance(cur, dict) or part not in cur:
                ok = False
                break
            cur = cur[part]
        if ok and cur not in (None, "", [], {}):
            return cur
    return None

def pick_status(row):
    return g(row, "attach_status", "status", "attached_status") or "UNKNOWN"

def pick_doc(row):
    return g(row, "doc", "document.doc", "document.document_number", "registry.document_number", "document_number") or ""

def pick_town_raw(row):
    return g(row, "town_raw", "town", "registry.town_raw", "registry.town", "document.town_raw", "source.town_raw") or ""

def pick_addr_raw(row):
    return g(row, "addr_raw", "address", "registry.addr_raw", "registry.address", "document.addr_raw", "source.addr_raw") or ""

def pick_town_norm(row):
    return g(row, "town_norm", "norm.town", "normalized.town", "parsed.town_norm", "registry.town_norm") or ""

def pick_addr_norm(row):
    return g(row, "addr_norm", "norm.addr", "normalized.addr", "parsed.addr_norm", "registry.addr_norm") or ""

def pick_reason(row):
    # prefer explicit bucket/reason if attach script writes one
    return (g(row, "unknown_reason", "reason", "attach_reason", "bucket") or "UNKNOWN")

def sample_obj(row):
    return {
        "doc": pick_doc(row),
        "town_raw": pick_town_raw(row),
        "addr_raw": pick_addr_raw(row),
        "town_norm": pick_town_norm(row),
        "addr_norm": pick_addr_norm(row),
        "status": pick_status(row),
        "reason": pick_reason(row)
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--attached", required=True)
    ap.add_argument("--maxSamples", type=int, default=8)
    args = ap.parse_args()

    total = 0
    status_counts = Counter()
    reason_counts = Counter()
    samples_by_reason = defaultdict(list)

    for row in iter_ndjson(args.attached):
        total += 1
        st = pick_status(row)
        status_counts[st] += 1

        if st != "ATTACHED_A":
            r = pick_reason(row)
            reason_counts[r] += 1
            if len(samples_by_reason[r]) < args.maxSamples:
                samples_by_reason[r].append(sample_obj(row))

    print("=== Post-Attach Summary ===")
    print({"attached": args.attached, "total": total, "status": dict(status_counts)})

    print("\n=== NON-ATTACHED reasons (top 30) ===")
    for k,v in reason_counts.most_common(30):
        print(f"{k}: {v}")

    print("\n=== Samples ===")
    for reason, arr in reason_counts.most_common(10):
        print(f"\n--- {reason} ---")
        for s in samples_by_reason.get(reason, []):
            print(s)

if __name__ == "__main__":
    main()
