import argparse, json
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            yield json.loads(line)

def g(obj, *paths):
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
    return (
        g(row, "attach.status", "attach.attach_status", "attach.result", "attach.outcome")
        or g(row, "attach_status", "status", "attached_status")
        or "UNKNOWN"
    )

def pick_reason(row):
    return (
        g(row, "attach.reason", "attach.unknown_reason", "attach.bucket", "attach.attach_reason")
        or g(row, "unknown_reason", "reason", "attach_reason", "bucket")
        or "UNKNOWN"
    )

def pick_method(row):
    return (
        g(row, "attach.method", "attach.match_method", "attach.attach_method")
        or g(row, "match_method", "method", "attach_method")
        or ""
    )

def pick_doc(row):
    return (
        g(row, "document.doc")
        or g(row, "document.document_number")
        or g(row, "document.doc_number")
        or g(row, "document_number")
        or ""
    )

def pick_town_raw(row):
    return g(row, "property_ref.town_raw", "property_ref.town", "source.town_raw", "town_raw", "town") or ""

def pick_addr_raw(row):
    return g(row, "property_ref.addr_raw", "property_ref.address_raw", "property_ref.address", "source.addr_raw", "addr_raw", "address") or ""

def pick_town_norm(row):
    return g(row, "attach.town_norm", "property_ref.town_norm", "town_norm") or ""

def pick_addr_norm(row):
    return g(row, "attach.addr_norm", "property_ref.addr_norm", "addr_norm") or ""

def sample_obj(row):
    return {
        "doc": pick_doc(row),
        "town_raw": pick_town_raw(row),
        "addr_raw": pick_addr_raw(row),
        "town_norm": pick_town_norm(row),
        "addr_norm": pick_addr_norm(row),
        "status": pick_status(row),
        "reason": pick_reason(row),
        "method": pick_method(row),
        "property_id": row.get("property_id",""),
        "event_id": row.get("event_id","")
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
    for reason, _ in reason_counts.most_common(10):
        print(f"\n--- {reason} ---")
        for s in samples_by_reason.get(reason, []):
            print(s)

if __name__ == "__main__":
    main()
