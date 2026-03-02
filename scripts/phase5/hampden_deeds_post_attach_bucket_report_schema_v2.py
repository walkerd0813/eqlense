import argparse, json
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def pick(ev, keys, default=""):
    for k in keys:
        if k in ev and ev.get(k) not in (None, ""):
            return ev.get(k)
    return default

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--attached", required=True)
    ap.add_argument("--maxSamples", type=int, default=10)
    args = ap.parse_args()

    status_counts = Counter()
    bucket_counts = Counter()
    reason_counts = Counter()
    samples = defaultdict(list)

    total = 0
    seen_attach_obj = 0
    seen_top_level = 0

    for ev in iter_ndjson(args.attached):
        total += 1

        attach = ev.get("attach") if isinstance(ev.get("attach"), dict) else {}
        if attach:
            seen_attach_obj += 1
        else:
            seen_top_level += 1

        # Prefer attach.* fields; fall back to top-level legacy names if present
        status = attach.get("attach_status") or ev.get("attach_status") or "UNKNOWN"
        method = attach.get("attach_method") or ev.get("attach_method") or ""
        town_norm = attach.get("town_norm") or ev.get("town_norm") or ""
        addr_norm = attach.get("address_norm") or attach.get("addr_norm") or ev.get("address_norm") or ev.get("addr_norm") or ""
        bucket = attach.get("bucket") or ev.get("bucket") or ("MISSING_TOWN_OR_ADDRESS" if status == "MISSING_TOWN_OR_ADDRESS" else "UNKNOWN")

        # raw fields
        pref = ev.get("property_ref") if isinstance(ev.get("property_ref"), dict) else {}
        town_raw = pref.get("town_raw") or ev.get("town_raw") or ""
        addr_raw = pref.get("address_raw") or ev.get("addr_raw") or ev.get("address_raw") or ""

        # doc/id for samples
        doc = ""
        if isinstance(ev.get("document"), dict):
            doc = ev["document"].get("doc_type") or ev["document"].get("doc_id") or ev["document"].get("doc_no") or ""
        event_id = ev.get("event_id") or ""

        status_counts[status] += 1
        bucket_counts[bucket] += 1
        if status != "ATTACHED_A":
            reason_counts[bucket] += 1
            if len(samples[bucket]) < args.maxSamples:
                samples[bucket].append({
                    "event_id": event_id,
                    "doc": doc,
                    "town_raw": town_raw,
                    "addr_raw": addr_raw,
                    "town_norm": town_norm,
                    "addr_norm": addr_norm,
                    "status": status,
                    "method": method,
                    "bucket": bucket,
                    "property_id": ev.get("property_id","")
                })

    print("=== Post-Attach Summary (schema_v2) ===")
    print({
        "attached": args.attached,
        "total": total,
        "status": dict(status_counts),
        "attach_obj_rows": seen_attach_obj,
        "legacy_top_level_rows": seen_top_level
    })

    print("\n=== NON-ATTACHED buckets (top 30) ===")
    for k,v in reason_counts.most_common(30):
        print(f"{k}: {v}")

    print("\n=== Samples ===")
    for bucket, rows in samples.items():
        print(f"\n--- {bucket} ---")
        for r in rows:
            print(r)

if __name__ == "__main__":
    main()
