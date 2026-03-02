import os, json, glob, collections

EVENTS_DIR = r"C:\seller-app\backend\publicData\registry\hampden\_events_v1_4"

def safe_get(d, path):
    cur = d
    for k in path:
        if not isinstance(cur, dict): return None
        cur = cur.get(k)
    return cur

def norm(s):
    return (s or "").strip()

files = sorted(glob.glob(os.path.join(EVENTS_DIR, "*.ndjson")))
print("[info] events_dir:", EVENTS_DIR)
print("[info] files:", len(files))

by_file = []
by_type = collections.Counter()
missing_by_type = collections.Counter()
missing_by_file = collections.Counter()

for fp in files:
    total = 0
    have = 0
    miss = 0
    sample_miss = []
    sample_have = []
    with open(fp, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: 
                continue
            total += 1
            try:
                e = json.loads(line)
            except Exception:
                continue

            et = e.get("event_type") or e.get("eventType") or "UNKNOWN_TYPE"
            town = norm(safe_get(e, ["property_ref", "town_raw"]))
            addr = norm(safe_get(e, ["property_ref", "address_raw"]))

            by_type[et] += 1
            if town and addr:
                have += 1
                if len(sample_have) < 3:
                    sample_have.append({"event_type": et, "town_raw": town, "address_raw": addr, "event_id": e.get("event_id")})
            else:
                miss += 1
                missing_by_type[et] += 1
                missing_by_file[os.path.basename(fp)] += 1
                if len(sample_miss) < 3:
                    sample_miss.append({
                        "event_type": et,
                        "event_id": e.get("event_id"),
                        "town_raw": town,
                        "address_raw": addr,
                        "keys": list(e.keys())
                    })

    by_file.append({
        "file": os.path.basename(fp),
        "total": total,
        "locator_present": have,
        "locator_missing": miss,
        "pct_present": (have / total * 100.0) if total else 0.0,
        "sample_have": sample_have,
        "sample_missing": sample_miss
    })

print("\n=== Locator Coverage by File ===")
for r in sorted(by_file, key=lambda x: x["locator_present"], reverse=True):
    print(f'{r["file"]}: total={r["total"]} present={r["locator_present"]} missing={r["locator_missing"]} pct={r["pct_present"]:.2f}')

print("\n=== Missing Locator by Event Type ===")
for et, cnt in missing_by_type.most_common():
    print(f"{et}: {cnt} missing / {by_type[et]} total")
