import json
from collections import Counter

ATTACHED = r"C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_v1_7_19\events_attached_DEED_ONLY_v1_7_19.ndjson"

def dig(ev, path):
    cur = ev
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur

def get_status(ev):
    # try common shapes
    v = ev.get("status")
    if isinstance(v, str) and v:
        return v
    v = dig(ev, ["attach","status"])
    if isinstance(v, str) and v:
        return v
    v = dig(ev, ["attach","attach_status"])
    if isinstance(v, str) and v:
        return v
    return ""

def get_amount(ev):
    for p in [
        ["consideration","amount"],
        ["document","consideration","amount"],
        ["attach","consideration","amount"],
        ["attach","event","consideration","amount"],
    ]:
        v = dig(ev, p)
        if v is not None:
            return v
    return None

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

stats = {
    "UNKNOWN_total": 0,
    "arm_length_ge_100k": 0,
    "non_arm_length_lt_100k": 0,
    "nominal_le_100": 0,
    "missing_amount": 0,
    "invalid_amount": 0
}

buckets = Counter()

with open(ATTACHED, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        ev = json.loads(line)

        if get_status(ev) != "UNKNOWN":
            continue

        stats["UNKNOWN_total"] += 1

        amt = get_amount(ev)
        if amt is None:
            stats["missing_amount"] += 1
            buckets["MISSING_AMOUNT"] += 1
            continue

        amt_f = safe_float(amt)
        if amt_f is None:
            stats["invalid_amount"] += 1
            buckets["INVALID_AMOUNT"] += 1
            continue

        if amt_f >= 100000:
            stats["arm_length_ge_100k"] += 1
            buckets["ARM_LENGTH_GE_100K"] += 1
        elif amt_f <= 100:
            stats["nominal_le_100"] += 1
            buckets["NOMINAL_LE_100"] += 1
        else:
            stats["non_arm_length_lt_100k"] += 1
            buckets["NON_ARM_LENGTH_LT_100K"] += 1

print("=== UNKNOWN CONSIDERATION AUDIT (DEED_ONLY v1_7_19) ===")
print(json.dumps(stats, indent=2))

print("\nBuckets:")
for k, v in buckets.most_common():
    print(f"{k}: {v}")
