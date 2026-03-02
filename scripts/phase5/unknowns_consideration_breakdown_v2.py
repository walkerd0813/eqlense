import json, sys
from collections import Counter, defaultdict

IN_FILE = sys.argv[1]

buckets = Counter()
rows = []

def bucket(amount):
    if amount is None: return "MISSING"
    if amount <= 100: return "<=100"
    if amount <= 1000: return "100-1k"
    if amount <= 10000: return "1k-10k"
    return ">10k"

with open(IN_FILE, "r", encoding="utf-8") as f:
    for line in f:
        ev = json.loads(line)
        att = ev.get("attach", {})
        if att.get("attach_status") not in ("UNKNOWN", "PARTIAL_MULTI"):
            continue

        cons = ev.get("consideration", {})
        amt = cons.get("amount")
        b = bucket(amt)
        buckets[b] += 1

        if b in (">10k", "1k-10k"):
            rows.append({
                "event_id": ev.get("event_id"),
                "town": (ev.get("property_ref") or {}).get("town_norm"),
                "address": (ev.get("property_ref") or {}).get("address_norm"),
                "book": (ev.get("recording") or {}).get("book"),
                "page": (ev.get("recording") or {}).get("page"),
                "amount": amt,
                "attach_status": att.get("attach_status")
            })

print("\n=== UNKNOWN / PARTIAL_MULTI — CONSIDERATION BREAKDOWN ===")
for k,v in buckets.most_common():
    print(f"{k:>10} : {v}")

print("\n=== CANDIDATES WORTH FIXING (>= $1k) ===")
for r in rows[:25]:
    print(r)
print(f"\nTotal >=$1k candidates: {len(rows)}")
