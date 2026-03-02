import json

P = r"publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_24.ndjson"

def pick(e, path):
    cur = e
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

town_paths = [
    ("town", ["town"]),
    ("property_ref.town", ["property_ref","town"]),
    ("property_ref.address_norm.town", ["property_ref","address_norm","town"]),
    ("property_ref.town_raw", ["property_ref","town_raw"]),
]

addr_paths = [
    ("addr", ["addr"]),
    ("property_ref.address_raw", ["property_ref","address_raw"]),
    ("property_ref.address", ["property_ref","address"]),
    ("property_ref.full_address", ["property_ref","full_address"]),
]

n = 0
with open(P, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        e = json.loads(line)
        n += 1
        print("\n--- row %d event_id=%s ---" % (n, e.get("event_id")))

        found_town = False
        for name, path in town_paths:
            v = pick(e, path)
            if v:
                print("TOWN via %s = %s" % (name, v))
                found_town = True
                break
        if not found_town:
            print("TOWN via (none)")

        found_addr = False
        for name, path in addr_paths:
            v = pick(e, path)
            if v:
                print("ADDR via %s = %s" % (name, v))
                found_addr = True
                break
        if not found_addr:
            print("ADDR via (none)")

        if n >= 10:
            break
