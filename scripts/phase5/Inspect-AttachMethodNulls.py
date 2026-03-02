import json, collections, argparse

ap = argparse.ArgumentParser()
ap.add_argument("--in", dest="inp", required=True)
ap.add_argument("--sample", type=int, default=20)
args = ap.parse_args()

ct = collections.Counter()
ct2 = collections.Counter()
samples = []

with open(args.inp, "r", encoding="utf-8") as f:
    for line in f:
        r = json.loads(line)
        a = r.get("attach") or {}
        st = a.get("attach_status")
        m  = a.get("attach_method")
        ct[(st, m)] += 1
        ct2[(st, "method_is_null" if m is None else "method_present")] += 1
        if m is None and len(samples) < args.sample:
            samples.append({
                "event_id": r.get("event_id"),
                "status": st,
                "bucket": a.get("bucket"),
                "town": (a.get("town_norm") or r.get("town")),
                "addr": (a.get("address_norm") or r.get("addr")),
            })

print("=== status x method(null/present) ===")
for k,v in ct2.most_common():
    print(k, v)

print("\n=== top (status, attach_method) pairs ===")
for k,v in ct.most_common(25):
    print(k, v)

print("\n=== sample rows where attach_method is null ===")
for s in samples:
    print(s)
