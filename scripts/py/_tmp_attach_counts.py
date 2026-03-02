import json, collections

path = r"C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_v1_7_9\events_attached_DEED_ONLY_v1_7_9.ndjson"
c = collections.Counter()

with open(path,"r",encoding="utf-8") as f:
    for line in f:
        line=line.strip()
        if not line:
            continue
        ev = json.loads(line)
        st = (ev.get("attach") or {}).get("attach_status") or "NO_ATTACH_FIELD"
        c[st] += 1

print("Attach status counts:", dict(c))
