import json

ATTACHED = r"C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_v1_7_19\events_attached_DEED_ONLY_v1_7_19.ndjson"
N = 30

def dig(ev, path):
    cur = ev
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur

status_paths = [
    ["status"],
    ["attach","status"],
    ["attach","attach_status"],
    ["attach","match","status"],
]

consider_paths = [
    ["consideration","amount"],
    ["document","consideration","amount"],
    ["attach","consideration","amount"],
    ["attach","event","consideration","amount"],
]

seen = 0
with open(ATTACHED, "r", encoding="utf-8") as f:
    for line in f:
        line=line.strip()
        if not line: 
            continue
        ev = json.loads(line)
        seen += 1
        if seen == 1:
            print("TOP KEYS:", sorted(list(ev.keys())))

        found_status = None
        for p in status_paths:
            v = dig(ev,p)
            if isinstance(v,str):
                found_status = (p, v)
                break

        found_amt = None
        for p in consider_paths:
            v = dig(ev,p)
            if v is not None:
                found_amt = (p, v)
                break

        print(f"--- sample {seen} ---")
        print("event_id:", ev.get("event_id"))
        print("status_found:", found_status)
        print("amount_found:", found_amt)

        if seen >= N:
            break
