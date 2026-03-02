import json

ev = r"""C:\seller-app\backend\publicData\registry\hampden\_events_DEED_ONLY_v1\deed_events.ndjson"""
with open(ev,"r",encoding="utf-8") as f:
    for i,line in enumerate(f):
        o=json.loads(line)

        print("TOP KEYS:", sorted(list(o.keys()))[:60])

        # show any top-level keys containing 'town' or 'city'
        hits=[k for k in o.keys() if ("town" in k.lower() or "city" in k.lower() or "muni" in k.lower())]
        print("TOP-LEVEL TOWN/CITY KEYS:", hits)

        # check common nest spots
        for path in ["registry","document","property","address","source","meta"]:
            if isinstance(o.get(path), dict):
                kk=[k for k in o[path].keys() if ("town" in k.lower() or "city" in k.lower() or "muni" in k.lower())]
                if kk:
                    print(f"{path} keys:", kk)

        # address_candidates (your earlier pipeline used this)
        ac = o.get("address_candidates")
        if isinstance(ac, list) and ac:
            c0 = ac[0]
            if isinstance(c0, dict):
                kk=[k for k in c0.keys() if ("town" in k.lower() or "city" in k.lower() or "muni" in k.lower())]
                print("address_candidates[0] keys:", kk)
                # print a mini sample
                sample = {k:c0.get(k) for k in kk[:10]}
                print("address_candidates[0] sample:", sample)

        print("SAMPLE EVENT_ID:", o.get("event_id") or o.get("id"))
        break

