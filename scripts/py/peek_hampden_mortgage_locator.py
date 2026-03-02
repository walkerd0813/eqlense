import json

p = r"C:\seller-app\backend\publicData\registry\hampden\_events_v1_4\mortgage_events.ndjson"
n=0
missing=0
samples=[]
with open(p,"r",encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        e=json.loads(line)
        n+=1
        pref=e.get("property_ref") or {}
        town=(pref.get("town_raw") or pref.get("town") or "").strip()
        addr=(pref.get("address_raw") or pref.get("address") or "").strip()
        if not town or not addr:
            missing+=1
            if len(samples)<10:
                samples.append({
                    "event_id": e.get("event_id"),
                    "event_type": e.get("event_type"),
                    "town_raw": town,
                    "address_raw": addr,
                    "property_ref_keys": sorted(list(pref.keys())),
                    "source_keys": sorted(list((e.get("source") or {}).keys())),
                    "document_keys": sorted(list((e.get("document") or {}).keys())),
                    "recording_keys": sorted(list((e.get("recording") or {}).keys())),
                })

print({"total":n,"missing_locator":missing,"missing_pct": (missing/n*100 if n else 0), "samples":samples})
