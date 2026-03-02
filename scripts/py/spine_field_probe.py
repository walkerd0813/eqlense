import json

spine = r"C:\seller-app\backend\publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"

with open(spine,"r",encoding="utf-8") as f:
  line = f.readline()
obj = json.loads(line)

print("TOP_KEYS:", sorted(list(obj.keys()))[:60])
for k in ["town","town_raw","city","source_city","full_address","address","address_raw","address_norm"]:
  v = obj.get(k, None)
  if isinstance(v, dict):
    print(k, "=> dict keys:", list(v.keys())[:40])
  else:
    print(k, "=>", v)
