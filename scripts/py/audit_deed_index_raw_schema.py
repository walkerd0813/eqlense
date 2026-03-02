import json, collections, sys

path = r"C:\seller-app\backend\publicData\registry\hampden\_raw_from_index_v1\deed_index_raw_v1_2.ndjson"

KEYS = [
  "raw_block","document_number_raw","document_number","seq",
  "entry_date_raw","recorded_time_raw","doc_type","town_code",
  "property_address","amount","doc_date_raw","favor_of","grantors","grantees"
]

total = 0
present = collections.Counter()
samples = []

with open(path,"r",encoding="utf-8") as f:
    for line in f:
        line=line.strip()
        if not line: 
            continue
        total += 1
        obj = json.loads(line)
        for k in KEYS:
            v = obj.get(k, None)
            if v is None: 
                continue
            if isinstance(v,str) and v.strip()=="":
                continue
            present[k] += 1
        if len(samples) < 5:
            samples.append({k: obj.get(k) for k in ["town_code","property_address","document_number_raw","raw_block","source"]})

print({"total": total, "present_counts": dict(present), "samples": samples})
