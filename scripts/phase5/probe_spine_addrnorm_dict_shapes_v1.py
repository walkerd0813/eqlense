import json

SPINE = r"publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"

NEED_TOWNS = set([
  "AGAWAM","BRIMFIELD","CHICOPEE","EAST LONGMEADOW","GRANVILLE","HAMPDEN","HOLLAND",
  "HOLYOKE","LONGMEADOW","LUDLOW","MONSON","PALMER","RUSSELL","SOUTHWICK","SPRINGFIELD",
  "TOLLAND","WEST SPRINGFIELD","WESTFIELD","WILBRAHAM"
])

def norm_town(x):
  if x is None: return ""
  if isinstance(x,str): s=x
  elif isinstance(x,dict):
    for k in ["norm","value","text","name","raw"]:
      v=x.get(k)
      if isinstance(v,str) and v.strip():
        s=v; break
    else:
      return ""
  else:
    s=str(x)
  s=s.upper().strip()
  if s.endswith(" MA"): s=s[:-3].strip()
  return " ".join(s.replace(","," ").split())

def take_addr_obj(r):
  pr = r.get("property_ref") or r.get("ref") or {}
  # try common places
  for k in ["address_norm","address","addr_norm","addr","site_address","site_address_norm"]:
    if k in r: return r.get(k)
    if k in pr: return pr.get(k)
  return None

def take_town_obj(r):
  pr = r.get("property_ref") or r.get("ref") or {}
  for k in ["town_norm","town","municipality","city","city_norm"]:
    if k in r: return r.get(k)
    if k in pr: return pr.get(k)
  return None

samples = []
seen_keys = {}
rows=0
match_rows=0
dict_rows=0

with open(SPINE,"r",encoding="utf-8") as f:
  for line in f:
    if not line.strip(): continue
    rows += 1
    r=json.loads(line)
    t = norm_town(take_town_obj(r))
    if t not in NEED_TOWNS: 
      continue
    match_rows += 1
    a = take_addr_obj(r)
    if isinstance(a,dict):
      dict_rows += 1
      ks = tuple(sorted(a.keys()))
      seen_keys[ks] = seen_keys.get(ks,0)+1
      if len(samples) < 12:
        samples.append({"town":t, "address_norm_keys":list(a.keys()), "address_norm_obj":a})
    if match_rows >= 50000:
      break

# summarize top dict key-shapes
top = sorted(seen_keys.items(), key=lambda kv: kv[1], reverse=True)[:10]
print({
  "scanned_rows": rows,
  "rows_in_need_towns": match_rows,
  "rows_with_address_norm_dict_in_need_towns": dict_rows,
  "top10_dict_key_shapes": [{"keys":list(k), "count":c} for k,c in top],
  "samples": samples
})
