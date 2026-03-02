import argparse, json, re, time
from collections import defaultdict, Counter

NEED_TOWNS = set([
  "AGAWAM","BRIMFIELD","CHICOPEE","EAST LONGMEADOW","GRANVILLE","HAMPDEN","HOLLAND",
  "HOLYOKE","LONGMEADOW","LUDLOW","MONSON","PALMER","RUSSELL","SOUTHWICK","SPRINGFIELD",
  "TOLLAND","WEST SPRINGFIELD","WESTFIELD","WILBRAHAM"
])

SUF_CANON = {
  "TERR":"TER","TERRACE":"TER","TER":"TER",
  "CIR":"CIR","CIRCLE":"CIR",
  "ST":"ST","STREET":"ST",
  "RD":"RD","ROAD":"RD",
  "DR":"DR","DRIVE":"DR",
  "AVE":"AVE","AV":"AVE","AVENUE":"AVE",
  "BLVD":"BLVD","BOULEVARD":"BLVD",
  "LN":"LN","LANE":"LN","LA":"LN",
  "CT":"CT","COURT":"CT",
  "PL":"PL","PLACE":"PL",
  "WAY":"WAY",
  "PKY":"PKY","PARKWAY":"PKY",
  "HWY":"HWY","HIGHWAY":"HWY",
  "EXT":"EXT","EXTN":"EXT","EXTENSION":"EXT"
}

UNIT_WORDS = set(["UNIT","APT","APARTMENT","#","NO","SUITE","STE","BLDG","FLOOR","FL"])

def as_str(x):
  if x is None: return ""
  if isinstance(x, str): return x
  if isinstance(x, (int,float)): return str(x)
  return ""

def norm_spaces(s: str) -> str:
  return " ".join(s.replace("\t"," ").replace("\r"," ").replace("\n"," ").split())

def norm_town(x) -> str:
  if isinstance(x, dict):
    for k in ("town_norm","norm","value","text","name","raw","town","city","municipality"):
      v = x.get(k)
      if isinstance(v,str) and v.strip():
        x = v
        break
  s = as_str(x).upper().strip()
  if s.endswith(" MA"): s = s[:-3].strip()
  s = norm_spaces(s.replace(","," "))
  return s

def clean_addr(s: str) -> str:
  s = as_str(s).upper()
  s = s.replace(",", " ")
  s = re.sub(r"[^\w\s#\-\/]", " ", s)  # keep dash for ranges, # for units
  s = norm_spaces(s)
  return s

def strip_unit_tokens(tokens):
  out = []
  for t in tokens:
    if t in UNIT_WORDS: break
    if t.startswith("#"): break
    out.append(t)
  return out

def canon_suffix(tok: str) -> str:
  return SUF_CANON.get(tok, tok)

def split_num_and_rest(addr_clean: str):
  toks = addr_clean.split()
  if not toks: return ("", [])
  first = toks[0]
  if re.match(r"^\d+([A-Z])?$", first) or re.match(r"^\d+-\d+$", first) or re.match(r"^\d+[A-Z]$", first):
    return (first, toks[1:])
  return ("", toks)

def take_town_event(e):
  pr = e.get("property_ref") or e.get("property") or {}
  for k in ("town","town_norm","city","city_norm","municipality"):
    v = e.get(k)
    if v is None: v = pr.get(k)
    if v is not None:
      t = norm_town(v)
      if t: return t
  return ""

def take_addr_event(e):
  pr = e.get("property_ref") or e.get("property") or {}
  # try common keys
  candidates = [
    e.get("addr"), e.get("address"), e.get("site_address"), e.get("full_address"),
    pr.get("addr"), pr.get("address"), pr.get("site_address"), pr.get("full_address"),
  ]
  for v in candidates:
    if isinstance(v, str) and v.strip():
      return v
  return ""

def take_town_spine(r):
  pr = r.get("property_ref") or r.get("ref") or {}
  for k in ("town_norm","town","city_norm","city","municipality"):
    v = r.get(k)
    if v is None: v = pr.get(k)
    if v is not None:
      t = norm_town(v)
      if t: return t
  return ""

def take_addr_str_spine(r):
  pr = r.get("property_ref") or r.get("ref") or {}
  # prefer normalized string fields; ignore dict-shaped patch objects
  for k in ("address_norm","addr_norm","site_address_norm"):
    v = r.get(k)
    if isinstance(v,str) and v.strip(): return v
    v = pr.get(k)
    if isinstance(v,str) and v.strip(): return v
  for k in ("address","addr","site_address","full_address"):
    v = r.get(k)
    if isinstance(v,str) and v.strip(): return v
    v = pr.get(k)
    if isinstance(v,str) and v.strip(): return v
  return ""

def build_spine_indexes(spine_path: str):
  t0=time.time()
  full_index = defaultdict(list)
  street_index = defaultdict(list)
  street_nosuf = defaultdict(list)
  debug = Counter()

  with open(spine_path,"r",encoding="utf-8") as f:
    for line in f:
      if not line.strip(): continue
      r=json.loads(line)
      town = take_town_spine(r)
      if town not in NEED_TOWNS: 
        continue

      addr_raw = take_addr_str_spine(r)
      if not addr_raw:
        debug["no_addr_str"] += 1
        continue

      addr = clean_addr(addr_raw)
      num, rest = split_num_and_rest(addr)
      if not num:
        debug["no_leading_num"] += 1
        continue

      rest_tokens = strip_unit_tokens(rest)
      if not rest_tokens:
        debug["no_rest_tokens"] += 1
        continue

      suf = canon_suffix(rest_tokens[-1])
      street_tokens = rest_tokens[:-1] if len(rest_tokens) >= 2 else []
      if not street_tokens:
        debug["no_street_tokens"] += 1
        continue

      street = " ".join(street_tokens)
      full_canon = f"{num} {street} {suf}"

      pid = r.get("property_id") or (r.get("property_ref") or {}).get("property_id") or (r.get("ref") or {}).get("property_id") or ""
      if not pid:
        debug["no_property_id"] += 1
        continue

      full_index[f"{town}|{full_canon}"].append(pid)
      street_index[f"{town}|{num}|{street}|{suf}"].append(pid)
      street_nosuf[f"{town}|{num}|{street}"].append(pid)
      debug["indexed_rows"] += 1

  keys = {
    "full_keys": len(full_index),
    "street_keys": len(street_index),
    "street_nosuf_keys": len(street_nosuf),
    "debug": dict(debug),
    "elapsed_s": round(time.time()-t0,2)
  }
  return full_index, street_index, street_nosuf, keys

def uniq_or_none(lst):
  if not lst: return None
  s=set(lst)
  if len(s)==1: return list(s)[0]
  return "__COLLISION__"

def reattach(events_in: str, spine_path: str, out_path: str, audit_path: str, debug_n=12):
  full_index, street_index, street_nosuf, keys = build_spine_indexes(spine_path)
  print("[ok] spine index built:", json.dumps(keys, indent=2))

  stats = Counter()
  dbg = []

  with open(events_in,"r",encoding="utf-8") as fin, open(out_path,"w",encoding="utf-8") as fout:
    for line in fin:
      if not line.strip(): continue
      e=json.loads(line)

      scope = as_str(e.get("attach_scope")).upper()
      status = as_str(e.get("attach_status")).upper()

      if scope == "MULTI" or status in ("ATTACHED_A","PARTIAL_MULTI"):
        stats["preserved"] += 1
        fout.write(json.dumps(e, ensure_ascii=False) + "\n")
        continue

      town = norm_town(take_town_event(e))
      addr_raw = take_addr_event(e)
      addr = clean_addr(addr_raw)
      num, rest = split_num_and_rest(addr)

      if len(dbg) < debug_n:
        dbg.append({"event_id": e.get("event_id"), "town": town, "addr_raw": addr_raw, "addr_clean": addr, "num": num, "attach_scope": scope, "attach_status": status})

      if not num:
        e["match_method"] = "no_num"
        e["why"] = "no_num"
        stats["single_unknown_no_num"] += 1
        fout.write(json.dumps(e, ensure_ascii=False) + "\n")
        continue

      rest_tokens = strip_unit_tokens(rest)
      if len(rest_tokens) < 2:
        stats["single_unknown_no_street"] += 1
        fout.write(json.dumps(e, ensure_ascii=False) + "\n")
        continue

      suf = canon_suffix(rest_tokens[-1])
      street = " ".join(rest_tokens[:-1])
      full_canon = f"{num} {street} {suf}"

      # 1) full exact
      pid = uniq_or_none(full_index.get(f"{town}|{full_canon}", []))
      if pid and pid != "__COLLISION__":
        e["attach_status"]="ATTACHED_A"
        e["match_method"]="axis2_full_address_exact"
        e["why"]=None
        e["attachments_n"]=1
        e["attachments"]=[{"property_id":pid,"confidence":"A","method":"town+full_addr_canon_exact"}]
        stats["attached_full_exact"] += 1
        fout.write(json.dumps(e, ensure_ascii=False) + "\n")
        continue
      if pid == "__COLLISION__":
        e["match_method"]="collision"
        e["why"]="collision"
        stats["collision_full"] += 1
        fout.write(json.dumps(e, ensure_ascii=False) + "\n")
        continue

      # 2) street exact
      pid = uniq_or_none(street_index.get(f"{town}|{num}|{street}|{suf}", []))
      if pid and pid != "__COLLISION__":
        e["attach_status"]="ATTACHED_A"
        e["match_method"]="axis2_street_unique_exact"
        e["why"]=None
        e["attachments_n"]=1
        e["attachments"]=[{"property_id":pid,"confidence":"A","method":"town+num+street+suffix_exact"}]
        stats["attached_street_exact"] += 1
        fout.write(json.dumps(e, ensure_ascii=False) + "\n")
        continue
      if pid == "__COLLISION__":
        e["match_method"]="collision"
        e["why"]="collision"
        stats["collision_street"] += 1
        fout.write(json.dumps(e, ensure_ascii=False) + "\n")
        continue

      # 3) no-suffix unique
      pid = uniq_or_none(street_nosuf.get(f"{town}|{num}|{street}", []))
      if pid and pid != "__COLLISION__":
        e["attach_status"]="ATTACHED_A"
        e["match_method"]="axis2_street_unique_nosuf"
        e["why"]=None
        e["attachments_n"]=1
        e["attachments"]=[{"property_id":pid,"confidence":"A","method":"town+num+street_unique_nosuf"}]
        stats["attached_street_nosuf"] += 1
        fout.write(json.dumps(e, ensure_ascii=False) + "\n")
        continue

      e["match_method"] = "no_match"
      e["why"] = "no_match"
      stats["still_unknown_no_match"] += 1
      fout.write(json.dumps(e, ensure_ascii=False) + "\n")

  audit = {
    "in": events_in,
    "spine": spine_path,
    "out": out_path,
    "index_keys": keys,
    "stats": dict(stats),
    "debug_samples": dbg,
    "notes": "v1_18c: robust event addr/town fetch + debug samples"
  }
  with open(audit_path,"w",encoding="utf-8") as f:
    json.dump(audit,f,indent=2)

  print("[debug] samples:", json.dumps(dbg, indent=2))
  print(json.dumps({"out": out_path, "audit": audit_path, "stats": dict(stats)}, indent=2))

def main():
  ap=argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  ap.add_argument("--debug_n", type=int, default=12)
  args=ap.parse_args()
  print("=== AXIS2 REATTACH (>=10k) v1_18c (event addr/town robust fetch + debug) ===")
  print("[info] events:", args.events)
  print("[info] spine :", args.spine)
  reattach(args.events, args.spine, args.out, args.audit, args.debug_n)

if __name__=="__main__":
  main()
