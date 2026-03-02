import json, re, os
from collections import defaultdict, Counter

EVENTS = None
SPINE  = None
OUT    = None

SUFFIX_MAP = {
  "LA":"LN","LANE":"LN","LN":"LN",
  "STREET":"ST","ST":"ST","AVENUE":"AVE","AVE":"AVE","ROAD":"RD","RD":"RD",
  "DRIVE":"DR","DR":"DR","TERRACE":"TERR","TER":"TERR","TERR":"TERR",
  "CIRCLE":"CIR","CIR":"CIR","COURT":"CT","CT":"CT","BOULEVARD":"BLVD","BLVD":"BLVD",
  "PLACE":"PL","PL":"PL","PKWAY":"PKWY","PKWY":"PKWY","PKY":"PKWY"
}
DIR_MAP = {"NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W","N":"N","S":"S","E":"E","W":"W"}
UNIT_PAT = re.compile(r"\b(?:UNIT|APT|APARTMENT|#|SUITE|NO|NUMBER)\s*([A-Z0-9\-]+)\b", re.I)

def norm_tokens(s):
  s = (s or "")
  s = str(s).upper().strip()
  s = re.sub(r"[.,;:]", " ", s)
  s = re.sub(r"\s+"," ",s).strip()
  return s

def norm_town(t):
  t = norm_tokens(t)
  t = re.sub(r"^(CITY OF|TOWN OF)\s+","",t).strip()
  t = re.sub(r",\s*MA$","",t).strip()
  t = re.sub(r"\s+MA$","",t).strip()
  return t

def split_addr(addr):
  s = norm_tokens(addr)
  # strip unit phrases
  s2 = UNIT_PAT.sub(" ", s)
  s2 = re.sub(r"\s+"," ",s2).strip()
  m = re.match(r"^\s*(\d+)\s+(.*)$", s2)
  if not m:
    return None, "", s2
  return int(m.group(1)), m.group(2).strip(), s2

def norm_street(street, mode="alias"):
  s = norm_tokens(street)
  toks = s.split(" ") if s else []
  toks = [DIR_MAP.get(t,t) for t in toks]
  if not toks:
    return ""
  last = toks[-1]
  if mode == "alias":
    if last in SUFFIX_MAP:
      toks[-1] = SUFFIX_MAP[last]
  elif mode == "nosuf":
    if last in SUFFIX_MAP:
      toks = toks[:-1]
  return " ".join([t for t in toks if t]).strip()

def build_spine_set(spine_path):
  by_town = defaultdict(set)
  with open(spine_path,'r',encoding='utf-8') as f:
    for line in f:
      if not line.strip(): continue
      r=json.loads(line)
      town = norm_town(r.get("town"))
      no   = r.get("street_no")
      st   = r.get("street_name")
      if not town or not no or not st: 
        continue
      try:
        no_i = int(no) if not isinstance(no,int) else no
      except:
        continue
      if no_i<=0: 
        continue
      st_alias = norm_street(st, "alias")
      st_nosuf = norm_street(st, "nosuf")
      if st_alias:
        by_town[town].add((no_i, st_alias))
      if st_nosuf:
        by_town[town].add((no_i, st_nosuf))
  return by_town

def main(events_path, spine_path, out_path):
  spine = build_spine_set(spine_path)
  stats = Counter()
  samples = {"parse_fail": [], "found": [], "not_found": []}

  with open(events_path,'r',encoding='utf-8') as f:
    for i,line in enumerate(f):
      if not line.strip(): 
        continue
      e=json.loads(line)
      town = norm_town(e.get("town") or "")
      addr = e.get("addr") or ""
      no, street, cleaned = split_addr(addr)

      if not town:
        stats["missing_town"] += 1
        if len(samples["parse_fail"])<20:
          samples["parse_fail"].append({"event_id": e.get("event_id"), "town": e.get("town"), "addr": addr, "cleaned": cleaned, "reason":"missing_town"})
        continue

      if no is None or not street:
        stats["parse_no_leading_number"] += 1
        if len(samples["parse_fail"])<20:
          samples["parse_fail"].append({"event_id": e.get("event_id"), "town": town, "addr": addr, "cleaned": cleaned, "reason":"no_leading_number"})
        continue

      st_alias = norm_street(street, "alias")
      st_nosuf = norm_street(street, "nosuf")

      townset = spine.get(town, set())
      hit = False
      if st_alias and (no, st_alias) in townset:
        hit = True
      elif st_nosuf and (no, st_nosuf) in townset:
        hit = True

      if hit:
        stats["FOUND_IN_SPINE"] += 1
        if len(samples["found"])<30:
          samples["found"].append({"event_id": e.get("event_id"), "town": town, "addr": addr, "no": no, "street_alias": st_alias, "street_nosuf": st_nosuf})
      else:
        stats["NOT_FOUND_IN_SPINE"] += 1
        if len(samples["not_found"])<30:
          samples["not_found"].append({"event_id": e.get("event_id"), "town": town, "addr": addr, "no": no, "street_alias": st_alias, "street_nosuf": st_nosuf})

  out = {
    "inputs": {"events": events_path, "spine": spine_path},
    "summary": dict(stats),
    "samples": samples
  }

  os.makedirs(os.path.dirname(out_path), exist_ok=True)
  with open(out_path,'w',encoding='utf-8') as w:
    json.dump(out,w,indent=2)

  print("[ok] wrote", out_path)
  print("summary:", dict(stats))

if __name__=="__main__":
  import sys
  events_path = sys.argv[1]
  spine_path  = sys.argv[2]
  out_path    = sys.argv[3]
  main(events_path, spine_path, out_path)
