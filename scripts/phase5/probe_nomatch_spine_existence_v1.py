import json, re, sys
from collections import defaultdict, Counter

SPINE = r"publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"
EVENTS = r"publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__reattached_axis2_v1_24.ndjson"
OUT = r"publicData\_audit\registry\hampden_nomatch_spine_existence_v1.json"

SUFFIX_MAP = {
  "LA":"LN","LANE":"LN","LN":"LN",
  "STREET":"ST","ST":"ST","AVENUE":"AVE","AVE":"AVE","ROAD":"RD","RD":"RD",
  "DRIVE":"DR","DR":"DR","TERRACE":"TERR","TER":"TERR","TERR":"TERR",
  "CIRCLE":"CIR","CIR":"CIR","COURT":"CT","CT":"CT","BOULEVARD":"BLVD","BLVD":"BLVD",
  "PLACE":"PL","PL":"PL"
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
  s = UNIT_PAT.sub(" ", s)
  s = re.sub(r"\s+"," ",s).strip()
  m = re.match(r"^\s*(\d+)\s+(.*)$", s)
  if not m:
    return None, ""
  return int(m.group(1)), m.group(2).strip()

def norm_street(street, mode="alias"):
  s = norm_tokens(street)
  toks = s.split(" ") if s else []
  # directionals as tokens
  toks = [DIR_MAP.get(t,t) for t in toks]
  if not toks:
    return ""
  if mode in ("alias","nosuf"):
    last = toks[-1]
    if last in SUFFIX_MAP:
      toks[-1] = SUFFIX_MAP[last] if mode=="alias" else ""  # strip suffix for nosuf probe
      toks = [t for t in toks if t]
  return " ".join(toks).strip()

def build_spine_sets(spine_path):
  # store per-town sets for quick membership checks
  by_town = defaultdict(set)
  with open(spine_path,'r',encoding='utf-8') as f:
    for line in f:
      if not line.strip(): continue
      r = json.loads(line)
      town = norm_town(r.get("town"))
      no = r.get("street_no")
      st = r.get("street_name")
      if not town or not no or not st: 
        continue
      try:
        no_i = int(no) if not isinstance(no,int) else no
      except:
        continue
      if no_i<=0: 
        continue
      st_alias = norm_street(st, "alias")
      st_raw   = norm_street(st, "raw") if False else None  # unused
      st_nosuf = norm_street(st, "nosuf")
      by_town[town].add((no_i, st_alias))
      if st_nosuf:
        by_town[town].add((no_i, st_nosuf))
  return by_town

def main():
  spine = build_spine_sets(SPINE)

  rows=[]
  stats=Counter()

  with open(EVENTS,'r',encoding='utf-8') as f:
    for line in f:
      if not line.strip(): continue
      e=json.loads(line)
      if e.get("attach_status")!="UNKNOWN": 
        continue
      if e.get("why")!="no_match":
        continue

      town = norm_town(e.get("town"))
      addr = e.get("addr") or ""
      no, street = split_addr(addr)
      if not town or not no or not street:
        stats["skip_missing_parse"] += 1
        continue

      st_alias = norm_street(street,"alias")
      st_nosuf = norm_street(street,"nosuf")

      townset = spine.get(town,set())

      hit_alias = (no, st_alias) in townset if st_alias else False
      hit_nosuf = (no, st_nosuf) in townset if st_nosuf else False

      status = "FOUND_IN_SPINE" if (hit_alias or hit_nosuf) else "NOT_FOUND_IN_SPINE"
      stats[status] += 1

      if len(rows) < 200:
        rows.append({
          "event_id": e.get("event_id"),
          "town": town,
          "addr": addr,
          "parsed": {"street_no": no, "street_alias": st_alias, "street_nosuf": st_nosuf},
          "spine_hit": {"alias": hit_alias, "nosuf": hit_nosuf},
          "status": status
        })

  out = {
    "inputs": {"events": EVENTS, "spine": SPINE},
    "summary": dict(stats),
    "samples": rows
  }

  import os
  os.makedirs(os.path.dirname(OUT), exist_ok=True)
  with open(OUT,'w',encoding='utf-8') as w:
    json.dump(out,w,indent=2)

  print("[ok] wrote", OUT)
  print("summary:", dict(stats))

if __name__=="__main__":
  main()
