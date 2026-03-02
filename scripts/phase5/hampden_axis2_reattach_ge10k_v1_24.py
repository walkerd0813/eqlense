import argparse, json, re, time
from collections import defaultdict

def now(): return time.time()
def up(s): return (s or "").strip().upper()
def ws(s): return re.sub(r"\s+"," ", (s or "").strip())

TOWN_ALIAS = {
  "FEEDING HILLS": "AGAWAM",
  "SIXTEEN ACRES": "SPRINGFIELD",
}

SUFFIX_SET = set([
  "ST","STREET","AVE","AV","AVENUE","RD","ROAD","DR","DRIVE","LN","LA","LANE",
  "CT","COURT","CIR","CR","CIRCLE","TER","TE","TERR","TERRACE",
  "PKWY","PKY","PARKWAY","BLVD","BOULEVARD","PL","PLACE","WAY",
  "HL","HILL","SQ","SQUARE"
])

def norm_town(town_raw):
  t = up(town_raw)
  t = re.sub(r"\(.*?\)", "", t).strip()
  t = ws(t)
  return TOWN_ALIAS.get(t, t)

def street_no_prefix(street_no_or_addr):
  s = up(str(street_no_or_addr or ""))
  m = re.match(r"^\s*(\d+)", s)
  return m.group(1) if m else ""

def clean_text(s):
  s = up(s)
  s = re.sub(r"[^A-Z0-9# ]+", " ", s)
  return ws(s)

def extract_unit(addr):
  # returns unit like "305" or "G" or ""
  s = clean_text(addr)
  # normalize "# 305" => "#305"
  s = re.sub(r"#\s+(\w+)", r"#\1", s)
  # patterns: UNIT X | APT X | STE X | #X
  m = re.search(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE)\s+([A-Z0-9]+)\b", s)
  if m: return m.group(1)
  m = re.search(r"#([A-Z0-9]+)\b", s)
  if m: return m.group(1)
  return ""

def street_base_from_tokens(tokens):
  # strip trailing suffix tokens
  t = list(tokens)
  while t and t[-1] in SUFFIX_SET:
    t.pop()
  return " ".join(t).strip()

def norm_street_base_from_spine(street_name):
  s = clean_text(street_name)
  toks = s.split(" ") if s else []
  return street_base_from_tokens(toks)

def norm_street_base_from_addr(addr):
  s = clean_text(addr)
  # remove leading number
  s = re.sub(r"^\d+\s+", "", s)
  # remove unit tail fragments so they don't pollute base
  s = re.sub(r"\b(?:UNIT|APT|APARTMENT|STE|SUITE)\b.*$", "", s).strip()
  s = re.sub(r"#\w+\b.*$", "", s).strip()
  toks = s.split(" ") if s else []
  return street_base_from_tokens(toks)

def build_spine_index(spine_path, towns_needed, progress_every=200000):
  idx_full = defaultdict(list)  # town|no|base -> [property_id...]
  idx_unit = defaultdict(list)  # town|no|base|unit -> [property_id...]
  scanned=kept=town_skip=no_key=0
  t0=now()

  towns_needed=set(towns_needed)

  with open(spine_path,"r",encoding="utf-8") as f:
    for line in f:
      scanned += 1
      r=json.loads(line)

      town = norm_town(r.get("town"))
      if town not in towns_needed:
        town_skip += 1
        if scanned % progress_every == 0:
          print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} elapsed_s={now()-t0:.1f}")
        continue

      no_pref = street_no_prefix(r.get("street_no"))
      base = norm_street_base_from_spine(r.get("street_name") or "")
      if not town or not no_pref or not base:
        no_key += 1
        continue

      pid = r.get("property_id")
      if not pid:
        continue

      k = f"{town}|{no_pref}|{base}"
      idx_full[k].append(pid)

      u = up(str(r.get("unit") or "")).replace("#","").strip()
      if u:
        ku = f"{town}|{no_pref}|{base}|{u}"
        idx_unit[ku].append(pid)

      kept += 1

      if scanned % progress_every == 0:
        print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} elapsed_s={now()-t0:.1f}")

  print("[ok] spine index built full=",len(idx_full),"unit=",len(idx_unit),
        "debug=",{"scanned_rows":scanned,"kept_rows":kept,"no_key":no_key,"town_skip":town_skip},
        "elapsed_s=",round(now()-t0,1))
  return idx_full, idx_unit

def attach_one(town, addr):
  townN = norm_town(town)
  no_pref = street_no_prefix(addr)
  base = norm_street_base_from_addr(addr)
  unit = extract_unit(addr)

  if not no_pref:
    return ("UNKNOWN","no_num",None,"NO_NUM")

  if not base:
    return ("UNKNOWN","no_street",None,"NO_STREET")

  # unit-exact first
  if unit:
    ku = f"{townN}|{no_pref}|{base}|{unit}"
    c = UNIT_COUNTS.get(ku,0)
    if c == 1:
      return ("ATTACHED_A","axis2_no_base_unit_unique", UNIT_FIRST[ku], None)
    elif c > 1:
      return ("UNKNOWN","collision",None,"COLLISION_UNIT")

  # base unique
  k = f"{townN}|{no_pref}|{base}"
  c = FULL_COUNTS.get(k,0)
  if c == 1:
    return ("ATTACHED_A","axis2_no_base_unique", FIRST[k], None)
  elif c > 1:
    return ("UNKNOWN","collision",None,"COLLISION")

  return ("UNKNOWN","no_match",None,"NO_MATCH")

def main():
  ap=argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args=ap.parse_args()

  print("=== AXIS2 REATTACH (>=10k) v1_24 (no_prefix + street_base + unit + town_alias) ===")
  print("[info] events:", args.events)
  print("[info] spine :", args.spine)

  # load events + towns needed
  events=[]
  towns=set()
  with open(args.events,"r",encoding="utf-8") as f:
    for line in f:
      r=json.loads(line)
      events.append(r)
      towns.add(norm_town(r.get("town")))
  print("[info] events rows:", len(events), "towns_needed:", len(towns))

  global FULL_COUNTS, FIRST, UNIT_COUNTS, UNIT_FIRST
  idx_full, idx_unit = build_spine_index(args.spine, towns)

  # precompute counts + first pid
  FULL_COUNTS={}
  FIRST={}
  for k,arr in idx_full.items():
    FULL_COUNTS[k]=len(arr)
    FIRST[k]=arr[0]

  UNIT_COUNTS={}
  UNIT_FIRST={}
  for k,arr in idx_unit.items():
    UNIT_COUNTS[k]=len(arr)
    UNIT_FIRST[k]=arr[0]

  stats={"attached_a":0,"still_unknown":0}
  out_rows=0

  with open(args.out,"w",encoding="utf-8") as fo:
    for r in events:
      town=r.get("town") or ""
      addr=r.get("addr") or ""

      st, mm, pid, why = attach_one(town, addr)

      a = r.get("attach")
      if not isinstance(a,dict):
        a={}
      a["attach_scope"]=a.get("attach_scope") or "SINGLE"
      a["attach_status"]=st
      a["match_method"]=mm
      a["why"]=why
      if pid:
        a["property_id"]=pid
      r["attach"]=a

      if st=="ATTACHED_A": stats["attached_a"] += 1
      else: stats["still_unknown"] += 1

      fo.write(json.dumps(r, ensure_ascii=False) + "\n")
      out_rows += 1

  with open(args.audit,"w",encoding="utf-8") as fa:
    json.dump({"out":args.out,"events":args.events,"spine":args.spine,"stats":stats}, fa, indent=2)

  print("[done] wrote out_rows=",out_rows,"stats=",stats,"audit=",args.audit)

if __name__=="__main__":
  main()
