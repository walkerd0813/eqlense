import json, re
from collections import defaultdict

def norm_ws(s): return re.sub(r"\s+", " ", (s or "").strip())
def up(s): return norm_ws(s).upper()

DIR_TOKENS = {"N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST"}

SUF_ALIAS_TO_SPINE = {
  "STREET":"ST","ST":"ST","ST.":"ST",
  "AVENUE":"AV","AVE":"AV","AV":"AV","AV.":"AV","AVE.":"AV",
  "ROAD":"RD","RD":"RD","RD.":"RD",
  "DRIVE":"DR","DR":"DR","DR.":"DR",
  "LANE":"LN","LN":"LN","LN.":"LN",
  "BOULEVARD":"BL","BLVD":"BL","BLVD.":"BL","BL":"BL",
  "PARKWAY":"PW","PKWY":"PW","PKWY.":"PW","PW":"PW",
  "TERRACE":"TE","TERR":"TE","TER":"TE","TE":"TE",
  "CIRCLE":"CR","CIR":"CR","CR":"CR",
  "HILL":"HL","HL":"HL",
  "COURT":"CT","CT":"CT",
  "PLACE":"PL","PL":"PL",
  "WAY":"WY","WY":"WY",
}

def collapse_runs(s):
  return re.sub(r"([A-Z])\1{2,}", r"\1\1", s)

def split_tokens(s):
  return [t for t in re.split(r"[^A-Z0-9#]+", up(s)) if t]

def normalize_unit_from_tokens(toks):
  unit=None
  out=[]
  i=0
  while i<len(toks):
    t=toks[i]
    if t in ("UNIT","APT","APARTMENT","STE","SUITE","#"):
      if i+1<len(toks):
        unit=toks[i+1].lstrip("#"); i+=2; continue
    if t.startswith("#") and len(t)>1:
      unit=t[1:]; i+=1; continue
    out.append(t); i+=1
  return out, unit

def norm_suffix(tok):
  tok=up(tok)
  return SUF_ALIAS_TO_SPINE.get(tok, tok)

def canonicalize_event_addr(addr_raw):
  s=collapse_runs(up(addr_raw))
  toks=split_tokens(s)
  num=None; rest=[]
  for t in toks:
    if num is None and t.isdigit():
      num=t; continue
    rest.append(t)
  rest=[t for t in rest if t not in DIR_TOKENS]
  rest, unit = normalize_unit_from_tokens(rest)
  if rest and rest[-1].isalpha():
    rest[-1]=norm_suffix(rest[-1])
  street=" ".join(rest).strip() if rest else None
  full=(num+" "+street) if num and street else None
  base=None
  if rest:
    base_tokens=rest[:-1] if rest[-1].isalpha() else rest
    base=" ".join(base_tokens).strip() if base_tokens else None
  return {"num":num,"street":street,"full":full,"base":base,"unit":unit}

def spine_get_town(r):
  for k in ("town","city","municipality","site_city","addr_city","address_city","muni","source_city","source_town"):
    v=r.get(k)
    if isinstance(v,str) and v.strip(): return up(v)
  return None

def spine_get_unit(r):
  for k in ("unit","addr_unit","unit_no","unit_number","apt","apartment","suite"):
    v=r.get(k)
    if isinstance(v,str) and v.strip(): return up(v)
  return None

def build_index(spine_path, towns_needed):
  idx_full=defaultdict(list)
  idx_full_unit=defaultdict(list)
  idx_nosuf=defaultdict(list)

  with open(spine_path,"r",encoding="utf-8") as f:
    for line in f:
      if not line.strip(): continue
      r=json.loads(line)
      town=spine_get_town(r)
      if town not in towns_needed: 
        continue
      pid=r.get("property_id")
      if not pid: 
        continue

      sn=r.get("street_no"); st=r.get("street_name")
      if isinstance(sn,(int,float)): sn=str(int(sn))
      if isinstance(sn,str): sn=sn.strip()
      if isinstance(st,str): st=up(st)

      if sn and st:
        toks=st.split()
        if toks and toks[-1].isalpha(): toks[-1]=norm_suffix(toks[-1])
        st2=" ".join(toks).strip()
        full=f"{sn} {st2}"
        idx_full[(town,full)].append(pid)
        u=spine_get_unit(r)
        if u: idx_full_unit[(town,full,u)].append(pid)
        base_tokens=toks[:-1] if (toks and toks[-1].isalpha()) else toks
        base=" ".join(base_tokens).strip() if base_tokens else None
        if base: idx_nosuf[(town,sn,base)].append(pid)
      else:
        fa=r.get("full_address") or ""
        if isinstance(fa,str) and fa.strip():
          k=canonicalize_event_addr(fa)
          if k["full"]: idx_full[(town,k["full"])].append(pid)
          u=spine_get_unit(r)
          if u and k["full"]: idx_full_unit[(town,k["full"],u)].append(pid)
          if k["num"] and k["base"]: idx_nosuf[(town,k["num"],k["base"])].append(pid)

  return idx_full, idx_full_unit, idx_nosuf

def main():
  events_path=r"publicData\registry\hampden\_attached_DEED_ONLY_v1_8_1_MULTI\axis2_candidates_ge_10k__events_hydrated_v1.ndjson"
  spine_path=r"publicData\properties\_attached\phase4_assessor_unknown_classify_v1\properties__phase4_assessor_canonical_unknown_classified__2025-12-27T16-50-45-410__V1.ndjson"

  # probe these exact ones:
  targets = [
    ("SPRINGFIELD","86 PAULK TER"),
    ("EAST LONGMEADOW","89 PINE GROVE CIR"),
    ("HOLYOKE","75 CHERRY HL"),
    ("EAST LONGMEADOW","101 MELWOOOD AVE"),
  ]
  towns=set([t[0] for t in targets])
  idx_full, idx_full_unit, idx_nosuf = build_index(spine_path, towns)

  print("index sizes", len(idx_full), len(idx_full_unit), len(idx_nosuf))

  for town, addr in targets:
    k=canonicalize_event_addr(addr)
    print("\n=== EVENT ===")
    print("town_raw:", town, "addr_raw:", addr)
    print("canon:", k)

    full_key=(up(town), k["full"])
    nosuf_key=(up(town), k["num"], k["base"])
    unit_key=(up(town), k["full"], up(k["unit"]) if k["unit"] else None)

    print("lookup full_key:", full_key, "hits:", idx_full.get(full_key, []))
    if unit_key[2]:
      print("lookup unit_key:", unit_key, "hits:", idx_full_unit.get(unit_key, []))
    print("lookup nosuf_key:", nosuf_key, "hits:", idx_nosuf.get(nosuf_key, []))

if __name__=='__main__':
  main()
