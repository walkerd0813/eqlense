# scripts\phase5\hampden_axis2_reattach_ge10k_v1_27.py
import argparse, json, re, time
from collections import Counter, defaultdict

def norm_ws(s): return re.sub(r"\s+", " ", (s or "").strip())
def up(s): return norm_ws(s).upper()

DIR_TOKENS = {"N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST"}

# normalize toward YOUR SPINE DIALECT (short forms seen in spine: TE, CR, AV, ST, RD, DR, LN, BL, PW)
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
  "WAY":"WY","WY":"WY",
  "COURT":"CT","CT":"CT",
  "PLACE":"PL","PL":"PL",
  "TRAIL":"TR","TRL":"TR","TR":"TR",
  "EXT":"EXTN","EXTN":"EXTN","EXTENSION":"EXTN",
}

def collapse_runs(s):
  # deterministic typo smoother: MELWOOOD -> MELWOOD (OOO->OO)
  return re.sub(r"([A-Z])\1{2,}", r"\1\1", s)

def split_tokens(s):
  return [t for t in re.split(r"[^A-Z0-9#]+", up(s)) if t]

def normalize_unit_from_tokens(toks):
  unit = None
  out = []
  i = 0
  while i < len(toks):
    t = toks[i]
    if t in ("UNIT","APT","APARTMENT","STE","SUITE","#"):
      if i+1 < len(toks):
        unit = toks[i+1].lstrip("#")
        i += 2
        continue
    if t.startswith("#") and len(t) > 1:
      unit = t[1:]
      i += 1
      continue
    out.append(t)
    i += 1
  return out, unit

def norm_suffix(tok):
  tok = up(tok)
  return SUF_ALIAS_TO_SPINE.get(tok, tok)

def canonicalize_event_addr(addr_raw):
  s = collapse_runs(up(addr_raw))
  toks = split_tokens(s)

  # extract number
  num = None
  rest = []
  for t in toks:
    if num is None and t.isdigit():
      num = t
      continue
    rest.append(t)

  # strip directionals for matching
  rest = [t for t in rest if t not in DIR_TOKENS]

  # unit normalize
  rest, unit = normalize_unit_from_tokens(rest)

  # suffix normalize last alpha token
  if rest and rest[-1].isalpha():
    rest[-1] = norm_suffix(rest[-1])

  street = " ".join(rest).strip() if rest else None
  full = (num + " " + street) if num and street else None

  # no-suffix street_base (for unique fallback)
  base = None
  if rest:
    base_tokens = rest[:-1] if (rest[-1].isalpha()) else rest
    base = " ".join(base_tokens).strip() if base_tokens else None

  return {"num": num, "street": street, "full": full, "base": base, "unit": unit}

def spine_get_town(r):
  for k in ("town","city","municipality","site_city","addr_city","address_city","muni","source_city","source_town"):
    v = r.get(k)
    if isinstance(v, str) and v.strip():
      return up(v)
  return None

def spine_get_unit(r):
  for k in ("unit","addr_unit","unit_no","unit_number","apt","apartment","suite"):
    v = r.get(k)
    if isinstance(v, str) and v.strip():
      return up(v)
  return None

def decide_unique(pids):
  if not pids: return None
  u = list(dict.fromkeys(pids))
  return u[0] if len(u)==1 else None

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  print("=== AXIS2 REATTACH (>=10k) v1_27 (spine street_no+street_name primary index + unit field) ===")
  print("[info] events:", args.events)
  print("[info] spine :", args.spine)

  events=[]
  towns_needed=set()
  with open(args.events,"r",encoding="utf-8") as f:
    for line in f:
      if not line.strip(): continue
      ev=json.loads(line)
      t=ev.get("town"); a=ev.get("addr")
      if isinstance(t,str) and t.strip(): towns_needed.add(up(t))
      events.append(ev)

  print("[info] events rows:", len(events), "towns_needed:", len(towns_needed))

  idx_full = defaultdict(list)          # (town, "86 PAULK TE") -> [pid]
  idx_full_unit = defaultdict(list)     # (town, full, unit) -> [pid]
  idx_nosuf = defaultdict(list)         # (town, num, base) -> [pid]

  scanned=kept=town_skip=no_key=0
  t0=time.time()

  with open(args.spine,"r",encoding="utf-8") as f:
    for line in f:
      if not line.strip(): continue
      scanned += 1
      r=json.loads(line)

      town = spine_get_town(r)
      if town not in towns_needed:
        town_skip += 1
        continue

      pid = r.get("property_id")
      if not pid:
        no_key += 1
        continue

      # PRIMARY: use structured fields if present (this is the main change)
      sn = r.get("street_no")
      st = r.get("street_name")

      if isinstance(sn,(int,float)): sn = str(int(sn))
      if isinstance(sn,str): sn = sn.strip()
      if isinstance(st,str): st = up(st)

      kept += 1

      if sn and st:
        # normalize suffix in spine street_name last token to spine dialect (idempotent)
        toks = st.split()
        if toks and toks[-1].isalpha():
          toks[-1] = norm_suffix(toks[-1])
        st2 = " ".join(toks).strip()
        full = f"{sn} {st2}"
        idx_full[(town, full)].append(pid)

        u = spine_get_unit(r)
        if u:
          idx_full_unit[(town, full, u)].append(pid)

        base_tokens = toks[:-1] if (toks and toks[-1].isalpha()) else toks
        base = " ".join(base_tokens).strip() if base_tokens else None
        if base:
          idx_nosuf[(town, sn, base)].append(pid)

      else:
        # fallback: parse full_address if structured fields missing
        fa = r.get("full_address") or ""
        if isinstance(fa,str) and fa.strip():
          k = canonicalize_event_addr(fa)
          if k["full"]:
            idx_full[(town, k["full"])].append(pid)
            u = spine_get_unit(r)
            if u and k["full"]:
              idx_full_unit[(town, k["full"], u)].append(pid)
          if k["num"] and k["base"]:
            idx_nosuf[(town, k["num"], k["base"])].append(pid)

      if scanned % 200000 == 0:
        print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} elapsed_s={time.time()-t0:.1f}")

  print("[ok] spine index built full=", len(idx_full), "unit=", len(idx_full_unit), "nosuf=", len(idx_nosuf),
        "debug=", {"scanned_rows": scanned, "kept_rows": kept, "no_key": no_key, "town_skip": town_skip},
        "elapsed_s=", round(time.time()-t0,1))

  stats=Counter()
  out_rows=0

  with open(args.out,"w",encoding="utf-8") as out:
    for ev in events:
      town = up(ev.get("town") or "")
      addr = ev.get("addr") or ""
      k = canonicalize_event_addr(addr) if isinstance(addr,str) else {"num":None,"street":None,"full":None,"base":None,"unit":None}

      st="UNKNOWN"; mm="no_match"; why="NO_MATCH"; pid=None

      if not k["num"]:
        st="UNKNOWN"; mm="no_num"; why="NO_NUM"
      elif town and k["full"]:
        u = decide_unique(idx_full.get((town, k["full"]), []))
        if u:
          st="ATTACHED_A"; mm="axis2_full_structured_exact"; why=None; pid=u
        elif k["unit"]:
          uu = decide_unique(idx_full_unit.get((town, k["full"], up(k["unit"])), []))
          if uu:
            st="ATTACHED_A"; mm="axis2_full+unit_structured_exact"; why=None; pid=uu
        if st!="ATTACHED_A" and k["base"]:
          u2 = decide_unique(idx_nosuf.get((town, k["num"], k["base"]), []))
          if u2:
            st="ATTACHED_A"; mm="axis2_street_nosuf_unique"; why=None; pid=u2

      ev_out=dict(ev)
      ev_out["attach_status"]=st
      ev_out["match_method"]=mm
      ev_out["why"]=why
      ev_out["matched_property_id"]=pid
      out.write(json.dumps(ev_out, ensure_ascii=False) + "\n")
      out_rows += 1
      stats["attached_a" if st=="ATTACHED_A" else "still_unknown"] += 1

  with open(args.audit,"w",encoding="utf-8") as f:
    json.dump({"script":"hampden_axis2_reattach_ge10k_v1_27.py","events":args.events,"spine":args.spine,"out":args.out,"stats":dict(stats)}, f, indent=2)

  print("[done] wrote out_rows=", out_rows, "stats=", dict(stats), "audit=", args.audit)

if __name__=="__main__":
  main()
