#!/usr/bin/env python3
import argparse, json, re
from collections import Counter, defaultdict

STREET_SUFFIX = {
  "ST","STREET","AVE","AVENUE","RD","ROAD","DR","DRIVE","LN","LANE","CT","COURT",
  "PL","PLACE","PKY","PKWY","PARKWAY","BLVD","BOULEVARD","TER","TERR","TERRACE",
  "WAY","HWY","HIGHWAY","CIR","CIRCLE","PLZ","PLAZA","SQ","SQUARE","EXT","EXTENSION"
}
DIRS = {"N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST"}

def read_ndjson(path):
  with open(path, "r", encoding="utf-8") as f:
    for line in f:
      line=line.strip()
      if not line:
        continue
      yield json.loads(line)

def write_ndjson(path, rows):
  with open(path, "w", encoding="utf-8") as f:
    for r in rows:
      f.write(json.dumps(r, ensure_ascii=False) + "\n")

def get_str(v):
  if v is None:
    return ""
  if isinstance(v, str):
    return v
  if isinstance(v, dict):
    for k in ("addr","address","address_raw","address_norm","full_address","text","value"):
      vv = v.get(k)
      if isinstance(vv, str) and vv.strip():
        return vv
    for vv in v.values():
      if isinstance(vv, str) and vv.strip():
        return vv
    return ""
  try:
    return str(v)
  except Exception:
    return ""

def norm_town(s: str) -> str:
  s = get_str(s).strip().upper()
  s = re.sub(r"\s+", " ", s)
  return s

def split_num_and_rest(addr: str):
  addr = get_str(addr).strip().upper()
  addr = re.sub(r"\s+", " ", addr)
  if not addr:
    return None, None, addr
  m = re.match(r"^(\d+(?:-\d+)?)\s+(.*)$", addr)
  if not m:
    return None, None, addr
  return m.group(1), m.group(2), addr

def street_tokens(rest: str):
  toks = [t for t in re.split(r"[\s,]+", rest.strip().upper()) if t]
  unit_markers = {"UNIT","APT","APARTMENT","#","SUITE","STE","FL","FLOOR"}
  out=[]
  for t in toks:
    if t in unit_markers:
      break
    out.append(t)
  return out

def norm_street(rest: str) -> str:
  toks = street_tokens(rest)
  toks = [t for t in toks if t != "&"]
  while toks and toks[0] in DIRS:
    toks = toks[1:]
  while toks and toks[-1] in DIRS:
    toks = toks[:-1]
  if toks and toks[-1] in STREET_SUFFIX:
    toks = toks[:-1]
  s = " ".join(toks)
  s = re.sub(r"[^A-Z0-9 ]+", "", s)
  s = re.sub(r"\s+", " ", s).strip()
  return s

def edit_distance_lev1(a: str, b: str) -> int:
  if a == b:
    return 0
  la, lb = len(a), len(b)
  if abs(la-lb) > 1:
    return 2
  if la == lb:
    dif = sum(1 for i in range(la) if a[i] != b[i])
    return 1 if dif == 1 else 2
  if la + 1 == lb:
    i=j=0
    dif=0
    while i<la and j<lb:
      if a[i]==b[j]:
        i+=1; j+=1
      else:
        dif+=1; j+=1
        if dif>1: return 2
    return 1
  if lb + 1 == la:
    return edit_distance_lev1(b,a)
  return 2

def build_spine_index(spine_path: str):
  idx = defaultdict(list)
  for r in read_ndjson(spine_path):
    town = norm_town(r.get("town") or r.get("city") or r.get("municipality") or "")
    addr = get_str(r.get("address") or r.get("addr") or r.get("site_addr") or r.get("full_address") or "")
    num_raw, rest, _ = split_num_and_rest(addr)
    if not num_raw or not rest:
      continue
    if "-" in num_raw:
      continue
    try:
      no = int(num_raw)
    except Exception:
      continue
    st = norm_street(rest)
    if not st:
      continue
    pid = r.get("property_id") or r.get("propertyId") or r.get("id") or None
    idx[(town,no)].append({"property_id": pid, "street": st, "addr": addr})
  return idx

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--in", dest="inp", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  args = ap.parse_args()

  spine_idx = build_spine_index(args.spine)

  out_rows=[]
  stats = Counter()
  audit = {
    "version": "v1_34_0",
    "notes": "Conservative rescue for UNKNOWN rows: tiny range-no (<=4) + fuzzy street lev<=1 within same town+number, unique only.",
    "counts": {}
  }

  for row in read_ndjson(args.inp):
    attach_status = row.get("attach_status") or row.get("attachStatus") or ""
    scope = row.get("attach_scope") or row.get("attachScope") or "SINGLE"

    if attach_status != "UNKNOWN" or scope != "SINGLE":
      stats["pass_through"] += 1
      out_rows.append(row)
      continue

    town = norm_town(row.get("town") or row.get("city") or "")
    addr = get_str(row.get("addr") or row.get("address") or "")
    num_raw, rest, _ = split_num_and_rest(addr)
    if not num_raw:
      stats["no_num"] += 1
      out_rows.append(row)
      continue

    nums=[]
    if "-" in num_raw:
      parts = num_raw.split("-", 1)
      try:
        a=int(parts[0]); b=int(parts[1])
        if a>0 and b>0 and abs(a-b) <= 4:
          nums=[a,b]
      except Exception:
        nums=[]
    else:
      try:
        nums=[int(num_raw)]
      except Exception:
        nums=[]

    if not nums:
      stats["no_num"] += 1
      out_rows.append(row)
      continue

    stq = norm_street(rest or "")
    if not stq:
      stats["no_street"] += 1
      out_rows.append(row)
      continue

    cand=[]
    for no in nums:
      cand.extend(spine_idx.get((town,no), []))

    if not cand:
      stats["no_spine_candidates_same_no"] += 1
      out_rows.append(row)
      continue

    scored=[]
    for c in cand:
      d = edit_distance_lev1(stq, c["street"])
      if d <= 1:
        scored.append((d,c))

    if not scored:
      stats["spine_has_same_no_but_no_close_street"] += 1
      out_rows.append(row)
      continue

    scored.sort(key=lambda x: x[0])
    best_d = scored[0][0]
    best = [c for d,c in scored if d==best_d]

    if len(best) != 1:
      stats["collision"] += 1
      out_rows.append(row)
      continue

    chosen = best[0]
    row["attach_status"] = "ATTACHED_B"
    if len(nums)==2:
      row["match_method"] = "axis2_range_no_unique"
    else:
      row["match_method"] = "axis2_street_no_fuzzy_unique_leq1" if best_d==1 else "axis2_street_unique_exact"
    row["why"] = "NONE"
    if row.get("property_id") is None and chosen.get("property_id") is not None:
      row["property_id"] = chosen.get("property_id")

    stats["attach_fuzzy_unique"] += 1
    out_rows.append(row)

  audit["counts"] = dict(stats)
  audit_path = re.sub(r"\.ndjson$", "", args.out) + "__audit_v1_34_0.json"
  write_ndjson(args.out, out_rows)
  with open(audit_path, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print("[done] v1_34_0 rescue")
  for k,v in stats.most_common():
    print(f"  {k}: {v}")
  print("[ok] OUT  ", args.out)
  print("[ok] AUDIT", audit_path)

if __name__ == "__main__":
  main()
