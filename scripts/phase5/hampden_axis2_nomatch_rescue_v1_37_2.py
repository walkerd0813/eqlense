\
#!/usr/bin/env python3
import argparse, json, re, sys, os
from collections import Counter, defaultdict

STREET_SUFFIX_MAP = {
  "STREET":"ST","ST":"ST","ROAD":"RD","RD":"RD","AVENUE":"AVE","AVE":"AVE",
  "BOULEVARD":"BLVD","BLVD":"BLVD","DRIVE":"DR","DR":"DR","LANE":"LN","LN":"LN",
  "COURT":"CT","CT":"CT","CIRCLE":"CIR","CIR":"CIR","PLACE":"PL","PL":"PL",
  "TERRACE":"TER","TER":"TER","PARKWAY":"PKY","PKY":"PKY","HIGHWAY":"HWY","HWY":"HWY",
  "WAY":"WAY","TRAIL":"TRL","TRL":"TRL"
}

TOWN_KEYS = {
  "town","city","municipality","muni","community","locality","jurisdiction","mailing_city","post_town"
}
ADDR_KEYS = {
  "addr","address","address_raw","address1","address_line1","site_address","property_address",
  "location","full_address","street_address","situs","situs_address","premise","premise_address"
}

def is_str(x):
  return isinstance(x, str) and x.strip() != ""

def norm_spaces(s: str) -> str:
  return re.sub(r"\s+", " ", s.strip())

def norm_town(s: str) -> str:
  s = norm_spaces(s).upper()
  # strip MA, commas etc
  s = re.sub(r",\s*MA(\s+\d{5})?$", "", s)
  s = s.replace(",", " ")
  return norm_spaces(s)

def norm_addr_basic(s: str) -> str:
  s = norm_spaces(s).upper()
  # kill punctuation except dash and #
  s = re.sub(r"[^\w\s\-\#]", " ", s)
  s = norm_spaces(s)
  # normalize suffix at end if present
  toks = s.split(" ")
  if toks:
    last = toks[-1]
    if last in STREET_SUFFIX_MAP:
      toks[-1] = STREET_SUFFIX_MAP[last]
  return " ".join(toks)

def extract_house_no(addr: str):
  # Accept leading number like 151 or 151-153 or 19-21
  m = re.match(r"^\s*(\d+)(?:\s*-\s*(\d+))?\b", addr)
  if not m:
    return None, None
  a = int(m.group(1))
  b = int(m.group(2)) if m.group(2) else None
  return a, b

def strip_unit(addr: str) -> str:
  # remove trailing UNIT/APT/# bits for street comparison
  s = addr
  s = re.sub(r"\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b.*$", "", s).strip()
  return norm_spaces(s)

def walk_find_first(obj, keyset):
  """Find first string value for any key in keyset (case-insensitive) recursively."""
  if isinstance(obj, dict):
    for k, v in obj.items():
      kl = str(k).lower()
      if kl in keyset and is_str(v):
        return v
    for _, v in obj.items():
      r = walk_find_first(v, keyset)
      if r is not None:
        return r
  elif isinstance(obj, list):
    for v in obj:
      r = walk_find_first(v, keyset)
      if r is not None:
        return r
  return None

def walk_collect_strings(obj, keyset, out):
  """Collect candidate strings for keys in keyset recursively (keep order)."""
  if isinstance(obj, dict):
    for k, v in obj.items():
      kl = str(k).lower()
      if kl in keyset and is_str(v):
        out.append(v)
    for _, v in obj.items():
      walk_collect_strings(v, keyset, out)
  elif isinstance(obj, list):
    for v in obj:
      walk_collect_strings(v, keyset, out)

def recover_town_addr(row):
  town = row.get("town")
  addr = row.get("addr")

  recovered = {"town_from": None, "addr_from": None}

  # town
  if not is_str(town):
    t = walk_find_first(row, TOWN_KEYS)
    if is_str(t):
      town = t
      recovered["town_from"] = "recursive_keys"

  # addr
  if not is_str(addr):
    # try gather address-like strings, prefer ones containing a leading number
    cands = []
    walk_collect_strings(row, ADDR_KEYS, cands)
    best = None
    for c in cands:
      if re.match(r"^\s*\d+\b", c):
        best = c
        break
    if best is None and cands:
      best = cands[0]
    if is_str(best):
      addr = best
      recovered["addr_from"] = "recursive_keys"

  # reconstruct from common parts if still missing
  if not is_str(addr):
    house = row.get("house_no") or row.get("house_number") or row.get("st_no") or row.get("street_no")
    street = row.get("street") or row.get("street_name") or row.get("st_name")
    if is_str(str(house)) and is_str(street):
      addr = f"{house} {street}"
      recovered["addr_from"] = "parts"

  return town, addr, recovered

def build_spine_index(spine_path):
  # index: town -> house_no -> list of (street_norm, property_id)
  idx = defaultdict(lambda: defaultdict(list))
  with open(spine_path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      r = json.loads(line)
      prop_id = r.get("property_id")
      t = r.get("town") or r.get("city")
      a = r.get("address") or r.get("addr") or r.get("site_address") or r.get("full_address")
      if not (is_str(prop_id) and is_str(t) and is_str(a)):
        continue
      tN = norm_town(str(t))
      aS = strip_unit(str(a))
      aN = norm_addr_basic(aS)
      h1, h2 = extract_house_no(aN)
      if h1 is None:
        continue
      # street tokenization: remove leading number portion
      street_part = re.sub(r"^\d+(?:\s*-\s*\d+)?\s+", "", aN)
      idx[tN][h1].append((street_part, prop_id))
      if h2 is not None:
        # store range end as well for lookups (rare in spine but safe)
        idx[tN][h2].append((street_part, prop_id))
  return idx

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--in", dest="inp", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  spine_idx = build_spine_index(args.spine)

  counters = Counter()
  samples = defaultdict(list)

  with open(args.inp, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
    for line in fin:
      line = line.strip()
      if not line:
        continue
      row = json.loads(line)

      counters["rows_in"] += 1

      if row.get("attach_status") != "UNKNOWN" or row.get("match_method") != "no_match" or row.get("why") != "no_match":
        counters["pass_through"] += 1
        fout.write(json.dumps(row, ensure_ascii=False) + "\n")
        continue

      # Recover town/addr if missing
      town, addr, rec = recover_town_addr(row)
      if not is_str(town) or not is_str(addr):
        counters["eligible_but_missing_town_or_addr"] += 1
        if len(samples["missing_town_or_addr"]) < 10:
          samples["missing_town_or_addr"].append({
            "event_id": row.get("event_id"),
            "town": town,
            "addr": addr,
            "town_from": rec["town_from"],
            "addr_from": rec["addr_from"]
          })
        fout.write(json.dumps(row, ensure_ascii=False) + "\n")
        continue

      tN = norm_town(town)
      aS = strip_unit(addr)
      aN = norm_addr_basic(aS)
      h1, h2 = extract_house_no(aN)
      if h1 is None:
        counters["eligible_but_no_house_no"] += 1
        fout.write(json.dumps(row, ensure_ascii=False) + "\n")
        continue

      street_part = re.sub(r"^\d+(?:\s*-\s*\d+)?\s+", "", aN)

      cands = spine_idx.get(tN, {}).get(h1, [])
      if not cands:
        counters["eligible_no_spine_candidates_for_house"] += 1
        fout.write(json.dumps(row, ensure_ascii=False) + "\n")
        continue

      # exact street match only (suffix-normalized via norm_addr_basic)
      hits = [pid for (st, pid) in cands if st == street_part]
      hits = list(dict.fromkeys(hits))  # unique, stable order

      if len(hits) == 1:
        # rescue attach B (strict but still "inferred from addr text" not deed id)
        pid = hits[0]
        row["attach_status"] = "ATTACHED_B"
        row["match_method"] = "axis2_nomatch_rescue_strict_unique"
        row["why"] = "NONE"
        row["attachments_n"] = 1
        row["attachments"] = [{"property_id": pid, "confidence": "B", "method": "town+house+street_exact_unique"}]
        # also backfill town/addr if missing
        if not is_str(row.get("town")):
          row["town"] = town
        if not is_str(row.get("addr")):
          row["addr"] = addr

        counters["rescued"] += 1
        if rec["town_from"] or rec["addr_from"]:
          counters["rescued_after_recovery"] += 1
        if len(samples["rescued"]) < 10:
          samples["rescued"].append({
            "event_id": row.get("event_id"),
            "town": town,
            "addr": addr,
            "property_id": pid,
            "town_from": rec["town_from"],
            "addr_from": rec["addr_from"]
          })
      else:
        if len(hits) == 0:
          counters["eligible_no_street_hit"] += 1
        else:
          counters["eligible_collision_multi_hit"] += 1
          if len(samples["collision"]) < 10:
            samples["collision"].append({
              "event_id": row.get("event_id"),
              "town": town,
              "addr": addr,
              "hits_n": len(hits)
            })

      fout.write(json.dumps(row, ensure_ascii=False) + "\n")

  audit = {
    "in": args.inp,
    "spine": args.spine,
    "out": args.out,
    "counters": dict(counters),
    "samples": {k:v for k,v in samples.items()}
  }
  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print("[done] v1_37_2 NO_MATCH rescue")
  for k in ["rows_in","pass_through","rescued","rescued_after_recovery",
            "eligible_but_missing_town_or_addr","eligible_but_no_house_no",
            "eligible_no_spine_candidates_for_house","eligible_no_street_hit",
            "eligible_collision_multi_hit"]:
    if counters.get(k):
      print(f"  {k}: {counters[k]}")
  print(f"[ok] OUT   {args.out}")
  print(f"[ok] AUDIT {args.audit}")

if __name__ == "__main__":
  main()
