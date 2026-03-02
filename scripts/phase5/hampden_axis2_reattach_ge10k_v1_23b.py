import argparse, json, re, time
from collections import defaultdict

def norm_ws(s):
  return re.sub(r"\s+", " ", (s or "").strip())

def up(s):
  return norm_ws(s).upper()

def coerce_addr(x):
  """
  Accepts str/int/None/dict. Returns best-effort string.
  If dict looks like {value/text/address/...}, use that.
  Otherwise return "" and let caller fall back to address_raw.
  """
  if x is None:
    return ""
  if isinstance(x, str):
    return x
  if isinstance(x, (int, float)):
    return str(x)
  if isinstance(x, dict):
    # common “string holder” keys
    for k in ("value","text","addr","address","full","normalized","address_norm","address_clean","address_raw"):
      v = x.get(k)
      if isinstance(v, str) and v.strip():
        return v
    return ""
  # last resort
  try:
    return str(x)
  except Exception:
    return ""

SUFFIX_ALIAS = {
  # core street suffixes
  "STREET":"ST","ST":"ST",
  "ROAD":"RD","RD":"RD",
  "AVENUE":"AVE","AVE":"AVE",
  "BOULEVARD":"BLVD","BLVD":"BLVD",
  "DRIVE":"DR","DR":"DR",
  "COURT":"CT","CT":"CT",
  "CIRCLE":"CIR","CIR":"CIR",
  "TERRACE":"TERR","TERR":"TERR",
  "PLACE":"PL","PL":"PL",
  "WAY":"WAY",
  "LANE":"LN","LN":"LN",
  # Hampden-specific offender
  "LA":"LN",   # <<< critical: LA -> LN
}

UNIT_MARKERS = ("UNIT", "#", "APT", "APARTMENT")

def normalize_addr_tokens(addr):
  # robust: accept dict/None/etc
  raw = coerce_addr(addr)
  s = up(raw)
  if not s:
    return ""

  # normalize punctuation
  s = re.sub(r"[,\.;]", " ", s)
  s = norm_ws(s)

  # normalize "#G" -> "UNIT G"
  s = re.sub(r"\s+#\s*([A-Z0-9\-]+)\b", r" UNIT \1", s)

  toks = s.split(" ")
  out = []
  for t in toks:
    if not t:
      continue
    # strip trailing commas etc already done
    # map suffix aliases
    out.append(SUFFIX_ALIAS.get(t, t))
  s2 = " ".join(out)
  s2 = norm_ws(s2)
  return s2

def parse_num_and_rest(addr_norm):
  # expects already normalized tokens
  if not addr_norm:
    return "", ""
  m = re.match(r"^(\d+[A-Z]?)\s+(.*)$", addr_norm)
  if not m:
    return "", addr_norm
  return m.group(1), m.group(2)

def key_full(town, addr_norm):
  return f"{town}|{addr_norm}"

def key_street(town, rest):
  return f"{town}|{rest}"

def key_street_unit(town, rest, unit):
  return f"{town}|{rest}|{unit}"

def extract_unit(addr_norm):
  # returns unit token if present (UNIT 9 / UNIT G / UNIT 305)
  if not addr_norm:
    return ""
  m = re.search(r"\bUNIT\s+([A-Z0-9\-]+)\b", addr_norm)
  return m.group(1) if m else ""

def get_event_addr_string(rec):
  """
  Prefer rec.addr if string.
  Else use property_ref.address_norm if string.
  Else property_ref.address_clean/address_raw.
  If those are dicts, coerce_addr returns "" and we fall back.
  """
  a = rec.get("addr")
  a = coerce_addr(a)
  if a:
    return a

  pr = rec.get("property_ref") or {}
  for k in ("address_norm","address_clean","address_raw"):
    v = coerce_addr(pr.get(k))
    if v:
      return v

  return ""

def build_spine_index(spine_path, towns_needed):
  t0 = time.time()
  towns_needed = set([up(x) for x in towns_needed if x])

  idx_full = defaultdict(list)
  idx_street = defaultdict(list)
  idx_street_unit = defaultdict(list)
  street_only_counts = defaultdict(int)

  scanned = kept = town_skip = no_key = 0

  with open(spine_path, "r", encoding="utf-8") as f:
    for line in f:
      scanned += 1
      try:
        r = json.loads(line)
      except Exception:
        continue

      town = up(r.get("town") or "")
      if town and town not in towns_needed:
        town_skip += 1
        continue

      addr = coerce_addr(r.get("full_address"))
      if not addr:
        # some spine rows may have only street_no/street_name/unit
        sn = coerce_addr(r.get("street_no"))
        st = coerce_addr(r.get("street_name"))
        un = coerce_addr(r.get("unit"))
        if sn and st:
          addr = f"{sn} {st}" + (f" UNIT {un}" if un else "")
        else:
          no_key += 1
          continue

      kept += 1
      pid = r.get("property_id") or r.get("property_uid") or r.get("row_uid") or ""

      addr_norm = normalize_addr_tokens(addr)
      if not addr_norm:
        no_key += 1
        continue

      num, rest = parse_num_and_rest(addr_norm)
      unit = extract_unit(addr_norm)

      if addr_norm:
        idx_full[key_full(town, addr_norm)].append(pid)

      if rest:
        idx_street[key_street(town, rest)].append(pid)
        street_only_counts[key_street(town, rest)] += 1

      if rest and unit:
        idx_street_unit[key_street_unit(town, rest, unit)].append(pid)

      if scanned % 200000 == 0:
        print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} elapsed_s={time.time()-t0:.1f}")

  debug = {"scanned_rows": scanned, "kept_rows": kept, "no_key": no_key, "town_skip": town_skip}
  print(f"[ok] spine index built full={len(idx_full)} street={len(idx_street)} street_unit={len(idx_street_unit)} debug={debug} elapsed_s={time.time()-t0:.1f}")
  return idx_full, idx_street, idx_street_unit, street_only_counts

def try_attach_single(town, addr_raw, idx_full, idx_street, idx_street_unit, street_only_counts):
  # normalize event addr safely
  addr_norm = normalize_addr_tokens(addr_raw)
  if not addr_norm:
    return "UNKNOWN", "no_addr", None, "NO_ADDR"

  num, rest = parse_num_and_rest(addr_norm)
  if not num:
    return "UNKNOWN", "no_num", None, "NO_NUM"

  unit = extract_unit(addr_norm)

  # 1) full exact
  kf = key_full(town, addr_norm)
  hits = idx_full.get(kf) or []
  if len(hits) == 1:
    return "ATTACHED_A", "axis2_full_address_exact", hits[0], None
  if len(hits) > 1:
    return "UNKNOWN", "collision", None, "COLLISION"

  # 2) street+unit exact (condos)
  if unit and rest:
    ku = key_street_unit(town, rest, unit)
    hitsu = idx_street_unit.get(ku) or []
    if len(hitsu) == 1:
      return "ATTACHED_A", "axis2_street+unit_exact", hitsu[0], None
    if len(hitsu) > 1:
      return "UNKNOWN", "collision", None, "COLLISION"

  # 3) street unique (only if unique in spine within town)
  ks = key_street(town, rest)
  c = street_only_counts.get(ks, 0)
  if c == 1:
    hs = idx_street.get(ks) or []
    if len(hs) == 1:
      return "ATTACHED_A", "axis2_street_unique_exact", hs[0], None

  return "UNKNOWN", "no_match", None, "NO_MATCH"

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  print("=== AXIS2 REATTACH (>=10k) v1_23b (DICT-safe addr + LA->LN) ===")
  print("[info] events:", args.events)
  print("[info] spine :", args.spine)

  # load events + towns_needed
  events = []
  towns_needed = set()
  with open(args.events, "r", encoding="utf-8") as f:
    for line in f:
      r = json.loads(line)
      events.append(r)
      pr = r.get("property_ref") or {}
      town = up(pr.get("town_norm") or pr.get("town_raw") or r.get("town") or "")
      if town:
        towns_needed.add(town)

  print(f"[info] events rows: {len(events)} towns_needed: {len(towns_needed)}")

  print("[info] building spine index (town-filtered)...")
  idx_full, idx_street, idx_street_unit, street_only_counts = build_spine_index(args.spine, towns_needed)

  stats = defaultdict(int)
  t0 = time.time()

  with open(args.out, "w", encoding="utf-8") as out:
    for r in events:
      pr = r.get("property_ref") or {}
      town = up(pr.get("town_norm") or pr.get("town_raw") or r.get("town") or "")
      addr_raw = get_event_addr_string(r)

      st, mm, pid, why = try_attach_single(town, addr_raw, idx_full, idx_street, idx_street_unit, street_only_counts)

      r["match_method"] = mm
      r["why"] = why

      r.setdefault("attach", {})
      r["attach"]["attach_status"] = st
      if st == "ATTACHED_A":
        r["attach"]["property_id"] = pid

      stats["rows"] += 1
      if st == "ATTACHED_A":
        stats["attached_a"] += 1
      else:
        stats["still_unknown"] += 1

      out.write(json.dumps(r, ensure_ascii=False) + "\n")

  audit = {
    "script": "hampden_axis2_reattach_ge10k_v1_23b.py",
    "events": args.events,
    "spine": args.spine,
    "out": args.out,
    "stats": dict(stats),
    "elapsed_s": round(time.time() - t0, 2),
  }
  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print(f"[done] wrote out_rows={stats['rows']} stats={{'attached_a': {stats['attached_a']}, 'still_unknown': {stats['still_unknown']}}} audit={args.audit}")

if __name__ == "__main__":
  main()
