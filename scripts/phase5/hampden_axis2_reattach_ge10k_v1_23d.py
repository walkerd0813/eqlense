import argparse, json, re, time
from collections import defaultdict

def norm_ws(s):
  return re.sub(r"\s+", " ", (s or "").strip())

def up(s):
  return norm_ws(s).upper()

def coerce_addr(x):
  if x is None: return ""
  if isinstance(x, str): return x
  if isinstance(x, (int, float)): return str(x)
  if isinstance(x, dict):
    for k in ("value","text","addr","address","full","normalized","address_norm","address_clean","address_raw"):
      v = x.get(k)
      if isinstance(v, str) and v.strip():
        return v
    return ""
  try: return str(x)
  except Exception: return ""

# Expanded Hampden-safe aliases
SUFFIX_ALIAS = {
  "STREET":"ST","ST":"ST",
  "ROAD":"RD","RD":"RD",
  "AVENUE":"AVE","AVE":"AVE",
  "BOULEVARD":"BLVD","BLVD":"BLVD",
  "DRIVE":"DR","DR":"DR",
  "COURT":"CT","CT":"CT",
  "CIRCLE":"CIR","CIR":"CIR",
  "TERRACE":"TERR","TERR":"TERR",
  "TER":"TERR",                # <<< BIG ONE
  "PLACE":"PL","PL":"PL",
  "WAY":"WAY",
  "LANE":"LN","LN":"LN","LA":"LN",
  "HIGHWAY":"HWY","HWY":"HWY",
  "PARKWAY":"PKWY","PKWY":"PKWY","PKY":"PKWY",
  "HILL":"HILL","HL":"HILL",   # <<< BIG ONE
}

# suffix tokens we can drop for "nosuf" matching
DROP_SUFFIX = set(["ST","RD","AVE","BLVD","DR","CT","CIR","TERR","PL","WAY","LN","HWY","PKWY","HILL"])

def normalize_addr_tokens(addr):
  raw = coerce_addr(addr)
  s = up(raw)
  if not s:
    return ""
  s = re.sub(r"[,\.;]", " ", s)
  s = norm_ws(s)

  # UNIT normalization
  s = re.sub(r"\s#\s*([A-Z0-9\-]+)\b", r" UNIT \1", s)
  s = re.sub(r"\bAPARTMENT\s+([A-Z0-9\-]+)\b", r"UNIT \1", s)
  s = re.sub(r"\bAPT\s+([A-Z0-9\-]+)\b", r"UNIT \1", s)
  s = re.sub(r"#\s*([A-Z0-9\-]+)\b", r" UNIT \1", s)
  s = norm_ws(s)

  toks = s.split(" ")
  out = []
  for t in toks:
    if not t: continue
    out.append(SUFFIX_ALIAS.get(t, t))
  return norm_ws(" ".join(out))

def parse_num_and_rest(addr_norm):
  if not addr_norm:
    return "", ""
  m = re.match(r"^(\d+[A-Z]?)\s+(.*)$", addr_norm)
  if not m:
    return "", addr_norm
  return m.group(1), m.group(2)

def extract_unit(addr_norm):
  if not addr_norm:
    return ""
  m = re.search(r"\bUNIT\s+([A-Z0-9\-]+)\b", addr_norm)
  return m.group(1) if m else ""

def rest_nosuf(rest):
  if not rest:
    return ""
  toks = rest.split(" ")
  if toks and toks[-1] in DROP_SUFFIX:
    toks = toks[:-1]
  return norm_ws(" ".join(toks))

def k_full(town, addr_norm): return f"{town}|{addr_norm}"
def k_street(town, rest): return f"{town}|{rest}"
def k_street_nosuf(town, rest_ns): return f"{town}|{rest_ns}"
def k_street_unit(town, rest, unit): return f"{town}|{rest}|{unit}"

def get_event_addr_string(rec):
  a = coerce_addr(rec.get("addr"))
  if a: return a
  pr = rec.get("property_ref") or {}
  for k in ("address_norm","address_clean","address_raw"):
    v = coerce_addr(pr.get(k))
    if v: return v
  return ""

def build_spine_index(spine_path, towns_needed):
  t0 = time.time()
  towns_needed = set([up(x) for x in towns_needed if x])

  idx_full = defaultdict(list)
  idx_street = defaultdict(list)
  idx_street_unit = defaultdict(list)
  idx_street_ns = defaultdict(list)

  cnt_street = defaultdict(int)
  cnt_street_ns = defaultdict(int)

  scanned=kept=town_skip=no_key=0

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

      full_addr = coerce_addr(r.get("full_address"))
      sn = coerce_addr(r.get("street_no"))
      st = coerce_addr(r.get("street_name"))
      un = coerce_addr(r.get("unit"))

      if not full_addr:
        if sn and st:
          full_addr = f"{sn} {st}" + (f" UNIT {un}" if un else "")
        else:
          no_key += 1
          continue

      kept += 1
      pid = r.get("property_id") or r.get("property_uid") or r.get("row_uid") or ""

      addr_norm = normalize_addr_tokens(full_addr)
      if not addr_norm:
        no_key += 1
        continue

      num, rest = parse_num_and_rest(addr_norm)
      unit = extract_unit(addr_norm)
      rest_ns = rest_nosuf(rest)

      idx_full[k_full(town, addr_norm)].append(pid)

      if rest:
        idx_street[k_street(town, rest)].append(pid)
        cnt_street[k_street(town, rest)] += 1

      if rest_ns:
        idx_street_ns[k_street_nosuf(town, rest_ns)].append(pid)
        cnt_street_ns[k_street_nosuf(town, rest_ns)] += 1

      if rest and unit:
        idx_street_unit[k_street_unit(town, rest, unit)].append(pid)

      # Derived "UNIT <unit>" variant when spine stores unit separately
      if sn and st and un:
        derived = normalize_addr_tokens(f"{sn} {st} UNIT {un}")
        if derived and derived != addr_norm:
          dnum, drest = parse_num_and_rest(derived)
          dunit = extract_unit(derived)
          drest_ns = rest_nosuf(drest)

          idx_full[k_full(town, derived)].append(pid)
          if drest:
            idx_street[k_street(town, drest)].append(pid)
            cnt_street[k_street(town, drest)] += 1
          if drest_ns:
            idx_street_ns[k_street_nosuf(town, drest_ns)].append(pid)
            cnt_street_ns[k_street_nosuf(town, drest_ns)] += 1
          if drest and dunit:
            idx_street_unit[k_street_unit(town, drest, dunit)].append(pid)

      if scanned % 200000 == 0:
        print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} elapsed_s={time.time()-t0:.1f}")

  debug = {"scanned_rows": scanned, "kept_rows": kept, "no_key": no_key, "town_skip": town_skip}
  print(f"[ok] spine index built full={len(idx_full)} street={len(idx_street)} street_unit={len(idx_street_unit)} street_nosuf={len(idx_street_ns)} debug={debug} elapsed_s={time.time()-t0:.1f}")
  return idx_full, idx_street, idx_street_unit, idx_street_ns, cnt_street, cnt_street_ns

def try_attach_single(town, addr_raw, idx_full, idx_street, idx_street_unit, idx_street_ns, cnt_street, cnt_street_ns):
  addr_norm = normalize_addr_tokens(addr_raw)
  if not addr_norm:
    return "UNKNOWN", "no_addr", None, "NO_ADDR"

  num, rest = parse_num_and_rest(addr_norm)
  if not num:
    return "UNKNOWN", "no_num", None, "NO_NUM"

  unit = extract_unit(addr_norm)
  rest_ns = rest_nosuf(rest)

  # 1) full exact
  hits = idx_full.get(k_full(town, addr_norm)) or []
  if len(hits) == 1: return "ATTACHED_A", "axis2_full_address_exact", hits[0], None
  if len(hits) > 1:  return "UNKNOWN", "collision", None, "COLLISION"

  # 2) street+unit exact
  if unit and rest:
    hitsu = idx_street_unit.get(k_street_unit(town, rest, unit)) or []
    if len(hitsu) == 1: return "ATTACHED_A", "axis2_street+unit_exact", hitsu[0], None
    if len(hitsu) > 1:  return "UNKNOWN", "collision", None, "COLLISION"

  # 3) street unique exact
  if rest:
    ks = k_street(town, rest)
    if cnt_street.get(ks, 0) == 1:
      hs = idx_street.get(ks) or []
      if len(hs) == 1: return "ATTACHED_A", "axis2_street_unique_exact", hs[0], None

  # 4) street unique NOSUF (drops final suffix token like TERR/ST/RD/etc.)
  if rest_ns:
    kn = k_street_nosuf(town, rest_ns)
    if cnt_street_ns.get(kn, 0) == 1:
      hn = idx_street_ns.get(kn) or []
      if len(hn) == 1: return "ATTACHED_A", "axis2_street_unique_nosuf", hn[0], None

  return "UNKNOWN", "no_match", None, "NO_MATCH"

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  print("=== AXIS2 REATTACH (>=10k) v1_23d (TER/HL/PKY aliases + street_nosuf + probe compat) ===")
  print("[info] events:", args.events)
  print("[info] spine :", args.spine)

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
  idx_full, idx_street, idx_street_unit, idx_street_ns, cnt_street, cnt_street_ns = build_spine_index(args.spine, towns_needed)

  stats = defaultdict(int)
  t0 = time.time()

  with open(args.out, "w", encoding="utf-8") as out:
    for r in events:
      pr = r.get("property_ref") or {}
      town = up(pr.get("town_norm") or pr.get("town_raw") or r.get("town") or "")
      addr_raw = get_event_addr_string(r)

      st, mm, pid, why = try_attach_single(town, addr_raw, idx_full, idx_street, idx_street_unit, idx_street_ns, cnt_street, cnt_street_ns)

      # --- compatibility: write BOTH legacy + new attach dict ---
      r["attach_scope"] = "SINGLE"
      r["attach_status"] = st
      r["match_method"] = mm
      r["why"] = why
      r.setdefault("attach", {})
      r["attach"]["attach_status"] = st
      r["attach"]["match_method"] = mm
      if why: r["attach"]["why"] = why
      if st == "ATTACHED_A":
        r["attach"]["property_id"] = pid

      stats["rows"] += 1
      if st == "ATTACHED_A":
        stats["attached_a"] += 1
      else:
        stats["still_unknown"] += 1

      out.write(json.dumps(r, ensure_ascii=False) + "\n")

  audit = {
    "script": "hampden_axis2_reattach_ge10k_v1_23d.py",
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
