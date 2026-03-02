import argparse, json, os, re
from collections import Counter

# ---------------------------
# Additive-only matching helpers
# ---------------------------

WS_RE = re.compile(r"\s+")
TRAIL_Y_RE = re.compile(r"\s+Y\s*$", re.IGNORECASE)
TOWN_ADDR_ARTIFACT_RE = re.compile(r"\s+ADDR\s*$", re.IGNORECASE)

UNIT_TOKEN_RE = re.compile(r"\b(UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR|RM|ROOM)\b", re.IGNORECASE)
HASH_UNIT_RE = re.compile(r"\s+#\s*\w+\s*$", re.IGNORECASE)
# Condo-style "G-59" or "B-12" at end (only when it looks like a unit, not a highway number)
CONDOSUF_RE = re.compile(r"\s+[A-Z]\-\d+\s*$", re.IGNORECASE)

# Minimal safe suffix aliases (expandable)
SUFFIX_ALIAS = {
  "AVENUE": ["AVE"],
  "AVE": ["AVENUE"],
  "STREET": ["ST"],
  "ST": ["STREET"],
  "ROAD": ["RD"],
  "RD": ["ROAD"],
  "DRIVE": ["DR"],
  "DR": ["DRIVE"],
  "LANE": ["LN"],
  "LN": ["LANE"],
  "LA": ["LN","LANE"],           # Hampden artifact
  "COURT": ["CT"],
  "CT": ["COURT"],
  "PLACE": ["PL"],
  "PL": ["PLACE"],
  "TERRACE": ["TER","TERR"],
  "TERR": ["TER","TERRACE"],
  "TER": ["TERR","TERRACE"],
  "CIRCLE": ["CIR"],
  "CIR": ["CIRCLE"],
  "BOULEVARD": ["BLVD"],
  "BLVD": ["BOULEVARD"],
  "HIGHWAY": ["HWY","HGY"],
  "HWY": ["HIGHWAY","HGY"],
  "HGY": ["HWY","HIGHWAY"],      # Hampden artifact
}

# Hard safety caps (institutional: prevent runaway / false matches)
MAX_RANGE_WIDTH = 6          # maximum count of candidate house numbers we will generate
MAX_CANDIDATES_TRIED = 40    # overall cap per event (prevents weird explosions)

def collapse_ws(s: str) -> str:
  return WS_RE.sub(" ", (s or "").strip())

def normalize_town(town_raw: str) -> str:
  t = collapse_ws(town_raw)
  t = TRAIL_Y_RE.sub("", t)
  t = TOWN_ADDR_ARTIFACT_RE.sub("", t)
  t = collapse_ws(t).upper()
  return t

def normalize_addr(addr_raw: str) -> str:
  a = collapse_ws(addr_raw)
  a = TRAIL_Y_RE.sub("", a)
  a = collapse_ws(a).upper()
  return a

def strip_unit(addr_norm: str) -> str:
  a = addr_norm
  # kill anything after UNIT/APT token
  m = UNIT_TOKEN_RE.search(a)
  if m:
    a = a[:m.start()].strip()
  a = HASH_UNIT_RE.sub("", a).strip()
  a = CONDOSUF_RE.sub("", a).strip()
  a = collapse_ws(a).upper()
  return a

def suffix_variants(addr_norm: str):
  # only flips last token when it is a known suffix alias
  toks = addr_norm.split()
  if len(toks) < 2:
    return []
  last = toks[-1]
  out = []
  if last in SUFFIX_ALIAS:
    for alt in SUFFIX_ALIAS[last]:
      out.append(" ".join(toks[:-1] + [alt]))
  return out

RANGE_RE = re.compile(r"^(\d+)\s*-\s*(\d+)\s+(.*)$")

def expand_range(addr_norm: str):
  """
  Expand:
   - 123-125 MAIN ST  -> 123 MAIN ST, 125 MAIN ST (odd/odd parity step=2)
   - 481-4 COLD SPRING AVE -> 481..484 COLD SPRING AVE (right-side short form)
  """
  m = RANGE_RE.match(addr_norm)
  if not m:
    return []
  a_str, b_str, rest = m.group(1), m.group(2), m.group(3).strip()
  try:
    a = int(a_str)
    b_raw = b_str
    # short-form like "481-4" => 481-484; "133-37" => 133-137
    if len(b_raw) < len(a_str):
      prefix = a_str[:len(a_str)-len(b_raw)]
      b = int(prefix + b_raw)
    else:
      b = int(b_raw)
  except:
    return []

  if b < a:
    a, b = b, a

  # cap width
  width = (b - a) + 1
  if width <= 0:
    return []
  if width > 50:
    # definitely too wide; skip
    return []

  # parity-aware step (odd-odd or even-even => step 2)
  step = 2 if (a % 2) == (b % 2) else 1

  nums = list(range(a, b + 1, step))
  if len(nums) > MAX_RANGE_WIDTH:
    # too many candidates; keep it safe
    return []

  return [f"{n} {rest}" for n in nums]

def looks_like_multi_address(e):
  # If the event has raw_lines with multiple "Town:" occurrences, treat as multi-address ambiguous
  raw_lines = None
  try:
    raw_lines = e.get("document", {}).get("raw_block", None)
  except:
    raw_lines = None

  if isinstance(raw_lines, str):
    # crude but effective: multiple Town: Addr: lines indicates multiple addresses in one record
    return raw_lines.count("Town:") >= 2
  return False

def resolve_spine_path(spine_input_path: str) -> str:
  """
  Your CURRENT json pointer file points at the actual NDJSON spine.
  If user passes a json pointer, resolve it to the ndjson.
  """
  p = spine_input_path
  if p.lower().endswith(".json"):
    with open(p, "r", encoding="utf-8") as f:
      obj = json.load(f)
    nd = obj.get("properties_ndjson") or obj.get("properties_path") or obj.get("ndjson")
    if not nd:
      raise RuntimeError(f"Could not resolve NDJSON from pointer JSON: {p}")
    return nd
  return p

def iter_spine_rows(ndjson_path: str):
  with open(ndjson_path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      yield json.loads(line)

def build_spine_index(spine_input: str, allowed_towns: set):
  resolved = resolve_spine_path(spine_input)
  idx = {}
  meta = {"spine_path_input": spine_input, "spine_path_resolved": resolved}
  rows_seen = 0
  rows_indexed = 0
  for p in iter_spine_rows(resolved):
    rows_seen += 1
    town_raw = p.get("town") or p.get("town_raw") or ""
    addr_raw = p.get("full_address") or p.get("address") or p.get("address_raw") or ""
    t = normalize_town(town_raw)
    if not t or (allowed_towns and t not in allowed_towns):
      continue
    a = normalize_addr(addr_raw)
    if not a:
      continue
    key = f"{t}|{a}"
    pid = p.get("property_id") or p.get("propertyId") or p.get("id")
    if not pid:
      continue
    # if duplicates, keep first (stable)
    if key not in idx:
      idx[key] = pid
      rows_indexed += 1
  meta.update({"spine_rows_seen": rows_seen, "spine_rows_indexed": rows_indexed, "spine_index_keys": len(idx)})
  return idx, meta

def load_events(events_dir: str):
  files = []
  for fn in os.listdir(events_dir):
    if fn.lower().endswith(".ndjson"):
      files.append(os.path.join(events_dir, fn))
  files.sort()
  for path in files:
    with open(path, "r", encoding="utf-8") as f:
      for line in f:
        line = line.strip()
        if not line:
          continue
        yield json.loads(line)

def derive_allowed_towns_from_events(events_dir: str):
  towns = set()
  for e in load_events(events_dir):
    pr = e.get("property_ref", {})
    t_raw = pr.get("town_raw") or pr.get("town") or ""
    t = normalize_town(t_raw)
    if t:
      towns.add(t)
  return towns

def attach_one_event(e, spine_idx):
  pr = e.get("property_ref", {})
  town_raw = (pr.get("town_raw") or pr.get("town") or "")
  addr_raw = (pr.get("address_raw") or pr.get("address") or "")

  if not collapse_ws(town_raw) or not collapse_ws(addr_raw):
    e["attach"] = {"attach_status": "MISSING_TOWN_OR_ADDRESS"}
    return e

  # multi-address ambiguity: do NOT auto-attach
  if looks_like_multi_address(e):
    e["attach"] = {"attach_status": "UNKNOWN_MULTI_ADDRESS_AMBIGUOUS"}
    return e

  town_norm = normalize_town(town_raw)
  addr_norm = normalize_addr(addr_raw)

  candidates = []
  # candidate 1: direct
  candidates.append((addr_norm, "direct"))

  # candidate 2: unit-stripped variant
  su = strip_unit(addr_norm)
  if su and su != addr_norm:
    candidates.append((su, "strip_unit"))

  # candidate 3: suffix variants (on both base + unit-stripped)
  for base in [addr_norm, su]:
    for v in suffix_variants(base):
      if v and v != base:
        candidates.append((v, "suffix_alias"))
      v_su = strip_unit(v)
      if v_su and v_su not in [v, base, su]:
        candidates.append((v_su, "suffix_alias+strip_unit"))

  # candidate 4: range expansion (on base + suffix variants + unit-stripped)
  # NOTE: safe caps enforced inside expand_range()
  range_sources = [addr_norm, su]
  range_sources.extend([c for (c, _) in candidates])  # include suffix forms
  seen = set()
  range_sources2 = []
  for s in range_sources:
    if s and s not in seen:
      seen.add(s)
      range_sources2.append(s)

  for rs in range_sources2:
    exp = expand_range(rs)
    for ex in exp:
      candidates.append((ex, "range_expand"))
      ex_su = strip_unit(ex)
      if ex_su and ex_su != ex:
        candidates.append((ex_su, "range_expand+strip_unit"))
      for sv in suffix_variants(ex):
        candidates.append((sv, "range_expand+suffix_alias"))
        sv_su = strip_unit(sv)
        if sv_su and sv_su != sv:
          candidates.append((sv_su, "range_expand+suffix_alias+strip_unit"))

  # de-dupe preserving first method
  dedup = []
  seen = set()
  for c, m in candidates:
    c = collapse_ws(c).upper()
    if not c:
      continue
    if c in seen:
      continue
    seen.add(c)
    dedup.append((c, m))
    if len(dedup) >= MAX_CANDIDATES_TRIED:
      break

  # try candidates
  hits = []
  for cand_addr, method in dedup:
    key = f"{town_norm}|{cand_addr}"
    pid = spine_idx.get(key)
    if pid:
      hits.append((pid, cand_addr, method))
      # We DO NOT early-return if more hits might exist; still we can safely take first hit if unique.
      # We'll resolve below.

  if len(hits) == 1:
    pid, cand_addr, method = hits[0]
    e["attach"] = {
      "attach_status": "ATTACHED_A",
      "attach_method": method,
      "town_norm": town_norm,
      "address_norm": cand_addr,
      "candidates_tried": len(dedup),
    }
    e["property_id"] = pid
    return e

  if len(hits) > 1:
    # ambiguous -> UNKNOWN (institution-safe)
    e["attach"] = {
      "attach_status": "UNKNOWN_MULTI_MATCH",
      "town_norm": town_norm,
      "address_norm": addr_norm,
      "candidates_tried": len(dedup),
      "hits": len(hits),
    }
    return e

  # no match found
  e["attach"] = {
    "attach_status": "UNKNOWN_OTHER_KEY_MISMATCH",
    "town_norm": town_norm,
    "address_norm": addr_norm,
    "candidates_tried": len(dedup),
  }
  return e

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--eventsDir", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  # derive Hampden towns from deed events (prevents indexing the world)
  allowed_towns = derive_allowed_towns_from_events(args.eventsDir)
  print(f"[done] allowed_towns_count: {len(allowed_towns)}")

  spine_idx, spine_meta = build_spine_index(args.spine, allowed_towns)
  print(f"[done] spine_path_resolved: {spine_meta['spine_path_resolved']}")
  print(f"[done] spine_rows_seen: {spine_meta['spine_rows_seen']}")
  print(f"[done] spine_rows_indexed: {spine_meta['spine_rows_indexed']}")
  print(f"[done] spine_index_keys: {spine_meta['spine_index_keys']}")

  os.makedirs(os.path.dirname(args.out), exist_ok=True)
  os.makedirs(os.path.dirname(args.audit), exist_ok=True)

  counts = Counter()
  match_methods = Counter()
  events_total = 0

  with open(args.out, "w", encoding="utf-8") as out_f:
    for e in load_events(args.eventsDir):
      events_total += 1
      e2 = attach_one_event(e, spine_idx)
      st = e2.get("attach", {}).get("attach_status") or "UNKNOWN"
      counts[st] += 1
      if st == "ATTACHED_A":
        mm = e2.get("attach", {}).get("attach_method") or "direct"
        match_methods[mm] += 1
      out_f.write(json.dumps(e2, ensure_ascii=False) + "\n")

  audit = {
    **spine_meta,
    "events_dir": args.eventsDir,
    "out": args.out,
    "events_total": events_total,
    "attach_status_counts": dict(counts),
    "match_methods": dict(match_methods),
    "version": "v1_7_11_additive_only",
    "limits": {"MAX_RANGE_WIDTH": MAX_RANGE_WIDTH, "MAX_CANDIDATES_TRIED": MAX_CANDIDATES_TRIED},
  }

  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print(f"[done] events_total: {events_total}")
  print(f"[done] attach_status_counts: {dict(counts)}")
  if match_methods:
    print(f"[done] match_methods: {dict(match_methods)}")
  print(f"[done] out: {args.out}")
  print(f"[done] audit: {args.audit}")

if __name__ == "__main__":
  main()
