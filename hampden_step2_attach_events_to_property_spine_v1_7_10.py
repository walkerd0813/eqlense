# hampden_step2_attach_events_to_property_spine_v1_7_10.py
import argparse, json, os, re
from collections import Counter, defaultdict
from datetime import datetime, timezone

# ----------------------------
# Normalization (deterministic)
# ----------------------------
SUFFIX_CANON = {
  "AVENUE":"AVE","AVE":"AVE",
  "STREET":"ST","ST":"ST",
  "ROAD":"RD","RD":"RD",
  "DRIVE":"DR","DR":"DR",
  "LANE":"LN","LN":"LN",
  "COURT":"CT","CT":"CT",
  "PLACE":"PL","PL":"PL",
  "TERRACE":"TER","TER":"TER",
  "CIRCLE":"CIR","CIR":"CIR",
  "BOULEVARD":"BLVD","BLVD":"BLVD",
  "HIGHWAY":"HWY","HWY":"HWY",
}

SUFFIX_ALIAS = {
  "LA":["LN"],
  "LN":["LA"],
  "HGY":["HWY"],
  "HWY":["HGY"],
  "TERR":["TER"],
  "TER":["TERR"],
}

UNIT_RE = re.compile(r"\b(UNIT|APT|APARTMENT|#)\s*([A-Z0-9\-]+)\b", re.I)
RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s+(.+)$")

def collapse_ws(s: str) -> str:
  return re.sub(r"\s+", " ", s).strip()

def strip_trailing_y(s: str) -> str:
  # registry lines often end with a literal "Y"
  return re.sub(r"\s+Y\s*$", "", s or "").strip()

def strip_addr_artifacts_town(town: str) -> str:
  t = (town or "").upper()
  t = t.replace(" ADDR", "")
  t = strip_trailing_y(t)
  return collapse_ws(t)

def strip_quotes_commas(s: str) -> str:
  s = (s or "").replace('"', "").replace("'", "")
  s = s.replace(",", " ")
  return s

def normalize_suffix(tokens):
  if not tokens:
    return tokens
  last = tokens[-1]
  canon = SUFFIX_CANON.get(last, last)
  tokens[-1] = canon
  return tokens

def normalize_address(addr: str) -> str:
  a = (addr or "").upper()
  a = strip_quotes_commas(a)
  a = strip_trailing_y(a)
  a = collapse_ws(a)
  # standardize common punctuation
  a = a.replace("–","-").replace("—","-")
  # token normalize
  toks = a.split(" ")
  toks = [t for t in toks if t]
  toks = normalize_suffix(toks)
  return " ".join(toks)

def strip_unit(addr_norm: str) -> str:
  # remove UNIT/APT/# segments
  return collapse_ws(UNIT_RE.sub("", addr_norm))

def suffix_variants(addr_norm: str):
  toks = addr_norm.split()
  if not toks:
    return [addr_norm]
  last = toks[-1]
  out = {addr_norm}
  if last in SUFFIX_ALIAS:
    for alt in SUFFIX_ALIAS[last]:
      out.add(" ".join(toks[:-1] + [alt]))
  return list(out)

def expand_range(addr_norm: str, max_span: int = 8):
  m = RANGE_RE.match(addr_norm)
  if not m:
    return None
  a0 = int(m.group(1)); a1 = int(m.group(2)); rest = m.group(3).strip()
  lo, hi = min(a0,a1), max(a0,a1)
  span = hi - lo
  if span <= 0:
    return None
  if span > max_span:
    return []  # signal: range too wide
  # parity rule
  step = 1
  if (lo % 2) == (hi % 2):
    step = 2
  nums = list(range(lo, hi+1, step))
  return [f"{n} {rest}" for n in nums]

def extract_all_locators_from_raw_block(raw_block: str):
  # Pull every Town/Addr pair from the deed text block
  if not raw_block:
    return []
  pairs = []
  for m in re.finditer(r"Town:\s*([A-Z ]+)\s+Addr:\s*([0-9A-Z \-]+)", raw_block):
    town = collapse_ws(strip_trailing_y(m.group(1).upper()))
    addr = collapse_ws(strip_trailing_y(m.group(2).upper()))
    if town and addr:
      pairs.append((town, addr))
  return pairs

# ----------------------------
# Spine loader (NDJSON only)
# ----------------------------
def resolve_spine_path(spine_path):
  # CURRENT json pointer -> ndjson path
  if spine_path.lower().endswith(".json"):
    with open(spine_path, "r", encoding="utf-8") as f:
      obj = json.load(f)
    cand = obj.get("properties_ndjson")
    if cand and os.path.exists(cand):
      return cand
  return spine_path

def iter_ndjson(path):
  with open(path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      yield json.loads(line)

def build_spine_index(spine_ndjson_path, allowed_towns_set):
  idx = defaultdict(list)
  rows_seen = 0
  rows_indexed = 0

  # Choose best address field (avoid comma poisoning)
  def pick_addr(p):
    # Institutional rule:
    # do NOT discard comma-containing addresses; we normalize commas out in normalize_address().
    # Only discard if it looks like a non-property blob (very long) or clearly not an address.
    for k in ("full_address", "address", "property_address"):
      v = p.get(k)
      if isinstance(v, str) and v.strip():
        vv = v.strip()
        if len(vv) > 140:
          continue
        return vv

    # address_label is last resort; allow commas (they'll normalize away)
    v = p.get("address_label")
    if isinstance(v, str) and v.strip():
      vv = v.strip()
      if len(vv) > 140:
        return None
      return vv
    return None

  for p in iter_ndjson(spine_ndjson_path):
    rows_seen += 1
    town = p.get("town") or p.get("municipality") or ""
    town_n = strip_addr_artifacts_town(str(town))
    if town_n not in allowed_towns_set:
      continue
    addr = pick_addr(p)
    if not addr:
      continue
    addr_n = normalize_address(str(addr))
    key = f"{town_n}|{addr_n}"
    pid = p.get("property_id")
    if pid:
      idx[key].append(pid)
      rows_indexed += 1

  return idx, {"spine_rows_seen": rows_seen, "spine_rows_indexed": rows_indexed, "spine_index_keys": len(idx)}

# ----------------------------
# Attach logic (deterministic)
# ----------------------------
def candidate_keys_for_locator(town_raw, addr_raw, raw_block=None):
  town_n = strip_addr_artifacts_town(town_raw)
  addr_n = normalize_address(addr_raw)

  # multi-address support (from raw block)
  locators = []
  if raw_block:
    extracted = extract_all_locators_from_raw_block(raw_block)
    if extracted:
      for t,a in extracted:
        locators.append((strip_addr_artifacts_town(t), normalize_address(a)))
    else:
      locators.append((town_n, addr_n))
  else:
    locators.append((town_n, addr_n))

  # build candidate address list per locator
  out = []
  for t, a in locators:
    if not t or not a:
      continue

    # 1) direct + suffix variants
    base_addrs = set()
    for v in suffix_variants(a):
      base_addrs.add(v)

    # 2) unit strip + suffix variants
    a2 = strip_unit(a)
    if a2 != a:
      for v in suffix_variants(a2):
        base_addrs.add(v)

    # 3) range expansion + suffix variants
    exp = expand_range(a, max_span=8)
    if exp is not None:
      # exp == [] means too wide; keep as-is (do nothing)
      for ea in exp:
        ea = normalize_address(ea)
        for v in suffix_variants(ea):
          base_addrs.add(v)

    for cand in base_addrs:
      out.append((t, cand))
  return town_n, addr_n, out

def attach_event(ev, spine_idx):
  # prefer deed document.raw_block for multi-address parsing
  raw_block = None
  doc = ev.get("document") or {}
  if isinstance(doc, dict):
    raw_block = doc.get("raw_block")

  pref = ev.get("property_ref") or {}
  town_raw = (pref.get("town_raw") or pref.get("town") or "").strip()
  addr_raw = (pref.get("address_raw") or pref.get("address") or "").strip()

  town_n, addr_n, candidates = candidate_keys_for_locator(town_raw, addr_raw, raw_block=raw_block)

  if not town_raw or not addr_raw:
    return ("MISSING_TOWN_OR_ADDRESS", None)

  tried = []
  hits = set()
  methods = Counter()

  for t, a in candidates:
    key = f"{t}|{a}"
    tried.append(key)
    if key in spine_idx:
      for pid in spine_idx[key]:
        hits.add(pid)

  if len(hits) == 1:
    pid = list(hits)[0]
    # best-effort method labeling (audit only)
    method = "direct"
    if UNIT_RE.search(normalize_address(addr_raw)):
      method = "strip_unit_or_unit_present"
    if RANGE_RE.match(normalize_address(addr_raw)):
      method = "range_expand_or_range_present"
    return ("ATTACHED_A", {"property_id": pid, "attach_method": method, "town_norm": town_n, "address_norm": addr_n})
  if len(hits) >= 2:
    return ("UNKNOWN_MULTI_ADDRESS_AMBIGUOUS", {"candidates_tried": tried[:50]})
  return ("UNKNOWN_OTHER_KEY_MISMATCH", {"candidates_tried": tried[:50]})

# ----------------------------
def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--eventsDir", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  # Hampden towns set (from your LOCATORFIX logic; keep deterministic)
  allowed = {
    "AGAWAM","BLANDFORD","BRIMFIELD","CHESTER","CHICOPEE","EAST LONGMEADOW","GRANVILLE",
    "HAMPDEN","HOLLAND","HOLYOKE","LONGMEADOW","LUDLOW","MONSON","PALMER","RUSSELL",
    "SOUTHWICK","SPRINGFIELD","TOLLAND","WALES","WEST SPRINGFIELD","WESTFIELD","WILBRAHAM"
  }

  spine_resolved = resolve_spine_path(args.spine)
  spine_idx, spine_meta = build_spine_index(spine_resolved, allowed)

  counts = Counter()
  samples = defaultdict(list)

  os.makedirs(os.path.dirname(args.out), exist_ok=True)
  os.makedirs(os.path.dirname(args.audit), exist_ok=True)

  # Only DEED files in the eventsDir
  deed_path = os.path.join(args.eventsDir, "deed_events.ndjson")
  if not os.path.exists(deed_path):
    raise FileNotFoundError(f"Missing deed_events.ndjson at {deed_path}")

  with open(args.out, "w", encoding="utf-8") as out:
    total = 0
    for ev in iter_ndjson(deed_path):
      total += 1
      status, payload = attach_event(ev, spine_idx)
      counts[status] += 1

      if status.startswith("UNKNOWN") and len(samples[status]) < 8:
        pr = ev.get("property_ref") or {}
        samples[status].append({
          "doc": (ev.get("recording") or {}).get("document_number") or (ev.get("recording") or {}).get("document_number_raw"),
          "town_raw": pr.get("town_raw") or pr.get("town"),
          "addr_raw": pr.get("address_raw") or pr.get("address"),
          "detail": payload
        })

      if status == "ATTACHED_A":
        ev["attach"] = {
          "attach_status": "ATTACHED_A",
          "attach_method": payload["attach_method"],
          "town_norm": payload["town_norm"],
          "address_norm": payload["address_norm"],
        }
        ev["property_id"] = payload["property_id"]
      else:
        ev["attach"] = {"attach_status": "UNKNOWN", "reason": status, **(payload or {})}

      out.write(json.dumps(ev, ensure_ascii=False) + "\n")

  audit = {
    "created_at": datetime.now(timezone.utc).isoformat(),
    "events_dir": args.eventsDir,
    "spine_path_input": args.spine,
    "spine_path_resolved": spine_resolved,
    "allowed_towns_count": len(allowed),
    "spine_rows_seen": spine_meta["spine_rows_seen"],
    "spine_rows_indexed": spine_meta["spine_rows_indexed"],
    "spine_index_keys": spine_meta["spine_index_keys"],
    "events_total": total,
    "attach_status_counts": dict(counts),
    "unknown_samples": dict(samples),
  }

  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print("[done] allowed_towns_count:", len(allowed))
  print("[done] spine_path_resolved:", spine_resolved)
  print("[done] spine_rows_seen:", spine_meta["spine_rows_seen"])
  print("[done] spine_rows_indexed:", spine_meta["spine_rows_indexed"])
  print("[done] spine_index_keys:", spine_meta["spine_index_keys"])
  print("[done] events_total:", total)
  print("[done] attach_status_counts:", dict(counts))
  print("[done] out:", args.out)
  print("[done] audit:", args.audit)

if __name__ == "__main__":
  main()

