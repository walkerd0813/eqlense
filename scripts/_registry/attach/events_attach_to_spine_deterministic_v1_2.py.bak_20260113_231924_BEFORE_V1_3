#!/usr/bin/env python3
import argparse, json, os, re, hashlib
from datetime import datetime, timezone

# ============================================================
# Registry Events -> Property Spine Attachment (Deterministic)
# v1_2: global improvements
#   - Unit-aware: strip unit for matching, keep unit token as evidence
#   - Range expansion (bounded)
#   - Suffix alias variants (bounded)
#   - Non-address detection (MD/plan/notation etc.)
#   - Collision-safe (never guess)
# ============================================================

NUMWORD = {
  "ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5",
  "SIX":"6","SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10"
}

SUFFIX_ALIAS = {
  "AVE":"AV","AV":"AVE",
  "ST":"STREET","STREET":"ST",
  "RD":"ROAD","ROAD":"RD",
  "DR":"DRIVE","DRIVE":"DR",
  "LN":"LANE","LANE":"LN",
  "PKY":"PKWY","PKWY":"PKY",
  "BLVD":"BOULEVARD","BOULEVARD":"BLVD",
  "TER":"TERRACE","TERRACE":"TER",
  "CT":"COURT","COURT":"CT",
  "PL":"PLACE","PLACE":"PL",
  "CIR":"CIRCLE","CIRCLE":"CIR",
  "HWY":"HIGHWAY","HIGHWAY":"HWY",
  "HGY":"HWY",  # known typo in some indexes
}

RE_MULTI_RANGE = re.compile(r"^(?P<a>\d+)\s*-\s*(?P<b>\d+)\s*(?P<rest>.+)$")
RE_UNIT = re.compile(
  r"\b(UNIT|APT|APARTMENT|#|PH|PENTHOUSE|BSMT|BASEMENT|FL|FLOOR|RM|ROOM|STE|SUITE)\b\s*([A-Z0-9\-]+)?\b",
  re.IGNORECASE
)
RE_LOT  = re.compile(r"\bLOT\b[\s#]*([0-9]+|[A-Z])?\b", re.IGNORECASE)

# common "not really an address" tokens found in registry indexes
RE_NON_ADDRESS = re.compile(
  r"\b(MD\s*\d+|NOTATION\s+DEED|PLAN\s+\d+|C\s*PL\s*\d+|BK\s*\d+|PG\s*\d+)\b",
  re.IGNORECASE
)

def utc_now_iso():
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def sha256_file(path: str) -> str:
  h = hashlib.sha256()
  with open(path, "rb") as f:
    for chunk in iter(lambda: f.read(1024*1024), b""):
      h.update(chunk)
  return h.hexdigest()

def norm_town(t):
  if not t: return None
  return re.sub(r"\s+"," ", str(t).strip()).upper()

def norm_addr(a):
  """
  Uppercase, compress whitespace, normalize leading spelled numbers.
  Do NOT do fuzzy transforms; keep deterministic and reversible-ish.
  """
  if a is None: return None
  s = str(a)
  s = re.sub(r"\s+"," ", s.strip()).upper()
  # common index artifact: trailing verification token 'Y'
  s = re.sub(r"\s+Y$", "", s).strip()
  # remove stray punctuation that commonly appears in indexes
  s = s.replace(",", " ").replace(";", " ").replace("  ", " ")
  s = re.sub(r"\s+"," ", s).strip()
  parts = s.split(" ")
  if parts and parts[0] in NUMWORD:
    parts[0] = NUMWORD[parts[0]]
  return " ".join(parts)

def pick_event_town_and_addr(ev: dict):
  pr = ev.get("property_ref") or {}
  town = pr.get("town_code") or pr.get("town_norm") or pr.get("town_raw") or pr.get("city") or pr.get("municipality")
  addr = pr.get("address_norm") or pr.get("address_raw") or pr.get("address") or pr.get("full_address") or pr.get("site_address")
  return norm_town(town), norm_addr(addr)

def pick_spine_addr_str(r: dict):
  # Only accept true strings
  for k in ["address_norm","address","address_raw","address_full","site_address","full_address","address1"]:
    v = r.get(k)
    if isinstance(v, str) and v.strip():
      return v
  return None

def is_non_address(addr_norm: str) -> bool:
  if not addr_norm: return False
  # If it doesn't start with a number, it's usually not a parcel address (conservative)
  if not re.match(r"^\d+\b", addr_norm):
    # still allow rare "0 SOMETHING"? not typical; treat as non-address.
    return True
  # if it contains clear non-address tokens (MD ####, PLAN refs, NOTATION DEED)
  if RE_NON_ADDRESS.search(addr_norm):
    return True
  return False

def extract_unit(addr_norm: str):
  """
  Returns (street_only, unit_token or None)
  """
  if not addr_norm: return (addr_norm, None)
  m = RE_UNIT.search(addr_norm)
  unit_tok = None
  if m:
    kw = (m.group(1) or "").upper()
    val = (m.group(2) or "").upper().strip()
    unit_tok = (kw + (" " + val if val else "")).strip()
  # remove all unit fragments anywhere
  s = re.sub(RE_UNIT, " ", addr_norm).strip()
  s = re.sub(r"\s+"," ", s).strip()
  return (s, unit_tok)

def strip_lot(addr_norm):
  if not addr_norm: return addr_norm
  s = re.sub(RE_LOT, " ", addr_norm).strip()
  s = re.sub(r"\s+"," ", s).strip()
  return s

def expand_range(addr_norm):
  """
  Deterministic range policy:
    - if span <= 10: expand all integers
    - else: endpoints only
  """
  m = RE_MULTI_RANGE.match(addr_norm or "")
  if not m: return []
  a = int(m.group("a")); b = int(m.group("b"))
  rest = (m.group("rest") or "").strip()
  lo = min(a,b); hi = max(a,b)
  span = hi - lo
  if not rest:
    return []
  if span <= 10:
    return [f"{n} {rest}" for n in range(lo, hi+1)]
  return [f"{lo} {rest}", f"{hi} {rest}"]

def suffix_alias_variants(addr_norm):
  toks = (addr_norm or "").split(" ")
  if not toks: return []
  last = toks[-1]
  out = []
  if last in SUFFIX_ALIAS:
    out.append(" ".join(toks[:-1] + [SUFFIX_ALIAS[last]]))
  # completeness around HWY variants
  if last == "HGY":
    out.append(" ".join(toks[:-1] + ["HWY"]))
    out.append(" ".join(toks[:-1] + ["HIGHWAY"]))
  if last == "HWY":
    out.append(" ".join(toks[:-1] + ["HIGHWAY"]))
    out.append(" ".join(toks[:-1] + ["HGY"]))
  if last == "HIGHWAY":
    out.append(" ".join(toks[:-1] + ["HWY"]))
  # de-dupe
  seen=set(); ded=[]
  for v in out:
    if v and v not in seen:
      seen.add(v); ded.append(v)
  return ded

def addr_variants(addr_norm):
  """
  Deterministic-only variants. No fuzzy. Bounded list.
  Returns list of (variant, method_tag, unit_token_or_none)
  """
  out=[]
  seen=set()

  def add(v, tag, unit_tok=None):
    if not v: return
    if v in seen: return
    seen.add(v); out.append((v, tag, unit_tok))

  if not addr_norm:
    return out

  add(addr_norm, "direct", None)

  # street-only (strip unit) is the biggest win
  street_only, unit_tok = extract_unit(addr_norm)
  if street_only and street_only != addr_norm:
    add(street_only, "strip_unit", unit_tok)

  # strip lot references
  sl = strip_lot(addr_norm)
  if sl and sl != addr_norm:
    add(sl, "strip_lot", None)

  # combos
  if street_only:
    sul = strip_lot(street_only)
    if sul and sul not in (addr_norm, street_only, sl):
      add(sul, "strip_unit+lot", unit_tok)

  # range expansions (run on direct + street_only if different)
  for v in expand_range(addr_norm):
    add(v, "range_expand", None)
  if street_only and street_only != addr_norm:
    for v in expand_range(street_only):
      add(v, "strip_unit+range_expand", unit_tok)

  # suffix alias variants (run on direct + street_only)
  for v in suffix_alias_variants(addr_norm):
    add(v, "suffix_alias", None)
  if street_only and street_only != addr_norm:
    for v in suffix_alias_variants(street_only):
      add(v, "strip_unit+suffix_alias", unit_tok)

  # also suffix alias for lot-stripped variants
  if sl and sl != addr_norm:
    for v in suffix_alias_variants(sl):
      add(v, "strip_lot+suffix_alias", None)
  if street_only:
    sul = strip_lot(street_only)
    if sul and sul not in (addr_norm, street_only, sl):
      for v in suffix_alias_variants(sul):
        add(v, "strip_unit+lot+suffix_alias", unit_tok)

  return out

def load_spine_pointer(spine_current_json):
  """
  Resolve the actual spine ndjson from CURRENT pointer JSON.
  If file itself contains a direct ndjson path, return it.
  Else crawl JSON for existing .ndjson paths.
  """
  with open(spine_current_json, "r", encoding="utf-8") as f:
    raw = f.read().strip()

  raw2 = raw.strip().strip('"')
  if raw2.lower().endswith(".ndjson") and os.path.exists(raw2):
    return raw2

  try:
    obj = json.loads(raw)
  except Exception:
    raise RuntimeError("Could not parse spine CURRENT pointer JSON and it wasn't a direct .ndjson path.")

  found=[]
  def walk(x):
    if isinstance(x, dict):
      for v in x.values(): walk(v)
    elif isinstance(x, list):
      for v in x: walk(v)
    elif isinstance(x, str):
      s = x.strip().strip('"')
      if s.lower().endswith(".ndjson"):
        found.append(s)
  walk(obj)

  for p in found:
    if os.path.exists(p):
      return p

  raise RuntimeError("Could not resolve spine ndjson path from CURRENT pointer JSON.")

def build_spine_index(spine_ndjson, towns_set):
  """
  index key: TOWN|ADDR
  value: property_id or "__COLLISION__"
  """
  index={}
  collisions=0
  rows_seen=0
  rows_indexed=0

  with open(spine_ndjson, "r", encoding="utf-8") as f:
    for line in f:
      line=line.strip()
      if not line: continue
      rows_seen += 1
      try:
        r=json.loads(line)
      except Exception:
        continue

      town = norm_town(r.get("town_norm") or r.get("town") or r.get("municipality") or r.get("city"))
      if not town or (towns_set and town not in towns_set):
        continue

      addr_raw = pick_spine_addr_str(r)
      if not addr_raw:
        continue

      addr = norm_addr(addr_raw)
      pid  = r.get("property_id")
      if not addr or not pid:
        continue

      # if spine row itself looks non-address (rare), skip indexing
      if is_non_address(addr):
        continue

      rows_indexed += 1

      # IMPORTANT: index all deterministic variants too (bounded)
      for av, _tag, _unit_tok in addr_variants(addr):
        key = f"{town}|{av}"
        if key in index and index[key] != pid:
          index[key] = "__COLLISION__"
          collisions += 1
        else:
          index[key] = pid

  return index, {
    "spine_rows_seen": rows_seen,
    "spine_rows_indexed": rows_indexed,
    "spine_keys": len(index),
    "collision_keys": collisions
  }

def attach_one(town_norm, addr_norm, index):
  """
  Returns:
    status, property_id, match_method, match_key, unit_token, unknown_reason
  """
  if not town_norm or not addr_norm:
    return ("MISSING_TOWN_OR_ADDRESS", None, None, None, None, "MISSING_TOWN_OR_ADDRESS")

  if is_non_address(addr_norm):
    return ("UNKNOWN", None, "non_address", f"{town_norm}|{addr_norm}", None, "NON_ADDRESS")

  for av, tag, unit_tok in addr_variants(addr_norm):
    key = f"{town_norm}|{av}"
    if key in index:
      if index[key] == "__COLLISION__":
        return ("UNKNOWN", None, "collision", key, unit_tok, "COLLISION")
      return ("ATTACHED_A", index[key], tag, key, unit_tok, None)

  return ("UNKNOWN", None, "no_match", f"{town_norm}|{addr_norm}", None, "NO_MATCH")

def ensure_parent_dir(path: str):
  d = os.path.dirname(path)
  if d and not os.path.exists(d):
    os.makedirs(d, exist_ok=True)

def main():
  ap=argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)   # CURRENT pointer JSON
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  ap.add_argument("--engine_id", required=True)
  ap.add_argument("--county", required=False, default="")
  args=ap.parse_args()

  ensure_parent_dir(args.out)
  ensure_parent_dir(args.audit)

  # Determine town scope from events (deterministic + faster)
  towns=set()
  total_events=0
  with open(args.events, "r", encoding="utf-8") as f:
    for line in f:
      line=line.strip()
      if not line: continue
      total_events += 1
      try:
        ev=json.loads(line)
      except Exception:
        continue
      t,a = pick_event_town_and_addr(ev)
      if t: towns.add(t)

  spine_ndjson = load_spine_pointer(args.spine)
  index, spine_stats = build_spine_index(spine_ndjson, towns)

  ev_sha = sha256_file(args.events)[:12]
  run_id = f"{args.engine_id}|{args.county}|{ev_sha}|{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

  audit = {
    "schema_version": "registry_attach_audit_v0_1",
    "engine_id": args.engine_id,
    "run_id": run_id,
    "created_at_utc": utc_now_iso(),
    "county": args.county,
    "events_in": args.events,
    "events_in_sha256": sha256_file(args.events),
    "spine_pointer": args.spine,
    "spine_ndjson": spine_ndjson,
    "towns_used_count": len(towns),
    "spine_stats": spine_stats,
    "events_total": 0,
    "attach_status_counts": {},
    "method_counts": {},
    "unknown_reason_counts": {},
    "unit_token_present_count": 0
  }

  def bump(d,k,n=1):
    d[k]=d.get(k,0)+n

  with open(args.events, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
    for line in fin:
      line=line.strip()
      if not line: continue
      audit["events_total"] += 1
      ev=json.loads(line)

      town_norm, addr_norm = pick_event_town_and_addr(ev)
      st,pid,meth,mkey,unit_tok,unknown_reason = attach_one(town_norm, addr_norm, index)

      ev.setdefault("attach", {})
      ev["attach"]["attach_scope"] = "SINGLE"
      ev["attach"]["attach_status"] = st
      ev["attach"]["property_id"] = pid
      ev["attach"]["match_method"] = meth
      ev["attach"]["match_key"] = mkey
      ev["attach"]["unit_token"] = unit_tok
      ev["attach"]["unknown_reason"] = unknown_reason
      ev["attach"]["attachments"] = []

      if unit_tok:
        audit["unit_token_present_count"] += 1

      bump(audit["attach_status_counts"], st)
      bump(audit["method_counts"], meth or "none")
      if unknown_reason:
        bump(audit["unknown_reason_counts"], unknown_reason)

      fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print(json.dumps(audit["attach_status_counts"], indent=2))
  print("[done] wrote:", args.out)
  print("[done] audit:", args.audit)

if __name__ == "__main__":
  main()