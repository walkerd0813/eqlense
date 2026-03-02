#!/usr/bin/env python3
import argparse, json, os, re, hashlib
from datetime import datetime, timezone

# ============================================================
# Registry Attach -> Property Spine (Deterministic) v1_1
# Improvements over v1_0:
#  - Unit-aware indexing (town|base|unit) + base-only unique-only fallback
#  - Better UNKNOWN reason bucketing (unit/range/suffix/collision/no_match)
#  - Still NO fuzzy, NO nearest, NO guessing
# ============================================================

NUMWORD = {
  "ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5","SIX":"6","SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10"
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
  "HGY":"HWY",
}

RE_MULTI_RANGE = re.compile(r"^(?P<a>\d+)\s*-\s*(?P<b>\d+)\s*(?P<rest>.+)$")
RE_HAS_NUM = re.compile(r"^\d+\b")
RE_UNIT_TOK = re.compile(r"\b(UNIT|APT|APARTMENT|#|PH|PENTHOUSE|BSMT|BASEMENT|FL|FLOOR|RM|ROOM|STE|SUITE)\b", re.IGNORECASE)
RE_UNIT_FULL = re.compile(r"\b(UNIT|APT|APARTMENT|#|PH|PENTHOUSE|BSMT|BASEMENT|FL|FLOOR|RM|ROOM|STE|SUITE)\b\s*([A-Z0-9\-]+)?\b", re.IGNORECASE)
RE_LOT  = re.compile(r"\bLOT\b[\s#]*([0-9]+|[A-Z])?\b", re.IGNORECASE)

def utc_now_iso():
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def sha256_file(path):
  h = hashlib.sha256()
  with open(path, "rb") as f:
    for chunk in iter(lambda: f.read(1024*1024), b""):
      h.update(chunk)
  return h.hexdigest()

def norm_town(t):
  if not t: return None
  return re.sub(r"\s+"," ", str(t).strip()).upper()

def norm_addr(a):
  if a is None: return None
  s = re.sub(r"\s+"," ", str(a).strip()).upper()
  s = re.sub(r"\s+Y$", "", s).strip()
  parts = s.split(" ")
  if parts and parts[0] in NUMWORD:
    parts[0] = NUMWORD[parts[0]]
  return " ".join(parts).strip() if s else None

def pick_spine_addr_str(r: dict):
  # deterministic: only index real strings
  candidates = [
    r.get("address_base_norm"),
    r.get("address_norm"),
    r.get("address"),
    r.get("address_raw"),
    r.get("address_full"),
    r.get("site_address"),
    r.get("full_address"),
    r.get("address1"),
  ]
  for v in candidates:
    if isinstance(v, str) and v.strip():
      return v
  return None

def pick_spine_unit_str(r: dict):
  # optional, if spine has it; only strings
  for k in ["unit_norm","unit","apt","apartment","suite","ste","unit_number","unit_no","address_unit"]:
    v = r.get(k)
    if isinstance(v, str) and v.strip():
      return v
  return None

def strip_unit(addr_norm):
  if not addr_norm: return addr_norm
  s = re.sub(RE_UNIT_FULL, "", addr_norm).strip()
  s = re.sub(r"\s+"," ", s).strip()
  return s

def strip_lot(addr_norm):
  if not addr_norm: return addr_norm
  s = re.sub(RE_LOT, "", addr_norm).strip()
  s = re.sub(r"\s+"," ", s).strip()
  return s

def expand_range(addr_norm):
  m = RE_MULTI_RANGE.match(addr_norm or "")
  if not m: return []
  a = int(m.group("a")); b = int(m.group("b"))
  rest = m.group("rest").strip()
  lo = min(a,b); hi = max(a,b)
  span = hi - lo
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
  if last == "HGY":
    out.append(" ".join(toks[:-1] + ["HWY"]))
    out.append(" ".join(toks[:-1] + ["HIGHWAY"]))
  if last == "HWY":
    out.append(" ".join(toks[:-1] + ["HIGHWAY"]))
    out.append(" ".join(toks[:-1] + ["HGY"]))
  if last == "HIGHWAY":
    out.append(" ".join(toks[:-1] + ["HWY"]))
  # dedupe
  seen=set(); ded=[]
  for v in out:
    if v and v not in seen:
      seen.add(v); ded.append(v)
  return ded

def condo_cdr_variants(addr_norm):
  if not addr_norm: return []
  toks = addr_norm.split(" ")
  if "CDR" not in toks: return []
  i = toks.index("CDR")
  cands = [
    " ".join(toks[:i] + ["C","DR"] + toks[i+1:]),
    " ".join(toks[:i] + ["CIR","DR"] + toks[i+1:]),
    " ".join(toks[:i] + ["CIRCLE","DR"] + toks[i+1:]),
  ]
  seen=set(); out=[]
  for v in cands:
    if v and v not in seen:
      seen.add(v); out.append(v)
  return out

def addr_variants(addr_norm):
  out=[]
  seen=set()
  def add(v, tag):
    if not v: return
    if v in seen: return
    seen.add(v); out.append((v, tag))

  if not addr_norm:
    return out

  add(addr_norm, "direct")

  su = strip_unit(addr_norm)
  if su != addr_norm: add(su, "strip_unit")

  sl = strip_lot(addr_norm)
  if sl != addr_norm: add(sl, "strip_lot")

  sul = strip_lot(su)
  if sul and sul not in (addr_norm, su, sl): add(sul, "strip_unit+lot")

  for v in expand_range(addr_norm):
    add(v, "range_expand")

  for v in suffix_alias_variants(addr_norm):
    add(v, "suffix_alias")

  for v in condo_cdr_variants(addr_norm):
    add(v, "condo_cdr_expand")

  for base, tag in [(su,"strip_unit"), (sl,"strip_lot"), (sul,"strip_unit+lot")]:
    if not base: continue
    for v in suffix_alias_variants(base):
      add(v, f"{tag}+suffix_alias")
    for v in condo_cdr_variants(base):
      add(v, f"{tag}+condo_cdr")

  return out

def extract_unit(addr_norm):
  """
  Returns (base_norm, unit_norm, unit_present_bool)
  unit_norm canonical:
    - UNIT 3
    - APT 2B
    - STE 1204
    - # 3  -> UNIT 3 (normalize # -> UNIT)
  """
  if not addr_norm:
    return (addr_norm, None, False)

  m = RE_UNIT_FULL.search(addr_norm)
  if not m:
    return (addr_norm, None, False)

  tok = (m.group(1) or "").upper()
  val = (m.group(2) or "").upper().strip() if m.group(2) else ""
  if tok == "#": tok = "UNIT"
  if tok == "APARTMENT": tok = "APT"
  if tok == "SUITE": tok = "STE"
  if tok == "FLOOR": tok = "FL"
  if tok == "PENTHOUSE": tok = "PH"
  unit = tok if not val else f"{tok} {val}"

  base = strip_unit(addr_norm)
  return (base, unit, True)

def load_spine_pointer(spine_current_json):
  with open(spine_current_json, "r", encoding="utf-8") as f:
    raw = f.read().strip()

  # direct ndjson path in file
  if raw.lower().endswith(".ndjson"):
    p = raw.strip().strip('"')
    if os.path.exists(p):
      return p

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

def build_spine_indexes(spine_ndjson, towns_set):
  # town|base -> pid or __COLLISION__
  index_base = {}
  # town|base|unit -> pid or __COLLISION__
  index_unit = {}

  stats = {
    "spine_rows_seen": 0,
    "spine_rows_indexed": 0,
    "base_keys": 0,
    "unit_keys": 0,
    "collision_base": 0,
    "collision_unit": 0
  }

  def put(idx, key, pid, coll_key):
    if key in idx and idx[key] != pid:
      if idx[key] != "__COLLISION__":
        idx[key] = "__COLLISION__"
        stats[coll_key] += 1
    else:
      idx[key] = pid

  with open(spine_ndjson, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line: continue
      stats["spine_rows_seen"] += 1
      try:
        r = json.loads(line)
      except Exception:
        continue

      town = norm_town(r.get("town_norm") or r.get("town") or r.get("municipality") or r.get("city"))
      if not town or (towns_set and town not in towns_set):
        continue

      pid = r.get("property_id")
      if not pid:
        continue

      addr_raw = pick_spine_addr_str(r)
      if not addr_raw:
        continue

      addr = norm_addr(addr_raw)
      if not addr:
        continue

      base, unit, _has = extract_unit(addr)

      # base variants (range/suffix/condo expansions apply to base)
      # NOTE: also consider lot strip variants deterministically
      stats["spine_rows_indexed"] += 1

      # base-only index
      for bv, _tag in addr_variants(base):
        k = f"{town}|{bv}"
        put(index_base, k, pid, "collision_base")

      # unit-aware index (if spine has unit, use it; else use parsed from address)
      unit_raw = pick_spine_unit_str(r)
      unit2 = norm_addr(unit_raw) if unit_raw else unit  # keep already canonical-ish
      if unit2:
        # keep unit token tight; do NOT run address variants on unit
        k_unit = f"{town}|{base}|{unit2}"
        put(index_unit, k_unit, pid, "collision_unit")

  stats["base_keys"] = len(index_base)
  stats["unit_keys"] = len(index_unit)
  return index_base, index_unit, stats

def classify_addr_features(addr_norm):
  # used for unknown reason stats
  feats = {
    "addr_no_number": False,
    "addr_no_streetname": False,
    "addr_range": False,
    "addr_unit": False,
    "addr_no_suffix_token": False
  }
  if not addr_norm:
    return feats
  toks = addr_norm.split()
  feats["addr_no_number"] = (RE_HAS_NUM.search(addr_norm) is None)
  feats["addr_no_streetname"] = (len(toks) < 2)
  feats["addr_range"] = (RE_MULTI_RANGE.match(addr_norm) is not None)
  feats["addr_unit"] = (RE_UNIT_TOK.search(addr_norm) is not None)
  # suffix token heuristic: last token looks like a known suffix family
  last = toks[-1] if toks else ""
  feats["addr_no_suffix_token"] = (last not in SUFFIX_ALIAS and last not in SUFFIX_ALIAS.values() and last not in ["WAY","PKWY","PKY","CIR","CIRCLE"])
  return feats

def attach_one(town_norm, addr_norm, index_base, index_unit):
  """
  Deterministic attach policy:
    If unit present:
      1) try unit-aware exact on (town|base|unit)
      2) else try base-only ONLY if unique
    If no unit:
      1) try base-only ONLY if unique
  Variants apply to base only (suffix/range/condo) and are deterministic.
  """
  if not town_norm or not addr_norm:
    return ("MISSING_TOWN_OR_ADDRESS", None, None, None, "missing_fields")

  base, unit, has_unit = extract_unit(addr_norm)

  # unit-aware path
  if has_unit and unit:
    # try exact base first, then base variants
    for bv, tag in addr_variants(base):
      k = f"{town_norm}|{bv}|{unit}"
      if k in index_unit:
        if index_unit[k] == "__COLLISION__":
          return ("UNKNOWN", None, "collision_unit", k, "collision_unit")
        return ("ATTACHED_A", index_unit[k], f"unit_exact:{tag}", k, "unit_match")
    # fall through to base-only unique-only

  # base-only unique-only (no auto-pick on collisions)
  for bv, tag in addr_variants(base):
    k = f"{town_norm}|{bv}"
    if k in index_base:
      if index_base[k] == "__COLLISION__":
        return ("UNKNOWN", None, "collision_base", k, "collision_base")
      return ("ATTACHED_A", index_base[k], f"base_unique:{tag}", k, "base_match")

  # no match
  return ("UNKNOWN", None, "no_match", f"{town_norm}|{addr_norm}", "no_match")

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)   # CURRENT pointer json or direct ndjson
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  ap.add_argument("--engine_id", required=True)
  ap.add_argument("--county", required=True)
  args = ap.parse_args()

  os.makedirs(os.path.dirname(args.out), exist_ok=True)
  os.makedirs(os.path.dirname(args.audit), exist_ok=True)

  ev_sha = sha256_file(args.events)
  run_id = f"{args.engine_id}|{args.county}|{ev_sha[:12]}|{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

  # towns scope: only towns present in events (keeps index smaller, still deterministic)
  towns=set()
  with open(args.events, "r", encoding="utf-8") as f:
    for line in f:
      line=line.strip()
      if not line: continue
      try:
        ev=json.loads(line)
      except Exception:
        continue
      pr = ev.get("property_ref") or {}
      t = pr.get("town_norm") or pr.get("town_code") or pr.get("town_raw") or pr.get("town")
      tn = norm_town(t)
      if tn: towns.add(tn)

  spine_ndjson = args.spine
  if spine_ndjson.lower().endswith(".json"):
    spine_ndjson = load_spine_pointer(args.spine)

  index_base, index_unit, spine_stats = build_spine_indexes(spine_ndjson, towns)

  audit = {
    "schema_version": "registry_attach_audit_v0_1",
    "engine_version": "v1_1",
    "engine_id": args.engine_id,
    "county": args.county,
    "run_id": run_id,
    "created_at_utc": utc_now_iso(),
    "events_in": args.events,
    "events_sha256": ev_sha,
    "spine_pointer": args.spine,
    "spine_ndjson": spine_ndjson,
    "towns_used_count": len(towns),
    "spine_stats": spine_stats,
    "events_total": 0,
    "attach_status_counts": {},
    "unknown_reason_counts": {},
    "method_counts": {}
  }

  def bump(d,k,n=1):
    d[k] = d.get(k,0) + n

  with open(args.events, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
    for line in fin:
      line=line.strip()
      if not line: continue
      audit["events_total"] += 1

      try:
        ev = json.loads(line)
      except Exception:
        continue

      pr = ev.get("property_ref") or {}
      town_norm = norm_town(pr.get("town_norm") or pr.get("town_code") or pr.get("town_raw") or pr.get("town"))
      addr_norm = norm_addr(pr.get("address_norm") or pr.get("address_raw") or pr.get("address") or pr.get("site_address"))

      # multi handling (kept deterministic; no auto-pick)
      multi = bool(pr.get("primary_is_multi") is True and isinstance(pr.get("multi_address"), list) and len(pr.get("multi_address"))>0)
      ev.setdefault("attach", {})

      if multi:
        attachments=[]
        st,pid,meth,mkey,reason = attach_one(town_norm, addr_norm, index_base, index_unit)
        attachments.append({"town_norm":town_norm,"address_norm":addr_norm,"attach_status":st,"property_id":pid,"match_method":meth,"match_key":mkey,"reason":reason})
        bump(audit["method_counts"], meth or "none")

        for item in pr.get("multi_address", []):
          t2 = norm_town(item.get("town_norm") or item.get("town_code") or item.get("town_raw") or item.get("town"))
          a2 = norm_addr(item.get("address_norm") or item.get("address_raw") or item.get("address"))
          st2,pid2,meth2,mkey2,reason2 = attach_one(t2, a2, index_base, index_unit)
          attachments.append({"town_norm":t2,"address_norm":a2,"attach_status":st2,"property_id":pid2,"match_method":meth2,"match_key":mkey2,"reason":reason2})
          bump(audit["method_counts"], meth2 or "none")

        attached_ct = sum(1 for a in attachments if a["attach_status"]=="ATTACHED_A")
        if attached_ct == len(attachments): overall="ATTACHED_A"
        elif attached_ct > 0: overall="PARTIAL_MULTI"
        else: overall="UNKNOWN"

        ev["attach"]["attach_scope"] = "MULTI"
        ev["attach"]["attachments"] = attachments
        ev["attach"]["attach_status"] = overall
        ev["attach"]["property_id"] = None
        ev["attach"]["match_method"] = None
        ev["attach"]["match_key"] = None

        bump(audit["attach_status_counts"], overall)
        if overall == "UNKNOWN":
          bump(audit["unknown_reason_counts"], "multi_unknown")

      else:
        st,pid,meth,mkey,reason = attach_one(town_norm, addr_norm, index_base, index_unit)

        ev["attach"]["attach_scope"] = "SINGLE"
        ev["attach"]["attach_status"] = st
        ev["attach"]["property_id"] = pid
        ev["attach"]["match_method"] = meth
        ev["attach"]["match_key"] = mkey
        ev["attach"]["attachments"] = []

        bump(audit["attach_status_counts"], st)
        bump(audit["method_counts"], meth or "none")

        if st == "UNKNOWN":
          bump(audit["unknown_reason_counts"], reason or "unknown")
          feats = classify_addr_features(addr_norm)
          for fk, fv in feats.items():
            if fv:
              bump(audit["unknown_reason_counts"], fk)

      fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print(json.dumps(audit["attach_status_counts"], indent=2))
  print("[done] wrote:", args.out)
  print("[done] audit:", args.audit)

if __name__ == "__main__":
  main()
