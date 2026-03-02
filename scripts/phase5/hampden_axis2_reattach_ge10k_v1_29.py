import argparse, json, re, os, time, datetime, collections

# v1_29 — fixes:
# 1) suffix canonicalization to SPINE DIALECT (TER/TERR/TERRACE -> TE, CIR->CR, AVE->AV)
# 2) ALWAYS overwrite attach block (even UNKNOWN) so evidence.events_in is correct
# 3) match_key always uses canonical full address (spine dialect)

SUF_TO_SPINE = {
  # core
  "ST":"ST","STREET":"ST",
  "RD":"RD","ROAD":"RD",
  "DR":"DR","DRIVE":"DR",
  "AVE":"AV","AV":"AV","AVENUE":"AV",
  "BLVD":"BLVD","BOULEVARD":"BLVD",

  # observed in spine
  "CIR":"CR","CIRCLE":"CR","CR":"CR",
  "CT":"CT","COURT":"CT",
  "LN":"LN","LANE":"LN",
  "PL":"PL","PLACE":"PL",
  "PKWY":"PKWY","PARKWAY":"PKWY",
  "WAY":"WAY",

  # Hampden: THIS IS THE ONE THAT MATTERS
  "TER":"TE","TERR":"TE","TERRACE":"TE","TE":"TE",

  # common
  "HL":"HL","HILL":"HL",
  "CI":"CI",
  "WY":"WY", "W":"WY",  # defensive
}

UNIT_RE = re.compile(r"\b(?:UNIT|APT|APARTMENT|#)\s*([A-Z0-9\-]+)\b", re.I)
LOT_RE  = re.compile(r"\bLOT\s+([A-Z0-9\-]+)\b", re.I)
RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s+", re.I)

def norm_suffix(tok: str) -> str:
  tok = (tok or "").strip().upper().replace(".", "")
  return SUF_TO_SPINE.get(tok, tok)

def clean_addr_text(s: str) -> str:
  s = (s or "").strip().upper()
  s = re.sub(r"[,\t]+", " ", s)
  s = re.sub(r"\s+", " ", s).strip()
  return s

def parse_unit(addr: str):
  m = UNIT_RE.search(addr or "")
  return m.group(1).strip().upper() if m else None

def strip_unit(addr: str) -> str:
  return re.sub(r"\b(?:UNIT|APT|APARTMENT|#)\s*[A-Z0-9\-]+\b", "", addr, flags=re.I).strip()

def canonicalize_addr_to_spine_dialect(addr_raw: str):
  """
  Returns dict:
    num: house number as string (or None)
    street: "STREETNAME SUFFIX" in spine dialect (suffix shortened)
    full: "NUM STREET" (street already spine dialect)
    base: street name without suffix (for nosuf index)
    unit: parsed unit or None
  """
  a = clean_addr_text(addr_raw)

  # handle ranges like "151-153 CATHARINE ST" -> keep first num only, drop second num token
  mrange = RANGE_RE.match(a)
  if mrange:
    # convert "151-153 CATHARINE ST" => "151 CATHARINE ST"
    a = re.sub(r"^\s*\d+\s*-\s*\d+\s+", mrange.group(1) + " ", a)

  unit = parse_unit(a)
  a2 = strip_unit(a)

  # remove trailing LOT fragments from canonical street (we treat LOT as non-addressable in spine)
  # e.g. "BALSAM HL RD LOT 66" -> "BALSAM HL RD"
  a2 = re.sub(r"\bLOT\b.*$", "", a2, flags=re.I).strip()

  # split tokens
  toks = a2.split()
  if not toks:
    return {"num": None, "street": "", "full": "", "base": "", "unit": unit}

  # find number
  num = None
  rest = toks
  if toks[0].isdigit():
    num = toks[0]
    rest = toks[1:]
  else:
    # sometimes addr is like "SANDALWOOD DR UNIT 88" (no leading number)
    # if last token is numeric and earlier looks like street, treat it as num
    if len(toks) >= 2 and toks[-1].isdigit():
      num = toks[-1]
      rest = toks[:-1]

  # normalize suffix if last token is alpha
  if rest:
    last = rest[-1]
    if re.fullmatch(r"[A-Z]{1,10}", last or ""):
      rest[-1] = norm_suffix(last)

  street = " ".join(rest).strip()
  full = (f"{num} {street}".strip() if num else street).strip()

  # base = street without suffix token (if it looks like a suffix)
  base = street
  if street:
    stoks = street.split()
    if len(stoks) >= 2:
      maybe = stoks[-1]
      # if suffix is in map keys or map values, treat as suffix and drop it for base
      if maybe in SUF_TO_SPINE or maybe in set(SUF_TO_SPINE.values()):
        base = " ".join(stoks[:-1]).strip()

  return {"num": num, "street": street, "full": full, "base": base, "unit": unit}

def build_spine_indexes(spine_path: str, towns_needed: set):
  idx_full = collections.defaultdict(list)       # (TOWN, FULL) -> [property_id]
  idx_unit = collections.defaultdict(list)       # (TOWN, FULL, UNIT) -> [property_id]
  idx_nosuf = collections.defaultdict(list)      # (TOWN, NUM, BASE) -> [property_id]

  scanned = kept = town_skip = 0
  t0 = time.time()

  with open(spine_path, "r", encoding="utf-8") as f:
    for line in f:
      scanned += 1
      if scanned % 2400000 == 0:
        print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} elapsed_s={time.time()-t0:.1f}")

      try:
        r = json.loads(line)
      except Exception:
        continue

      town = str(r.get("town") or "").strip().upper()
      if town not in towns_needed:
        town_skip += 1
        continue

      sn = str(r.get("street_no") or "").strip()
      st = str(r.get("street_name") or "").strip().upper()
      fa = str(r.get("full_address") or "").strip().upper()
      pid = r.get("property_id")

      if not pid:
        continue

      # derive canonical FULL from street_no + street_name if possible
      full = ""
      base = ""
      if sn and st:
        # ensure street_name suffix is in spine dialect already; still run through canonicalizer defensively
        st_canon = canonicalize_addr_to_spine_dialect("0 " + st)["street"]
        full = f"{sn} {st_canon}".strip()
        base = canonicalize_addr_to_spine_dialect(full)["base"]
      elif fa:
        k = canonicalize_addr_to_spine_dialect(fa)
        full = k["full"]
        base = k["base"]
        sn = k["num"] or sn

      if not full:
        continue

      kept += 1
      idx_full[(town, full)].append(pid)
      if sn and base:
        idx_nosuf[(town, sn, base)].append(pid)

      # optional unit indexing from unit field in spine (if present)
      unit = str(r.get("unit") or "").strip().upper()
      if unit:
        idx_unit[(town, full, unit)].append(pid)

  meta = {"scanned": scanned, "kept": kept, "town_skip": town_skip}
  return idx_full, idx_unit, idx_nosuf, meta

def make_attach_unknown(town, addr_raw, match_key, why, args):
  return {
    "attach_scope": "SINGLE",
    "attach_status": "UNKNOWN",
    "property_id": None,
    "match_method": "no_match",
    "match_key": match_key,
    "attachments": [],
    "evidence": {
      "join_method": "axis2 unique-only (>=10k) — spine dialect suffix canonicalization",
      "join_basis": "axis2_reattach_ge10k_v1_29",
      "spine_path": args.spine,
      "events_in": args.events,
      "out_path": args.out,
    },
    "why": why,
  }

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  print("=== AXIS2 REATTACH (>=10k) v1_29 (overwrite attach always + suffix fixes) ===")
  print("[info] events:", args.events)
  print("[info] spine :", args.spine)

  # towns_needed from events
  towns_needed = set()
  events = []
  with open(args.events, "r", encoding="utf-8") as f:
    for line in f:
      r = json.loads(line)
      events.append(r)
      t = str(r.get("town") or r.get("property_ref", {}).get("town_raw") or "").strip().upper()
      if t:
        towns_needed.add(t)

  print("[info] events rows:", len(events), "towns_needed:", len(towns_needed))

  idx_full, idx_unit, idx_nosuf, idxmeta = build_spine_indexes(args.spine, towns_needed)
  print("[ok] spine index built full=", len(idx_full), "unit=", len(idx_unit), "nosuf=", len(idx_nosuf), "meta=", idxmeta)

  stats = collections.Counter()
  unknown_bucket = collections.Counter()

  os.makedirs(os.path.dirname(args.out), exist_ok=True)
  os.makedirs(os.path.dirname(args.audit), exist_ok=True)

  with open(args.out, "w", encoding="utf-8") as out:
    for r in events:
      town = str(r.get("town") or r.get("property_ref", {}).get("town_raw") or "").strip().upper()
      addr_raw = str(r.get("addr") or r.get("property_ref", {}).get("address_raw") or "").strip()
      k = canonicalize_addr_to_spine_dialect(addr_raw)

      # match_key should be CANONICAL FULL (spine dialect)
      match_key = f"{town}|{k['full']}".strip()

      attached = False
      pid = None
      method = None

      # UNIT exact
      if k["unit"]:
        hits = idx_unit.get((town, k["full"], str(k["unit"]).upper()), [])
        if len(hits) == 1:
          pid = hits[0]
          method = "AXIS2_STREET+UNIT_EXACT"
          attached = True

      # FULL exact
      if not attached:
        hits = idx_full.get((town, k["full"]), [])
        if len(hits) == 1:
          pid = hits[0]
          method = "AXIS2_FULL_ADDRESS_EXACT"
          attached = True

      # NOSUF (unique by (town,num,base))
      if not attached and k["num"] and k["base"]:
        hits = idx_nosuf.get((town, k["num"], k["base"]), [])
        if len(hits) == 1:
          pid = hits[0]
          method = "AXIS2_STREET_UNIQUE_NOSUF"
          attached = True
        elif len(hits) > 1:
          # collision
          r["attach"] = {
            "attach_scope":"SINGLE",
            "attach_status":"UNKNOWN",
            "property_id":None,
            "match_method":"collision",
            "match_key":match_key,
            "attachments":[],
            "evidence":{
              "join_method":"axis2 unique-only (>=10k) — collision in nosuf index",
              "join_basis":"axis2_reattach_ge10k_v1_29",
              "spine_path":args.spine,
              "events_in":args.events,
              "out_path":args.out,
            },
            "why":"collision",
          }
          unknown_bucket["collision|PLAIN"] += 1
          stats["still_unknown"] += 1
          out.write(json.dumps(r, ensure_ascii=False) + "\n")
          continue

      if attached and pid:
        r["attach"] = {
          "attach_scope":"SINGLE",
          "attach_status":"ATTACHED_A",
          "property_id":pid,
          "match_method":method,
          "match_key":match_key,
          "attachments":[{"property_id":pid,"match_method":method}],
          "evidence":{
            "join_method":"axis2 unique-only (>=10k) — spine dialect suffix canonicalization",
            "join_basis":"axis2_reattach_ge10k_v1_29",
            "spine_path":args.spine,
            "events_in":args.events,
            "out_path":args.out,
          },
        }
        stats["attached_a"] += 1
      else:
        # ALWAYS overwrite attach (this is the key fix)
        why = "no_match"
        if not k["num"]:
          why = "no_num"
        elif str(k["num"]) == "0":
          why = "zero_num"
        if LOT_RE.search(clean_addr_text(addr_raw)):
          why = (why + "|LOT")
        elif k["unit"]:
          why = (why + "|UNIT")
        elif RANGE_RE.match(clean_addr_text(addr_raw)):
          why = (why + "|RANGE")
        else:
          why = (why + "|PLAIN")

        r["attach"] = make_attach_unknown(town, addr_raw, match_key, why, args)
        unknown_bucket[why] += 1
        stats["still_unknown"] += 1

      out.write(json.dumps(r, ensure_ascii=False) + "\n")

  audit = {
    "created_at": datetime.datetime.utcnow().isoformat() + "Z",
    "script": "hampden_axis2_reattach_ge10k_v1_29.py",
    "events": args.events,
    "spine": args.spine,
    "out": args.out,
    "idx_meta": idxmeta,
    "stats": dict(stats),
    "top_unknown_buckets": unknown_bucket.most_common(20),
  }
  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print("[done] wrote out_rows=", len(events), "stats=", dict(stats), "audit=", args.audit)
  if unknown_bucket:
    print("TOP_UNKNOWN_BUCKETS")
    for k,c in unknown_bucket.most_common(10):
      print(c, k)

if __name__ == "__main__":
  main()
