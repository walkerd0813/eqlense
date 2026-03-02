import argparse, json, re, time

# Town aliases (post office / village names -> legal town)
TOWN_ALIAS = {
  "FEEDING HILLS": "AGAWAM",
}

# Suffix alias (token-level; safe swaps only)
SUFFIX_ALIAS = {
  "TERR": "TER",
  "TERRACE": "TER",
  "HILL": "HL",
  "LANE": "LN",
  "LA": "LN",         # your key win
  "PKY": "PKWY",
  "PARKWAY": "PKWY",
  "COURT": "CT",
  "CIRCLE": "CIR",
  "AVENUE": "AVE",
  "BOULEVARD": "BLVD",
  "DRIVE": "DR",
  "ROAD": "RD",
  "STREET": "ST",
}

NEIGHBORHOOD_PAREN_RE = re.compile(r"\([^)]*\)")
WS_RE = re.compile(r"\s+")

def norm_ws(s: str) -> str:
  return WS_RE.sub(" ", (s or "").strip())

def up(s: str) -> str:
  return norm_ws(s).upper()

def clean_addr_raw(addr: str) -> str:
  s = addr or ""
  # Remove parenthetical neighborhood tags: "(Sixteen Acres)" etc.
  s = NEIGHBORHOOD_PAREN_RE.sub(" ", s)
  s = norm_ws(s)

  # Normalize unit markers: "#305" / "#G" -> "UNIT 305" / "UNIT G"
  s = re.sub(r"\s#\s*([A-Za-z0-9\-]+)\b", r" UNIT \1", s)
  # Normalize "APT" variants to UNIT
  s = re.sub(r"\bAPARTMENT\b", "APT", s, flags=re.I)
  s = re.sub(r"\bAPT\b", "UNIT", s, flags=re.I)

  return norm_ws(s)

def normalize_tokens(s: str) -> str:
  # Uppercase, remove punctuation except hyphen
  x = up(s)
  x = re.sub(r"[^A-Z0-9\s\-]", " ", x)
  x = norm_ws(x)
  toks = x.split(" ")
  out = []
  for t in toks:
    out.append(SUFFIX_ALIAS.get(t, t))
  return " ".join(out)

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--in", dest="inp", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  t0 = time.time()
  stats = {
    "rows": 0,
    "town_aliased": 0,
    "addr_parenthetical_stripped": 0,
    "addr_unit_normalized": 0,
    "addr_changed": 0
  }

  with open(args.inp, "r", encoding="utf-8") as f, open(args.out, "w", encoding="utf-8") as w:
    for line in f:
      r = json.loads(line)
      pr = r.get("property_ref") or {}

      town_raw = pr.get("town_raw") or ""
      town_norm = pr.get("town_norm") or ""
      town = up(town_norm or town_raw)

      # Town alias
      town2 = TOWN_ALIAS.get(town, town)
      if town2 != town:
        stats["town_aliased"] += 1

      addr_raw = pr.get("address_raw") or ""
      before = addr_raw

      # Detect parentheticals
      if "(" in addr_raw and ")" in addr_raw:
        stats["addr_parenthetical_stripped"] += 1

      cleaned = clean_addr_raw(addr_raw)

      # Detect unit normalization
      if "#" in before or re.search(r"\bAPT\b|\bAPARTMENT\b", before, flags=re.I):
        stats["addr_unit_normalized"] += 1

      # Token normalize (suffix aliases)
      addr_norm = normalize_tokens(cleaned)

      if norm_ws(before) != norm_ws(cleaned):
        stats["addr_changed"] += 1

      # Write back normalized fields (do NOT delete the original)
      pr["town_norm"] = town2
      pr["address_clean"] = cleaned
      pr["address_norm"] = addr_norm
      r["property_ref"] = pr

      w.write(json.dumps(r, ensure_ascii=False) + "\n")
      stats["rows"] += 1

  audit = {
    "script": "hampden_axis2_preclean_events_v1.py",
    "in": args.inp,
    "out": args.out,
    "stats": stats,
    "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "elapsed_s": round(time.time() - t0, 2)
  }
  with open(args.audit, "w", encoding="utf-8") as a:
    json.dump(audit, a, indent=2)

  print("[done] preclean rows=", stats["rows"], "stats=", stats, "audit=", args.audit)

if __name__ == "__main__":
  main()
