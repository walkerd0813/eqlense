# scripts\phase5\hampden_axis2_reattach_ge10k_v1_25.py
import argparse, json, re, sys, time
from collections import Counter, defaultdict

def norm_ws(s): return re.sub(r"\s+", " ", (s or "").strip())
def up(s): return norm_ws(s).upper()

# Canonical suffix normalization (both sides: spine + events)
SUF_MAP = {
  # street types
  "STREET":"ST", "ST":"ST", "ST.":"ST",
  "AVENUE":"AV", "AVE":"AV", "AVE.":"AV", "AV":"AV", "AV.":"AV",
  "ROAD":"RD", "RD":"RD", "RD.":"RD",
  "DRIVE":"DR", "DR":"DR", "DR.":"DR",
  "LANE":"LN", "LN":"LN", "LN.":"LN",
  "PLACE":"PL", "PL":"PL", "PL.":"PL",
  "COURT":"CT", "CT":"CT", "CT.":"CT",
  "BOULEVARD":"BL", "BLVD":"BL", "BLVD.":"BL", "BL":"BL",
  "PARKWAY":"PW", "PKWY":"PW", "PKWY.":"PW", "PW":"PW",

  # problem children you proved
  "TERRACE":"TE", "TERR":"TE", "TER":"TE", "TE":"TE",
  "CIRCLE":"CI", "CIR":"CI", "CI":"CI",
  "CRESCENT":"CR", "CR":"CR",  # optional (keep if it’s real in your data)
  # keep existing short forms that appear in your spine dump
  "TR":"TR", "TRL":"TR", "TRAIL":"TR",
  "WAY":"WY", "WY":"WY",
  "EXTN":"EXTN", "EXT":"EXTN", "EXTENSION":"EXTN",
  "PARK":"PARK",
  "HL":"HL", "HILL":"HL",
  "LA":"LA",  # DO NOT force LA->LN globally (you have LA in spine in multiple towns)
}

DIR_TOKENS = {"N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST"}

def split_addr_tokens(addr):
  toks = [t for t in re.split(r"[^A-Z0-9#]+", up(addr)) if t]
  return toks

def normalize_unit_tokens(toks):
  # Normalize unit formats to a canonical "UNIT <X>" token stream
  # Accept: UNIT 3, APT 3, #3, STE 3, PH 3, FL 3
  out = []
  i = 0
  unit_val = None

  while i < len(toks):
    t = toks[i]
    if t in ("UNIT","APT","APARTMENT","STE","SUITE","PH","FL","FLOOR"):
      if i+1 < len(toks) and toks[i+1] not in DIR_TOKENS:
        unit_val = toks[i+1]
        i += 2
        continue
    if t.startswith("#") and len(t) > 1:
      unit_val = t[1:]
      i += 1
      continue
    out.append(t)
    i += 1

  return out, unit_val

def canonical_suffix(t):
  t = up(t)
  return SUF_MAP.get(t, t)

def canonicalize_for_key(town, addr):
  toks = split_addr_tokens(addr)

  # pull number (first numeric token)
  num = None
  rest = []
  for tok in toks:
    if num is None and tok.isdigit():
      num = tok
      continue
    rest.append(tok)

  # strip directionals from rest (match should not depend on them)
  rest = [t for t in rest if t not in DIR_TOKENS]

  # normalize units
  rest, unit_val = normalize_unit_tokens(rest)

  # suffix normalization: last alpha token only
  if rest:
    last = rest[-1]
    if last.isalpha():
      rest[-1] = canonical_suffix(last)

  # build street_base (without suffix) to enable safe fallback
  street_base = None
  suffix = None
  if rest:
    # if last token is alpha, treat as suffix
    if rest[-1].isalpha():
      suffix = rest[-1]
      base = rest[:-1]
    else:
      base = rest[:]
    street_base = " ".join(base).strip() if base else None

  full = None
  if num and rest:
    full = num + " " + " ".join(rest)

  return {
    "town": up(town),
    "num": num,
    "full": full,               # e.g. "86 PAULK TE"
    "street_base": street_base, # e.g. "PAULK"
    "suffix": suffix,
    "unit": unit_val
  }

def safe_get_town_addr(ev):
  # handle dict-shaped town/addr too
  t = ev.get("town")
  a = ev.get("addr")
  if isinstance(t, dict): t = t.get("value") or t.get("raw") or None
  if isinstance(a, dict): a = a.get("value") or a.get("raw") or None
  return t, a

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  print("=== AXIS2 REATTACH (>=10k) v1_25 (canonical suffix both sides + street_nosuf unique + broad unit) ===")
  print("[info] events:", args.events)
  print("[info] spine :", args.spine)

  # load events
  events = []
  towns_needed = set()
  with open(args.events, "r", encoding="utf-8") as f:
    for line in f:
      if not line.strip(): continue
      ev = json.loads(line)
      t, a = safe_get_town_addr(ev)
      if isinstance(t, str) and t.strip():
        towns_needed.add(up(t))
      events.append(ev)

  print("[info] events rows:", len(events), "towns_needed:", len(towns_needed))

  # build spine indexes (town-filtered)
  t0 = time.time()
  idx_full = defaultdict(list)        # (town, full) -> [pid...]
  idx_unit = defaultdict(list)        # (town, full, unit) -> [pid...]
  idx_nosuf = defaultdict(list)       # (town, num, street_base) -> [pid...]

  scanned = kept = town_skip = no_key = 0

  with open(args.spine, "r", encoding="utf-8") as f:
    for line in f:
      if not line.strip(): continue
      scanned += 1
      r = json.loads(line)
      town = up(r.get("town") or "")
      if town not in towns_needed:
        town_skip += 1
      else:
        addr = r.get("full_address") or ""
        pid = r.get("property_id")
        if not pid or not addr:
          no_key += 1
        else:
          kept += 1
          k = canonicalize_for_key(town, addr)
          if k["full"]:
            idx_full[(town, k["full"])].append(pid)
            if k["unit"]:
              idx_unit[(town, k["full"], k["unit"])].append(pid)
          if k["num"] and k["street_base"]:
            idx_nosuf[(town, k["num"], k["street_base"])].append(pid)

      if scanned % 200000 == 0:
        print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} elapsed_s={time.time()-t0:.1f}")

  print("[ok] spine index built full=", len(idx_full), "unit=", len(idx_unit), "nosuf=", len(idx_nosuf),
        "debug=", {"scanned_rows": scanned, "kept_rows": kept, "no_key": no_key, "town_skip": town_skip},
        "elapsed_s=", round(time.time()-t0,1))

  # attach logic
  stats = Counter()
  out_rows = 0

  def decide_unique(pids):
    if not pids: return None
    u = list(dict.fromkeys(pids))
    return u[0] if len(u) == 1 else None

  with open(args.out, "w", encoding="utf-8") as out:
    for ev in events:
      t, a = safe_get_town_addr(ev)
      town = up(t) if isinstance(t, str) else None
      addr = a if isinstance(a, str) else None

      attach = (ev.get("attach") or {})
      scope = attach.get("attach_scope") or ("MULTI" if (ev.get("attachments") or ev.get("attachments_n") or 0) else "SINGLE")

      st = "UNKNOWN"
      mm = "no_match"
      why = "NO_MATCH"
      pid = None

      if town and addr:
        k = canonicalize_for_key(town, addr)

        # 1) full exact (canonicalized)
        if k["full"]:
          pids = idx_full.get((town, k["full"]), [])
          u = decide_unique(pids)
          if u:
            st, mm, why, pid = "ATTACHED_A", "axis2_full_address_exact", None, u
          else:
            # 1b) unit-narrowing if a unit exists and full collides
            if k["unit"]:
              pidsu = idx_unit.get((town, k["full"], k["unit"]), [])
              uu = decide_unique(pidsu)
              if uu:
                st, mm, why, pid = "ATTACHED_A", "axis2_full+unit_exact", None, uu

        # 2) safe fallback: street_nosuf unique (ignores suffix dialect entirely)
        if st != "ATTACHED_A":
          if k["num"] and k["street_base"]:
            pids2 = idx_nosuf.get((town, k["num"], k["street_base"]), [])
            u2 = decide_unique(pids2)
            if u2:
              st, mm, why, pid = "ATTACHED_A", "axis2_street_nosuf_unique", None, u2

        # if still unknown but we had no number
        if st != "ATTACHED_A":
          if not k["num"]:
            mm, why = "no_num", "NO_NUM"

      # write attach block in the format probe script expects
      ev_out = dict(ev)
      ev_out["attach_scope"] = scope
      ev_out["attach_status"] = st
      ev_out["match_method"] = mm
      ev_out["why"] = why
      ev_out["matched_property_id"] = pid
      out.write(json.dumps(ev_out, ensure_ascii=False) + "\n")
      out_rows += 1
      stats["attached_a" if st=="ATTACHED_A" else "still_unknown"] += 1

  audit = {
    "script": "hampden_axis2_reattach_ge10k_v1_25.py",
    "events": args.events,
    "spine": args.spine,
    "out": args.out,
    "stats": dict(stats)
  }
  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print("[done] wrote out_rows=", out_rows, "stats=", dict(stats), "audit=", args.audit)

if __name__ == "__main__":
  main()
