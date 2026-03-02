# scripts\phase5\hampden_axis2_reattach_ge10k_v1_26.py
import argparse, json, re, time
from collections import Counter, defaultdict

def norm_ws(s): return re.sub(r"\s+", " ", (s or "").strip())
def up(s): return norm_ws(s).upper()

DIR_TOKENS = {"N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST"}

# NOTE: CIR/CIRCLE -> CR (matches your spine: "PINE GROVE CR")
SUF_MAP = {
  "STREET":"ST","ST":"ST","ST.":"ST",
  "AVENUE":"AV","AVE":"AV","AVE.":"AV","AV":"AV","AV.":"AV",
  "ROAD":"RD","RD":"RD","RD.":"RD",
  "DRIVE":"DR","DR":"DR","DR.":"DR",
  "LANE":"LN","LN":"LN","LN.":"LN",
  "PLACE":"PL","PL":"PL","PL.":"PL",
  "COURT":"CT","CT":"CT","CT.":"CT",
  "BOULEVARD":"BL","BLVD":"BL","BLVD.":"BL","BL":"BL",
  "PARKWAY":"PW","PKWY":"PW","PKWY.":"PW","PW":"PW",
  "TERRACE":"TE","TERR":"TE","TER":"TE","TE":"TE",

  # your data dialect:
  "CIRCLE":"CR","CIR":"CR","CR":"CR",

  "TR":"TR","TRL":"TR","TRAIL":"TR",
  "WAY":"WY","WY":"WY",
  "EXTN":"EXTN","EXT":"EXTN","EXTENSION":"EXTN",
  "PARK":"PARK",
  "HL":"HL","HILL":"HL",
  "LA":"LA",
}

def canonical_suffix(tok):
  tok = up(tok)
  return SUF_MAP.get(tok, tok)

def split_addr_tokens(addr):
  # basic tokenization; keep alnum and '#'
  toks = [t for t in re.split(r"[^A-Z0-9#]+", up(addr)) if t]
  return toks

def collapse_runs(s):
  # deterministic typo smoother: MELWOOOD -> MELWOOD (OOO->OO)
  # collapse any run of same letter to max 2
  return re.sub(r"([A-Z])\1{2,}", r"\1\1", s)

def normalize_unit_tokens(toks):
  out = []
  unit_val = None
  i = 0
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

def canonicalize_for_key(town, addr_raw):
  addr_raw = collapse_runs(up(addr_raw))
  toks = split_addr_tokens(addr_raw)

  # number
  num = None
  rest = []
  for tok in toks:
    if num is None and tok.isdigit():
      num = tok
      continue
    rest.append(tok)

  # strip directionals
  rest = [t for t in rest if t not in DIR_TOKENS]

  # unit normalize (from string)
  rest, unit_val = normalize_unit_tokens(rest)

  # suffix normalize: last alpha token
  if rest and rest[-1].isalpha():
    rest[-1] = canonical_suffix(rest[-1])

  # derive street_base and full
  street_base = None
  if rest:
    if rest[-1].isalpha():
      base = rest[:-1]
    else:
      base = rest[:]
    street_base = " ".join(base).strip() if base else None

  full = None
  if num and rest:
    full = num + " " + " ".join(rest)

  return {"town": up(town), "num": num, "full": full, "street_base": street_base, "unit": unit_val}

def safe_get_town_addr(ev):
  t = ev.get("town"); a = ev.get("addr")
  if isinstance(t, dict): t = t.get("value") or t.get("raw")
  if isinstance(a, dict): a = a.get("value") or a.get("raw")
  return t, a

def spine_get_town(r):
  # broaden town resolution
  for k in ("town","city","municipality","site_city","addr_city","address_city","town_norm","city_norm"):
    v = r.get(k)
    if isinstance(v, str) and v.strip():
      return v
  return None

def spine_get_unit(r):
  # broaden unit resolution
  for k in ("unit","addr_unit","unit_no","unit_number","apt","apartment","suite"):
    v = r.get(k)
    if isinstance(v, str) and v.strip():
      return up(v)
  return None

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--events", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  print("=== AXIS2 REATTACH (>=10k) v1_26 (spine-unit-field + CIR->CR + typo collapse + spine town lookup) ===")
  print("[info] events:", args.events)
  print("[info] spine :", args.spine)

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

  t0 = time.time()
  idx_full = defaultdict(list)          # (town, full) -> [pid...]
  idx_unit = defaultdict(list)          # (town, full, unit) -> [pid...]
  idx_nosuf = defaultdict(list)         # (town, num, street_base) -> [pid...]

  scanned = kept = town_skip = no_key = 0

  with open(args.spine, "r", encoding="utf-8") as f:
    for line in f:
      if not line.strip(): continue
      scanned += 1
      r = json.loads(line)

      town_raw = spine_get_town(r)
      town = up(town_raw) if town_raw else ""
      if town not in towns_needed:
        town_skip += 1
        continue

      addr = r.get("full_address") or ""
      pid = r.get("property_id")
      if not pid or not addr:
        no_key += 1
        continue

      kept += 1
      k = canonicalize_for_key(town, addr)

      if k["full"]:
        idx_full[(town, k["full"])].append(pid)

        # IMPORTANT: use spine unit field too (most condo units live here)
        spine_unit = spine_get_unit(r)
        if spine_unit:
          idx_unit[(town, k["full"], spine_unit)].append(pid)

        # also keep the embedded unit if present in full_address
        if k["unit"]:
          idx_unit[(town, k["full"], up(k["unit"]))].append(pid)

      if k["num"] and k["street_base"]:
        idx_nosuf[(town, k["num"], k["street_base"])].append(pid)

      if scanned % 200000 == 0:
        print(f"[progress] scanned_rows={scanned} kept_rows={kept} town_skip={town_skip} elapsed_s={time.time()-t0:.1f}")

  print("[ok] spine index built full=", len(idx_full), "unit=", len(idx_unit), "nosuf=", len(idx_nosuf),
        "debug=", {"scanned_rows": scanned, "kept_rows": kept, "no_key": no_key, "town_skip": town_skip},
        "elapsed_s=", round(time.time()-t0,1))

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

      attach_scope = ev.get("attach_scope") or ("MULTI" if (ev.get("attachments_n") or 0) else "SINGLE")

      st, mm, why, pid = "UNKNOWN", "no_match", "NO_MATCH", None

      if town and addr:
        k = canonicalize_for_key(town, addr)

        # full exact unique
        if k["full"]:
          u = decide_unique(idx_full.get((town, k["full"]), []))
          if u:
            st, mm, why, pid = "ATTACHED_A", "axis2_full_address_exact", None, u
          else:
            # unit-narrowing (works even if spine stores unit in separate field)
            if k["unit"]:
              uu = decide_unique(idx_unit.get((town, k["full"], up(k["unit"])), []))
              if uu:
                st, mm, why, pid = "ATTACHED_A", "axis2_full+unit_exact", None, uu

        # street base (no suffix) unique fallback
        if st != "ATTACHED_A":
          if k["num"] and k["street_base"]:
            u2 = decide_unique(idx_nosuf.get((town, k["num"], k["street_base"]), []))
            if u2:
              st, mm, why, pid = "ATTACHED_A", "axis2_street_nosuf_unique", None, u2

        if st != "ATTACHED_A" and not k["num"]:
          mm, why = "no_num", "NO_NUM"

      ev_out = dict(ev)
      ev_out["attach_scope"] = attach_scope
      ev_out["attach_status"] = st
      ev_out["match_method"] = mm
      ev_out["why"] = why
      ev_out["matched_property_id"] = pid

      out.write(json.dumps(ev_out, ensure_ascii=False) + "\n")
      out_rows += 1
      stats["attached_a" if st=="ATTACHED_A" else "still_unknown"] += 1

  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump({
      "script":"hampden_axis2_reattach_ge10k_v1_26.py",
      "events": args.events,
      "spine": args.spine,
      "out": args.out,
      "stats": dict(stats)
    }, f, indent=2)

  print("[done] wrote out_rows=", out_rows, "stats=", dict(stats), "audit=", args.audit)

if __name__ == "__main__":
  main()
