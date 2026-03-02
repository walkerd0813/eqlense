#!/usr/bin/env python
# Hampden STEP2 v1.7.7 - Attach events to Property Spine (locator + normalization fix, streaming-safe)
from __future__ import annotations
import argparse, glob, json, os, re
from datetime import datetime, timezone
from typing import Dict, Iterable, Tuple, Set

HAMPDEN_TOWNS = [
  "AGAWAM","BLANDFORD","BRIMFIELD","CHESTER","CHICOPEE","EAST LONGMEADOW","GRANVILLE","HAMPDEN",
  "HOLLAND","HOLYOKE","LONGMEADOW","LUDLOW","MONSON","MONTGOMERY","PALMER","RUSSELL","SOUTHWICK",
  "SPRINGFIELD","TOLLAND","WALES","WEST SPRINGFIELD","WESTFIELD","WILBRAHAM"
]

SUFFIX_MAP = {
  "AVENUE":"AVE","AVE.":"AVE",
  "STREET":"ST","ST.":"ST",
  "ROAD":"RD","RD.":"RD",
  "DRIVE":"DR","DR.":"DR",
  "LANE":"LN","LN.":"LN",
  "COURT":"CT","CT.":"CT",
  "PLACE":"PL","PL.":"PL",
  "TERRACE":"TER","TER.":"TER",
  "CIRCLE":"CIR","CIR.":"CIR",
}

_ws = re.compile(r"\s+")
_trailing_y = re.compile(r"\s+Y\s*$", re.IGNORECASE)
_addr_art = re.compile(r"\s+ADDR\s*$", re.IGNORECASE)

def now_utc_iso():
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def collapse_ws(s: str) -> str:
  return _ws.sub(" ", s).strip()

def norm_town(s: str) -> str:
  if not s: return ""
  s = s.strip()
  s = _trailing_y.sub("", s)
  s = _addr_art.sub("", s)
  s = collapse_ws(s)
  return s.upper()

def norm_addr(s: str) -> str:
  if not s: return ""
  s = s.strip()
  s = _trailing_y.sub("", s)
  s = _addr_art.sub("", s)
  s = collapse_ws(s).upper()
  if "," in s:
    s = s.split(",")[0].strip()
  parts = s.split(" ")
  if parts:
    last = parts[-1]
    if last in SUFFIX_MAP:
      parts[-1] = SUFFIX_MAP[last]
    elif last.endswith(".") and last[:-1] in SUFFIX_MAP:
      parts[-1] = SUFFIX_MAP[last[:-1]]
  return " ".join(parts).strip()

def resolve_spine_path(spine_path: str) -> str:
  if spine_path.lower().endswith(".json"):
    try:
      with open(spine_path, "r", encoding="utf-8") as f:
        obj = json.loads(f.read())
      nd = obj.get("properties_ndjson")
      if isinstance(nd, str) and nd.strip():
        return nd
    except Exception:
      pass
  return spine_path

def iter_ndjson(path: str) -> Iterable[dict]:
  with open(path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      try:
        yield json.loads(line)
      except json.JSONDecodeError:
        continue

def get_spine_locator(p: dict) -> Tuple[str,str]:
  town_raw = str(p.get("town") or "")
  addr_raw = str(p.get("full_address") or "")
  if not addr_raw:
    al = str(p.get("address_label") or "")
    if al and ("," not in al):
      addr_raw = al
  return town_raw, addr_raw

def get_event_locator(e: dict) -> Tuple[str,str]:
  pr = e.get("property_ref") or {}
  if isinstance(pr, dict):
    t = str(pr.get("town_raw") or "").strip()
    a = str(pr.get("address_raw") or "").strip()
    if t or a:
      return t, a
  return str(e.get("town_raw") or "").strip(), str(e.get("address_raw") or "").strip()

def build_spine_index(spine_ndjson: str, allowed_towns: Set[str]):
  idx: Dict[str,str] = {}
  seen = 0
  indexed = 0
  for p in iter_ndjson(spine_ndjson):
    seen += 1
    pid = p.get("property_id") or p.get("propertyId") or ""
    if not pid:
      continue
    t_raw, a_raw = get_spine_locator(p)
    t = norm_town(t_raw)
    if not t or t not in allowed_towns:
      continue
    a = norm_addr(a_raw)
    if not a:
      continue
    key = f"{t}|{a}"
    if key not in idx:
      idx[key] = pid
    indexed += 1
  meta = {"spine_rows_seen": seen, "spine_rows_indexed": indexed, "spine_index_keys": len(idx)}
  return idx, meta

def iter_event_files(events_dir: str) -> Iterable[str]:
  pats = [os.path.join(events_dir, "*_events.ndjson"), os.path.join(events_dir, "*.ndjson")]
  seen=set()
  for pat in pats:
    for fp in glob.glob(pat):
      if os.path.isdir(fp):
        continue
      bn = os.path.basename(fp).lower()
      if "events" not in bn:
        continue
      if fp in seen:
        continue
      seen.add(fp)
      yield fp

def attach_events(events_dir: str, spine_idx: Dict[str,str], out_path: str):
  os.makedirs(os.path.dirname(out_path), exist_ok=True)
  counts = {"ATTACHED_A":0, "UNKNOWN":0, "MISSING_TOWN_OR_ADDRESS":0}
  total = 0
  missing_samples = []
  unmatched_samples = []
  with open(out_path, "w", encoding="utf-8") as out:
    for fp in iter_event_files(events_dir):
      src = os.path.basename(fp)
      for e in iter_ndjson(fp):
        total += 1
        town_raw, addr_raw = get_event_locator(e)
        t = norm_town(town_raw)
        a = norm_addr(addr_raw)
        if not t or not a:
          counts["MISSING_TOWN_OR_ADDRESS"] += 1
          if len(missing_samples) < 25:
            missing_samples.append({"src": src, "event_id": e.get("event_id"), "event_type": e.get("event_type"), "town_raw": town_raw, "address_raw": addr_raw})
          e["attach_status"] = "MISSING_TOWN_OR_ADDRESS"
          e["property_id"] = None
          out.write(json.dumps(e, ensure_ascii=False) + "\n")
          continue
        key = f"{t}|{a}"
        pid = spine_idx.get(key)
        if pid:
          counts["ATTACHED_A"] += 1
          e["attach_status"] = "ATTACHED_A"
          e["property_id"] = pid
        else:
          counts["UNKNOWN"] += 1
          e["attach_status"] = "UNKNOWN"
          e["property_id"] = None
          if len(unmatched_samples) < 25:
            unmatched_samples.append({"src": src, "event_id": e.get("event_id"), "event_type": e.get("event_type"), "town_norm": t, "address_norm": a})
        out.write(json.dumps(e, ensure_ascii=False) + "\n")
  return total, counts, missing_samples, unmatched_samples

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--eventsDir", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  created_at = now_utc_iso()
  allowed_towns = set([t.upper() for t in HAMPDEN_TOWNS])

  spine_resolved = resolve_spine_path(args.spine)
  if not os.path.exists(spine_resolved):
    raise SystemExit(f"Spine not found: {spine_resolved}")

  spine_idx, spine_meta = build_spine_index(spine_resolved, allowed_towns)
  total, counts, missing_samples, unmatched_samples = attach_events(args.eventsDir, spine_idx, args.out)

  audit = {
    "created_at": created_at,
    "events_dir": args.eventsDir,
    "spine_path_input": args.spine,
    "spine_path_resolved": spine_resolved,
    "allowed_towns_count": len(allowed_towns),
    **spine_meta,
    "events_total": total,
    "counts": counts,
    "samples": {"missing_locator": missing_samples, "unmatched_locator": unmatched_samples[:15]},
    "script_version": "hampden_step2_attach_events_to_property_spine_v1_7_7"
  }

  os.makedirs(os.path.dirname(args.audit), exist_ok=True)
  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2)

  print(f"[done] allowed_towns_count: {len(allowed_towns)}")
  print(f"[done] spine_path_input: {args.spine}")
  print(f"[done] spine_path_resolved: {spine_resolved}")
  print(f"[done] spine_rows_seen: {spine_meta['spine_rows_seen']}")
  print(f"[done] spine_rows_indexed: {spine_meta['spine_rows_indexed']}")
  print(f"[done] spine_index_keys: {spine_meta['spine_index_keys']}")
  print(f"[done] events_total: {total}")
  print(f"[done] attach_status_counts: {counts}")
  print(f"[done] out: {args.out}")
  print(f"[done] audit: {args.audit}")

if __name__ == "__main__":
  main()
