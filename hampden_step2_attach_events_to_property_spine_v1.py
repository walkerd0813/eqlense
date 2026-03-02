#!/usr/bin/env python3
"""
Hampden STEP 2 v1 — Attach events to Property Spine (confidence-gated, UNKNOWN-first)

Inputs (from STEP 1):
  backend/publicData/registry/hampden/_events_v1/*.ndjson

Property spine (default; override with --spine):
  backend/publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json

Attachment rules (defensible, no guessing):
- Only attach when we have a strong address+town match to a property spine record.
- UNKNOWN is first-class: if we cannot match confidently, we emit attach.status="UNKNOWN".
- We do NOT rewrite parcels, zoning, or property truths. We only add an attach block to events.

This script builds an address index from the property spine:
  key = f"{town_norm}|{addr_norm}"

Normalization is conservative:
- uppercase
- collapse whitespace
- normalize STREET suffixes minimally (ST/STREET, AVE/AVENUE, RD/ROAD, etc.)
- keeps unit designator in addr_norm when present; but also tries a "no-unit" fallback
  with a lower confidence (to avoid unit mis-attach).

Outputs:
  backend/publicData/registry/hampden/_attached_v1/events_attached_v1.ndjson
  backend/publicData/_audit/registry/hampden_step2_attach_audit_v1.json
"""

import argparse, os, json, glob, re, hashlib
from collections import Counter, defaultdict
from datetime import datetime, timezone

SUFFIX_MAP = {
  " STREET":" ST", " ST.":" ST", " ST":" ST",
  " AVENUE":" AVE", " AVE.":" AVE", " AVE":" AVE",
  " ROAD":" RD", " RD.":" RD", " RD":" RD",
  " DRIVE":" DR", " DR.":" DR", " DR":" DR",
  " LANE":" LN", " LN.":" LN", " LN":" LN",
  " COURT":" CT", " CT.":" CT", " CT":" CT",
  " PLACE":" PL", " PL.":" PL", " PL":" PL",
  " TERRACE":" TER", " TER.":" TER", " TER":" TER",
  " BOULEVARD":" BLVD", " BLVD.":" BLVD", " BLVD":" BLVD",
  " PARKWAY":" PKWY", " PKWY.":" PKWY", " PKWY":" PKWY",
  " CIRCLE":" CIR", " CIR.":" CIR", " CIR":" CIR",
}

UNIT_RE = re.compile(r"\b(UNIT|APT|APARTMENT|#)\s*([A-Z0-9\-]+)\b", re.IGNORECASE)

def now_iso():
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def sha256_file(path, chunk=1024*1024):
  h=hashlib.sha256()
  with open(path,"rb") as f:
    while True:
      b=f.read(chunk)
      if not b: break
      h.update(b)
  return h.hexdigest()

def norm_town(s):
  if not s: return None
  s=str(s).upper().strip()
  s=re.sub(r"[^A-Z0-9\s\-]", " ", s)
  s=re.sub(r"\s+", " ", s).strip()
  return s

def norm_addr(s):
  if not s: return None
  s=str(s).upper().strip()
  s=re.sub(r"[\,\.\;]", " ", s)
  s=re.sub(r"\s+", " ", s).strip()
  # normalize common suffixes (end-of-string)
  for k,v in SUFFIX_MAP.items():
    if s.endswith(k):
      s = s[: -len(k)] + v
      break
  s=re.sub(r"\s+", " ", s).strip()
  return s

def strip_unit(addr_norm):
  if not addr_norm: return (None, None)
  m=UNIT_RE.search(addr_norm)
  if not m:
    return (addr_norm, None)
  unit = (m.group(1).upper(), m.group(2).upper())
  stripped = UNIT_RE.sub("", addr_norm)
  stripped = re.sub(r"\s+", " ", stripped).strip()
  return (stripped, f"{unit[0]} {unit[1]}")

def load_spine(spine_path):
  # Supports JSON array, NDJSON, or object with "properties" list.
  text = open(spine_path,"r",encoding="utf-8").read().strip()
  if not text:
    return []
  if text[0] == "[":
    data=json.loads(text)
    return data
  if text[0] == "{":
    obj=json.loads(text)
    for k in ("properties","items","rows"):
      if isinstance(obj.get(k), list):
        return obj[k]
    # if it looks like a single property, wrap it
    return [obj]
  # NDJSON
  rows=[]
  with open(spine_path,"r",encoding="utf-8") as f:
    for line in f:
      line=line.strip()
      if not line: continue
      try: rows.append(json.loads(line))
      except: pass
  return rows

def pick_spine_default(backend_root):
  candidates=[
    os.path.join(backend_root, "publicData","properties","_attached","CURRENT","CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"),
    os.path.join(backend_root, "publicData","properties","_attached","CURRENT","CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.ndjson"),
    os.path.join(backend_root, "publicData","properties","_attached","CURRENT","CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.jsonl"),
  ]
  for c in candidates:
    if os.path.exists(c):
      return c
  # fallback: any CURRENT_PROPERTIES*.json
  globbed=sorted(glob.glob(os.path.join(backend_root,"publicData","properties","_attached","CURRENT","CURRENT_PROPERTIES*.json")))
  if globbed:
    return globbed[0]
  return candidates[0]

def build_spine_index(spine_rows):
  idx=defaultdict(list)
  stats=Counter()
  for r in spine_rows:
    pid = r.get("property_id") or r.get("id")
    addr = r.get("address") or (r.get("address_norm") or (r.get("address_full") or None))
    town = r.get("town") or r.get("city") or r.get("municipality")
    # some records have nested address fields
    if isinstance(addr, dict):
      addr = addr.get("full") or addr.get("address") or addr.get("line1")
    if isinstance(town, dict):
      town = town.get("name") or town.get("town")
    t=norm_town(town)
    a=norm_addr(addr)
    if not pid or not t or not a:
      stats["skipped_missing_fields"] += 1
      continue
    a_no_unit, unit = strip_unit(a)
    key = f"{t}|{a}"
    idx[key].append({"property_id": pid, "parcel_id_norm": r.get("parcel_id_norm") or r.get("parcel_id") or r.get("parcel_id_raw"), "addr_norm": a, "town_norm": t})
    stats["indexed_full"] += 1
    # also index without unit to support fallback matching
    if a_no_unit and a_no_unit != a:
      key2=f"{t}|{a_no_unit}"
      idx[key2].append({"property_id": pid, "parcel_id_norm": r.get("parcel_id_norm") or r.get("parcel_id") or r.get("parcel_id_raw"), "addr_norm": a_no_unit, "town_norm": t})
      stats["indexed_no_unit"] += 1
  return idx, stats

def attach_one(event, idx):
  pref = event.get("property_ref") or {}
  town = norm_town(pref.get("town") or pref.get("city") or pref.get("municipality"))
  addr = norm_addr(pref.get("address") or pref.get("addr") or pref.get("street_address"))
  if not town or not addr:
    return {"status":"UNKNOWN","attach_confidence":0.0,"attach_method":"missing_town_or_address","candidates":0}
  a_no_unit, unit = strip_unit(addr)
  key_full=f"{town}|{addr}"
  key_nounit=f"{town}|{a_no_unit}" if a_no_unit else None

  # prefer full match first
  cands = idx.get(key_full, [])
  if len(cands)==1:
    c=cands[0]
    return {"status":"ATTACHED","property_id":c["property_id"],"parcel_id_norm":c.get("parcel_id_norm"),"attach_method":"town+addr_exact","attach_confidence":0.95,"candidates":1}
  if len(cands)>1:
    return {"status":"MULTI_MATCH","attach_method":"town+addr_exact","attach_confidence":0.70,"candidates":len(cands)}

  # fallback: nounit match (lower confidence, because condo/unit risk)
  if key_nounit:
    c2 = idx.get(key_nounit, [])
    if len(c2)==1:
      c=c2[0]
      conf = 0.82 if unit else 0.90
      method = "town+addr_no_unit_fallback" if unit else "town+addr_exact_no_unit"
      return {"status":"ATTACHED","property_id":c["property_id"],"parcel_id_norm":c.get("parcel_id_norm"),"attach_method":method,"attach_confidence":conf,"candidates":1}
    if len(c2)>1:
      return {"status":"MULTI_MATCH","attach_method":"town+addr_no_unit_fallback","attach_confidence":0.60,"candidates":len(c2)}

  return {"status":"UNKNOWN","attach_method":"no_match_in_spine_index","attach_confidence":0.0,"candidates":0}

def main():
  ap=argparse.ArgumentParser()
  ap.add_argument("--backendRoot", required=True)
  ap.add_argument("--eventsDir", default=None)
  ap.add_argument("--spine", default=None)
  ap.add_argument("--outDir", default=None)
  ap.add_argument("--audit", default=None)
  args=ap.parse_args()

  backend_root=args.backendRoot
  events_dir=args.eventsDir or os.path.join(backend_root,"publicData","registry","hampden","_events_v1")
  out_dir=args.outDir or os.path.join(backend_root,"publicData","registry","hampden","_attached_v1")
  audit_path=args.audit or os.path.join(backend_root,"publicData","_audit","registry","hampden_step2_attach_audit_v1.json")
  spine_path=args.spine or pick_spine_default(backend_root)

  os.makedirs(out_dir, exist_ok=True)
  os.makedirs(os.path.dirname(audit_path), exist_ok=True)

  # load spine
  if not os.path.exists(spine_path):
    raise SystemExit(f"[error] property spine file not found: {spine_path}")
  spine_rows = load_spine(spine_path)
  idx, idx_stats = build_spine_index(spine_rows)

  # scan event files
  event_files=sorted(glob.glob(os.path.join(events_dir,"*_events.ndjson")))
  if not event_files:
    raise SystemExit(f"[error] no *_events.ndjson found in {events_dir}")

  out_all=os.path.join(out_dir,"events_attached_v1.ndjson")
  counts=Counter()
  status_counts=Counter()
  type_status=defaultdict(Counter)

  with open(out_all,"w",encoding="utf-8") as out:
    for fp in event_files:
      etype=os.path.basename(fp).replace("_events.ndjson","").upper()
      with open(fp,"r",encoding="utf-8") as f:
        for line in f:
          line=line.strip()
          if not line: continue
          try: ev=json.loads(line)
          except: 
            counts["bad_json"]+=1
            continue
          attach = attach_one(ev, idx)
          ev["attach"]=attach
          ev["attach"]["attach_as_of_date"]=now_iso()[:10]
          ev["attach"]["spine_path"]=spine_path
          status=attach["status"]
          status_counts[status]+=1
          type_status[etype][status]+=1
          counts[etype]+=1
          out.write(json.dumps(ev, ensure_ascii=False)+"\n")

  audit={
    "created_at": now_iso(),
    "events_dir": events_dir,
    "out_dir": out_dir,
    "out_all": out_all,
    "spine_path": spine_path,
    "spine_rows": len(spine_rows),
    "spine_index_stats": dict(idx_stats),
    "event_type_counts": {k:int(v) for k,v in counts.items() if k not in ("bad_json",)},
    "attach_status_counts": dict(status_counts),
    "attach_status_by_type": {k: dict(v) for k,v in type_status.items()},
    "notes": [
      "UNKNOWN-first: no fuzzy guessing beyond town+addr exact and a controlled no-unit fallback.",
      "MULTI_MATCH is surfaced for later disambiguation; we do not auto-pick.",
    ]
  }
  with open(audit_path,"w",encoding="utf-8") as f:
    json.dump(audit,f,indent=2)

  print("[start] Hampden STEP 2 v1 - Attach events to Property Spine (confidence-gated)")
  print("[info] events_dir:", events_dir)
  print("[info] spine:", spine_path)
  print("[done] out:", out_all)
  print("[done] audit:", audit_path)
  print("[done] attach_status_counts:", dict(status_counts))
  print("[next] Build Market Radar V1 (Hampden only) using attached events + ZIP/property_type slices.")

if __name__=="__main__":
  main()
