import os
import os, json, argparse
from datetime import datetime, timezone

def get_event_locator(e: dict):
  """
  Canonical locator lives in e["property_ref"] for our Step1_4 pipeline.
  Fall back to root-level keys only if needed.
  Returns (town_raw, address_raw).
  """
  pr = e.get("property_ref") or {}
  if isinstance(pr, dict):
    t = (pr.get("town_raw") or "").strip()
    a = (pr.get("address_raw") or "").strip()
    if t or a:
      return t, a

  # fallback (older/odd formats)
  t = (e.get("town_raw") or "").strip()
  a = (e.get("address_raw") or "").strip()
  return t, a

SUFFIX_MAP = {
  "AVENUE":"AVE","STREET":"ST","ROAD":"RD","DRIVE":"DR","LANE":"LN",
  "COURT":"CT","PLACE":"PL","TERRACE":"TER","CIRCLE":"CIR"
}

def norm_ws(s: str) -> str:
  s = (s or "").replace("\u00A0"," ")
  s = " ".join(s.split())
  return s.strip()

def strip_artifacts(s: str) -> str:
  s = norm_ws(s).upper()
  # remove surrounding quotes if present
  if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
    s = s[1:-1].strip()
  # common trailing markers from PDFs
  if s.endswith(" Y"):
    s = s[:-2].rstrip()
  # town sometimes ends like "SPRINGFIELD           Addr"
  s = s.replace(" ADDR", "").strip()
  return norm_ws(s)

def normalize_town(town: str) -> str:
  return strip_artifacts(town)

def normalize_address(addr: str) -> str:
  s = strip_artifacts(addr)
  # minimal suffix standardization (token-based)
  parts = s.split(" ")
  if parts:
    last = parts[-1]
    if last in SUFFIX_MAP:
      parts[-1] = SUFFIX_MAP[last]
  return norm_ws(" ".join(parts))

def resolve_spine_source(spine_path: str) -> str:
  """
  Your CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json is often a JSON wrapper
  that points to an NDJSON file via note.properties_ndjson or properties_ndjson.
  This resolves that, otherwise returns original path.
  """
  with open(spine_path, "r", encoding="utf-8") as f:
    head = f.read(4096).lstrip()

  if not head:
    return spine_path

  # If it's pretty JSON (starts with { or [), load full JSON and resolve pointer.
  if head[0] in "{[":
    with open(spine_path, "r", encoding="utf-8") as f:
      obj = json.load(f)

    if isinstance(obj, dict):
      # common wrapper patterns
      ptr = None
      if isinstance(obj.get("note"), dict) and obj["note"].get("properties_ndjson"):
        ptr = obj["note"]["properties_ndjson"]
      elif obj.get("properties_ndjson"):
        ptr = obj["properties_ndjson"]
      elif isinstance(obj.get("note"), dict) and obj["note"].get("properties_path"):
        ptr = obj["note"]["properties_path"]

      if ptr:
        # relative paths should be resolved relative to backend root (cwd)
        ptr = ptr.replace("/", os.sep)
        if not os.path.isabs(ptr):
          ptr = os.path.join(os.getcwd(), ptr)
        return ptr

    # If it's actually an array of properties JSON, keep original path (we'll iterate it as JSON)
    return spine_path

  # Otherwise assume NDJSON
  return spine_path

def iter_spine_rows(spine_resolved_path: str):
  """
  Supports:
    - NDJSON (streaming)
    - JSON array of properties (only if file is small enough)
  IMPORTANT: The Phase4 property spine is huge. Never json.load() big files.
  """
  try:
    size = os.path.getsize(spine_resolved_path)
  except Exception:
    size = None

  # Heuristic:
  # - .ndjson => always stream
  # - very large files => always stream
  # - otherwise try json.load (could be a JSON array)
  is_ndjson = spine_resolved_path.lower().endswith(".ndjson")
  too_big = (size is not None and size > 64 * 1024 * 1024)  # 64MB threshold

  if (not is_ndjson) and (not too_big):
    try:
      with open(spine_resolved_path, "r", encoding="utf-8") as f:
        obj = json.load(f)
      if isinstance(obj, list):
        for p in obj:
          if isinstance(p, dict):
            yield p
        return
      # dict or other: not a list of properties
      return
    except Exception:
      # fall back to NDJSON streaming
      pass

  # NDJSON streaming path (safe for huge files)
  with open(spine_resolved_path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      yield json.loads(line)

def pick_best_address(p: dict) -> str:
  # prefer full_address, fall back to address_label only if it doesn't look like "OTHER CITY, MA ZIP"
  addr = p.get("full_address") or ""
  if addr:
    return addr
  al = p.get("address_label") or ""
  # block poisoning keys: commas tend to indicate city/state/zip strings
  if "," in al:
    return ""
  return al

def build_spine_index(spine_path: str, allowed_towns: set):
  resolved = resolve_spine_source(spine_path)

  idx = {}
  seen = 0
  indexed = 0

  for p in iter_spine_rows(resolved) or []:
    seen += 1
    town_raw = p.get("town") or ""
    addr_raw = pick_best_address(p)

    t = normalize_town(town_raw)
    a = normalize_address(addr_raw)

    if not t or not a:
      continue
    if allowed_towns and (t not in allowed_towns):
      continue

    key = f"{t}|{a}"
    pid = p.get("property_id") or p.get("propertyId") or p.get("id")
    if not pid:
      continue

    if key not in idx:
      idx[key] = pid
    indexed += 1

  meta = {
    "spine_path_input": spine_path,
    "spine_path_resolved": resolved,
    "spine_rows_seen": seen,
    "spine_rows_indexed": indexed,
    "spine_index_keys": len(idx),
  }
  return idx, meta

def iter_event_files(events_dir: str):
  for fn in os.listdir(events_dir):
    if fn.endswith(".ndjson"):
      yield os.path.join(events_dir, fn)

def extract_locator(e: dict):
  pr = e.get("property_ref") or {}
  town = pr.get("town_raw") or e.get("town_raw") or ""
  addr = pr.get("address_raw") or e.get("address_raw") or ""
  return town, addr

def build_allowed_towns_from_events(events_dir: str):
  towns = set()
  for fp in iter_event_files(events_dir):
    with open(fp, "r", encoding="utf-8") as f:
      for line in f:
        line = line.strip()
        if not line:
          continue
        e = json.loads(line)
        town_raw, _ = extract_locator(e)
        t = normalize_town(town_raw)
        if t:
          towns.add(t)
  return towns

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--eventsDir", required=True)
  ap.add_argument("--spine", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--audit", required=True)
  args = ap.parse_args()

  allowed_towns = build_allowed_towns_from_events(args.eventsDir)

  spine_idx, spine_meta = build_spine_index(args.spine, allowed_towns)

  counts = {"ATTACHED_A":0,"UNKNOWN":0,"MISSING_TOWN_OR_ADDRESS":0}
  samples_missing = []
  samples_unmatched = []

  os.makedirs(os.path.dirname(args.out), exist_ok=True)
  os.makedirs(os.path.dirname(args.audit), exist_ok=True)

  with open(args.out, "w", encoding="utf-8") as out:
    for fp in iter_event_files(args.eventsDir):
      with open(fp, "r", encoding="utf-8") as f:
        for line in f:
          line=line.strip()
          if not line:
            continue
          e = json.loads(line)

          town_raw, addr_raw = extract_locator(e)
          t = normalize_town(town_raw)
          a = normalize_address(addr_raw)

          if not t or not a:
            counts["MISSING_TOWN_OR_ADDRESS"] += 1
            if len(samples_missing) < 15:
              samples_missing.append({"src": os.path.basename(fp), "event_id": e.get("event_id"), "event_type": e.get("event_type"), "town_raw": town_raw, "address_raw": addr_raw})
            e["attach_status"] = "MISSING_TOWN_OR_ADDRESS"
            out.write(json.dumps(e, ensure_ascii=False) + "\n")
            continue

          key = f"{t}|{a}"
          pid = spine_idx.get(key)

          if pid:
            e["property_id"] = pid
            e["attach_status"] = "ATTACHED_A"
            counts["ATTACHED_A"] += 1
          else:
            e["attach_status"] = "UNKNOWN"
            counts["UNKNOWN"] += 1
            if len(samples_unmatched) < 15:
              samples_unmatched.append({"src": os.path.basename(fp), "event_id": e.get("event_id"), "event_type": e.get("event_type"), "town_norm": t, "address_norm": a})
          out.write(json.dumps(e, ensure_ascii=False) + "\n")

  audit = {
    "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z"),
    "events_dir": args.eventsDir,
    "spine_path": args.spine,
    "allowed_towns_count": len(allowed_towns),
    "allowed_towns_sample": sorted(list(allowed_towns))[:30],
    "spine_meta": spine_meta,
    "counts": counts,
    "samples": {
      "missing_locator": samples_missing,
      "unmatched_locator": samples_unmatched
    }
  }
  with open(args.audit, "w", encoding="utf-8") as f:
    json.dump(audit, f, indent=2, ensure_ascii=False)

  print("[done] allowed_towns_count:", len(allowed_towns))
  for k,v in spine_meta.items():
    print("[done]", k + ":", v)
  print("[done] attach_status_counts:", counts)
  print("[done] out:", args.out)
  print("[done] audit:", args.audit)

if __name__ == "__main__":
  main()



