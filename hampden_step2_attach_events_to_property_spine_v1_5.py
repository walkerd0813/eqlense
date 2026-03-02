import os, json, re, argparse, hashlib
from datetime import datetime, timezone

"""Hampden STEP 2 v1.5

Attach registry index-derived events to the Property Spine.

Key fix vs v1.4:
- Clean index-report artifacts that were leaking into town/address strings
  (e.g., "SPRINGFIELD ADDR" and trailing "Y" in address).
These artifacts prevented exact town+address matching.
"""

def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode('utf-8', errors='ignore')).hexdigest()

_WS = re.compile(r"\s+")

def normalize_town(s: str) -> str:
    if not s:
        return ""
    s = s.upper()
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = _WS.sub(" ", s).strip()
    if not s:
        return ""
    toks = [t for t in s.split(" ") if t]
    # Registry index reports often include a trailing "ADDR" marker.
    # Remove it safely wherever it appears.
    toks = [t for t in toks if t not in {"ADDR", "ADDRESS"}]
    # Sometimes the report includes "CITY" as a label (e.g., "HOLYOKE CITY").
    # The spine typically stores town names without the label.
    if len(toks) >= 2 and toks[-1] == "CITY":
        toks = toks[:-1]
    # Also strip common label tokens that are not part of the town name.
    toks = [t for t in toks if t not in {"TOWN", "OF"}]
    return " ".join(toks).strip()

def normalize_address(s: str) -> str:
    if not s:
        return ""
    s = s.upper()
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    s = re.sub(r"[^A-Z0-9 #\-./ ]+", " ", s)
    s = _WS.sub(" ", s).strip()
    if not s:
        return ""
    toks = [t for t in s.split(" ") if t]
    # Some index PDFs include a trailing single-letter marker (often "Y")
    # as part of the report layout. Drop it.
    if toks and len(toks[-1]) == 1 and toks[-1] in {"Y", "N"}:
        toks = toks[:-1]
    # Also drop trailing "ADDR" if it leaked into the address field.
    if toks and toks[-1] in {"ADDR", "ADDRESS"}:
        toks = toks[:-1]
    return " ".join(toks).strip()

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def build_spine_index(spine_path: str):
    """Build town+address -> property_id index.

Supports either:
- NDJSON property spine (preferred for scale)
- JSON array
- JSON wrapper with a meta object + ndjson pointer (your CURRENT_PROPERTIES_PHASE4* file)
"""

    spine_index = {}
    meta = {"spine_index_keys": 0, "spine_format": "unknown"}

    # Detect wrapper JSON that contains "properties_ndjson"
    try:
        with open(spine_path, "r", encoding="utf-8") as f:
            head = f.read(4096)
        if head.lstrip().startswith("{"):
            with open(spine_path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict) and obj.get("properties_ndjson"):
                ndjson_path = obj["properties_ndjson"]
                if not os.path.isabs(ndjson_path):
                    ndjson_path = os.path.join(os.path.dirname(spine_path), ndjson_path)
                meta["spine_format"] = "wrapper_json_with_properties_ndjson"
                meta["properties_ndjson"] = ndjson_path
                spine_path = ndjson_path
    except Exception:
        pass

    # NDJSON detection
    if spine_path.lower().endswith(".ndjson"):
        meta["spine_format"] = "ndjson"
        for rec in iter_ndjson(spine_path):
            pid = rec.get("property_id") or rec.get("propertyId")
            town_raw = rec.get("town") or rec.get("town_name") or rec.get("city")
            addr_raw = rec.get("address") or rec.get("address_raw") or rec.get("site_address")
            town = normalize_town(town_raw or "")
            addr = normalize_address(addr_raw or "")
            if not pid or not town or not addr:
                continue
            key = f"{town}|{addr}"
            if key not in spine_index:
                spine_index[key] = pid

        meta["spine_index_keys"] = len(spine_index)
        return spine_index, meta

    # JSON array (small) fallback
    try:
        with open(spine_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            meta["spine_format"] = "json_array"
            for rec in data:
                pid = rec.get("property_id") or rec.get("propertyId")
                town = normalize_town((rec.get("town") or rec.get("city") or ""))
                addr = normalize_address((rec.get("address") or rec.get("address_raw") or ""))
                if not pid or not town or not addr:
                    continue
                key = f"{town}|{addr}"
                if key not in spine_index:
                    spine_index[key] = pid
            meta["spine_index_keys"] = len(spine_index)
    except Exception:
        pass

    return spine_index, meta

def extract_locator(ev: dict):
    # Prefer explicit property_ref fields (from Hampden index normalization)
    pr = ev.get("property_ref") or {}
    town_raw = pr.get("town_raw") or pr.get("town") or ""
    addr_raw = pr.get("address_raw") or pr.get("address") or ""

    # Fallbacks
    if not town_raw or not addr_raw:
        rec = ev.get("recording") or {}
        town_raw = town_raw or rec.get("town") or rec.get("town_raw") or ""
        addr_raw = addr_raw or rec.get("address") or rec.get("address_raw") or ""

    town = normalize_town(town_raw)
    addr = normalize_address(addr_raw)
    return town_raw, addr_raw, town, addr

def attach_events(events_dir: str, spine_idx: dict, out_path: str):
    counts = {"total": 0, "ATTACHED_A": 0, "UNKNOWN": 0, "MISSING_TOWN_OR_ADDRESS": 0}
    samples_missing = []
    samples_unmatched = []

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as out:
        for fn in sorted(os.listdir(events_dir)):
            if not fn.endswith(".ndjson"):
                continue
            src = os.path.join(events_dir, fn)
            for ev in iter_ndjson(src):
                counts["total"] += 1

                town_raw, addr_raw, town, addr = extract_locator(ev)
                if not town or not addr:
                    counts["MISSING_TOWN_OR_ADDRESS"] += 1
                    counts["UNKNOWN"] += 1
                    if len(samples_missing) < 5:
                        samples_missing.append({
                            "src": fn,
                            "event_id": ev.get("event_id"),
                            "event_type": ev.get("event_type"),
                            "town_raw": town_raw,
                            "address_raw": addr_raw,
                            "available_keys": sorted(list(ev.keys()))
                        })
                    ev["attachment"] = {
                        "attach_status": "UNKNOWN",
                        "attach_method": "town_address_exact",
                        "attach_confidence": 0.0,
                        "reason": "missing town/address"
                    }
                    out.write(json.dumps(ev, ensure_ascii=False) + "\n")
                    continue

                key = f"{town}|{addr}"
                pid = spine_idx.get(key)
                if pid:
                    counts["ATTACHED_A"] += 1
                    ev["attachment"] = {
                        "attach_status": "ATTACHED_A",
                        "attach_method": "town_address_exact",
                        "attach_confidence": 1.0,
                        "property_id": pid,
                        "spine_key": key
                    }
                else:
                    counts["UNKNOWN"] += 1
                    if len(samples_unmatched) < 5:
                        samples_unmatched.append({
                            "src": fn,
                            "event_id": ev.get("event_id"),
                            "event_type": ev.get("event_type"),
                            "town_norm": town,
                            "address_norm": addr
                        })
                    ev["attachment"] = {
                        "attach_status": "UNKNOWN",
                        "attach_method": "town_address_exact",
                        "attach_confidence": 0.0,
                        "spine_key": key
                    }

                out.write(json.dumps(ev, ensure_ascii=False) + "\n")

    return {
        "counts": counts,
        "samples": {
            "missing_locator": samples_missing,
            "unmatched_locator": samples_unmatched
        }
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eventsDir", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    spine_idx, spine_meta = build_spine_index(args.spine)
    result = attach_events(args.eventsDir, spine_idx, args.out)

    audit = {
        "created_at": utc_now_iso(),
        "events_dir": os.path.abspath(args.eventsDir),
        "spine_path": os.path.abspath(args.spine),
        "spine_meta_detected": spine_meta,
        **result
    }

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[info] spine_index_keys:", spine_meta.get("spine_index_keys", 0))
    print("[done] out:", args.out)
    print("[done] audit:", args.audit)
    print("[done] attach_status_counts:", {k: v for k, v in audit["counts"].items() if k in ["ATTACHED_A", "UNKNOWN", "MISSING_TOWN_OR_ADDRESS"]})


if __name__ == "__main__":
    main()
