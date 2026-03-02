import os, json, re, argparse, hashlib
from datetime import datetime, timezone

"""Hampden STEP 2 v1.6

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

def build_spine_index(spine_path: str, backend_root: str = None):
    """Build an index: (town_norm,address_norm) -> property_id.

    Supports CURRENT wrapper JSON that points to a large NDJSON file via 'properties_ndjson'
    (or similar key). Path resolution tries:
      - absolute paths
      - relative to wrapper JSON directory
      - relative to backend_root (defaults to CWD)
    """
    if backend_root is None:
        backend_root = os.getcwd()

    meta = {"counts": {"SPINE_INDEX_KEYS": 0}, "spine_meta_detected": {}}

    # 1) Determine the actual records file we should stream
    records_path = None
    spine_meta = {}

    if spine_path.lower().endswith(".ndjson") and os.path.exists(spine_path):
        records_path = spine_path
        spine_meta = {"spine_wrapper": False, "records_path": records_path}
    else:
        try:
            with open(spine_path, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception as e:
            meta["spine_meta_detected"] = {"spine_wrapper": None, "error": "failed_to_read_spine_json", "detail": str(e)}
            return {}, 0, meta

        if not isinstance(obj, dict):
            meta["spine_meta_detected"] = {"spine_wrapper": True, "error": "spine_json_not_object"}
            return {}, 0, meta

        raw = None
        for k in ["properties_ndjson", "properties_file", "properties_path", "propertiesNdjson", "properties"]:
            if k in obj and isinstance(obj[k], str):
                raw = obj[k]
                break

        if not raw:
            meta["spine_meta_detected"] = {"spine_wrapper": True, "error": "no_properties_path_key", "raw_keys": list(obj.keys())}
            return {}, 0, meta

        # resolve candidate paths
        cand = None
        raw = raw.strip()
        if os.path.isabs(raw) and os.path.exists(raw):
            cand = raw
        else:
            wrapper_dir = os.path.dirname(spine_path)
            cand1 = os.path.normpath(os.path.join(wrapper_dir, raw))
            if os.path.exists(cand1):
                cand = cand1
            else:
                cand2 = os.path.normpath(os.path.join(backend_root, raw))
                if os.path.exists(cand2):
                    cand = cand2

        records_path = cand
        spine_meta = {"spine_wrapper": True, "properties_path_raw": raw, "properties_path_resolved": records_path, "raw_keys": list(obj.keys())}

    meta["spine_meta_detected"] = spine_meta

    if not records_path or not os.path.exists(records_path):
        return {}, 0, meta

    # 2) Stream records and build index
    idx = {}
    keys = 0
    with open(records_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            # property_id
            pid = rec.get("property_id") or rec.get("id") or rec.get("propertyId")
            if not pid:
                continue

            # town
            town = (
                rec.get("town_norm")
                or rec.get("town")
                or rec.get("city")
                or rec.get("municipality")
                or rec.get("townName")
            )
            # address
            addr = (
                rec.get("address_norm")
                or rec.get("address")
                or rec.get("address_line1")
                or rec.get("street_address")
                or rec.get("site_address")
                or rec.get("siteAddress")
            )

            town_n = normalize_town(town or "")
            addr_n = normalize_address(addr or "")
            if not town_n or not addr_n:
                continue

            idx[(town_n, addr_n)] = pid
            keys += 1

    meta["counts"]["SPINE_INDEX_KEYS"] = keys
    return idx, keys, meta

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
