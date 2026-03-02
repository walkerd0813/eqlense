# hampden_step2_attach_events_to_property_spine_v1_7_1.py
# FIX: spine can be NDJSON OR meta JSON wrapper with properties_ndjson OR JSON array.

import json, argparse, os
from datetime import datetime

def norm(s):
    return " ".join(s.upper().strip().split()) if isinstance(s, str) else ""

def extract_addr(v):
    if isinstance(v, str): return v
    if isinstance(v, dict):
        for k in ("norm","line1","raw","address","street"):
            if isinstance(v.get(k), str): return v[k]
    return ""

def extract_town(v):
    if isinstance(v, str): return v
    if isinstance(v, dict):
        for k in ("town","city","municipality","name"):
            if isinstance(v.get(k), str): return v[k]
    return ""

def iter_spine_records(spine_path):
    # Try NDJSON first
    try:
        with open(spine_path, "r", encoding="utf-8") as f:
            first = f.readline()
            if not first:
                return
            # If the first non-whitespace char is "{" we still might be NDJSON.
            # Attempt to parse first line. If it fails, treat file as full JSON.
            try:
                json.loads(first)
                # NDJSON mode: yield first then rest
                yield json.loads(first)
                for line in f:
                    line = line.strip()
                    if not line: continue
                    yield json.loads(line)
                return
            except Exception:
                pass
    except Exception:
        pass

    # Full JSON mode (meta wrapper or array)
    with open(spine_path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    # Meta wrapper pointing to ndjson
    if isinstance(obj, dict) and isinstance(obj.get("properties_ndjson"), str):
        nd = obj["properties_ndjson"]
        if not os.path.isabs(nd):
            nd = os.path.normpath(os.path.join(os.path.dirname(spine_path), nd))
        with open(nd, "r", encoding="utf-8") as f2:
            for line in f2:
                line = line.strip()
                if not line: continue
                yield json.loads(line)
        return

    # JSON array of properties
    if isinstance(obj, list):
        for rec in obj:
            if isinstance(rec, dict):
                yield rec
        return

    # Single dict property record (rare)
    if isinstance(obj, dict):
        yield obj

def build_spine_index(spine_path):
    idx = {}
    count = 0
    for p in iter_spine_records(spine_path):
        count += 1
        addr = extract_addr(
            p.get("address") or
            p.get("address_norm") or
            p.get("situs_address") or
            (p.get("location") or {}).get("address")
        )
        town = extract_town(
            p.get("town") or
            p.get("city") or
            p.get("municipality") or
            p.get("situs_city")
        )
        if not addr or not town:
            continue
        k = f"{norm(town)}|{norm(addr)}"
        pid = p.get("property_id")
        if pid:
            idx[k] = pid
    return idx, count

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eventsDir", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    a = ap.parse_args()

    spine_idx, spine_rows_seen = build_spine_index(a.spine)
    counts = {"ATTACHED_A": 0, "UNKNOWN": 0, "MISSING_TOWN_OR_ADDRESS": 0}

    os.makedirs(os.path.dirname(a.out), exist_ok=True)

    with open(a.out, "w", encoding="utf-8") as out:
        for fn in os.listdir(a.eventsDir):
            if not fn.endswith(".ndjson"):
                continue
            src_path = os.path.join(a.eventsDir, fn)
            with open(src_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    e = json.loads(line)

                    pr = e.get("property_ref") or {}
                    town = pr.get("town") or e.get("town_raw") or ""
                    addr = pr.get("address") or e.get("address_raw") or ""

                    if not town or not addr:
                        e["attach_status"] = "UNKNOWN"
                        counts["UNKNOWN"] += 1
                        counts["MISSING_TOWN_OR_ADDRESS"] += 1
                        out.write(json.dumps(e) + "\n")
                        continue

                    key = f"{norm(town)}|{norm(addr)}"
                    pid = spine_idx.get(key)

                    if pid:
                        e["property_id"] = pid
                        e["attach_status"] = "ATTACHED_A"
                        counts["ATTACHED_A"] += 1
                    else:
                        e["attach_status"] = "UNKNOWN"
                        counts["UNKNOWN"] += 1

                    out.write(json.dumps(e) + "\n")

    os.makedirs(os.path.dirname(a.audit), exist_ok=True)
    with open(a.audit, "w", encoding="utf-8") as af:
        json.dump({
            "created_at": datetime.utcnow().isoformat() + "Z",
            "spine_path": a.spine,
            "spine_rows_seen": spine_rows_seen,
            "spine_index_keys": len(spine_idx),
            "counts": counts
        }, af, indent=2)

    print("[done] spine_rows_seen:", spine_rows_seen)
    print("[done] spine_index_keys:", len(spine_idx))
    print("[done] attach_status_counts:", counts)

if __name__ == "__main__":
    main()
