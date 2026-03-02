import argparse, json, os
from datetime import datetime, timezone

def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def _get_zip(row):
    geo = row.get("geo") or {}
    if isinstance(geo, dict):
        # support geo.zip or geo.zip5
        z = geo.get("zip") or geo.get("zip5") or geo.get("zipcode")
        if z is not None:
            return str(z).strip()
    return None

def _iter_windows(row):
    """
    Supports either:
      windows = { "30": {...}, "90": {...} }
    or:
      windows = [ {"window_days":30, ...}, ... ]
    """
    w = row.get("windows")
    if w is None:
        return []

    if isinstance(w, dict):
        out = []
        for k, v in w.items():
            try:
                wd = int(k)
            except Exception:
                # maybe v has window_days
                wd = v.get("window_days") if isinstance(v, dict) else None
                try:
                    wd = int(wd)
                except Exception:
                    continue
            out.append((wd, v if isinstance(v, dict) else {}))
        return out

    if isinstance(w, list):
        out = []
        for item in w:
            if not isinstance(item, dict):
                continue
            wd = item.get("window_days") or item.get("window") or item.get("days")
            try:
                wd = int(wd)
            except Exception:
                continue
            out.append((wd, item))
        return out

    return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    built_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    audit = {
        "built_at": built_at,
        "infile": args.infile,
        "rows_in": 0,
        "rows_out": 0,
        "rows_skipped_no_zip": 0,
        "rows_skipped_no_windows": 0,
        "distinct_zips": 0,
        "distinct_property_types": 0,
        "distinct_window_days": 0
    }

    out_rows = []
    zset, pset, wset = set(), set(), set()

    for r in read_ndjson(args.infile):
        audit["rows_in"] += 1

        z = _get_zip(r)
        if not z:
            audit["rows_skipped_no_zip"] += 1
            continue

        pt = r.get("property_type")
        if pt is None:
            pt = ""  # allow join even if missing, but this should be rare
        pt = str(pt).strip()

        win_pairs = _iter_windows(r)
        if not win_pairs:
            audit["rows_skipped_no_windows"] += 1
            continue

        # carry-through blocks
        base_metrics = r.get("metrics") or {}
        base_inventory = r.get("inventory") or {}
        as_of = r.get("as_of")

        for (wd, wobj) in win_pairs:
            row_out = {
                "zip": z,
                "asset_bucket": pt,         # alias to join contract
                "window_days": int(wd),     # alias to join contract

                # preserve original fields for provenance/debug
                "as_of": as_of,
                "geo": r.get("geo") or {},
                "property_type": pt,

                # Keep original rollup blocks (no renames)
                "inventory": base_inventory,
                "metrics": base_metrics,

                # Window-specific metrics go under qa.windows or a dedicated block
                "qa": {
                    "mls_windows_obj": wobj
                }
            }
            out_rows.append(row_out)
            zset.add(z); pset.add(pt); wset.add(int(wd))

    audit["rows_out"] = len(out_rows)
    audit["distinct_zips"] = len(zset)
    audit["distinct_property_types"] = len(pset)
    audit["distinct_window_days"] = len(wset)

    write_ndjson(args.out, out_rows)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] explode_mls_rollup_windows_v0_1")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
