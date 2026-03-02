import argparse, json, os, re
from datetime import datetime, timezone

WIN_FROM_KEY = re.compile(r"^(\d{1,4})\s*d$", re.IGNORECASE)  # "30d" -> 30
WIN_NUMERIC = re.compile(r"^\d{1,4}$")
ZIP_RE = re.compile(r"^\d{5}$")

BUCKET_MAP = {
    "sf": "SF",
    "single family": "SF",
    "single_family": "SF",
    "singlefamily": "SF",

    "mf": "MF",
    "multi family": "MF",
    "multi_family": "MF",
    "multifamily": "MF",

    "condo": "CONDO",
    "condominium": "CONDO",

    "land": "LAND"
}

def _norm_bucket(pt):
    if pt is None:
        return ""
    s = str(pt).strip().lower()
    return BUCKET_MAP.get(s, str(pt).strip())

def _is_valid_zip(z: str) -> bool:
    if not z:
        return False
    z = str(z).strip()
    if not ZIP_RE.match(z):
        return False
    if z == "00000":
        return False
    return True

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
        z = geo.get("zip") or geo.get("zip5") or geo.get("zipcode")
        if z is not None:
            return str(z).strip()
    return None

def _extract_window_days_from_windows_dict(windows):
    out = []
    if not isinstance(windows, dict):
        return out

    for k, v in windows.items():
        ks = str(k).strip()
        wd = None

        m = WIN_FROM_KEY.match(ks)
        if m:
            wd = int(m.group(1))
        elif WIN_NUMERIC.match(ks):
            wd = int(ks)
        else:
            digits = re.findall(r"\d{1,4}", ks)
            if digits:
                try:
                    wd = int(digits[0])
                except Exception:
                    wd = None

        if wd is None:
            continue
        out.append((wd, v if isinstance(v, dict) else {}))

    return sorted(out, key=lambda t: t[0])

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
        "rows_skipped_bad_zip": 0,
        "rows_zip_00000": 0,
        "rows_skipped_no_windows": 0,
        "windows_key_samples": [],
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

        if z == "00000":
            audit["rows_zip_00000"] += 1

        if not _is_valid_zip(z):
            audit["rows_skipped_bad_zip"] += 1
            continue

        pt = _norm_bucket(r.get("property_type"))

        windows = r.get("windows")
        if isinstance(windows, dict) and len(audit["windows_key_samples"]) < 10:
            audit["windows_key_samples"].append(list(windows.keys())[:10])

        win_pairs = _extract_window_days_from_windows_dict(windows)
        if not win_pairs:
            audit["rows_skipped_no_windows"] += 1
            continue

        base_metrics = r.get("metrics") or {}
        base_inventory = r.get("inventory") or {}
        as_of = r.get("as_of")
        geo = r.get("geo") or {}

        for (wd, wobj) in win_pairs:
            row_out = {
                "zip": z,
                "asset_bucket": pt,
                "window_days": int(wd),

                "as_of": as_of,
                "geo": geo,
                "property_type": r.get("property_type"),

                "inventory": base_inventory,
                "metrics": base_metrics,

                "qa": {
                    "windows": wobj,
                    "windows_key": f"{wd}d",
                    "mls_metric_semantics": {
                        "rollup_metrics_fields_are_as_written": True,
                        "window_specific_fields_live_under": "qa.windows"
                    }
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

    print("[done] explode_mls_rollup_windows_v0_4")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
