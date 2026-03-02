#!/usr/bin/env python3
import argparse, json, os, re, datetime, hashlib
from collections import defaultdict

ZIP_RE = re.compile(r"^\d{5}$")

def ndjson_iter(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def parse_date_any(x):
    if not x:
        return None
    if isinstance(x, (datetime.date, datetime.datetime)):
        return x.date() if isinstance(x, datetime.datetime) else x
    s = str(x).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    try:
        return datetime.datetime.fromisoformat(s.replace("Z","")).date()
    except Exception:
        return None

def parse_int(x):
    try:
        if x is None: return None
        if isinstance(x, bool): return None
        if isinstance(x, int): return int(x)
        if isinstance(x, float): return int(x) if x.is_integer() else int(x)
        s = str(x).strip()
        if not s: return None
        return int(float(s))
    except Exception:
        return None

def norm_bucket(x):
    if x is None:
        return None
    s = str(x).strip().lower()
    if not s:
        return None
    if "condo" in s:
        return "CONDO"
    if "multi" in s or "2 fam" in s or "3 fam" in s or "4 fam" in s or "two" in s or "three" in s or "four" in s:
        return "MF"
    if "land" in s or "lot" in s or "vacant" in s:
        return "LAND"
    if "single" in s or s == "sf" or "residential" in s or "house" in s:
        return "SF"
    return "OTHER"

def classify_status(row):
    candidates = [
        ("status", row.get("status")),
        ("lifecycle", row.get("lifecycle")),
        ("listingStatus", row.get("listingStatus")),
        ("mlsStatus", row.get("mlsStatus")),
    ]
    for k,v in candidates:
        if v:
            return k, str(v).strip().upper()
    return None, None

def map_status(raw):
    if not raw:
        return "OTHER"
    s = raw.strip().upper()
    if "ACTIVE" in s:
        return "ACTIVE"
    if "PEND" in s or "UAG" in s or "UNDER AGRE" in s or "UNDER CONTRACT" in s or "CONTING" in s:
        return "PENDING"
    if "SOLD" in s or "CLOSED" in s or "CLSD" in s:
        return "CLOSED"
    if "WITHDRAW" in s:
        return "WITHDRAWN"
    if "CANCEL" in s:
        return "CANCELED"
    if ("TEMP" in s and ("OFF" in s or "REMOVED" in s)) or ("OFF" in s and "MARKET" in s) or "EXPIRE" in s:
        return "OFF_MARKET"
    return "OTHER"

def pick_bucket(row):
    if row.get("propertyType") is not None:
        return "propertyType", norm_bucket(row.get("propertyType"))
    if row.get("property_type") is not None:
        return "property_type", norm_bucket(row.get("property_type"))
    if row.get("asset_bucket") is not None:
        return "asset_bucket", norm_bucket(row.get("asset_bucket"))
    return None, None

def is_valid_zip(z):
    return bool(z) and isinstance(z,str) and ZIP_RE.match(z) and z != "00000"

def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    if p <= 0: return float(sorted_vals[0])
    if p >= 100: return float(sorted_vals[-1])
    k = (len(sorted_vals)-1) * (p/100.0)
    f = int(k)
    c = min(f+1, len(sorted_vals)-1)
    if f == c:
        return float(sorted_vals[f])
    d = k - f
    return float(sorted_vals[f] + (sorted_vals[c]-sorted_vals[f]) * d)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--as_of", required=True)
    ap.add_argument("--windows", default="30,90,180,365")
    args = ap.parse_args()

    as_of = parse_date_any(args.as_of)
    if not as_of:
        raise SystemExit("bad --as_of (expected YYYY-MM-DD)")

    windows = sorted(set(int(x) for x in str(args.windows).split(",") if str(x).strip()))

    counts = defaultdict(lambda: defaultdict(lambda: {
        "active": 0, "pending": 0, "closed": 0, "withdrawn": 0, "canceled": 0, "off_market": 0, "other": 0,
        "dom_samples": []
    }))

    audit = {
        "built_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of_date": as_of.isoformat(),
        "inputs": {"infile": os.path.abspath(args.infile)},
        "windows": windows,
        "scan": {"rows_in": 0, "rows_written": 0, "skipped_bad_zip": 0, "skipped_missing_zip": 0, "skipped_missing_bucket": 0, "skipped_bad_window": 0, "key_dupes_seen": 0},
        "field_keys_seen": {"zip": defaultdict(int), "bucket": defaultdict(int), "status": defaultdict(int), "days_on_market": defaultdict(int), "dates_list": defaultdict(int), "dates_contract": defaultdict(int), "dates_sale": defaultdict(int), "dates_off_market": defaultdict(int)},
        "event_counts_total": defaultdict(int),
    }

    for row in ndjson_iter(args.infile):
        audit["scan"]["rows_in"] += 1

        z = None
        if isinstance(row.get("address"), dict) and row["address"].get("zip") is not None:
            z = str(row["address"]["zip"]).strip()
            audit["field_keys_seen"]["zip"]["address.zip"] += 1
        elif row.get("zip") is not None:
            z = str(row.get("zip")).strip()
            audit["field_keys_seen"]["zip"]["zip"] += 1

        if not z:
            audit["scan"]["skipped_missing_zip"] += 1
            continue
        if not is_valid_zip(z):
            audit["scan"]["skipped_bad_zip"] += 1
            continue

        bucket_field, b = pick_bucket(row)
        if bucket_field:
            audit["field_keys_seen"]["bucket"][bucket_field] += 1
        if not b:
            audit["scan"]["skipped_missing_bucket"] += 1
            continue

        status_field, raw_status = classify_status(row)
        if status_field:
            audit["field_keys_seen"]["status"][status_field] += 1
        st = map_status(raw_status)
        audit["event_counts_total"][st] += 1

        dates = row.get("dates") if isinstance(row.get("dates"), dict) else {}
        list_date = parse_date_any(dates.get("listDate"))
        contract_date = parse_date_any(dates.get("contractDate"))
        sale_date = parse_date_any(dates.get("saleDate"))
        off_market_date = parse_date_any(dates.get("offMarketDate"))

        if dates.get("daysOnMarket") is not None:
            audit["field_keys_seen"]["days_on_market"]["dates.daysOnMarket"] += 1
        if dates.get("listDate") is not None:
            audit["field_keys_seen"]["dates_list"]["dates.listDate"] += 1
        if dates.get("contractDate") is not None:
            audit["field_keys_seen"]["dates_contract"]["dates.contractDate"] += 1
        if dates.get("saleDate") is not None:
            audit["field_keys_seen"]["dates_sale"]["dates.saleDate"] += 1
        if dates.get("offMarketDate") is not None:
            audit["field_keys_seen"]["dates_off_market"]["dates.offMarketDate"] += 1

        ref = None
        if st == "CLOSED":
            ref = sale_date or contract_date or list_date
        elif st == "PENDING":
            ref = contract_date or list_date
        elif st in ("WITHDRAWN", "CANCELED", "OFF_MARKET"):
            ref = off_market_date or list_date
        else:
            ref = list_date or contract_date or sale_date or off_market_date

        if not ref:
            audit["scan"]["skipped_bad_window"] += 1
            continue

        age_days = (as_of - ref).days
        if age_days < 0:
            audit["scan"]["skipped_bad_window"] += 1
            continue

        dom = parse_int(dates.get("daysOnMarket"))

        for w in windows:
            if age_days <= w:
                c = counts[(z, b)][w]
                if st == "ACTIVE":
                    c["active"] += 1
                elif st == "PENDING":
                    c["pending"] += 1
                elif st == "CLOSED":
                    c["closed"] += 1
                elif st == "WITHDRAWN":
                    c["withdrawn"] += 1
                elif st == "CANCELED":
                    c["canceled"] += 1
                elif st == "OFF_MARKET":
                    c["off_market"] += 1
                else:
                    c["other"] += 1
                if dom is not None and dom >= 0:
                    c["dom_samples"].append(float(dom))
                break

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    wrote = 0
    with open(args.out, "w", encoding="utf-8") as out:
        for (z, b), winmap in counts.items():
            for w in windows:
                c = winmap.get(w)
                if not c:
                    continue
                samples = sorted(c["dom_samples"])
                if samples:
                    dom_median = percentile(samples, 50)
                    dom_p25 = percentile(samples, 25)
                    dom_p75 = percentile(samples, 75)
                    dom_mean = sum(samples) / float(len(samples))
                else:
                    dom_median = dom_p25 = dom_p75 = dom_mean = None

                doc = {
                    "layer": "mls_liquidity",
                    "as_of_date": as_of.isoformat(),
                    "window_days": w,
                    "zip": z,
                    "asset_bucket": b,
                    "metrics": {
                        "active_count": c["active"],
                        "pending_count": c["pending"],
                        "closed_count": c["closed"],
                        "withdrawn_count": c["withdrawn"],
                        "canceled_count": c["canceled"],
                        "off_market_count": c["off_market"],
                        "other_count": c["other"],
                        "dom_samples": len(samples),
                        "dom_median": dom_median,
                        "dom_p25": dom_p25,
                        "dom_p75": dom_p75,
                        "dom_mean": dom_mean
                    },
                    "provenance": {
                        "source": "mls.normalized.listings",
                        "zip_field": "address.zip",
                        "bucket_field": "propertyType",
                        "status_field": "status|lifecycle (best-effort)",
                        "date_fields": {
                            "list": "dates.listDate",
                            "contract": "dates.contractDate",
                            "sale": "dates.saleDate",
                            "off_market": "dates.offMarketDate",
                            "dom": "dates.daysOnMarket"
                        }
                    }
                }
                out.write(json.dumps(doc, ensure_ascii=False) + "\n")
                wrote += 1

    audit["scan"]["rows_written"] = wrote
    audit["output"] = {"out": args.out, "rows_written": wrote, "sha256": sha256_file(args.out)}
    audit["field_keys_seen"] = {k: dict(v) for k, v in audit["field_keys_seen"].items()}
    audit["event_counts_total"] = dict(audit["event_counts_total"])

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] rollup MLS liquidity ZIP v0_2")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
