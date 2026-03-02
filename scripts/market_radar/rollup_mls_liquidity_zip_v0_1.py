#!/usr/bin/env python3
"""
Market Radar - MLS Liquidity ZIP Rollup (v0_1)

Purpose
- Build a Layer A (MLS) rollup artifact specifically for Liquidity P01.
- Input: backend/mls/normalized/listings.ndjson (5-year+ dataset)
- Output: NDJSON exploded by (zip, asset_bucket, window_days, as_of_date)

Key doctrine
- Observed behavior only (no prediction/advice).
- Windowed, provenance-safe, QA-rich, ZIP-hygiene enforced.

What this produces per (zip, bucket, window_days)
- inventory snapshot (counts of active listings as-of export, if lifecycle/status implies ACTIVE)
- event flows within window (pending/UAG events, withdrawals/off-market, closed sales)
- DOM medians from events (pending + sold) when daysOnMarket exists
- coverage flags and field-key evidence
"""

import argparse
import datetime
import json
import os
import re
import statistics
import sys
import hashlib
from collections import defaultdict

ZIP_RE = re.compile(r"^\d{5}$")

WINDOWS_DEFAULT = [30, 90, 180, 365]

# --------- helpers ---------

def ndjson_iter(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def sha256_file(path, chunk=1024 * 1024):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def parse_date_any(x):
    """Return datetime.date or None."""
    if not x:
        return None
    if isinstance(x, (int, float)):
        return None
    s = str(x).strip()
    if not s:
        return None

    # Common: YYYY-MM-DD
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass

    # Common: MM/DD/YYYY
    for fmt in ("%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass

    return None

def is_valid_zip(z):
    if z is None:
        return False
    z = str(z).strip()
    return bool(ZIP_RE.match(z)) and z != "00000"

def norm_bucket(x):
    if x is None:
        return None
    s = str(x).strip().lower()
    if not s:
        return None
    # common MLS propertyType variants
    if s in ("sf", "singlefamily", "single_family", "single family", "single-family", "residential"):
        return "SF"
    if s in ("mf", "multi", "multi_family", "multi family", "two family", "three family", "four family", "2 family", "3 family", "4 family", "multifamily"):
        return "MF"
    if "condo" in s:
        return "CONDO"
    if "land" in s or "lot" in s or "vacant" in s:
        return "LAND"
    # keep original short label if it's already one of ours
    if s.upper() in ("SF", "MF", "CONDO", "LAND", "OTHER", "UNKNOWN"):
        return s.upper()
    return "OTHER"

def pick_first(d, paths):
    """Try multiple dotted paths and return (value, keypath_used) or (None, None)."""
    for p in paths:
        cur = d
        ok = True
        for k in p.split("."):
            if not isinstance(cur, dict) or k not in cur:
                ok = False
                break
            cur = cur.get(k)
        if ok and cur is not None:
            return cur, p
    return None, None

def normalize_status(s):
    if s is None:
        return None
    t = str(s).strip().lower()
    if not t:
        return None
    # collapse punctuation
    t = re.sub(r"[\s\-_]+", " ", t)
    return t

def classify_lifecycle(row):
    """
    Return one of: ACTIVE, PENDING, CLOSED, WITHDRAWN, OFF_MARKET, OTHER, UNKNOWN
    based on normalized lifecycle/status fields.
    """
    lifecycle, _ = pick_first(row, ["lifecycle", "status", "mls.lifecycle", "mls.status"])
    t = normalize_status(lifecycle)
    if t is None:
        return "UNKNOWN"

    # ACTIVE
    if t in ("active", "new", "coming soon") or "active" in t:
        return "ACTIVE"

    # PENDING / UNDER AGREEMENT / CONTINGENT
    if "pending" in t or "under agreement" in t or "uag" in t or "contingent" in t or "under contract" in t:
        return "PENDING"

    # CLOSED / SOLD
    if "closed" in t or "sold" in t:
        return "CLOSED"

    # WITHDRAWN / CANCELLED
    if "withdraw" in t or "canceled" in t or "cancelled" in t:
        return "WITHDRAWN"

    # TEMP OFF / EXPIRED / OFF MARKET
    if "off market" in t or "temporarily" in t or "temp off" in t or "expired" in t:
        return "OFF_MARKET"

    return "OTHER"

def median_or_none(vals):
    vals = [v for v in vals if isinstance(v, (int, float)) and v >= 0]
    if not vals:
        return None
    try:
        return statistics.median(vals)
    except Exception:
        vals2 = sorted(vals)
        mid = len(vals2) // 2
        if len(vals2) % 2 == 1:
            return vals2[mid]
        return (vals2[mid - 1] + vals2[mid]) / 2.0

# --------- main ---------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="MLS normalized listings NDJSON (e.g., backend/mls/normalized/listings.ndjson)")
    ap.add_argument("--out", required=True, help="Output NDJSON (exploded)")
    ap.add_argument("--audit", required=True, help="Audit JSON output")
    ap.add_argument("--as_of", required=True, help="YYYY-MM-DD")
    ap.add_argument("--windows", default="30,90,180,365", help="Comma list of window days (default 30,90,180,365)")
    args = ap.parse_args()

    as_of = parse_date_any(args.as_of)
    if not as_of:
        raise SystemExit("Bad --as_of (expected YYYY-MM-DD)")

    windows = []
    for part in str(args.windows).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            windows.append(int(part))
        except Exception:
            pass
    if not windows:
        windows = list(WINDOWS_DEFAULT)
    windows = sorted(set(windows))

    audit = {
        "built_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of_date": as_of.isoformat(),
        "inputs": {"infile": args.infile},
        "windows": windows,
        "scan": {
            "rows_in": 0,
            "rows_written": 0,
            "skipped_bad_zip": 0,
            "skipped_missing_zip": 0,
            "skipped_missing_bucket": 0,
            "skipped_bad_window": 0,
            "key_dupes_seen": 0,
        },
        "field_keys_seen": {
            "zip": {},
            "bucket": {},
            "lifecycle": {},
            "days_on_market": {},
            "dates_list": {},
            "dates_contract": {},
            "dates_sale": {},
            "dates_off_market": {},
        },
        "event_counts_total": defaultdict(int),
        "output": {},
    }

    # Counters keyed by (zip, bucket) and per window
    # We'll output exploded rows for each window with:
    # - inventory snapshot counts (active as-of)
    # - flow counts for event dates within window:
    #   pending_events, closed_events, withdrawn_events, off_market_events, new_listings
    # - dom medians (pending + closed)
    counts = defaultdict(lambda: defaultdict(lambda: {
        "inventory_active": 0,              # snapshot-style count (based on lifecycle ACTIVE)
        "events_pending": 0,
        "events_closed": 0,
        "events_withdrawn": 0,
        "events_off_market": 0,
        "events_new_listings": 0,
        "dom_pending_samples": [],
        "dom_closed_samples": [],
    }))

    seen_keys = set()

    # Predefine field paths (robust to shape drift)
    ZIP_PATHS = ["address.zip", "address.zip5", "address.postalCode", "address.postal_code", "zip", "geo.zip"]
    BUCKET_PATHS = ["propertyType", "property_type", "mls.propertyType", "mls.property_type", "type"]
    DOM_PATHS = ["daysOnMarket", "days_on_market", "metrics.daysOnMarket", "metrics.days_on_market", "dom", "dates.daysOnMarket"]
    LISTDATE_PATHS = ["dates.listDate", "dates.list_date", "listDate", "list_date"]
    CONTRACTDATE_PATHS = ["dates.contractDate", "dates.contract_date", "contractDate", "contract_date"]
    SALEDATE_PATHS = ["dates.saleDate", "dates.sale_date", "saleDate", "sale_date"]
    OFFMARKETDATE_PATHS = ["dates.offMarketDate", "dates.off_market_date", "offMarketDate", "off_market_date", "dates.withdrawnDate", "dates.withdrawn_date"]

    # Scan input
    for row in ndjson_iter(args.infile):
        audit["scan"]["rows_in"] += 1

        z, zkey = pick_first(row, ZIP_PATHS)
        if zkey:
            audit["field_keys_seen"]["zip"][zkey] = audit["field_keys_seen"]["zip"].get(zkey, 0) + 1
        if not z:
            audit["scan"]["skipped_missing_zip"] += 1
            continue
        z = str(z).strip()
        if not is_valid_zip(z):
            audit["scan"]["skipped_bad_zip"] += 1
            continue

        ptype, bkey = pick_first(row, BUCKET_PATHS)
        if bkey:
            audit["field_keys_seen"]["bucket"][bkey] = audit["field_keys_seen"]["bucket"].get(bkey, 0) + 1
        b = norm_bucket(ptype)
        if not b:
            audit["scan"]["skipped_missing_bucket"] += 1
            continue

        lifecycle = classify_lifecycle(row)
        audit["event_counts_total"][lifecycle] += 1

        # Dates for windowed flows
        list_date, lk = pick_first(row, LISTDATE_PATHS)
        contract_date, ck = pick_first(row, CONTRACTDATE_PATHS)
        sale_date, sk = pick_first(row, SALEDATE_PATHS)
        offm_date, ok = pick_first(row, OFFMARKETDATE_PATHS)

        if lk: audit["field_keys_seen"]["dates_list"][lk] = audit["field_keys_seen"]["dates_list"].get(lk, 0) + 1
        if ck: audit["field_keys_seen"]["dates_contract"][ck] = audit["field_keys_seen"]["dates_contract"].get(ck, 0) + 1
        if sk: audit["field_keys_seen"]["dates_sale"][sk] = audit["field_keys_seen"]["dates_sale"].get(sk, 0) + 1
        if ok: audit["field_keys_seen"]["dates_off_market"][ok] = audit["field_keys_seen"]["dates_off_market"].get(ok, 0) + 1

        ld = parse_date_any(list_date)
        cd = parse_date_any(contract_date)
        sd = parse_date_any(sale_date)
        od = parse_date_any(offm_date)

        dom, dk = pick_first(row, DOM_PATHS)
        if dk:
            audit["field_keys_seen"]["days_on_market"][dk] = audit["field_keys_seen"]["days_on_market"].get(dk, 0) + 1
        try:
            dom_val = int(dom) if dom is not None and str(dom).strip() != "" else None
        except Exception:
            dom_val = None

        # Snapshot: active inventory count (not windowed, but repeated in each window row)
        if lifecycle == "ACTIVE":
            for w in windows:
                counts[(z, b)][w]["inventory_active"] += 1

        # Windowed flows: based on event-specific dates
        # - new listings: listDate within window
        # - pending: contractDate within window (fallback listDate)
        # - closed: saleDate within window
        # - withdrawn/off_market: offMarketDate within window (fallback listDate when missing)
        # Note: We count into ALL windows that satisfy age_days <= w (not "smallest only"),
        # because these are window aggregates (30d includes those within 30d, 90d includes those within 90d, etc.).
        def bump_if_in_window(event_date, field_name, dom_bucket=None):
            if not event_date:
                return
            age = (as_of - event_date).days
            if age < 0:
                return
            for w in windows:
                if age <= w:
                    counts[(z, b)][w][field_name] += 1
                    if dom_bucket and dom_val is not None and dom_val >= 0:
                        counts[(z, b)][w][dom_bucket].append(dom_val)

        # classify each lifecycle into which flow to bump
        if lifecycle in ("ACTIVE", "UNKNOWN", "OTHER"):
            # still allow new listing bump from listDate
            bump_if_in_window(ld, "events_new_listings")
        elif lifecycle == "PENDING":
            bump_if_in_window(cd or ld, "events_pending", dom_bucket="dom_pending_samples")
            bump_if_in_window(ld, "events_new_listings")
        elif lifecycle == "CLOSED":
            bump_if_in_window(sd, "events_closed", dom_bucket="dom_closed_samples")
            bump_if_in_window(ld, "events_new_listings")
        elif lifecycle == "WITHDRAWN":
            bump_if_in_window(od or ld, "events_withdrawn")
            bump_if_in_window(ld, "events_new_listings")
        elif lifecycle == "OFF_MARKET":
            bump_if_in_window(od or ld, "events_off_market")
            bump_if_in_window(ld, "events_new_listings")

    # Write exploded output
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    wrote = 0
    for (z, b), winmap in counts.items():
        for w in windows:
            r = winmap.get(w)
            if not r:
                continue
            key = (z, b, w)
            if key in seen_keys:
                audit["scan"]["key_dupes_seen"] += 1
                continue
            seen_keys.add(key)

    with open(args.out, "w", encoding="utf-8") as out:
        for (z, b), winmap in sorted(counts.items()):
            for w in windows:
                r = winmap.get(w)
                if not r:
                    continue

                doc = {
                    "layer": "mls_liquidity",
                    "as_of_date": as_of.isoformat(),
                    "window_days": int(w),
                    "zip": z,
                    "asset_bucket": b,
                    "inventory": {
                        "mls_active": int(r["inventory_active"]),
                    },
                    "flows": {
                        "new_listings": int(r["events_new_listings"]),
                        "pending": int(r["events_pending"]),
                        "closed": int(r["events_closed"]),
                        "withdrawn": int(r["events_withdrawn"]),
                        "off_market": int(r["events_off_market"]),
                    },
                    "metrics": {
                        "dom_median_pending": median_or_none(r["dom_pending_samples"]),
                        "dom_median_closed": median_or_none(r["dom_closed_samples"]),
                    },
                    "qa": {
                        "dom_pending_samples_n": len(r["dom_pending_samples"]),
                        "dom_closed_samples_n": len(r["dom_closed_samples"]),
                    }
                }

                out.write(json.dumps(doc, ensure_ascii=False) + "\n")
                wrote += 1

    audit["scan"]["rows_written"] = wrote
    # normalize default dicts
    audit["event_counts_total"] = dict(audit["event_counts_total"])
    audit["output"] = {
        "out": args.out,
        "rows_written": wrote,
        "sha256": sha256_file(args.out) if os.path.exists(args.out) else None,
    }

    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] rollup MLS liquidity ZIP v0_1")
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
