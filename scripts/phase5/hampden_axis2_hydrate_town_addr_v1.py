import argparse, json, re, sys, os
from datetime import datetime

def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def pick_str(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return norm_ws(v)
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    stats = {
        "rows": 0,
        "town_filled": 0,
        "addr_filled": 0,
        "town_still_none": 0,
        "addr_still_none": 0,
        "both_still_none": 0,
        "sample_bad": []
    }

    t0 = datetime.utcnow()

    with open(args.infile, "r", encoding="utf-8") as f_in, \
         open(args.out, "w", encoding="utf-8") as f_out:

        for line in f_in:
            if not line.strip():
                continue
            r = json.loads(line)
            stats["rows"] += 1

            pref = r.get("property_ref") or {}

            # hydrate town
            if not (isinstance(r.get("town"), str) and r["town"].strip()):
                town = pick_str(pref.get("town_norm"), pref.get("town_raw"))
                if town:
                    r["town"] = town.upper()
                    stats["town_filled"] += 1

            # hydrate addr
            if not (isinstance(r.get("addr"), str) and r["addr"].strip()):
                addr = pick_str(pref.get("address_norm"), pref.get("address_raw"))
                if addr:
                    r["addr"] = addr.upper()
                    stats["addr_filled"] += 1

            # basic auditing
            town_ok = isinstance(r.get("town"), str) and r["town"].strip()
            addr_ok  = isinstance(r.get("addr"), str) and r["addr"].strip()

            if not town_ok:
                stats["town_still_none"] += 1
            if not addr_ok:
                stats["addr_still_none"] += 1
            if (not town_ok) and (not addr_ok):
                stats["both_still_none"] += 1
                if len(stats["sample_bad"]) < 10:
                    stats["sample_bad"].append({
                        "event_id": r.get("event_id"),
                        "property_ref_keys": sorted(list(pref.keys()))[:50]
                    })

            f_out.write(json.dumps(r, ensure_ascii=False) + "\n")

    t1 = datetime.utcnow()
    audit = {
        "infile": args.infile,
        "out": args.out,
        "rows": stats["rows"],
        "stats": stats,
        "created_utc": t1.isoformat() + "Z",
        "elapsed_s": (t1 - t0).total_seconds()
    }
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] hydrate town/addr")
    print(json.dumps({"out": args.out, "audit": args.audit, "stats": stats}, indent=2))

if __name__ == "__main__":
    main()
