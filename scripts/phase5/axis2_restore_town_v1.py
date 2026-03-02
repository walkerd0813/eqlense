import argparse, json
from typing import Optional

def get_nested(d, path):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def pick_str(*vals) -> Optional[str]:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def town_from_spine(rec: dict) -> Optional[str]:
    # Try common keys we’ve used across the pipeline
    return pick_str(
        rec.get("town"),
        rec.get("city"),
        rec.get("municipality"),
        rec.get("address_town"),
        rec.get("address_city"),
        get_nested(rec, ["address", "town"]),
        get_nested(rec, ["address", "city"]),
        get_nested(rec, ["location", "town"]),
        get_nested(rec, ["location", "city"]),
    )

def town_from_row(row: dict) -> Optional[str]:
    # if source file stores town under a slightly different key
    return pick_str(
        row.get("town"),
        row.get("city"),
        get_nested(row, ["property_ref","town"]),
        get_nested(row, ["property_ref","city"]),
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--src", required=False, help="file that still contains town keyed by event_id")
    args = ap.parse_args()

    # Load spine town by property_id (stream load into dict; OK for MA if big, but if too big we’ll switch to sqlite later)
    spine_town = {}
    with open(args.spine, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): 
                continue
            r = json.loads(line)
            pid = r.get("property_id")
            if pid:
                t = town_from_spine(r)
                if t:
                    spine_town[pid] = t.upper()

    src_town = {}
    if args.src:
        with open(args.src, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                eid = r.get("event_id")
                if eid:
                    t = town_from_row(r)
                    if t:
                        src_town[eid] = t.upper()

    total = 0
    filled_from_spine = 0
    filled_from_src = 0
    still_missing = 0

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            row = json.loads(line)
            total += 1

            have_town = isinstance(row.get("town"), str) and row["town"].strip()
            if not have_town:
                pid = row.get("property_id")
                if pid and pid in spine_town:
                    row["town"] = spine_town[pid]
                    filled_from_spine += 1
                else:
                    eid = row.get("event_id")
                    if eid and eid in src_town:
                        row["town"] = src_town[eid]
                        filled_from_src += 1

            if not (isinstance(row.get("town"), str) and row["town"].strip()):
                still_missing += 1

            fout.write(json.dumps(row, ensure_ascii=False) + "\n")

    audit = {
        "total": total,
        "filled_from_spine": filled_from_spine,
        "filled_from_src": filled_from_src,
        "still_missing_town": still_missing,
        "src_used": bool(args.src),
        "spine_pid_town_loaded": len(spine_town),
        "src_eventid_town_loaded": len(src_town) if args.src else 0
    }
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
