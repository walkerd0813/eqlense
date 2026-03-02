import argparse, json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="ndjson source with town/addr")
    ap.add_argument("--infile", required=True, help="ndjson to restore into")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # Build lookup from src
    lut = {}
    src_rows = 0
    src_with_town = 0
    src_with_addr = 0

    with open(args.src, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            src_rows += 1
            eid = r.get("event_id")
            if not eid:
                continue
            town = r.get("town")
            addr = r.get("addr")
            if town is not None:
                src_with_town += 1
            if addr is not None:
                src_with_addr += 1
            # keep only what we need (don’t drag extra fields)
            if town is not None or addr is not None:
                lut[eid] = {"town": town, "addr": addr}

    total = 0
    restored_town = 0
    restored_addr = 0
    still_missing_town = 0
    still_missing_addr = 0
    missing_in_src = 0

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            r = json.loads(line)
            total += 1
            eid = r.get("event_id")

            before_town = r.get("town", None)
            before_addr = r.get("addr", None)

            if eid and eid in lut:
                patch = lut[eid]
                if before_town is None and patch.get("town") is not None:
                    r["town"] = patch["town"]
                    restored_town += 1
                if before_addr is None and patch.get("addr") is not None:
                    r["addr"] = patch["addr"]
                    restored_addr += 1
            else:
                if eid:
                    missing_in_src += 1

            if r.get("town") is None:
                still_missing_town += 1
            if r.get("addr") is None:
                still_missing_addr += 1

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    audit = {
        "src_rows": src_rows,
        "src_with_town": src_with_town,
        "src_with_addr": src_with_addr,
        "src_lookup_size": len(lut),
        "total_rows_out": total,
        "restored_town": restored_town,
        "restored_addr": restored_addr,
        "still_missing_town": still_missing_town,
        "still_missing_addr": still_missing_addr,
        "dst_event_ids_missing_in_src": missing_in_src,
    }

    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
