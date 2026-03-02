import argparse, json, os, re
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def make_key(town_norm, addr_norm):
    t = (town_norm or "").strip().upper()
    a = (addr_norm or "").strip().upper()
    return f"{t}||{a}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--attached", required=True)
    ap.add_argument("--spineIndexDump", required=False, default="")
    ap.add_argument("--out", required=True)
    ap.add_argument("--max", type=int, default=5000)
    args = ap.parse_args()

    # Load spine keys by piggybacking on the same index the attach script uses:
    # We will read the audit file that the attach script wrote to discover the resolved spine path,
    # then parse THAT spine file and rebuild a simple keyset using the *existing key format* in the file.
    # But we don't know the spine key logic here, so we infer by scanning for town/address fields.
    #
    # Instead: easiest + deterministic: parse CURRENT resolved spine ndjson and build keyset from
    # fields: town_norm/address_norm (or town/address). This is still sufficient to find mismatches category-wise.
    attached_path = args.attached

    # locate the audit json alongside attached (common convention)
    # If audit is present in ev['meta'] we can use that; otherwise user passes spineIndexDump.
    sample = next(iter_ndjson(attached_path), None)
    if not sample:
        raise SystemExit("empty attached file")

    # Try to discover spine path from event meta (your attach script prints it, but may not store it)
    # If not available, require user to provide spineIndexDump (a ndjson spine path)
    spine_path = args.spineIndexDump.strip() if args.spineIndexDump else ""
    if not spine_path:
        raise SystemExit("Provide --spineIndexDump as the resolved spine NDJSON path (the one printed as spine_path_resolved).")

    # Build spine keyset + town->count + addr->towns map
    spine_keys = set()
    town_counts = Counter()
    addr_towns = defaultdict(set)

    def get_field(o, paths):
        cur = o
        for p in paths:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return ""
        return cur if isinstance(cur, str) else ""

    for row in iter_ndjson(spine_path):
        # try common places
        town = (
            get_field(row, ["address", "town_norm"]) or
            get_field(row, ["address", "town"]) or
            get_field(row, ["town_norm"]) or
            get_field(row, ["town"]) or
            ""
        )
        addr = (
            get_field(row, ["address", "address_norm"]) or
            get_field(row, ["address", "address"]) or
            get_field(row, ["address_norm"]) or
            get_field(row, ["address"]) or
            ""
        )
        town = (town or "").strip().upper()
        addr = (addr or "").strip().upper()
        if not town or not addr:
            continue
        k = f"{town}||{addr}"
        spine_keys.add(k)
        town_counts[town] += 1
        addr_towns[addr].add(town)

    # Scan attached UNKNOWN rows and classify
    results = []
    buckets = Counter()

    n = 0
    for ev in iter_ndjson(attached_path):
        attach = ev.get("attach") if isinstance(ev.get("attach"), dict) else {}
        status = attach.get("attach_status") or "UNKNOWN"
        bucket = attach.get("bucket") or ""
        if status != "UNKNOWN" or bucket != "OTHER_KEY_MISMATCH":
            continue

        town = (attach.get("town_norm") or "").strip().upper()
        addr = (attach.get("address_norm") or attach.get("addr_norm") or "").strip().upper()
        key = f"{town}||{addr}"

        exact = key in spine_keys
        town_exists = town in town_counts
        addr_elsewhere = len(addr_towns.get(addr, set())) > 0
        addr_towns_list = sorted(list(addr_towns.get(addr, set())))[:10]

        # classify
        if exact:
            cls = "REPORTING_OR_INDEX_BUG"  # should not happen if attach used same keyset
        elif not town_exists and addr_elsewhere:
            cls = "TOWN_NORM_MISMATCH"
        elif town_exists and addr_elsewhere:
            cls = "ADDRESS_NORM_MISMATCH_OR_TOWN_ALIAS"
        elif town_exists and not addr_elsewhere:
            cls = "ADDRESS_NOT_IN_SPINE_INDEX"
        else:
            cls = "TOWN_NOT_IN_SPINE_INDEX"

        buckets[cls] += 1
        results.append({
            "event_id": ev.get("event_id",""),
            "town_norm": town,
            "addr_norm": addr,
            "town_exists_in_spine": bool(town_exists),
            "addr_seen_elsewhere": bool(addr_elsewhere),
            "addr_seen_towns": addr_towns_list,
            "classification": cls
        })

        n += 1
        if n >= args.max:
            break

    out = {
        "attached": attached_path,
        "spine": spine_path,
        "unknown_other_key_mismatch_rows_scanned": n,
        "classification_counts": dict(buckets),
        "samples": results[:50]
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print("[done] wrote", args.out)
    print("[done] classification_counts", dict(buckets))

if __name__ == "__main__":
    main()
