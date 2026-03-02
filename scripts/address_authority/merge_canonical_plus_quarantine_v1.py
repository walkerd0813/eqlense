import argparse, json, os, datetime, hashlib

def nowz():
    # keep it simple (avoid timezone imports)
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def jdump(p, obj):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def key_for(r: dict) -> str:
    # Prefer stable row_uid (your canonical dedupe key)
    k = (r.get("row_uid") or "").strip()
    if k:
        return "row_uid|" + k
    # fallback: property_uid then parcel_id then property_id
    for fld in ("property_uid", "parcel_id", "property_id"):
        v = (r.get(fld) or "").strip()
        if v:
            return fld + "|" + v
    # last resort: deterministic hash of address-ish fields
    town = (r.get("town") or "NONE").strip().upper()
    street_no = (r.get("street_no") or "").strip().upper()
    street_name = (r.get("street_name") or "").strip().upper()
    unit = (r.get("unit") or "").strip().upper()
    zipc = (r.get("zip") or "").strip()
    raw = f"{town}|{street_no}|{street_name}|{unit}|{zipc}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return "addrhash|" + h

def stream_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--canonical", required=True)
    ap.add_argument("--quarantine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", default="address.merge_canonical_plus_quarantine_v1")
    args = ap.parse_args()

    started = nowz()
    seen = set()

    counts = {
        "canonical_in": 0,
        "quarantine_in": 0,
        "written": 0,
        "deduped_total": 0,
        "dup_in_quarantine_skipped": 0,
        "dup_in_canonical_skipped": 0,
        "missing_key_fallback_used": 0
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as out:
        # Write canonical first (wins)
        for r in stream_ndjson(args.canonical):
            counts["canonical_in"] += 1
            k = key_for(r)
            if k.startswith("addrhash|"):
                counts["missing_key_fallback_used"] += 1
            if k in seen:
                counts["dup_in_canonical_skipped"] += 1
                continue
            seen.add(k)
            out.write(json.dumps(r, ensure_ascii=False) + "\n")
            counts["written"] += 1

        # Then quarantine (only new rows)
        for r in stream_ndjson(args.quarantine):
            counts["quarantine_in"] += 1
            k = key_for(r)
            if k.startswith("addrhash|"):
                counts["missing_key_fallback_used"] += 1
            if k in seen:
                counts["dup_in_quarantine_skipped"] += 1
                continue
            seen.add(k)
            out.write(json.dumps(r, ensure_ascii=False) + "\n")
            counts["written"] += 1

    counts["deduped_total"] = counts["written"]

    audit = {
        "engine_id": args.engine_id,
        "started_at": started,
        "finished_at": nowz(),
        "canonical": args.canonical,
        "quarantine": args.quarantine,
        "out": args.out,
        "counts": counts
    }
    jdump(args.audit, audit)

    print(json.dumps({"done": True, **counts, "out": args.out, "audit": args.audit}, indent=2))

if __name__ == "__main__":
    main()