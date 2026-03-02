import argparse, json, os, re, datetime
from collections import Counter, defaultdict

def nowz():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def ns(s): return re.sub(r"\s+"," ",(s or "").strip())
def up(s): return ns(s).upper()

def spine_key_from_row(r):
    t = up(r.get("town"))
    no = ns(r.get("street_no"))
    st = up(r.get("street_name"))
    if not t or not no or not st: 
        return ""
    return f"{t}|{no} {st}"

def best_event_fields(ev):
    # best-effort: you already have property_ref + evidence blocks in many events
    pr = ev.get("property_ref") or {}
    rec = ev.get("recording") or {}
    doc = ev.get("document") or {}
    out = {
        "event_id": ev.get("event_id"),
        "town": pr.get("town") or ev.get("town") or "",
        "street_no": pr.get("street_no") or ev.get("street_no") or "",
        "street_name": pr.get("street_name") or ev.get("street_name") or "",
        "unit": pr.get("unit") or ev.get("unit") or "",
        "full_address": pr.get("full_address") or ev.get("full_address") or "",
        "recording_date": (rec.get("recording_date") or rec.get("recording_date_raw") or ""),
        "book": rec.get("book"),
        "page": rec.get("page"),
        "doc_no": rec.get("document_number") or rec.get("document_number_raw"),
        "instrument": doc.get("type") or doc.get("document_type") or ev.get("event_type"),
    }
    # normalize empties
    for k,v in list(out.items()):
        if v is None: out[k] = ""
    return out

def load_spine_index(spine_path, needed_keys=None, cap_per_key=6):
    idx = defaultdict(list)
    scanned=0
    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): 
                continue
            scanned += 1
            try:
                r = json.loads(line)
            except:
                continue
            k = spine_key_from_row(r)
            if not k:
                continue
            if needed_keys is not None and k not in needed_keys:
                continue
            if len(idx[k]) >= cap_per_key:
                continue
            idx[k].append({
                "property_id": r.get("property_id"),
                "parcel_id": r.get("parcel_id"),
                "building_group_id": r.get("building_group_id"),
                "full_address": r.get("full_address"),
                "unit": r.get("unit"),
                "zip": r.get("zip"),
                "address_key": r.get("address_key"),
                "address_tier": r.get("address_tier"),
            })
    return idx, scanned

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--engine_id", default="diag.unknown_buckets_top_v1")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    counts = Counter()
    samples = {}
    keys_needed=set()

    rows=0
    with open(args.infile, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rows += 1
            try:
                ev = json.loads(line)
            except:
                continue
            a = ev.get("attach") or {}
            if a.get("attach_status") != "UNKNOWN":
                continue
            mm = a.get("match_method") or "(none)"
            mk = (a.get("match_key") or "").strip() or "(no_match_key)"
            bucket = f"{mm}||{mk}"
            counts[bucket] += 1
            if bucket not in samples:
                s = best_event_fields(ev)
                s["match_method"]=mm
                s["match_key"]=mk
                s["match_key_unit"]=a.get("match_key_unit")
                samples[bucket]=s
            if mk and mk != "(no_match_key)" and mk != "(no_match_key)":
                # only for base keys, not unit-keys
                if "UNIT|" not in mk:
                    keys_needed.add(mk)

    top = counts.most_common(args.top)
    top_buckets = [b for b,_ in top]

    # pull spine rows for the top match_keys only (fast)
    needed = set()
    for b,_n in top:
        mk = b.split("||",1)[1]
        if mk and mk != "(no_match_key)" and "UNIT|" not in mk:
            needed.add(mk)

    spine_idx, spine_scanned = load_spine_index(args.spine, needed_keys=needed, cap_per_key=8)

    report = []
    for b,n in top:
        mm, mk = b.split("||",1)
        item = {
            "count": n,
            "match_method": mm,
            "match_key": mk,
            "sample_event": samples.get(b) or {},
            "spine_rows_for_key": spine_idx.get(mk, []),
            "spine_row_count": len(spine_idx.get(mk, [])),
        }
        report.append(item)

    outdoc = {
        "engine_id": args.engine_id,
        "ran_at": nowz(),
        "inputs": {"infile": args.infile, "spine": args.spine},
        "stats": {
            "rows_scanned_infile": rows,
            "unique_unknown_buckets": len(counts),
            "spine_rows_scanned": spine_scanned,
        },
        "top": report
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(outdoc, f, ensure_ascii=False, indent=2)

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump({
            "engine_id": args.engine_id,
            "ran_at": nowz(),
            "out": args.out,
            "top_n": args.top,
            "rows_scanned_infile": rows,
            "unique_unknown_buckets": len(counts),
        }, f, ensure_ascii=False, indent=2)

    print(json.dumps({"done": True, "rows_scanned": rows, "top_written": len(report), "out": args.out}, indent=2))

if __name__=="__main__":
    main()