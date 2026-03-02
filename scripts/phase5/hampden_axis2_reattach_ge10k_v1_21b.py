import argparse, json, time, os, sys
from collections import defaultdict

def jloads(line):
    return json.loads(line)

def norm(s):
    return (s or "").strip().upper()

def build_needed_towns(events_path):
    towns=set()
    n=0
    with open(events_path,'r',encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            n += 1
            r=jloads(line)
            pr = r.get("property_ref") or {}
            t = pr.get("town_norm") or pr.get("town_raw") or r.get("town")
            t = norm(t)
            if t: towns.add(t)
    return towns, n

def spine_iter(path):
    with open(path,'r',encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield jloads(line)

def make_spine_key(row):
    # spine uses: town + full_address (per your new rule)
    town = norm(row.get("town"))
    fa = norm(row.get("full_address"))
    if not town or not fa:
        return None
    return f"{town}|{fa}"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args=ap.parse_args()

    t0=time.time()
    towns_needed, n_events = build_needed_towns(args.events)
    print(f"[info] events rows: {n_events} towns_needed: {len(towns_needed)}", flush=True)

    # Build a minimal spine index ONLY for towns present in events
    idx = {}  # key -> property_id
    debug = {"scanned_rows":0, "kept_rows":0, "no_key":0, "town_skip":0}
    last=time.time()

    print(f"[info] building spine index (town-filtered)...", flush=True)
    for row in spine_iter(args.spine):
        debug["scanned_rows"] += 1

        town = norm(row.get("town"))
        if town not in towns_needed:
            debug["town_skip"] += 1
        else:
            k = make_spine_key(row)
            if not k:
                debug["no_key"] += 1
            else:
                # keep first; collisions can be tracked later if needed
                if k not in idx:
                    idx[k] = row.get("property_id")
                debug["kept_rows"] += 1

        # progress print every ~200k rows or 10s
        if debug["scanned_rows"] % 200000 == 0 or (time.time()-last) > 10:
            last=time.time()
            print("[progress] scanned_rows=%d kept_rows=%d town_skip=%d idx=%d elapsed_s=%.1f" % (
                debug["scanned_rows"], debug["kept_rows"], debug["town_skip"], len(idx), time.time()-t0
            ), flush=True)

    print(f"[ok] spine index built idx_keys={len(idx)} debug={debug} elapsed_s={time.time()-t0:.1f}", flush=True)

    # Now reattach
    stats = defaultdict(int)
    out_n=0

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.events,'r',encoding='utf-8') as fin, open(args.out,'w',encoding='utf-8') as fout:
        for line in fin:
            if not line.strip(): continue
            r=jloads(line)
            pr = r.get("property_ref") or {}
            town = norm(pr.get("town_norm") or pr.get("town_raw") or r.get("town"))
            addr = norm(pr.get("address_norm") or pr.get("address_raw") or "")
            # NOTE: events have property_ref.address_*; spine uses full_address.
            # For now we try address_norm first; if that doesn't resemble full_address, it'll miss.
            # But at least we’re not burning 30 minutes silently.
            key = f"{town}|{addr}" if town and addr else None

            attach = r.get("attach") or {}
            if key and key in idx:
                attach["attach_status"] = "ATTACHED_A"
                attach["property_id"] = idx[key]
                attach["attach_method"] = "AXIS2_TOWN+FULLADDR"
                r["attach"] = attach
                stats["attached_a"] += 1
            else:
                stats["still_unknown"] += 1

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")
            out_n += 1

    audit = {
        "script": "hampden_axis2_reattach_ge10k_v1_21b.py",
        "events": args.events,
        "spine": args.spine,
        "out": args.out,
        "stats": dict(stats),
        "spine_debug": debug,
        "elapsed_s": time.time()-t0
    }
    with open(args.audit,'w',encoding='utf-8') as f:
        json.dump(audit, f, indent=2)

    print(f"[done] wrote out_rows={out_n} stats={dict(stats)} audit={args.audit}", flush=True)

if __name__=="__main__":
    main()
