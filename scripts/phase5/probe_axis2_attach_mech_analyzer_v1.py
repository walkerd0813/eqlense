import argparse, json
from collections import Counter, defaultdict

TOP_KEYS = ["attach_scope", "attach_status", "property_id", "match_method", "why"]

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def w_ndjson(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def pick_top_attach(r):
    # Top-level attach fields live at root (not inside r["attach"])
    top = {}
    for k in TOP_KEYS:
        if k in r:
            top[k] = r.get(k)
    # If none present, return None
    return top if any(k in r for k in TOP_KEYS) else None

def pick_nested_attach(r):
    a = r.get("attach")
    return a if isinstance(a, dict) else None

def norm_status(x):
    if x is None:
        return None
    return str(x).upper().strip()

def norm_method(x):
    if x is None:
        return None
    return str(x).lower().strip()

def classify(top, nested):
    # Presence flags
    has_top = top is not None
    has_nested = nested is not None

    if not has_top and not has_nested:
        return "NO_ATTACH_ANYWHERE"

    # Extract comparable bits
    t_stat = norm_status(top.get("attach_status")) if has_top else None
    n_stat = norm_status(nested.get("attach_status")) if has_nested else None
    t_meth = norm_method(top.get("match_method")) if has_top else None
    n_meth = norm_method(nested.get("match_method")) if has_nested else None
    t_pid  = top.get("property_id") if has_top else None
    n_pid  = nested.get("property_id") if has_nested else None

    # If only one side exists
    if has_top and not has_nested:
        return f"TOP_ONLY|{t_stat}|{t_meth}"
    if has_nested and not has_top:
        return f"NESTED_ONLY|{n_stat}|{n_meth}"

    # Both exist: classify relationship
    # “clean consistent”
    if (t_stat == n_stat) and (t_pid == n_pid) and (t_meth == n_meth):
        return f"BOTH_SAME|{t_stat}|{t_meth}"

    # Common conflict pattern: top attached, nested unknown/no_match
    if (t_stat in ("ATTACHED_A", "ATTACHED_B")) and (n_stat in (None, "UNKNOWN", "PARTIAL_MULTI")):
        return f"CONFLICT_TOP_ATTACHED_NESTED_{n_stat or 'NONE'}|top={t_meth}|nest={n_meth}"

    # Other mismatch patterns
    if t_stat != n_stat:
        return f"CONFLICT_STATUS|top={t_stat}|nest={n_stat}"
    if t_pid != n_pid:
        return "CONFLICT_PROPERTY_ID"
    if t_meth != n_meth:
        return "CONFLICT_METHOD"

    return "CONFLICT_OTHER"

def build_canonical_attach(r):
    """
    Canonical rule:
      - If top-level attach fields exist, they overwrite r["attach"] (nested).
      - Else if only nested exists, keep nested.
      - Then delete top-level attach fields from root to avoid dual-truth.
    """
    top = pick_top_attach(r)
    nested = pick_nested_attach(r)

    if top is not None:
        # Start from existing nested to preserve evidence/attachments when present
        new_attach = {}
        if isinstance(nested, dict):
            new_attach.update(nested)

        # Overwrite with top truth
        for k in TOP_KEYS:
            if k in top:
                new_attach[k] = top.get(k)

        r["attach"] = new_attach

        # Remove top-level duplicates
        for k in TOP_KEYS:
            if k in r:
                del r[k]
    else:
        # No top; leave as-is (nested is the truth)
        pass

    return r

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--max", type=int, default=10, help="max samples per bucket")
    ap.add_argument("--out_canonical", default=None, help="optional: write canonicalized NDJSON")
    args = ap.parse_args()

    ctr = Counter()
    samples = defaultdict(list)

    out_rows = [] if args.out_canonical else None

    n = 0
    has_top_n = 0
    has_nested_n = 0
    both_n = 0

    for r in iter_ndjson(args.inp):
        n += 1
        top = pick_top_attach(r)
        nested = pick_nested_attach(r)

        if top is not None:
            has_top_n += 1
        if nested is not None:
            has_nested_n += 1
        if (top is not None) and (nested is not None):
            both_n += 1

        bucket = classify(top or {}, nested or {})
        ctr[bucket] += 1

        if len(samples[bucket]) < args.max:
            # compact sample
            sid = r.get("event_id")
            town = (r.get("property_ref") or {}).get("town_norm") or r.get("town") or (r.get("property_ref") or {}).get("town_raw")
            addr = (r.get("property_ref") or {}).get("address_norm") or r.get("addr") or (r.get("property_ref") or {}).get("address_raw")
            samp = {
                "event_id": sid,
                "town": town,
                "addr": addr,
                "top": pick_top_attach(r) or None,
                "nested": (r.get("attach") if isinstance(r.get("attach"), dict) else None)
            }
            samples[bucket].append(samp)

        if out_rows is not None:
            out_rows.append(build_canonical_attach(r))

    print("IN:", args.inp)
    print("rows:", n)
    print("has_top:", has_top_n, "has_nested:", has_nested_n, "both:", both_n)
    print("\nTOP BUCKETS:")
    for k, v in ctr.most_common(20):
        print(f"{v:6d}  {k}")

    print("\nSAMPLES (up to --max each):")
    for k, v in ctr.most_common(12):
        print("\n===", k, "count=", v, "===")
        for s in samples[k]:
            print(json.dumps(s, ensure_ascii=False))

    if out_rows is not None:
        w_ndjson(args.out_canonical, out_rows)
        print("\n[ok] wrote canonicalized:", args.out_canonical)

if __name__ == "__main__":
    main()
