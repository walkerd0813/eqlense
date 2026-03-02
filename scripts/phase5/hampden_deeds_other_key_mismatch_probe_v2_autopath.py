import argparse, json, os, re
from collections import Counter, defaultdict

def iter_ndjson(path, limit=None):
    with open(path, "r", encoding="utf-8") as f:
        n=0
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
                n += 1
                if limit and n >= limit:
                    return
            except Exception:
                continue

def find_candidate_paths(sample_obj):
    paths = []

    def walk(x, path=""):
        if isinstance(x, dict):
            for k,v in x.items():
                np = f"{path}.{k}" if path else k
                paths.append((np, v))
                walk(v, np)
        elif isinstance(x, list):
            for i,v in enumerate(x[:2]):
                walk(v, f"{path}[{i}]")
    walk(sample_obj)

    def score_path(p):
        # prefer normalized fields
        s = 0
        if re.search(r'(town_norm|city_norm|muni_norm)', p, re.I): s += 50
        if re.search(r'(address_norm)', p, re.I): s += 50
        if re.search(r'(town|city|muni|locality)', p, re.I): s += 10
        if re.search(r'(addr|address|street)', p, re.I): s += 10
        # penalize weird deep lists
        if '[' in p: s -= 5
        # prefer within address-ish objects
        if re.search(r'^address\.', p, re.I): s += 5
        return s

    return sorted(set([p for p,_ in paths]), key=score_path, reverse=True)

def get_by_path(obj, path):
    cur = obj
    for part in path.split("."):
        if not part:
            continue
        m = re.match(r"^([^\[]+)\[(\d+)\]$", part)
        if m:
            k = m.group(1)
            idx = int(m.group(2))
            if isinstance(cur, dict) and k in cur and isinstance(cur[k], list) and len(cur[k]) > idx:
                cur = cur[k][idx]
            else:
                return ""
        else:
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return ""
    return cur if isinstance(cur, str) else ""

def infer_best_paths(spine_path, sample_n=500):
    # Build frequency counts for candidate town/address paths by observing which paths yield non-empty strings
    town_hits = Counter()
    addr_hits = Counter()

    first = next(iter_ndjson(spine_path, limit=1), None)
    if not first:
        raise SystemExit("Empty spine file.")

    candidates = find_candidate_paths(first)

    town_like = [p for p in candidates if re.search(r'(town|city|muni|locality)', p, re.I)]
    addr_like = [p for p in candidates if re.search(r'(addr|address|street)', p, re.I)]

    for row in iter_ndjson(spine_path, limit=sample_n):
        for p in town_like[:80]:
            v = get_by_path(row, p)
            if isinstance(v, str) and v.strip():
                town_hits[p] += 1
        for p in addr_like[:120]:
            v = get_by_path(row, p)
            if isinstance(v, str) and v.strip():
                addr_hits[p] += 1

    # choose the path that hits most often; tie-breaker prefers *_norm
    def pick_best(counter):
        if not counter:
            return ""
        best = counter.most_common(10)
        best.sort(key=lambda kv: (kv[1], 1 if re.search(r'_norm$', kv[0], re.I) else 0), reverse=True)
        return best[0][0]

    return pick_best(town_hits), pick_best(addr_hits), dict(town_hits.most_common(10)), dict(addr_hits.most_common(10))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--attached", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--max", type=int, default=5000)
    args = ap.parse_args()

    town_path, addr_path, town_top, addr_top = infer_best_paths(args.spine, sample_n=500)
    if not town_path or not addr_path:
        raise SystemExit(f"Could not infer town/address paths. town_path={town_path} addr_path={addr_path}")

    # Build spine keyset
    spine_keys = set()
    town_counts = Counter()
    addr_towns = defaultdict(set)

    for row in iter_ndjson(args.spine):
        town = (get_by_path(row, town_path) or "").strip().upper()
        addr = (get_by_path(row, addr_path) or "").strip().upper()
        if not town or not addr:
            continue
        k = f"{town}||{addr}"
        spine_keys.add(k)
        town_counts[town] += 1
        addr_towns[addr].add(town)

    # Scan attached UNKNOWN OTHER_KEY_MISMATCH
    results = []
    buckets = Counter()
    n = 0

    for ev in iter_ndjson(args.attached):
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

        if exact:
            cls = "EXACT_KEY_EXISTS_BUT_NOT_USED"
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
        "attached": args.attached,
        "spine": args.spine,
        "spine_detected_paths": {
            "town_path": town_path,
            "addr_path": addr_path,
            "town_top10": town_top,
            "addr_top10": addr_top
        },
        "spine_keyset_size": len(spine_keys),
        "unknown_other_key_mismatch_rows_scanned": n,
        "classification_counts": dict(buckets),
        "samples": results[:60]
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print("[done] wrote", args.out)
    print("[done] detected town_path:", town_path)
    print("[done] detected addr_path:", addr_path)
    print("[done] spine_keyset_size:", len(spine_keys))
    print("[done] classification_counts:", dict(buckets))

if __name__ == "__main__":
    main()
