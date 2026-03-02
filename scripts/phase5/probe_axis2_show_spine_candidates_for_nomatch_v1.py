import json, re, sys
from collections import defaultdict, Counter

def load_ndjson(path, limit=None):
    n = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue
            n += 1
            if limit and n >= limit:
                break

# tiny edit distance (Levenshtein) for short strings
def lev(a, b):
    if a == b: return 0
    if not a: return len(b)
    if not b: return len(a)
    la, lb = len(a), len(b)
    prev = list(range(lb+1))
    for i in range(1, la+1):
        cur = [i] + [0]*lb
        ca = a[i-1]
        for j in range(1, lb+1):
            cb = b[j-1]
            cur[j] = min(
                prev[j] + 1,
                cur[j-1] + 1,
                prev[j-1] + (0 if ca==cb else 1)
            )
        prev = cur
    return prev[lb]

def norm_ws(s):
    return re.sub(r"\s+", " ", (s or "").strip().upper())

def pick(evt, keys):
    cur = evt
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

def parse_event_parts(addr_raw):
    # very lightweight: assumes addr like "315 REGENCY PARK DR UNIT G"
    s = norm_ws(addr_raw)
    if not s:
        return None, None, None
    toks = s.split()

    # find leading number or range
    first = toks[0]
    street_no = None
    street_no_range = False
    if re.fullmatch(r"\d+", first):
        street_no = first
        rest = toks[1:]
    elif re.fullmatch(r"\d+\-\d+", first):
        street_no = first
        street_no_range = True
        rest = toks[1:]
    else:
        rest = toks[:]  # no number

    # pull UNIT/APT/# at end
    unit = None
    m = re.search(r"(?:\bUNIT\b|\bAPT\b|\bAPARTMENT\b|\#)\s*([A-Z0-9\-]+)\s*$", s)
    if m:
        unit = m.group(1).strip()
        # remove the unit phrase from street part
        s2 = re.sub(r"(?:\bUNIT\b|\bAPT\b|\bAPARTMENT\b|\#)\s*[A-Z0-9\-]+\s*$", "", s).strip()
    else:
        s2 = s

    # remove leading number token if present
    if street_no and not street_no_range:
        s2 = re.sub(r"^\d+\s+", "", s2).strip()
    if street_no and street_no_range:
        s2 = re.sub(r"^\d+\-\d+\s+", "", s2).strip()

    street_part = s2
    return street_no, street_part, unit

def build_spine_index(spine_path, max_rows=None):
    # index: town_norm -> street_no -> list of (street_norm, property_id, unit)
    idx = defaultdict(lambda: defaultdict(list))

    for r in load_ndjson(spine_path, limit=max_rows):
        town = norm_ws(r.get("town") or r.get("town_norm") or "")
        sn   = norm_ws(str(r.get("street_no") or ""))
        st   = norm_ws(r.get("street_name") or r.get("street_norm") or r.get("street") or "")
        pid  = r.get("property_id") or r.get("propertyId") or r.get("id")
        unit = norm_ws(str(r.get("unit") or ""))

        if not town or not sn or not st:
            continue
        idx[town][sn].append((st, pid, unit))
    return idx

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--max_events", type=int, default=25)
    ap.add_argument("--max_spine_rows", type=int, default=None)
    args = ap.parse_args()

    spine_idx = build_spine_index(args.spine, max_rows=args.max_spine_rows)

    printed = 0
    for e in load_ndjson(args.events):
        # must have attach object and be no_match
        attach = e.get("attach") or {}
        if (attach.get("attach_status") != "UNKNOWN") or (attach.get("match_method") != "no_match"):
            continue

        town = norm_ws(e.get("town") or pick(e, ["property_ref","town_raw"]) or pick(e, ["property_ref","town_norm"]) or "")
        addr = (e.get("addr") or pick(e, ["property_ref","address_raw"]) or pick(e, ["property_ref","address_norm"]) or "")
        street_no, street_part, unit = parse_event_parts(addr)

        print("\n=== EVENT ===")
        print("event_id:", e.get("event_id"))
        print("town:", town)
        print("addr_raw:", addr)
        print("parsed street_no:", street_no, "street_part:", norm_ws(street_part), "unit:", unit)

        # if range, just say so
        if street_no and re.fullmatch(r"\d+\-\d+", street_no):
            print("NOTE: street_no is a RANGE. Needs endpoint-range logic, not aliasing.")
            printed += 1
            if printed >= args.max_events: break
            continue

        # if no number, say so
        if not street_no or not re.fullmatch(r"\d+", street_no):
            print("NOTE: NO_NUM. Needs unit/street-only strategy, not street_no exact.")
            printed += 1
            if printed >= args.max_events: break
            continue

        # show spine candidates for same town+no
        cands = spine_idx.get(town, {}).get(street_no, [])
        if not cands:
            print("SPINE candidates: (none for same town+street_no) => likely missing in spine or different town spelling")
            printed += 1
            if printed >= args.max_events: break
            continue

        counts = Counter([st for (st, pid, u) in cands])
        top = counts.most_common(10)
        print("SPINE candidates for same town+street_no:", len(cands), "rows;", len(counts), "unique streets")
        for st, ct in top:
            # pick an example pid for this street
            ex = next((pid for (st2,pid,u) in cands if st2==st), None)
            print(f"  - {st}   (count={ct})   example_pid={ex}")

        # best close street
        ev_st = norm_ws(street_part)
        best = None
        for st, ct in top:
            d = lev(ev_st, st)
            if best is None or d < best[0]:
                best = (d, st)
        if best:
            print("closest_top_candidate:", best[1], "lev=", best[0])

        printed += 1
        if printed >= args.max_events:
            break

if __name__ == "__main__":
    main()
