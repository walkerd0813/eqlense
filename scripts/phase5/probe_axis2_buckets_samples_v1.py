import argparse, json
from collections import Counter

def it(p):
    with open(p,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if line:
                yield json.loads(line)

def pick(ev):
    a = ev.get("attach") or {}
    scope = (a.get("attach_scope") or "").upper()
    status = (a.get("attach_status") or "").upper()
    method = (a.get("match_method") or "").upper()
    why = (a.get("why") or "").upper()

    # normalize a common variant
    if method == "NOMATCH": method = "NO_MATCH"

    key = f"{scope}|{status}|{method or 'NONE'}|{why or 'NONE'}"
    return key

def slim(ev):
    pr = ev.get("property_ref") or {}
    a  = ev.get("attach") or {}
    d  = ev.get("document") or {}
    return {
        "event_id": ev.get("event_id"),
        "town": pr.get("town_norm") or pr.get("town_raw"),
        "addr": pr.get("address_norm") or pr.get("address_raw"),
        "attach_scope": a.get("attach_scope"),
        "attach_status": a.get("attach_status"),
        "match_method": a.get("match_method"),
        "why": a.get("why"),
        "attachments_n": len(a.get("attachments") or []),
        "docno_raw": (ev.get("recording") or {}).get("document_number_raw"),
        "raw_block_has_dash": ("-"*20 in ((d.get("raw_block") or "")))
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--max", type=int, default=10)
    args = ap.parse_args()

    counts = Counter()
    samples = {}  # key -> list

    for ev in it(args.inp):
        k = pick(ev)
        counts[k] += 1
        if len(samples.get(k, [])) < args.max:
            samples.setdefault(k, []).append(slim(ev))

    print("IN:", args.inp)
    print("\nTOP BUCKETS (by count):")
    for k,v in counts.most_common(15):
        print(v, k)

    # Print a few key buckets explicitly if present
    want_prefixes = [
        "SINGLE|UNKNOWN|NO_MATCH",
        "SINGLE|UNKNOWN|COLLISION",
        "SINGLE|UNKNOWN|NONE|NO_NUM",
        "MULTI|PARTIAL_MULTI",
        "MULTI|UNKNOWN",
        "SINGLE|ATTACHED",
        "SINGLE|ATTACHED_A",
    ]

    print("\nSAMPLES (key buckets):")
    printed = set()

    # first print the wanted ones
    for pref in want_prefixes:
        for k in samples.keys():
            if k.startswith(pref) and k not in printed:
                print("\n===", k, "count=", counts[k], "===")
                for row in samples[k]:
                    print(json.dumps(row, ensure_ascii=False))
                printed.add(k)

    # then print remaining top buckets not shown
    for k,_ in counts.most_common(8):
        if k in printed: 
            continue
        print("\n===", k, "count=", counts[k], "===")
        for row in samples.get(k, [])[:args.max]:
            print(json.dumps(row, ensure_ascii=False))
        printed.add(k)

if __name__ == "__main__":
    main()
