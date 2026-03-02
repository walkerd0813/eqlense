import json, argparse
from collections import Counter

def it(p):
    with open(p,'r',encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def g(ev, path, default=None):
    cur = ev
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    args = ap.parse_args()

    tot = 0
    buckets = Counter()
    scope_status = Counter()
    for ev in it(args.inp):
        tot += 1
        a = g(ev, ["attach"], {}) or {}
        scope = (a.get("attach_scope") or "NONE")
        status = (a.get("attach_status") or "NONE")
        mm = (a.get("match_method") or "NONE")
        why = (a.get("why") or "NONE")
        buckets[f"{scope}|{status}|{mm}|{why}"] += 1
        scope_status[f"{scope}|{status}"] += 1

    print("IN:", args.inp)
    print({"rows": tot})
    print("\nSCOPE|STATUS totals:")
    for k,v in scope_status.most_common():
        print(f"{v:6d}  {k}")

    print("\nTOP 20 FULL BUCKETS:")
    for k,v in buckets.most_common(20):
        print(f"{v:6d}  {k}")

if __name__ == "__main__":
    main()
