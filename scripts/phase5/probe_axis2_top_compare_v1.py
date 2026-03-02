import json, collections

def iter_ndjson(p):
    with open(p,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if line:
                yield json.loads(line)

def summarize(p):
    c_status = collections.Counter()
    c_method = collections.Counter()
    c_pair   = collections.Counter()
    n = 0
    for r in iter_ndjson(p):
        n += 1
        st = (r.get("attach_status") or "UNKNOWN")
        mm = (r.get("match_method") or "no_match")
        why = (r.get("why") or "NONE")
        c_status[st] += 1
        c_method[mm] += 1
        c_pair[(st, mm, why)] += 1
    return n, c_status, c_method, c_pair

def top10(counter):
    return counter.most_common(10)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True)
    ap.add_argument("--b", required=True)
    args = ap.parse_args()

    na, sa, ma, pa = summarize(args.a)
    nb, sb, mb, pb = summarize(args.b)

    print("[A]", args.a)
    print(" rows:", na)
    print(" status:", sa)
    print(" top methods:", top10(ma))
    print("")
    print("[B]", args.b)
    print(" rows:", nb)
    print(" status:", sb)
    print(" top methods:", top10(mb))
    print("")

    # delta on key statuses
    for key in ["ATTACHED_A","ATTACHED_B","UNKNOWN"]:
        da = sa.get(key,0)
        db = sb.get(key,0)
        print(f"DELTA {key}: B-A = {db-da:+d} (A={da}, B={db})")

    print("\nTop 15 bucket deltas (st,method,why):")
    all_keys = set(pa.keys()) | set(pb.keys())
    deltas = []
    for k in all_keys:
        deltas.append(( (pb.get(k,0) - pa.get(k,0)), k, pa.get(k,0), pb.get(k,0) ))
    deltas.sort(key=lambda x: abs(x[0]), reverse=True)
    for d,k,va,vb in deltas[:15]:
        print(f" {d:+5d}  {k}   A={va} B={vb}")

if __name__ == "__main__":
    main()
