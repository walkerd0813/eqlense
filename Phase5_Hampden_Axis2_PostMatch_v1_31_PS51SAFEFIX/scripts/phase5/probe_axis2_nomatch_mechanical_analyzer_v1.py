import argparse, json, re
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--max", type=int, default=8)
    args = ap.parse_args()

    c = Counter()
    buckets = defaultdict(list)

    for r in iter_ndjson(args.inp):
        status = (r.get("attach_status") or "").upper() or "UNKNOWN"
        method = (r.get("match_method") or "").upper() or "NO_METHOD"
        why = (r.get("why") or "").upper() or ""
        b = f"{status}|{method}|{why}"
        c[b] += 1
        if len(buckets[b]) < args.max:
            buckets[b].append({
                "event_id": r.get("event_id"),
                "town": r.get("town"),
                "addr": r.get("addr"),
                "attach_status": r.get("attach_status"),
                "match_method": r.get("match_method"),
                "why": r.get("why"),
                "property_id": r.get("property_id"),
                "docno_raw": r.get("recording", {}).get("document_number_raw") if isinstance(r.get("recording"), dict) else r.get("docno_raw"),
                "attachments_n": len(r.get("attachments") or [])
            })

    print("IN:", args.inp)
    print("\nTOP BUCKETS:")
    for k,v in c.most_common(12):
        print(f"  {v:>5}  {k}")

    print("\nSAMPLES:")
    for k,_ in c.most_common(6):
        print(f"\n=== {k} count={c[k]} ===")
        for s in buckets[k]:
            print(json.dumps(s, ensure_ascii=False))

if __name__ == "__main__":
    main()
