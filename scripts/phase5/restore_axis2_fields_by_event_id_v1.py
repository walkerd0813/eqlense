import argparse, json

FIELDS = ["town","addr","addr_norm","addr_key","house_no","street","unit","zip","raw_block_has_dash"]

def load_src(src_path):
    m = {}
    with open(src_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            r=json.loads(line)
            eid=r.get("event_id")
            if not eid: continue
            m[eid]={k:r.get(k) for k in FIELDS}
    return m

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--dst", required=True)
    ap.add_argument("--out", required=True)
    args=ap.parse_args()

    src=load_src(args.src)

    seen=0
    restored=0
    still_missing=0

    with open(args.dst, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line: continue
            r=json.loads(line)
            eid=r.get("event_id")
            seen += 1

            if eid in src:
                before = (r.get("town") is None) or (r.get("addr") is None)
                for k,v in src[eid].items():
                    if r.get(k) is None and v is not None:
                        r[k]=v
                after = (r.get("town") is None) or (r.get("addr") is None)
                if before and not after:
                    restored += 1
                if after:
                    still_missing += 1
            else:
                # if event_id not found in src, count it as still missing if key fields missing
                if (r.get("town") is None) or (r.get("addr") is None):
                    still_missing += 1

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    print("[done] restore_axis2_fields_by_event_id")
    print(" rows_seen:", seen)
    print(" rows_restored_from_src:", restored)
    print(" rows_still_missing_town_or_addr:", still_missing)

if __name__ == "__main__":
    main()
