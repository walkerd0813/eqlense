#!/usr/bin/env python
import argparse, json, re
from collections import Counter

def clean(s): return " ".join((s or "").strip().split())
def up(s): return clean(s).upper()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)          # ndjson: {map_par_id, property_id}
    ap.add_argument("--audit", required=True)
    args=ap.parse_args()

    c=Counter()
    out=open(args.out,"w",encoding="utf-8")
    seen=set()

    with open(args.spine,"r",encoding="utf-8") as f:
        for ln in f:
            ln=ln.strip()
            if not ln: 
                continue
            c["spine_rows_seen"] += 1
            r=json.loads(ln)
            pid=r.get("property_id")
            # Try common parcel id fields
            parcel = (r.get("parcel_id_norm") or r.get("parcel_id_raw") or r.get("parcel_id") or "")
            parcel = up(str(parcel))
            if not pid or not parcel:
                c["spine_missing_pid_or_parcel"] += 1
                continue

            # If your spine stores MA parcels like "ma:parcel:2001401010" then extract numeric tail too
            numeric_tail = None
            m=re.search(r'(\d{8,})$', parcel)
            if m:
                numeric_tail = m.group(1)

            for key in {parcel, numeric_tail}:
                if not key: 
                    continue
                k=f"{key}|{pid}"
                if k in seen:
                    continue
                seen.add(k)
                out.write(json.dumps({"map_par_id": key, "property_id": pid}, ensure_ascii=False) + "\n")
                c["pairs_written"] += 1

    out.close()
    audit={"counts":dict(c),"out":args.out}
    with open(args.audit,"w",encoding="utf-8") as fo:
        json.dump(audit,fo,indent=2)
    print(json.dumps(audit,indent=2))

if __name__=="__main__":
    main()
