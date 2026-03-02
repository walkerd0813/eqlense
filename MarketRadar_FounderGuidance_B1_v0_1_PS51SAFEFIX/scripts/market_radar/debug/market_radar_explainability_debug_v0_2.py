#!/usr/bin/env python3
import argparse, json, os
from typing import Optional, Tuple

def load_json(path: str):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def find_ndjson_row(path: str, zip5: str, bucket: Optional[str]=None) -> Optional[dict]:
    zip5 = str(zip5).zfill(5)
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                r=json.loads(line)
            except Exception:
                continue
            if str(r.get("zip","")).zfill(5)!=zip5:
                continue
            if bucket and str(r.get("asset_bucket"))!=bucket:
                continue
            return r
    return None

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--zip", required=True)
    ap.add_argument("--asset_bucket", default=None)
    ap.add_argument("--window_days", type=int, default=30)
    ap.add_argument("--as_of", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--expand_glossary", action="store_true")
    args=ap.parse_args()

    root=args.root
    mr_ptr=load_json(os.path.join(root,"publicData","marketRadar","CURRENT","CURRENT_MARKET_RADAR_POINTERS.json"))
    ind_ptr_path=os.path.join(root,"publicData","marketRadar","indicators","CURRENT","CURRENT_MARKET_RADAR_INDICATORS_POINTERS.json")
    ind_ptr=load_json(ind_ptr_path) if os.path.exists(ind_ptr_path) else None

    exp_path = mr_ptr.get("market_radar",{}).get("explainability_zip",{}).get("ndjson")
    if not exp_path:
        raise SystemExit("No explainability current pointer found.")

    row = find_ndjson_row(exp_path, args.zip, bucket=args.asset_bucket)
    out = {
        "meta": {
            "zip": str(args.zip).zfill(5),
            "asset_bucket": args.asset_bucket,
            "window_days": args.window_days,
            "as_of_arg": args.as_of,
            "pointers": {
                "market_radar": os.path.join(root,"publicData","marketRadar","CURRENT","CURRENT_MARKET_RADAR_POINTERS.json"),
                "indicators": ind_ptr_path
            }
        },
        "paths": {
            "explainability_zip": exp_path,
            "indicators_p01_zip_state": None
        },
        "rows": {
            "explainability": row
        },
        "diagnostics": {"notes": []}
    }

    if row is None:
        out["diagnostics"]["notes"].append("No explainability row found for that zip/bucket in CURRENT file.")
    else:
        # Expand glossary if contract pointer exists
        if args.expand_glossary:
            fc = mr_ptr.get("market_radar",{}).get("founder_guidance_contract_b1",{}).get("json")
            if fc and os.path.exists(fc):
                contract = load_json(fc)
                refs = row.get("glossary_refs") or []
                out["glossary_expanded"] = {k: contract.get("glossary",{}).get(k) for k in refs if k in contract.get("glossary",{})}

    txt = json.dumps(out, indent=2, ensure_ascii=False)
    if args.out:
        os.makedirs(os.path.dirname(os.path.join(root,args.out)) if not os.path.isabs(args.out) else os.path.dirname(args.out), exist_ok=True)
        out_path = os.path.join(root,args.out) if not os.path.isabs(args.out) else args.out
        with open(out_path,"w",encoding="utf-8") as f:
            f.write(txt)
        print(f"[ok] wrote {out_path}")
    print(txt)
    print("[done] founder debug extraction complete.")

if __name__=="__main__":
    main()
