#!/usr/bin/env python
import argparse, json, time, sys
import urllib.parse, urllib.request

BASE="https://services1.arcgis.com/hGdibHYSPO59RG1h/arcgis/rest/services/L3_TAXPAR_POLY_ASSESS_gdb/FeatureServer/0/query"

def req_json(url):
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))

def esc(s): return urllib.parse.quote(s, safe="")

def clean(s): return " ".join((s or "").strip().split())
def up(s): return clean(s).upper()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--towns_json", required=True)
    ap.add_argument("--out_keys", required=True)
    ap.add_argument("--out_stats", required=True)
    ap.add_argument("--per_page", type=int, default=2000)
    ap.add_argument("--sleep", type=float, default=0.05)
    args=ap.parse_args()

    towns=json.load(open(args.towns_json,"r",encoding="utf-8")).get("town_counts",{})
    town_list=[t for t in towns.keys() if t and t!=""]
    # keep it small (you said 7 towns earlier) but we’ll just use what’s in the JSON.
    out=open(args.out_keys,"w",encoding="utf-8")
    stats={"towns":{}, "total_features":0}

    for town in town_list:
        where=f"CITY = '{town}'"
        offset=0
        got=0
        while True:
            url=(BASE+
                 f"?where={esc(where)}"
                 f"&outFields=ADDR_NUM,FULL_STR,SITE_ADDR,CITY,MAP_PAR_ID,LOC_ID,PROP_ID"
                 f"&returnGeometry=false"
                 f"&resultOffset={offset}"
                 f"&resultRecordCount={args.per_page}"
                 f"&f=json")
            data=req_json(url)
            feats=data.get("features") or []
            if not feats:
                break

            for ft in feats:
                a=(ft.get("attributes") or {})
                city=up(a.get("CITY"))
                addr_num=clean(a.get("ADDR_NUM"))
                full_str=clean(a.get("FULL_STR"))
                site_addr=clean(a.get("SITE_ADDR"))

                if city and addr_num and full_str:
                    out.write(f"{city}|{up(addr_num+' '+full_str)}\n")
                    got+=1
                elif city and site_addr:
                    out.write(f"{city}|{up(site_addr)}\n")
                    got+=1

            stats["total_features"] += len(feats)
            offset += len(feats)
            if not data.get("exceededTransferLimit") and len(feats) < args.per_page:
                break
            time.sleep(args.sleep)

        stats["towns"][town]={"features_seen":offset,"keys_written":got}
        print("[town]",town,"features_seen",offset,"keys_written",got)

    out.close()
    with open(args.out_stats,"w",encoding="utf-8") as f:
        json.dump(stats,f,indent=2)
    print("[done] wrote keys:",args.out_keys)
    print("[done] wrote stats:",args.out_stats)

if __name__=="__main__":
    main()
