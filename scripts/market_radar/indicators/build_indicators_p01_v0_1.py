#!/usr/bin/env python3
from __future__ import annotations
import argparse, datetime, json, os, re, hashlib, shutil
from typing import Any, Dict, Iterable, Optional, Tuple

ENGINE_VERSION="v0_1"
SCHEMA_VERSION="market_radar_indicators_p01_v1"
ZIP_RE=re.compile(r"^\d{5}$")
WINDOWS=[30,90,180,365]

def utc_now_iso()->str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00","Z")
def sha256_file(p:str)->str:
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for c in iter(lambda:f.read(1024*1024), b""):
            h.update(c)
    return h.hexdigest()
def read_ndjson(p:str)->Iterable[Dict[str,Any]]:
    with open(p,"r",encoding="utf-8") as f:
        for ln in f:
            ln=ln.strip()
            if ln: yield json.loads(ln)
def write_ndjson(p:str, rows:Iterable[Dict[str,Any]])->int:
    os.makedirs(os.path.dirname(p), exist_ok=True)
    n=0
    with open(p,"w",encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r,ensure_ascii=False)+"\n"); n+=1
    return n
def write_json(p:str,o:Any)->None:
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p,"w",encoding="utf-8") as f:
        json.dump(o,f,indent=2,sort_keys=False)

def kget(d:Dict[str,Any], keys)->Optional[Any]:
    for k in keys:
        if k in d and d[k] is not None: return d[k]
    return None
def norm_bucket(b:Optional[str])->Optional[str]:
    if not b: return None
    b=str(b).strip().lower()
    m={"single":"single_family","sf":"single_family","single_family":"single_family",
       "condo":"condo","mf_2_4":"multifamily_2_4","multifamily_2_4":"multifamily_2_4",
       "mf_5+":"multifamily_5plus","multifamily_5plus":"multifamily_5plus","5plus":"multifamily_5plus",
       "land":"land","all":"all","unknown":"all"}
    return m.get(b,b)

def extract_key(r:Dict[str,Any])->Tuple[Optional[str],Optional[str],Optional[int]]:
    z=r.get("zip") or r.get("zip_code") or r.get("zipcode")
    b=r.get("asset_bucket") or r.get("bucket") or r.get("property_bucket") or r.get("propertyType")
    w=r.get("window_days") or r.get("window") or r.get("windowDays")
    z=str(z).strip() if z is not None else None
    b=norm_bucket(b)
    try: w=int(w) if w is not None else None
    except: w=None
    return z,b,w

def index_ndjson(p:str)->Dict[Tuple[str,str,int],Dict[str,Any]]:
    idx={}
    for r in read_ndjson(p):
        z,b,w=extract_key(r)
        if not z or not ZIP_RE.match(z): continue
        if not b or w is None: continue
        idx[(z,b,w)]=r
    return idx

def get_metric(row:Optional[Dict[str,Any]], keys)->float:
    if not row: return 0.0
    m=row.get("metrics") or row.get("m") or {}
    v=kget(m, keys)
    if isinstance(v,(int,float)): return float(v)
    v2=kget(row, keys)
    return float(v2) if isinstance(v2,(int,float)) else 0.0

def get_dom(row:Optional[Dict[str,Any]])->Optional[float]:
    if not row: return None
    m=row.get("metrics") or row.get("m") or {}
    v=kget(m, ["dom_median","dom_p50","dom50","days_on_market_median"])
    if isinstance(v,(int,float)) and v>0: return float(v)
    dom=m.get("dom") if isinstance(m.get("dom"),dict) else None
    if dom:
        v2=kget(dom, ["p50","median"])
        if isinstance(v2,(int,float)) and v2>0: return float(v2)
    return None

def clamp(x:float, lo:float, hi:float)->float:
    return lo if x<lo else hi if x>hi else x
def score_band(x:float, lo:float, hi:float)->float:
    if hi==lo: return 0.5
    return clamp((x-lo)/(hi-lo),0.0,1.0)

def unknown(reason:str)->Dict[str,Any]:
    return {"state":"UNKNOWN","reason":reason,"value":None}
def known(value:Any, conf:float, suff:bool, why:Dict[str,Any], note:Optional[str]=None)->Dict[str,Any]:
    o={"state":"KNOWN","confidence":clamp(conf,0.0,1.0),"sufficient":bool(suff),"value":value,"why":why}
    if note: o["note"]=note
    return o

def main()->None:
    ap=argparse.ArgumentParser()
    ap.add_argument("--deeds",required=True); ap.add_argument("--stock",required=True)
    ap.add_argument("--absorption",required=True); ap.add_argument("--liquidity",required=True)
    ap.add_argument("--price_discovery",required=True)
    ap.add_argument("--out",required=True); ap.add_argument("--audit",required=True); ap.add_argument("--as_of",required=True)
    ap.add_argument("--min_samples",type=int,default=10); ap.add_argument("--min_stock",type=int,default=30)
    a=ap.parse_args()

    deeds=index_ndjson(a.deeds); stock=index_ndjson(a.stock); absor=index_ndjson(a.absorption); liq=index_ndjson(a.liquidity)

    # union keys (only selected windows)
    keys=set()
    for src in (deeds,stock,absor,liq):
        for (z,b,w) in src.keys():
            if w in WINDOWS: keys.add((z,b,w))
    keys=sorted(keys)

    scan={"keys_total":len(keys),"known":0,"unknown":0,"insufficient_samples":0,"insufficient_stock":0,"missing_metric":0}
    out_rows=[]

    for (z,b,w) in keys:
        deeds_cnt=int(get_metric(deeds.get((z,b,w)), ["deeds_arms_length","arms_length","armsLength","arms_length_count"]))
        stock_cnt=int(get_metric(stock.get((z,b,w)), ["stock_parcels","parcels","parcel_count","parcels_total"]))
        mls_closed=int(get_metric(absor.get((z,b,w)), ["closed_sales","mls_closed","sold","sales_closed","closed"]))

        # cross-window helpers
        deeds90=int(get_metric(deeds.get((z,b,90)), ["deeds_arms_length","arms_length"]))
        deeds180=int(get_metric(deeds.get((z,b,180)), ["deeds_arms_length","arms_length"]))
        dom30=get_dom(liq.get((z,b,30))); dom90=get_dom(liq.get((z,b,90))); dom180=get_dom(liq.get((z,b,180)))

        # absorption rate (prefer months-of-supply inversion)
        def abs_rate(win:int)->Optional[float]:
            r=absor.get((z,b,win))
            if not r: return None
            mos=get_metric(r, ["months_of_supply","mos","monthsSupply"])
            if mos and mos>0: return 1.0/mos
            inv=get_metric(r, ["inventory","active_listings","active","inventory_active"])
            cls=get_metric(r, ["closed_sales","mls_closed","sold","sales_closed","closed"])
            return (cls/inv) if inv and inv>0 else None

        abs90=abs_rate(90); abs180=abs_rate(180)

        indicators={}

        # 1) TBI
        if deeds_cnt < a.min_samples:
            indicators["tbi_transaction_breadth"]=unknown("INSUFFICIENT_SAMPLES"); scan["insufficient_samples"]+=1
        elif stock_cnt < a.min_stock:
            indicators["tbi_transaction_breadth"]=unknown("INSUFFICIENT_STOCK"); scan["insufficient_stock"]+=1
        else:
            v=clamp(deeds_cnt/float(stock_cnt),0.0,1.0)
            conf=0.6+0.4*score_band(deeds_cnt,a.min_samples,a.min_samples*5)
            indicators["tbi_transaction_breadth"]=known(v,conf,True,{
              "what_changed":f"{deeds_cnt} arms-length transfers over {w}d vs ~{stock_cnt} parcels.",
              "compared_to":"parcel-universe normalization (stock)",
              "window_days":w
            })

        # 2) divergence
        if deeds_cnt < a.min_samples or mls_closed < a.min_samples:
            indicators["divergence_deeds_mls"]=unknown("INSUFFICIENT_SAMPLES")
        else:
            denom=deeds_cnt+mls_closed
            v=clamp((deeds_cnt-mls_closed)/float(denom),-1.0,1.0) if denom>0 else None
            conf=0.55+0.45*score_band(min(deeds_cnt,mls_closed),a.min_samples,a.min_samples*5)
            indicators["divergence_deeds_mls"]=known(v,conf,True,{
              "what_changed":f"Deeds={deeds_cnt} vs MLS closed={mls_closed} over {w}d.",
              "compared_to":"intent (MLS) vs commitment (deeds)",
              "window_days":w
            })

        # 3) absorption acceleration
        if abs90 is None or abs180 is None or abs180==0:
            indicators["momentum_absorption_accel"]=unknown("MISSING_METRIC"); scan["missing_metric"]+=1
        else:
            v=clamp((abs90-abs180)/abs180,-1.0,1.0)
            conf=0.5+0.5*score_band(mls_closed,a.min_samples,a.min_samples*6)
            indicators["momentum_absorption_accel"]=known(v,conf,mls_closed>=a.min_samples,{
              "what_changed":"Absorption rate changed between 180d and 90d windows.",
              "compared_to":"rolling-window baseline (180d)",
              "window_days":90
            })

        # 4) liquidity stability (DOM drift proxy)
        vals=[x for x in [dom30,dom90,dom180] if x and x>0]
        if len(vals)<2 or deeds_cnt < a.min_samples:
            indicators["volatility_liquidity_stability"]=unknown("MISSING_METRIC" if len(vals)<2 else "INSUFFICIENT_SAMPLES")
        else:
            diffs=[]
            for a1,b1 in [(dom30,dom90),(dom90,dom180),(dom30,dom180)]:
                if a1 and b1 and a1>0 and b1>0:
                    diffs.append(abs(a1-b1)/max(a1,b1))
            drift=sum(diffs)/len(diffs) if diffs else None
            v=clamp(1.0-drift,0.0,1.0) if drift is not None else None
            conf=0.55+0.45*score_band(deeds_cnt,a.min_samples,a.min_samples*6)
            indicators["volatility_liquidity_stability"]=known(v,conf,True,{
              "what_changed":f"DOM median drift across 30/90/180d: dom30={dom30}, dom90={dom90}, dom180={dom180}.",
              "compared_to":"window drift (stability proxy)",
              "window_days":180
            })

        # 5) rotation pressure (within-ZIP proxy)
        if deeds180<=0 or deeds_cnt < a.min_samples:
            indicators["rotation_capital_pressure"]=unknown("INSUFFICIENT_SAMPLES" if deeds_cnt<a.min_samples else "MISSING_METRIC")
        else:
            v=clamp((deeds90-deeds180)/float(deeds180),-1.0,1.0)
            conf=0.5+0.5*score_band(deeds90,a.min_samples,a.min_samples*6)
            indicators["rotation_capital_pressure"]=known(v,conf,True,{
              "what_changed":f"Deed activity shifted: 90d={deeds90} vs 180d={deeds180}.",
              "compared_to":"within-ZIP baseline (180d)",
              "window_days":90
            }, note="v0_1 is within-ZIP; adjacency/neighbor rotation added later.")

        # 6) off-market share proxy
        if deeds_cnt < a.min_samples:
            indicators["off_market_participation"]=unknown("INSUFFICIENT_SAMPLES")
        else:
            off=max(deeds_cnt-mls_closed,0)
            v=clamp(off/float(deeds_cnt),0.0,1.0) if deeds_cnt>0 else None
            conf=0.55+0.45*score_band(deeds_cnt,a.min_samples,a.min_samples*6)
            indicators["off_market_participation"]=known(v,conf,True,{
              "what_changed":f"Off-market proxy: max(deeds-MLS_closed,0)/deeds = max({deeds_cnt}-{mls_closed},0)/{deeds_cnt}.",
              "compared_to":"MLS-originated closings proxy",
              "window_days":w
            })

        any_known=any(v.get("state")=="KNOWN" for v in indicators.values())
        scan["known"]+=1 if any_known else 0
        scan["unknown"]+=0 if any_known else 1

        out_rows.append({
          "schema_version":SCHEMA_VERSION,"engine_version":ENGINE_VERSION,"as_of_date":a.as_of,
          "zip":z,"asset_bucket":b,"window_days":w,
          "lineage":{"as_of_date":a.as_of,"engine_version":ENGINE_VERSION,"inputs":{"deeds":a.deeds,"stock":a.stock,"absorption":a.absorption,"liquidity":a.liquidity,"price_discovery":a.price_discovery}},
          "inputs_snapshot":{"deeds_arms_length":deeds_cnt,"stock_parcels":stock_cnt,"mls_closed":mls_closed},
          "indicators":indicators
        })

    wrote=write_ndjson(a.out,out_rows)
    out_sha=sha256_file(a.out)
    write_json(a.out+".sha256.json", {"path":a.out,"sha256":out_sha,"built_at_utc":utc_now_iso()})
    write_json(a.audit,{
      "built_at_utc":utc_now_iso(),"as_of_date":a.as_of,
      "inputs":{"deeds":a.deeds,"stock":a.stock,"absorption":a.absorption,"liquidity":a.liquidity,"price_discovery":a.price_discovery},
      "config":{"min_samples":a.min_samples,"min_stock":a.min_stock},
      "scan":scan,"out":a.out,"sha256":out_sha
    })

if __name__=="__main__": main()
