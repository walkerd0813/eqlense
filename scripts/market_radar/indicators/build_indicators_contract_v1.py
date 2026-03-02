#!/usr/bin/env python3
from __future__ import annotations
import argparse, datetime, json, os, hashlib
from typing import Any, Dict
SCHEMA_VERSION='market_radar_indicator_contract_v1'
ENGINE_VERSION='v0_1'
def utc_now_iso()->str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec='seconds').replace('+00:00','Z')
def sha256_file(p:str)->str:
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for chunk in iter(lambda:f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()
def write_json(p:str,o:Any)->None:
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p,'w',encoding='utf-8') as f:
        json.dump(o,f,indent=2,sort_keys=False)
def main()->None:
    ap=argparse.ArgumentParser()
    ap.add_argument('--out',required=True); ap.add_argument('--audit',required=True); ap.add_argument('--as_of',required=True)
    a=ap.parse_args()
    contract:Dict[str,Any]={
      'schema_version':SCHEMA_VERSION,'engine_version':ENGINE_VERSION,'built_at_utc':utc_now_iso(),'as_of_date':a.as_of,
      'indicator_set':'P01',
      'indicators':[
        {'key':'tbi_transaction_breadth','name':'Transaction Breadth Index (TBI)','unit':'pct','range':[0.0,1.0],
         'unknown_rule':'UNKNOWN if deeds_arms_length < min_samples OR stock_parcels < min_stock','inputs_required':['deeds_zip','stock_zip']},
        {'key':'divergence_deeds_mls','name':'Deed–MLS Divergence Index','unit':'zscore_like','range':[-1.0,1.0],
         'unknown_rule':'UNKNOWN if deeds_arms_length < min_samples OR mls_closed < min_samples','inputs_required':['deeds_zip','absorption_zip']},
        {'key':'momentum_absorption_accel','name':'Absorption Acceleration Index','unit':'delta','range':[-1.0,1.0],
         'unknown_rule':'UNKNOWN if missing 90d or 180d absorption metrics','inputs_required':['absorption_zip']},
        {'key':'volatility_liquidity_stability','name':'Liquidity Stability Index','unit':'score','range':[0.0,1.0],
         'unknown_rule':'UNKNOWN if deeds_arms_length < min_samples OR missing liquidity metrics','inputs_required':['liquidity_p01_zip','deeds_zip']},
        {'key':'rotation_capital_pressure','name':'Geographic Capital Rotation Signal','unit':'score','range':[-1.0,1.0],
         'unknown_rule':'UNKNOWN if deeds_arms_length < min_samples','inputs_required':['deeds_zip']},
        {'key':'off_market_participation','name':'Off-Market Participation Index','unit':'pct','range':[0.0,1.0],
         'unknown_rule':'UNKNOWN if deeds_arms_length < min_samples','inputs_required':['deeds_zip','absorption_zip']},
      ],
      'gates':{'min_samples':10,'min_stock':30,'zip_regex':r'^\d{5}$','bucket_allowed':['single_family','condo','multifamily_2_4','multifamily_5plus','land','all'],'windows_days':[30,90,180,365]},
      'explainability_contract':{'rule':'Observed behavior + window + comparison. No advice, no prediction.','required_fields':['what_changed','compared_to','window_days'],
        'user_scopes':{'founder':'full','pro':'1-2 sentence','homeowner':'neutral summary'}},
      'unknown_state':{'allowed':['KNOWN','UNKNOWN','CONFLICTED'],'reason_codes':['MISSING_INPUT','INSUFFICIENT_SAMPLES','INSUFFICIENT_STOCK','MISSING_METRIC','BAD_ZIP','BAD_BUCKET','BAD_WINDOW']},
      'lineage':{'required':['as_of_date','source_pointer_keys','input_paths']},
      'time_sensitivity':{'default_half_life_days':90},
      'messaging_layer':{'internal_language':'allowed','pro_language':'observations only','homeowner_language':'neutral, broker handoff'}
    }
    write_json(a.out, contract)
    out_sha=sha256_file(a.out)
    write_json(a.out+'.sha256.json', {'path':a.out,'sha256':out_sha,'built_at_utc':utc_now_iso()})
    write_json(a.audit, {'ok':True,'built_at_utc':utc_now_iso(),'as_of_date':a.as_of,'engine_version':ENGINE_VERSION,'out':a.out,'sha256':out_sha})
if __name__=='__main__': main()
