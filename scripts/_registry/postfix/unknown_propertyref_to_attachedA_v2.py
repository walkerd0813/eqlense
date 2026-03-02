#!/usr/bin/env python3
"""
unknown_propertyref_to_attachedA_v2.py

Deterministic property_ref rescue:
- Only touches events where attach.attach_status == UNKNOWN (or attach_status == UNKNOWN).
- Builds spine indices:
    base_index:  TOWN|STREET_NO|STREET_NAME_NORM  -> single spine row (else collision)
    unit_index:  TOWN|STREET_NO|STREET_NAME_NORM|UNIT|UNITVAL -> single spine row (else collision)
- From event.property_ref.{town_code,address_norm/address_raw} derive candidate keys and attach if UNIQUE.

Institutional rules:
- No fuzzy/nearest.
- If multiple spine matches or collisions -> do NOT attach.
"""

import argparse, json, re, datetime
from typing import Dict, Tuple, Optional, List

def nowz() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')

def up(s: Optional[str]) -> str:
    return (s or '').strip().upper()

_ws = re.compile(r'\s+')
def norm_ws(s: str) -> str:
    return _ws.sub(' ', (s or '').strip())

_SUFFIX = {
    'RD':'ROAD','RD.':'ROAD',
    'DR':'DRIVE','DR.':'DRIVE',
    'ST':'STREET','ST.':'STREET',
    'AVE':'AVENUE','AVE.':'AVENUE',
    'AV':'AVENUE',
    'BLVD':'BOULEVARD','BLVD.':'BOULEVARD',
    'CT':'COURT','CT.':'COURT',
    'TER':'TERRACE','TER.':'TERRACE',
    'PL':'PLACE','PL.':'PLACE',
    'PKWY':'PARKWAY','HWY':'HIGHWAY',
    'LN':'LANE','LN.':'LANE',
    'CIR':'CIRCLE','CIR.':'CIRCLE',
    'SQ':'SQUARE','SQ.':'SQUARE',
}

def norm_street_name(raw: str) -> str:
    s = up(norm_ws(raw))
    toks = s.split()
    if not toks:
        return ''
    last = toks[-1]
    if last in _SUFFIX and len(toks) >= 2:
        toks[-1] = _SUFFIX[last]
    return ' '.join(toks)

_UNIT_PAT = re.compile(r'(?:\bUNIT\b|\bAPT\b|\bAPARTMENT\b|\bPH\b|\bPENTHOUSE\b|#)\s*([A-Z0-9\-]+)\b', re.I)

def split_unit(addr: str) -> Tuple[str, Optional[str]]:
    a = norm_ws(addr)
    m = _UNIT_PAT.search(a)
    if not m:
        return a, None
    unit = up(m.group(1))
    a2 = _UNIT_PAT.sub('', a)
    return norm_ws(a2), unit

_RANGE_PAT = re.compile(r'^\s*(\d+)\s*[- ]\s*(\d+)\b')
_HALF_PAT = re.compile(r'^\s*(\d+)\s+1/2\b')

def parse_street_no(no_raw: str) -> Tuple[str, Dict[str,bool]]:
    flags = {'is_range': False, 'is_half': False}
    s = norm_ws(no_raw)
    m = _RANGE_PAT.match(s)
    if m:
        flags['is_range'] = True
        return f"{m.group(1)}-{m.group(2)}", flags
    m2 = _HALF_PAT.match(s)
    if m2:
        flags['is_half'] = True
        return f"{m2.group(1)} 1/2", flags
    return s.split()[0], flags

def parse_addr(addr: str) -> Tuple[Optional[str], Optional[str]]:
    a = up(norm_ws(addr))
    if not a:
        return None, None
    toks = a.split()
    if not toks:
        return None, None

    if len(toks) >= 2 and toks[0].isdigit() and toks[1].isdigit():
        street_no = f"{toks[0]}-{toks[1]}"
        street = ' '.join(toks[2:]) if len(toks) > 2 else ''
        return street_no, (street or None)

    if '-' in toks[0] and toks[0].replace('-','').isdigit():
        street_no = toks[0]
        street = ' '.join(toks[1:]) if len(toks) > 1 else ''
        return street_no, (street or None)

    if len(toks) >= 2 and toks[0].isdigit() and toks[1] == '1/2':
        street_no = f"{toks[0]} 1/2"
        street = ' '.join(toks[2:]) if len(toks) > 2 else ''
        return street_no, (street or None)

    if toks[0].isdigit() or toks[0] == '0':
        street_no = toks[0]
        street = ' '.join(toks[1:]) if len(toks) > 1 else ''
        return street_no, (street or None)

    return None, None

def get_attach_status(ev: dict) -> str:
    a = ev.get('attach') or {}
    return up(a.get('attach_status') or ev.get('attach_status') or '')

def apply_attach(ev: dict, spine_row: dict, status: str, method: str, match_key_used: str):
    if 'attach' not in ev or not isinstance(ev.get('attach'), dict):
        ev['attach'] = {}
    ev['attach']['attach_status'] = status
    ev['attach']['property_id'] = spine_row.get('property_id') or spine_row.get('property_uid') or spine_row.get('parcel_id')
    ev['attach']['match_method'] = method
    ev['attach']['match_key'] = match_key_used
    ev['attach']['match_key_used'] = match_key_used
    ev['attach_status'] = status
    ev['match_method'] = method
    ev['match_key'] = match_key_used

def build_spine_indices(spine_path: str) -> Tuple[Dict[str,dict], Dict[str,dict], dict]:
    base_index: Dict[str, dict] = {}
    unit_index: Dict[str, dict] = {}
    stats = {'rows_scanned': 0, 'base_keys': 0, 'unit_keys': 0, 'base_collisions': 0, 'unit_collisions': 0}

    def put_unique(idx: Dict[str,dict], key: str, row: dict, which: str):
        if key in idx:
            if idx[key] is not None:
                idx[key] = None
                stats[f'{which}_collisions'] += 1
            return
        idx[key] = row
        stats[f'{which}_keys'] += 1

    with open(spine_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            stats['rows_scanned'] += 1
            r = json.loads(line)

            town = up(r.get('town') or (r.get('property_uid','').split('|')[0] if '|' in (r.get('property_uid') or '') else ''))
            if not town:
                continue

            ak = (r.get('address_key') or '').strip()
            street_no = None
            street_name = None
            if ak.startswith('A|'):
                parts = ak.split('|')
                if len(parts) >= 4:
                    street_no = parts[1]
                    street_name = parts[2]
            if not street_no or not street_name:
                addr_label = up(r.get('address_label') or '')
                head = addr_label.split(',')[0] if addr_label else ''
                sn, st = parse_addr(head)
                street_no = street_no or sn
                street_name = street_name or st

            if not street_no or not street_name:
                continue

            sn_norm, _ = parse_street_no(street_no)
            st_norm = norm_street_name(street_name)

            base_key = f"{town}|{sn_norm}|{st_norm}"
            put_unique(base_index, base_key, r, 'base')

            unit_val = (r.get('unit') or '').strip()
            if unit_val:
                ukey = f"{town}|{sn_norm}|{st_norm}|UNIT|{up(unit_val)}"
                put_unique(unit_index, ukey, r, 'unit')

    return base_index, unit_index, stats

def attempt_attach(base_index: Dict[str,dict], unit_index: Dict[str,dict],
                   town: str, street_no: str, street_name_norm: str,
                   unit: Optional[str]) -> Tuple[Optional[dict], str, str]:
    if unit:
        ukey = f"{town}|{street_no}|{street_name_norm}|UNIT|{up(unit)}"
        hit = unit_index.get(ukey)
        if hit is None:
            return None, 'collision', ukey
        if hit:
            return hit, 'ok', ukey

    bkey = f"{town}|{street_no}|{street_name_norm}"
    hit = base_index.get(bkey)
    if hit is None:
        return None, 'collision', bkey
    if hit:
        return hit, 'ok', bkey
    return None, 'no_match', bkey

def street_no_variants(sn_raw: str) -> List[str]:
    sn = norm_ws(sn_raw)
    out: List[str] = []
    if '-' in sn and sn.replace('-','').isdigit():
        a,b = sn.split('-',1)
        out.extend([sn, a, b])
        return out
    if '1/2' in sn:
        out.extend([sn, sn.split()[0]])
        return out
    out.append(sn)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--infile', required=True)
    ap.add_argument('--spine', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--audit', required=True)
    ap.add_argument('--engine_id', required=True)
    args = ap.parse_args()

    audit = {
        'engine_id': args.engine_id,
        'started_at': nowz(),
        'infile': args.infile,
        'spine': args.spine,
        'out': args.out,
        'audit': args.audit,
        'rows_scanned': 0,
        'rows_unknown_in': 0,
        'rows_attached': 0,
        'rows_bad': 0,
        'rows_multi_or_collision': 0,
        'rows_no_match': 0,
        'detail_counts': {},
        'spine_stats': {},
    }

    def bump(k: str):
        audit['detail_counts'][k] = audit['detail_counts'].get(k, 0) + 1

    base_index, unit_index, spine_stats = build_spine_indices(args.spine)
    audit['spine_stats'] = spine_stats

    with open(args.infile, 'r', encoding='utf-8') as fin, open(args.out, 'w', encoding='utf-8') as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            audit['rows_scanned'] += 1
            ev = json.loads(line)

            if get_attach_status(ev) != 'UNKNOWN':
                fout.write(json.dumps(ev, ensure_ascii=False) + '\n')
                continue

            audit['rows_unknown_in'] += 1

            pref = ev.get('property_ref') or {}
            town = up(pref.get('town_code') or ev.get('town') or '')
            addr0 = pref.get('address_norm') or pref.get('address_raw') or ev.get('full_address') or ''
            addr0 = norm_ws(addr0)

            if not town or not addr0:
                audit['rows_bad'] += 1
                bump('missing_town_or_addr')
                fout.write(json.dumps(ev, ensure_ascii=False) + '\n')
                continue

            head = up(addr0).split(',')[0].strip()
            head_wo_unit, unit = split_unit(head)

            sn_raw, st_raw = parse_addr(head_wo_unit)
            if not sn_raw or not st_raw:
                audit['rows_bad'] += 1
                bump('addr_parse_fail')
                fout.write(json.dumps(ev, ensure_ascii=False) + '\n')
                continue

            st_norm = norm_street_name(st_raw)

            attached = False
            for snv in street_no_variants(sn_raw):
                got, why, _key_used = attempt_attach(base_index, unit_index, town, snv, st_norm, unit)
                if got:
                    mk = f"{town}|{snv}|{st_norm}" + (f"|UNIT|{up(unit)}" if unit else '')
                    apply_attach(ev, got, 'ATTACHED_A', 'postfix|property_ref_rescue_v2', mk)
                    audit['rows_attached'] += 1
                    bump('attached')
                    attached = True
                    break
                if why == 'collision':
                    audit['rows_multi_or_collision'] += 1
                    bump('collision')
                    break

            if not attached:
                audit['rows_no_match'] += 1
                bump('no_match')

            fout.write(json.dumps(ev, ensure_ascii=False) + '\n')

    audit['finished_at'] = nowz()
    with open(args.audit, 'w', encoding='utf-8') as f:
        json.dump(audit, f, indent=2, ensure_ascii=False)

if __name__ == '__main__':
    main()
