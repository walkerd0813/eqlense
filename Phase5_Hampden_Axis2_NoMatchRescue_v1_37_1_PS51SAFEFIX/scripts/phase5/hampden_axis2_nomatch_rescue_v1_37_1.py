#!/usr/bin/env python3
import argparse, json, re
from collections import defaultdict, Counter
from difflib import SequenceMatcher

SUFFIX_MAP = {
  'ST':'ST', 'STREET':'ST',
  'RD':'RD','ROAD':'RD',
  'AVE':'AVE','AV':'AVE','AVENUE':'AVE',
  'BLVD':'BLVD','BOULEVARD':'BLVD',
  'DR':'DR','DRIVE':'DR',
  'LN':'LN','LANE':'LN',
  'CT':'CT','COURT':'CT',
  'PL':'PL','PLACE':'PL',
  'PKY':'PKY','PARKWAY':'PKY',
  'CIR':'CIR','CIRCLE':'CIR',
  'HWY':'HWY','HIGHWAY':'HWY',
  'TER':'TER','TERR':'TER','TERRACE':'TER',
  'WAY':'WAY'
}
UNIT_TOKENS = set(['UNIT','APT','#','APARTMENT','SUITE','STE'])

def town_norm(x):
    if not x: return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+"," ", s)
    return s or None

def pick_first_str(obj, paths):
    """Try multiple dotted paths, return first non-empty string."""
    for p in paths:
        cur=obj
        ok=True
        for part in p.split('.'):
            if isinstance(cur, dict) and part in cur:
                cur=cur[part]
            else:
                ok=False
                break
        if not ok: continue
        if cur is None: continue
        if isinstance(cur, str):
            s=cur.strip()
            if s: return s
        # sometimes address comes as dict like {"raw": "..."}
        if isinstance(cur, dict):
            for k in ('raw','text','value','addr','address'):
                v=cur.get(k)
                if isinstance(v,str) and v.strip():
                    return v.strip()
    return None

def recover_town_addr(r):
    town = r.get('town')
    addr = r.get('addr')

    if not town or not isinstance(town,str) or not town.strip():
        town = pick_first_str(r, [
            'recording.town','recording.municipality','recording.city',
            'property_ref.town','property_ref.city','property_ref.municipality',
            'property.town','property.city','property.municipality'
        ])

    if not addr or not isinstance(addr,str) or not addr.strip():
        addr = pick_first_str(r, [
            'recording.addr','recording.address','recording.site_address',
            'property_ref.addr','property_ref.address','property_ref.site_address',
            'property.addr','property.address','property.site_address',
            'meta.addr','meta.address'
        ])

    return town_norm(town), (addr.strip().upper() if isinstance(addr,str) and addr.strip() else None)

def normalize_suffix(tokens):
    if not tokens: return tokens
    t = tokens[:]
    last = t[-1]
    last2 = SUFFIX_MAP.get(last, last)
    t[-1]=last2
    return t

def strip_unit(tokens):
    out=[]
    for tok in tokens:
        if tok in UNIT_TOKENS:
            break
        out.append(tok)
    return out

def parse_addr(addr):
    if not addr: return None
    s = str(addr).strip().upper()
    s = re.sub(r"\s+"," ", s)
    # common range: 19-21 THOMAS AVE
    m = re.match(r"^(\d+)\s*[-/]\s*(\d+)\s+(.*)$", s)
    if m:
        lo=int(m.group(1)); hi=int(m.group(2)); rest=m.group(3)
        toks=[t for t in re.split(r"[^A-Z0-9]+", rest) if t]
        toks=strip_unit(toks)
        toks=normalize_suffix(toks)
        return ('range', lo, hi, ' '.join(toks))
    m = re.match(r"^(\d+)\s+(.*)$", s)
    if not m:
        return None
    no=int(m.group(1)); rest=m.group(2)
    toks=[t for t in re.split(r"[^A-Z0-9]+", rest) if t]
    toks=strip_unit(toks)
    toks=normalize_suffix(toks)
    street=' '.join(toks)
    if not street:
        return None
    return ('single', no, street)

def edit_distance_limited(a: str, b: str, limit=2):
    if a == b:
        return 0
    if abs(len(a) - len(b)) > limit:
        return limit+1
    prev = list(range(len(b)+1))
    for i,ca in enumerate(a, start=1):
        cur=[i]
        row_min = cur[0]
        for j,cb in enumerate(b, start=1):
            ins = cur[j-1] + 1
            dele = prev[j] + 1
            sub = prev[j-1] + (0 if ca==cb else 1)
            v = ins if ins < dele else dele
            if sub < v:
                v = sub
            cur.append(v)
            if v < row_min:
                row_min = v
        if row_min > limit:
            return limit+1
        prev = cur
    return prev[-1]

def score(a: str, b: str):
    r = SequenceMatcher(None, a, b).ratio()
    d = edit_distance_limited(a, b, limit=2)
    return r, d

def load_needed_keys(in_path):
    keys=set()
    eligible=0
    with open(in_path,'r',encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            r=json.loads(line)
            if r.get('attach_status') != 'UNKNOWN':
                continue
            why = str(r.get('why','')).lower().strip()
            if why not in ('no_match','collision'):
                continue
            t, a = recover_town_addr(r)
            parsed = parse_addr(a)
            if not t or not parsed:
                continue
            if parsed[0]=='single':
                keys.add((t, parsed[1]))
                eligible += 1
            elif parsed[0]=='range':
                lo,hi = parsed[1],parsed[2]
                if hi-lo <= 4:
                    for n in range(lo,hi+1):
                        keys.add((t,n))
                    eligible += 1
    return keys, eligible

def build_spine_index(spine_path, needed_keys):
    idx=defaultdict(list)
    with open(spine_path,'r',encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            r=json.loads(line)
            t = town_norm(r.get('town') or r.get('municipality') or r.get('city'))
            if not t:
                continue
            hn = r.get('house_number') or r.get('addr_number') or r.get('street_number') or r.get('number')
            hn_int=None
            if hn is not None:
                try:
                    hn_int=int(str(hn).strip())
                except:
                    hn_int=None
            if hn_int is None:
                addr = r.get('address') or r.get('addr') or r.get('site_address')
                p=parse_addr(addr)
                if p and p[0]=='single':
                    hn_int=p[1]
            if hn_int is None:
                continue
            key=(t,hn_int)
            if key not in needed_keys:
                continue
            addr = (r.get('address') or r.get('addr') or r.get('site_address') or '').strip().upper()
            p=parse_addr(addr)
            street = p[2] if p and p[0]=='single' else None
            if not street:
                continue
            idx[key].append({
                'property_id': r.get('property_id') or r.get('id'),
                'address': addr,
                'street_norm': street,
            })
    return idx

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--in', dest='inp', required=True)
    ap.add_argument('--spine', required=True)
    ap.add_argument('--out', required=True)
    args=ap.parse_args()

    needed, eligible = load_needed_keys(args.inp)
    print('[info] eligible UNKNOWN/(no_match|collision) rows:', eligible)
    print('[info] needed (town,house#) keys:', len(needed))

    idx = build_spine_index(args.spine, needed) if needed else {}

    counts=Counter()

    def try_rescue(row):
        t, a = recover_town_addr(row)
        if not t or not a:
            counts['no_town_or_addr'] += 1
            return row
        p=parse_addr(a)
        if not p:
            counts['unparseable_addr'] += 1
            return row
        if p[0]=='single':
            nums=[p[1]]
            street=p[2]
        else:
            lo,hi,street = p[1],p[2],p[3]
            if hi-lo>4:
                counts['range_too_wide'] += 1
                return row
            nums=list(range(lo,hi+1))

        # gather candidates
        cands=[]
        for n in nums:
            cands.extend(idx.get((t,n), []))
        if not cands:
            counts['no_spine_candidates_same_no'] += 1
            return row

        # strong within-town street uniqueness
        strong=[]
        for c in cands:
            rratio, dist = score(street, c['street_norm'])
            if rratio >= 0.96 and dist <= 1:
                strong.append((rratio, dist, c))

        if len(strong) != 1:
            counts['not_unique_strong'] += 1
            return row

        best = strong[0][2]
        # attach as B (still fuzzy-ish but extremely strict)
        row['attach_status'] = 'ATTACHED_B'
        row['match_method'] = 'axis2_unknown_nomatch_rescue_strong_unique'
        row['why'] = 'NONE'
        row['attachments_n'] = 1
        row['attachments'] = [{
            'property_id': best.get('property_id'),
            'address': best.get('address'),
            'method': 'within_town_house_no_strong_unique',
        }]
        counts['rescued'] += 1
        return row

    with open(args.inp,'r',encoding='utf-8') as fin, open(args.out,'w',encoding='utf-8') as fout:
        for line in fin:
            if not line.strip():
                continue
            row=json.loads(line)
            if row.get('attach_status')=='UNKNOWN' and str(row.get('why','')).lower().strip() in ('no_match','collision'):
                row = try_rescue(row)
            else:
                counts['pass_through'] += 1
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")

    audit_path = re.sub(r"\.ndjson$", "__audit_v1_37_1.json", args.out)
    with open(audit_path,'w',encoding='utf-8') as f:
        json.dump({'counts': counts}, f, indent=2)
    print('[done] v1_37_1 NO_MATCH rescue')
    for k,v in counts.most_common():
        print(' ',k+':', v)
    print('[ok] OUT  ', args.out)
    print('[ok] AUDIT', audit_path)

if __name__=='__main__':
    main()
