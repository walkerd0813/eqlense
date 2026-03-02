import os, json, argparse, datetime, collections

def utc_now_z():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

def doc_type_from_pdf(pdf):
    n = (pdf or '').lower()
    if 'discharge_mortgage' in n: return 'DM'
    if 'mortgage' in n and 'discharge' not in n: return 'MTG'
    if 'forclosure_deeds' in n or 'foreclosure_deeds' in n: return 'FDEED'
    if 'master_deeds' in n or 'master deeds' in n: return 'MSDD'
    if 'deeds' in n: return 'DEED'
    if 'assign' in n: return 'ASN'
    if 'release' in n: return 'REL'
    if 'mass_taxliens' in n: return 'MTL'
    if 'fed_taxliens' in n: return 'FTL'
    if 'manicipal_liens' in n or 'municipal_liens' in n: return 'MUNL'
    if 'lispenden' in n or 'lispendens' in n: return 'LIS'
    if 'easement' in n: return 'ESMT'
    if 'discharge-generic' in n or 'discharge_generic' in n: return 'DIS'
    if 'liens' in n: return 'LIEN'
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in_events', required=True)
    ap.add_argument('--out_dir', required=True)
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    ts = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    out_events = os.path.join(args.out_dir, f'events__POSTFIX_DOCTYPE_FROM_FILENAME__{ts}.ndjson')
    out_report = os.path.join(args.out_dir, f'qa__POSTFIX_DOCTYPE_FROM_FILENAME__{ts}.json')

    total = 0
    overridden = 0
    by_pdf_total = collections.Counter()
    by_pdf_over = collections.Counter()
    by_pair = collections.Counter()

    with open(args.in_events, 'r', encoding='utf-8') as fin, open(out_events, 'w', encoding='utf-8') as fout:
        for line in fin:
            line = line.strip()
            if not line: continue
            total += 1
            r = json.loads(line)
            src = r.get('source') or {}
            pdf = src.get('pdf') or '?'
            want = doc_type_from_pdf(pdf)
            have = (r.get('doc_type_code') or 'UNKNOWN')
            r['doc_type_code_row'] = have
            by_pdf_total[pdf] += 1
            if want and have != want:
                by_pair[(have, want)] += 1
                r['doc_type_code'] = want
                r['doc_type_desc'] = want
                overridden += 1
                by_pdf_over[pdf] += 1
            fout.write(json.dumps(r, ensure_ascii=False) + '\\n')

    report = {
        'ok': True,
        'engine': 'events.otr_postfix_doctype_from_filename_v1',
        'ran_at': utc_now_z(),
        'inputs': {'in_events': args.in_events},
        'outputs': {'out_events': out_events, 'out_report': out_report},
        'stats': {'total_events': total, 'events_overridden': overridden},
        'top_pdf_overrides': [
            {'pdf': pdf, 'overrides': n, 'total': by_pdf_total[pdf], 'pct': round(n*100.0/by_pdf_total[pdf], 2)}
            for pdf, n in by_pdf_over.most_common(50)
        ],
        'top_pair_before': [
            {'from': a, 'to': b, 'count': n} for (a,b), n in by_pair.most_common(100)
        ]
    }
    with open(out_report, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)

    print(json.dumps({'ok': True, 'total_events': total, 'events_overridden': overridden, 'out_events': out_events, 'out_report': out_report}))

if __name__ == '__main__':
    main()

