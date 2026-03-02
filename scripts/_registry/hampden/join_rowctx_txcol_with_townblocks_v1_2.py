import json, argparse, os, re
from collections import defaultdict

RE_STRAY_INT = re.compile(r'^\d{1,4}$')

def read_ndjson(path):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

def is_blank(v):
    return v is None or (isinstance(v, str) and v.strip() == '')

def tb_meta(ev):
    m = ev.get('meta') or {}
    return (m.get('page_index'), m.get('record_index'))

def tb_inst(ev):
    rec = ev.get('recording') or {}
    return rec.get('inst_raw') or ev.get('inst_raw')

def tb_book(ev):
    rec = ev.get('recording') or {}
    return rec.get('book_page_raw') or ev.get('book_page_raw')

def tb_time(ev):
    rec = ev.get('recording') or {}
    return rec.get('recorded_at_raw') or ev.get('recorded_at_raw')

def build_indexes(rowctx_rows):
    by_page_inst = defaultdict(dict)
    by_page_timebook = defaultdict(dict)
    by_page_recidx = defaultdict(dict)
    max_idx_by_page = defaultdict(int)

    for rc in rowctx_rows:
        p = rc.get('page_index')
        i = rc.get('record_index')
        if p is None or i is None:
            continue
        if i > max_idx_by_page[p]:
            max_idx_by_page[p] = i

        by_page_recidx[p][i] = rc

        inst = rc.get('inst_raw')
        if inst:
            by_page_inst[p][inst] = rc

        t = rc.get('recorded_at_raw')
        b = rc.get('book_page_raw')
        if t and b:
            by_page_timebook[p][(t, b)] = rc

    return by_page_inst, by_page_timebook, by_page_recidx, max_idx_by_page

def _ref_page_part(refbp):
    if not isinstance(refbp, str):
        return None
    if '-' not in refbp:
        return None
    parts = refbp.split('-', 1)
    if len(parts) != 2:
        return None
    return parts[1].strip()

def should_overwrite_cons(existing_raw, rc_cons, refbp_raw, prefer_overwrite):
    # Overwrite consideration if:
    # - prefer_overwrite is True
    # - existing is blank
    # - existing looks like stray int (e.g. '187')
    # - existing equals the page part of ref_book_page_raw (e.g. '187' when refbp is '23649-187')
    if is_blank(rc_cons):
        return False
    if prefer_overwrite:
        return True
    if is_blank(existing_raw):
        return True
    if isinstance(existing_raw, str):
        ex = existing_raw.strip()
        if RE_STRAY_INT.match(ex):
            return True
        rp = _ref_page_part(refbp_raw)
        if rp and ex == rp:
            return True
    return False

def attach_rowctx(ev, rc, prefer_overwrite):
    rec = ev.get('recording')
    if not isinstance(rec, dict):
        rec = {}
        ev['recording'] = rec

    for k_src, k_dst in [
        ('recorded_at_raw', 'recorded_at_raw'),
        ('book_page_raw', 'book_page_raw'),
        ('inst_raw', 'inst_raw'),
        ('grp_seq_raw', 'grp_seq_raw'),
        ('ref_book_page_raw', 'ref_book_page_raw'),
    ]:
        v = rc.get(k_src)
        if is_blank(v):
            continue
        if prefer_overwrite or is_blank(rec.get(k_dst)):
            rec[k_dst] = v

    cons = ev.get('consideration')
    if not isinstance(cons, dict):
        cons = {}
        ev['consideration'] = cons

    existing_raw = cons.get('amount_raw')
    rc_cons = rc.get('consideration_raw')
    refbp_raw = rec.get('ref_book_page_raw') or rc.get('ref_book_page_raw')

    if should_overwrite_cons(existing_raw, rc_cons, refbp_raw, prefer_overwrite):
        cons['amount_raw'] = rc_cons

    ev['rowctx'] = {
        'source': 'ROWCTX_JOIN_V1_2',
        'page_index': rc.get('page_index'),
        'record_index': rc.get('record_index'),
        'matched_by': rc.get('_matched_by')
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--townblocks', required=True)
    ap.add_argument('--rowctx', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--qa', required=True)
    ap.add_argument('--prefer_overwrite', action='store_true')
    args = ap.parse_args()

    townblocks = list(read_ndjson(args.townblocks))
    rowctx = list(read_ndjson(args.rowctx))

    by_page_inst, by_page_timebook, by_page_recidx, max_idx_by_page = build_indexes(rowctx)

    counts = {
        'townblocks_seen': len(townblocks),
        'rowctx_seen': len(rowctx),
        'matched_by_inst': 0,
        'matched_by_time_book': 0,
        'matched_by_record_index': 0,
        'matched_by_record_index_flipped': 0,
        'unmatched': 0,
    }

    out_rows = []
    unmatched_samples = []

    for ev in townblocks:
        p, ridx = tb_meta(ev)
        if p is None or ridx is None:
            counts['unmatched'] += 1
            if len(unmatched_samples) < 20:
                unmatched_samples.append({'why': 'missing_meta', 'meta': ev.get('meta')})
            out_rows.append(ev)
            continue

        rc = None
        matched_by = None

        inst = tb_inst(ev)

        if inst and inst in by_page_inst.get(p, {}):
            rc = dict(by_page_inst[p][inst])
            matched_by = 'inst'
            counts['matched_by_inst'] += 1

        if rc is None:
            t = tb_time(ev)
            b = tb_book(ev)
            if t and b and (t, b) in by_page_timebook.get(p, {}):
                rc = dict(by_page_timebook[p][(t, b)])
                matched_by = 'time_book'
                counts['matched_by_time_book'] += 1

        if rc is None:
            base = by_page_recidx.get(p, {}).get(ridx)
            if base is not None:
                rc = dict(base)
                matched_by = 'record_index'
                counts['matched_by_record_index'] += 1

        if rc is None:
            mx = max_idx_by_page.get(p, 0)
            if mx:
                flip = (mx + 1 - ridx)
                base = by_page_recidx.get(p, {}).get(flip)
                if base is not None:
                    rc = dict(base)
                    matched_by = 'record_index_flipped'
                    counts['matched_by_record_index_flipped'] += 1

        if rc is None:
            counts['unmatched'] += 1
            if len(unmatched_samples) < 20:
                unmatched_samples.append({'page_index': p, 'record_index': ridx, 'inst': inst})
            out_rows.append(ev)
            continue

        rc['_matched_by'] = matched_by
        attach_rowctx(ev, rc, prefer_overwrite=args.prefer_overwrite)
        out_rows.append(ev)

    os.makedirs(os.path.dirname(args.qa), exist_ok=True)
    with open(args.qa, 'w', encoding='utf-8') as f:
        json.dump({
            'engine': 'join_rowctx_txcol_with_townblocks_v1_2',
            'inputs': {'townblocks': args.townblocks, 'rowctx': args.rowctx},
            'counts': counts,
            'unmatched_samples': unmatched_samples,
            'flags': {
                'prefer_overwrite': bool(args.prefer_overwrite),
                'auto_overwrite_consideration_on_stray_int': True,
                'auto_overwrite_consideration_if_equals_refbp_page_part': True
            },
            'note': 'Inst/time-book preferred first. Fallback flips record_index per page. Consideration overwrites if blank OR looks like stray int (e.g. 187) OR equals ref_book_page page-part; --prefer_overwrite forces overwrite of recording + consideration.'
        }, f, indent=2)

    write_ndjson(args.out, out_rows)
    print(f'[done] events_out={len(out_rows)} out={args.out} qa={args.qa}')

if __name__ == '__main__':
    main()
