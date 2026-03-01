import json, argparse, os, re
from collections import defaultdict

RE_STRAY_INT = re.compile(r"^\d{1,4}$")

def _ref_page_part(refbp_raw: str):
    if not refbp_raw:
        return None
    s = str(refbp_raw).strip()
    if "-" not in s:
        return None
    parts = s.split("-", 1)
    tail = parts[1].strip() if len(parts) > 1 else None
    return tail or None


def read_ndjson(path):
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # normalize trailing single-letter OCR artifacts like " Y" in address_raw
    TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.IGNORECASE)

    def _clean_row(r):
        try:
            refs = r.get('property_refs')
            if isinstance(refs, list):
                for rf in refs:
                    if isinstance(rf, dict):
                        addr = rf.get('address_raw')
                        if isinstance(addr, str) and TRAILING_Y_RE.search(addr):
                            rf['address_raw'] = TRAILING_Y_RE.sub('', addr).strip()
        except Exception:
            pass

        try:
            parties = r.get('parties')
            if isinstance(parties, dict):
                pr = parties.get('parties_raw')
                if isinstance(pr, list):
                    for p in pr:
                        if isinstance(p, dict):
                            name = p.get('name_raw')
                            if isinstance(name, str) and TRAILING_Y_RE.search(name):
                                p['name_raw'] = TRAILING_Y_RE.sub('', name).strip()
        except Exception:
            pass

        return r

    with open(path, 'w', encoding='utf-8') as f:
        for r in rows:
            rr = _clean_row(r)
            f.write(json.dumps(rr, ensure_ascii=False) + '\n')

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
    if not isinstance(refbp, str) or '-' not in refbp:
        return None
    parts = refbp.split('-', 1)
    if len(parts) != 2:
        return None
    return parts[1].strip()

def should_overwrite_cons(existing_raw, rc_cons, refbp_raw, prefer_overwrite):
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

def attach_rowctx(ev, rc, prefer_overwrite, matched_by):
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

    # v1_3_1 micro-fix: if RowCtx has no consideration but townblocks produced
    # a stray integer that matches the ref-page part (e.g., 23649-187 -> 187),
    # null it out so it doesn't masquerade as money.
    try:
        ex = str(cons.get("amount_raw") or "").strip()
        rp = _ref_page_part(refbp_raw)

        if (rc_cons is None or str(rc_cons).strip() == "") and rp and RE_STRAY_INT.match(ex) and ex == rp:
            cons["amount_raw"] = None
    except Exception:
        pass

    ev['rowctx'] = {
        'source': 'ROWCTX_JOIN_V1_3',
        'page_index': rc.get('page_index'),
        'record_index': rc.get('record_index'),
        'matched_by': matched_by
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--townblocks', required=True)
    ap.add_argument('--rowctx', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--qa', required=True)
    ap.add_argument('--prefer_overwrite', action='store_true')
    ap.add_argument('--prefer_flip_on_index_fallback', action='store_true',
                    help='When inst/time-book are unavailable, prefer flipped record_index over direct (recommended).')
    args = ap.parse_args()

    # If a cross-chunk patched stitched file exists, prefer it over the original stitched input.
    tb_path = args.townblocks
    try:
        patched = tb_path.replace('__STITCHED_v1.ndjson', '__STITCHED_v1__CROSSCHUNK_PATCHED_v1.ndjson')
        if os.path.exists(patched):
            print(f"[info] Using patched townblocks file: {patched}")
            tb_path = patched
    except Exception:
        tb_path = args.townblocks

    townblocks = list(read_ndjson(tb_path))
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

        # 1) inst match (best)
        if inst and inst in by_page_inst.get(p, {}):
            rc = by_page_inst[p][inst]
            matched_by = 'inst'
            counts['matched_by_inst'] += 1

        # 2) time+book match (second best)
        if rc is None:
            t = tb_time(ev)
            b = tb_book(ev)
            if t and b and (t, b) in by_page_timebook.get(p, {}):
                rc = by_page_timebook[p][(t, b)]
                matched_by = 'time_book'
                counts['matched_by_time_book'] += 1

        # 3/4) record_index fallbacks
        if rc is None:
            mx = max_idx_by_page.get(p, 0)
            flip = (mx + 1 - ridx) if mx else None

            # Key change vs v1_2: when we must fall back to record_index,
            # default to flipped mapping (townblocks top-down vs rowctx bottom-up)
            # unless user turns it off.

            cand = []

            # prefer flipped mapping first (tb top-down vs rowctx bottom-up)
            if flip is not None:
                cand.append(('record_index_flipped', flip))

            # then try direct record_index
            if ridx is not None:
                cand.append(('record_index', ridx))

            page_map = by_page_recidx.get(p, {})

            for mode, idx in cand:
                base = page_map.get(idx)
                if base is not None:
                    rc = base
                    matched_by = mode
                    if mode == 'record_index':
                        counts['matched_by_record_index'] += 1
                    else:
                        counts['matched_by_record_index_flipped'] += 1
                    break


        if rc is None:
            counts['unmatched'] += 1
            if len(unmatched_samples) < 20:
                unmatched_samples.append({'page_index': p, 'record_index': ridx, 'inst': inst})
            out_rows.append(ev)
            continue

        attach_rowctx(ev, rc, prefer_overwrite=args.prefer_overwrite, matched_by=matched_by)
        out_rows.append(ev)

    os.makedirs(os.path.dirname(args.qa), exist_ok=True)
    with open(args.qa, 'w', encoding='utf-8') as f:
        json.dump({
            'engine': 'join_rowctx_txcol_with_townblocks_v1_3',
            'inputs': {'townblocks': args.townblocks, 'rowctx': args.rowctx},
            'counts': counts,
            'unmatched_samples': unmatched_samples,
            'note': 'v1_3 keeps v1_2 overwrite/stray-int logic and changes record_index fallback to prefer flipped index by default (to reconcile tb top-down vs rowctx bottom-up).'
        }, f, indent=2)

    write_ndjson(args.out, out_rows)
    print(f"[done] events_out={len(out_rows)} out={args.out} qa={args.qa}")

if __name__ == '__main__':
    main()
