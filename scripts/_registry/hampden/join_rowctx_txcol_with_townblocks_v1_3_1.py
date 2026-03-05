import json, argparse, os, re
from collections import defaultdict, deque

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
    # allow multiple RowCtx per key (e.g., duplicate record_index or inst)
    by_page_inst = defaultdict(lambda: defaultdict(list))
    by_page_timebook = defaultdict(lambda: defaultdict(list))
    by_page_recidx = defaultdict(lambda: defaultdict(list))
    max_idx_by_page = defaultdict(int)

    # collect per-page lists first
    page_to_rcs = defaultdict(list)
    for rc in rowctx_rows:
        p = rc.get('page_index')
        i = rc.get('record_index')
        if p is None:
            continue
        page_to_rcs[p].append(rc)

    # normalize ordering: map rowctx on each page to a 1-based normalized record index
    for p, rcs in page_to_rcs.items():
        # sort by numeric record_index when available to get top->bottom order
        try:
            sorted_rcs = sorted(rcs, key=lambda r: (int(r.get('record_index')) if r.get('record_index') is not None else 0))
        except Exception:
            sorted_rcs = list(rcs)

        for norm_idx, rc in enumerate(sorted_rcs, start=1):
            # annotate normalized index for downstream matching
            rc['_norm_record_index'] = norm_idx
            by_page_recidx[p][norm_idx].append(rc)
            # update max
            if norm_idx > max_idx_by_page[p]:
                max_idx_by_page[p] = norm_idx

            inst = rc.get('inst_raw')
            if inst:
                by_page_inst[p][inst].append(rc)

            t = rc.get('recorded_at_raw')
            b = rc.get('book_page_raw')
            if t and b:
                by_page_timebook[p][(t, b)].append(rc)

    return by_page_inst, by_page_timebook, by_page_recidx, max_idx_by_page

def detect_page_flip(tb_by_page, by_page_recidx, by_page_inst):
    """
    Heuristic: for each page, compare the ordering of instrument numbers between
    townblocks and rowctx. If the majority of matched instrument pairs are in
    reversed order, mark the page as flipped.
    Returns: dict page -> bool (True if flipped)
    """
    flip_by_page = {}
    for p, tbs in tb_by_page.items():
        # tb inst order
        tb_inst_order = []
        for tb in sorted(tbs, key=lambda t: int((t.get('meta') or {}).get('record_index') or 0)):
            inst = tb_inst(tb)
            if inst:
                tb_inst_order.append(str(inst))

        # rc inst order by normalized index
        rc_inst_order = []
        page_map = by_page_recidx.get(p, {})
        if not page_map:
            flip_by_page[p] = False
            continue
        for idx in sorted(page_map.keys()):
            for rc in page_map.get(idx, []):
                ri = rc.get('inst_raw') or rc.get('inst')
                if ri:
                    rc_inst_order.append(str(ri))

        # build list of matched insts present in both sequences
        matched = [i for i in tb_inst_order if i in set(rc_inst_order)]
        if len(matched) < 3:
            flip_by_page[p] = False
            continue

        # positions of matched insts in rc order according to tb order
        pos = [rc_inst_order.index(i) for i in matched]

        # inversion count
        inv = 0
        n = len(pos)
        for i in range(n):
            for j in range(i+1, n):
                if pos[i] > pos[j]:
                    inv += 1
        total_pairs = n * (n - 1) / 2
        # if more than half pairs are inverted, treat as reversed
        flip_by_page[p] = (inv > (total_pairs / 2))

    return flip_by_page

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
        'record_index_norm': rc.get('_norm_record_index'),
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

    # precompute expected x_center per normalized record index on each page
    page_expected_x = {}
    for p, page_map in by_page_recidx.items():
        page_expected_x[p] = {}
        for idx, rcs in page_map.items():
            xs = []
            for rc in rcs:
                try:
                    qa = rc.get('qa') if isinstance(rc, dict) else None
                    xc = None
                    if isinstance(qa, dict):
                        xc = qa.get('x_center')
                    if xc is None:
                        xc = rc.get('x_center')
                    if xc is not None:
                        xs.append(float(xc))
                except Exception:
                    continue
            if xs:
                xs_sorted = sorted(xs)
                mid = len(xs_sorted) // 2
                # median
                if len(xs_sorted) % 2 == 1:
                    med = xs_sorted[mid]
                else:
                    med = (xs_sorted[mid-1] + xs_sorted[mid]) / 2.0
                page_expected_x[p][idx] = med

    # Normalize townblocks per-page to produce normalized record indices
    tb_by_page = defaultdict(list)
    for tb in townblocks:
        p = (tb.get('meta') or {}).get('page_index')
        ridx = (tb.get('meta') or {}).get('record_index')
        if p is None or ridx is None:
            continue
        tb_by_page[p].append(tb)

    tb_ridx_to_norms = defaultdict(lambda: defaultdict(deque))
    for p, tbs in tb_by_page.items():
        try:
            sorted_tbs = sorted(tbs, key=lambda t: int((t.get('meta') or {}).get('record_index') or 0))
        except Exception:
            sorted_tbs = list(tbs)

        for norm_idx, tb in enumerate(sorted_tbs, start=1):
            orig = (tb.get('meta') or {}).get('record_index')
            tb_ridx_to_norms[p][orig].append(norm_idx)

    # Detect per-page index ordering inversion (rowctx vs townblocks)
    # Disabled: enforce single top->bottom ordering only.
    flip_by_page = {}  # disabled: enforce single top->bottom ordering

    counts = {
        'townblocks_seen': len(townblocks),
        'rowctx_seen': len(rowctx),
        'matched_by_inst': 0,
        'matched_by_time_book': 0,
        'matched_by_record_index': 0,
        'matched_by_record_index_flipped': 0,
        'deduped_inst': 0,
        'unmatched': 0,
    }

    out_rows = []
    unmatched_samples = []
    deduped_samples = []
    seen_insts = set()
    used_rcs = set()

    def _pick_unused(candidates, preferred_x=None):
        """Pick an unused candidate. If `preferred_x` is provided and
        candidates contain numeric `x_center`, choose the candidate whose
        `x_center` is closest to `preferred_x`.
        """
        if not candidates:
            return None
        # filter unused
        unused = [c for c in candidates if id(c) not in used_rcs]
        if not unused:
            return None
        if preferred_x is None:
            c = unused[0]
            used_rcs.add(id(c))
            return c

        # choose candidate with minimal abs(x_center - preferred_x)
        best = None
        best_d = None
        for c in unused:
            try:
                xc = None
                qa = c.get('qa') if isinstance(c, dict) else None
                if isinstance(qa, dict):
                    xc = qa.get('x_center')
                if xc is None:
                    xc = c.get('x_center')
                if xc is None:
                    d = float('inf')
                else:
                    d = abs(float(xc) - float(preferred_x))
            except Exception:
                d = float('inf')
            if best is None or d < best_d:
                best = c
                best_d = d
        if best is None:
            best = unused[0]
        used_rcs.add(id(best))
        return best

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

        # 1) inst match (best) - choose first unused RowCtx for this inst
        if inst and inst in by_page_inst.get(p, {}):
            rc = _pick_unused(by_page_inst[p][inst])
            if rc is not None:
                matched_by = 'inst'
                counts['matched_by_inst'] += 1

        # 2) time+book match (second best)
        if rc is None:
            t = tb_time(ev)
            b = tb_book(ev)
            if t and b and (t, b) in by_page_timebook.get(p, {}):
                rc = _pick_unused(by_page_timebook[p][(t, b)])
                if rc is not None:
                    matched_by = 'time_book'
                    counts['matched_by_time_book'] += 1

        # 3/4) record_index fallbacks (use normalized tb index when possible)
        if rc is None:
            # map townblock original record_index to normalized index (if available)
            tb_norm = None
            try:
                if p in tb_ridx_to_norms and ridx in tb_ridx_to_norms[p] and len(tb_ridx_to_norms[p][ridx]) > 0:
                    tb_norm = tb_ridx_to_norms[p][ridx].popleft()
            except Exception:
                tb_norm = None

            # fall back to the original if normalization unavailable
            use_idx = tb_norm if tb_norm is not None else ridx

            # Enforce a single ordering: top->bottom direct normalized index only.
            cand = []
            if use_idx is not None:
                cand.append(('record_index', use_idx))

            page_map = by_page_recidx.get(p, {})
            for mode, idx in cand:
                bases = page_map.get(idx)
                if bases:
                    preferred_x = page_expected_x.get(p, {}).get(idx)
                    rc = _pick_unused(bases, preferred_x=preferred_x)
                    if rc is not None:
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

        # deduplicate by instrument id when available: prefer first-seen
        rc_inst = None
        try:
            rc_inst = (rc.get('inst_raw') or rc.get('inst'))
            if rc_inst is not None:
                rc_inst = str(rc_inst).strip()
        except Exception:
            rc_inst = None

        if rc_inst and rc_inst in seen_insts:
            counts['deduped_inst'] += 1
            if len(deduped_samples) < 50:
                deduped_samples.append({'inst': rc_inst, 'page_index': p, 'record_index': ridx, 'matched_by': matched_by})
            # skip attaching duplicate rowctx to avoid appending duplicate joined events
            out_rows.append(ev)
            continue

        # attach and mark instrument as seen
        attach_rowctx(ev, rc, prefer_overwrite=args.prefer_overwrite, matched_by=matched_by)
        if rc_inst:
            seen_insts.add(rc_inst)
        out_rows.append(ev)

    os.makedirs(os.path.dirname(args.qa), exist_ok=True)
    with open(args.qa, 'w', encoding='utf-8') as f:
        json.dump({
            'engine': 'join_rowctx_txcol_with_townblocks_v1_3',
            'inputs': {'townblocks': args.townblocks, 'rowctx': args.rowctx},
            'counts': counts,
            'unmatched_samples': unmatched_samples,
            'note': 'v1_3 normalizes per-page RowCtx and TownBlock record ordering (top->bottom) and uses normalized indices for deterministic matching; keeps overwrite/stray-int logic and prefers flipped mapping when falling back.'
        }, f, indent=2)

    write_ndjson(args.out, out_rows)
    print(f"[done] events_out={len(out_rows)} out={args.out} qa={args.qa}")

if __name__ == '__main__':
    main()
