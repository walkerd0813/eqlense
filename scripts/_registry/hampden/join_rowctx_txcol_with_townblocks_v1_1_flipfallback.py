import json, argparse, os
from collections import defaultdict

def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s: 
                continue
            yield json.loads(s)

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def tb_meta(ev):
    m = ev.get("meta") or {}
    return (m.get("page_index"), m.get("record_index"))

def tb_inst(ev):
    rec = ev.get("recording") or {}
    return rec.get("inst_raw") or ev.get("inst_raw")

def tb_book(ev):
    rec = ev.get("recording") or {}
    return rec.get("book_page_raw") or ev.get("book_page_raw")

def tb_time(ev):
    rec = ev.get("recording") or {}
    return rec.get("recorded_at_raw") or ev.get("recorded_at_raw")

def rc_key(rc):
    return (rc.get("page_index"), rc.get("record_index"))

def build_indexes(rowctx_rows):
    by_page_inst = defaultdict(dict)
    by_page_timebook = defaultdict(dict)
    by_page_recidx = defaultdict(dict)
    max_idx_by_page = defaultdict(int)

    for rc in rowctx_rows:
        p = rc.get("page_index")
        i = rc.get("record_index")
        if p is None or i is None:
            continue
        if i > max_idx_by_page[p]:
            max_idx_by_page[p] = i

        by_page_recidx[p][i] = rc

        inst = rc.get("inst_raw")
        if inst:
            by_page_inst[p][inst] = rc

        t = rc.get("recorded_at_raw")
        b = rc.get("book_page_raw")
        if t and b:
            by_page_timebook[p][(t,b)] = rc

    return by_page_inst, by_page_timebook, by_page_recidx, max_idx_by_page

def attach_rowctx(ev, rc):
    # attach into event.recording fields (canonical place)
    rec = ev.get("recording")
    if not isinstance(rec, dict):
        rec = {}
        ev["recording"] = rec

    # only fill if empty
    for k_src, k_dst in [
        ("recorded_at_raw", "recorded_at_raw"),
        ("book_page_raw", "book_page_raw"),
        ("inst_raw", "inst_raw"),
        ("grp_seq_raw", "grp_seq_raw"),
        ("ref_book_page_raw", "ref_book_page_raw"),
    ]:
        if rec.get(k_dst) in (None, "", " "):
            v = rc.get(k_src)
            if v not in (None, "", " "):
                rec[k_dst] = v

    # consideration goes into event.consideration.amount_raw if empty
    cons = ev.get("consideration")
    if not isinstance(cons, dict):
        cons = {}
        ev["consideration"] = cons
    if cons.get("amount_raw") in (None, "", " "):
        v = rc.get("consideration_raw")
        if v not in (None, "", " "):
            cons["amount_raw"] = v

    # also keep rowctx object (debuggable)
    ev["rowctx"] = {
        "source": "ROWCTX_JOIN_V1_1_FLIPFALLBACK",
        "page_index": rc.get("page_index"),
        "record_index": rc.get("record_index"),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--townblocks", required=True)
    ap.add_argument("--rowctx", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--qa", required=True)
    ap.add_argument("--prefer_overwrite", action="store_true")
    args = ap.parse_args()

    townblocks = list(read_ndjson(args.townblocks))
    rowctx = list(read_ndjson(args.rowctx))

    by_page_inst, by_page_timebook, by_page_recidx, max_idx_by_page = build_indexes(rowctx)

    counts = {
        "townblocks_seen": len(townblocks),
        "rowctx_seen": len(rowctx),
        "matched_by_inst": 0,
        "matched_by_time_book": 0,
        "matched_by_record_index": 0,
        "matched_by_record_index_flipped": 0,
        "unmatched": 0,
    }

    out_rows = []
    unmatched_samples = []

    for ev in townblocks:
        p, ridx = tb_meta(ev)
        if p is None or ridx is None:
            counts["unmatched"] += 1
            if len(unmatched_samples) < 20:
                unmatched_samples.append({"why":"missing_meta", "meta": ev.get("meta")})
            out_rows.append(ev)
            continue

        rc = None

        # 1) inst match
        inst = tb_inst(ev)
        if inst and inst in by_page_inst.get(p, {}):
            rc = by_page_inst[p][inst]
            counts["matched_by_inst"] += 1

        # 2) time+book match
        if rc is None:
            t = tb_time(ev)
            b = tb_book(ev)
            if t and b and (t,b) in by_page_timebook.get(p, {}):
                rc = by_page_timebook[p][(t,b)]
                counts["matched_by_time_book"] += 1

        # 3) direct record_index
        if rc is None:
            rc = by_page_recidx.get(p, {}).get(ridx)
            if rc is not None:
                counts["matched_by_record_index"] += 1

        # 4) flipped record_index fallback (fixes top-down vs bottom-up mismatch)
        if rc is None:
            mx = max_idx_by_page.get(p, 0)
            if mx:
                flip = (mx + 1 - ridx)
                rc = by_page_recidx.get(p, {}).get(flip)
                if rc is not None:
                    counts["matched_by_record_index_flipped"] += 1

        if rc is None:
            counts["unmatched"] += 1
            if len(unmatched_samples) < 20:
                unmatched_samples.append({"page_index":p, "record_index":ridx, "inst":inst})
            out_rows.append(ev)
            continue

        # attach
        attach_rowctx(ev, rc)
        out_rows.append(ev)

    os.makedirs(os.path.dirname(args.qa), exist_ok=True)
    with open(args.qa, "w", encoding="utf-8") as f:
        json.dump({
            "engine":"join_rowctx_txcol_with_townblocks_v1_1_flipfallback",
            "inputs":{"townblocks":args.townblocks, "rowctx":args.rowctx},
            "counts":counts,
            "unmatched_samples":unmatched_samples,
            "note":"Fallback flips record_index direction per page to reconcile townblocks(top-down) vs rowctx(bottom-up). Inst/time-book are preferred first."
        }, f, indent=2)

    write_ndjson(args.out, out_rows)
    print(f"[done] events_out={len(out_rows)} out={args.out} qa={args.qa}")

if __name__ == "__main__":
    main()