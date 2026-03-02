import argparse, os, re, json, glob
from collections import defaultdict, Counter

RE_TRAILING_Y = re.compile(r"(?:\s+Y|\s+Y\.)\s*$", re.IGNORECASE)

def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
            except Exception:
                # keep going; count in caller if desired
                continue

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def pick_preferred_events_file(chunk_dir):
    # Prefer crosschunk patched if present, else stitched
    patched = glob.glob(os.path.join(chunk_dir, "*CROSSCHUNK_PATCHED_v1.ndjson"))
    if patched:
        return patched[0], "crosschunk_patched"
    stitched = glob.glob(os.path.join(chunk_dir, "*STITCHED_v1.ndjson"))
    if stitched:
        return stitched[0], "stitched"
    base = glob.glob(os.path.join(chunk_dir, "events__*__v1_*.ndjson"))
    if base:
        return base[0], "base"
    return None, None

def find_join_file(chunk_dir):
    j = glob.glob(os.path.join(chunk_dir, "join__DEED__*__v1_3_1.ndjson"))
    return j[0] if j else None

def find_join_qa_file(chunk_dir):
    j = glob.glob(os.path.join(chunk_dir, "join__DEED__*__QA.json"))
    return j[0] if j else None

def find_stitch_qa_files(chunk_dir):
    out = {}
    v1 = glob.glob(os.path.join(chunk_dir, "qa__TB_STITCH__*__v1.json"))
    if v1: out["stitch_v1"] = v1[0]
    cx = glob.glob(os.path.join(chunk_dir, "qa__TB_STITCH__*__CROSSCHUNK_PATCH_v1.json"))
    if cx: out["crosschunk_patch"] = cx[0]
    return out

def get_first(dct, keys, default=None):
    for k in keys:
        if isinstance(dct, dict) and k in dct and dct[k] not in (None, "", [], {}):
            return dct[k]
    return default

def normalize_party_name(s):
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    return s

def extract_parties(event):
    # Try a few schema variants
    parties = []
    # common: parties: [{name, role}]
    if isinstance(event.get("parties"), list):
        for p in event["parties"]:
            if isinstance(p, dict):
                nm = normalize_party_name(p.get("name") or p.get("party_name") or p.get("raw") or p.get("text"))
                if nm: parties.append(nm)
            elif isinstance(p, str):
                nm = normalize_party_name(p)
                if nm: parties.append(nm)
    # sometimes grantor/grantee arrays
    for k in ("grantors", "grantees", "grantor", "grantee"):
        v = event.get(k)
        if isinstance(v, list):
            for p in v:
                if isinstance(p, dict):
                    nm = normalize_party_name(p.get("name") or p.get("raw") or p.get("text"))
                    if nm: parties.append(nm)
                elif isinstance(p, str):
                    nm = normalize_party_name(p)
                    if nm: parties.append(nm)
        elif isinstance(v, str):
            nm = normalize_party_name(v)
            if nm: parties.append(nm)
    return parties

def extract_address(event):
    # property_refs[0] tends to hold address_raw / address
    pr0 = None
    if isinstance(event.get("property_refs"), list) and event["property_refs"]:
        pr0 = event["property_refs"][0]
    addr = None
    addr_raw = None
    if isinstance(pr0, dict):
        addr = pr0.get("address") or pr0.get("addr") or pr0.get("address_norm")
        addr_raw = pr0.get("address_raw") or pr0.get("addr_raw") or pr0.get("raw_address")
    # sometimes top-level
    addr = addr or event.get("address") or event.get("address_norm")
    addr_raw = addr_raw or event.get("address_raw")
    return addr, addr_raw

def has_digit(s):
    return isinstance(s, str) and any(ch.isdigit() for ch in s)

def inspect_events(events_path):
    c = Counter()
    examples = defaultdict(list)

    for ev in read_ndjson(events_path):
        c["total_events"] += 1

        addr, addr_raw = extract_address(ev)
        addr_any = addr_raw or addr

        if not addr_any:
            c["missing_address"] += 1
        else:
            if RE_TRAILING_Y.search(addr_any):
                c["address_trailing_Y"] += 1
                if len(examples["address_trailing_Y"]) < 5:
                    examples["address_trailing_Y"].append(addr_any)
            # “bad address” heuristic: no digit AND looks non-empty
            if isinstance(addr_any, str) and addr_any.strip():
                if not has_digit(addr_any):
                    c["address_no_digit"] += 1
                    if len(examples["address_no_digit"]) < 5:
                        examples["address_no_digit"].append(addr_any)

        descr = get_first(ev, ["descr_loc", "descr", "description", "location"])
        if not descr:
            c["missing_descr"] += 1

        town = get_first(ev, ["town", "town_name", "municipality"])
        if not town:
            c["missing_town"] += 1

        parties = extract_parties(ev)
        if not parties:
            c["missing_parties"] += 1
        else:
            for nm in parties:
                if RE_TRAILING_Y.search(nm):
                    c["party_trailing_Y"] += 1
                    if len(examples["party_trailing_Y"]) < 5:
                        examples["party_trailing_Y"].append(nm)
                if isinstance(nm, str) and re.search(r"[|•■�]", nm):
                    c["party_weird_chars"] += 1
                    if len(examples["party_weird_chars"]) < 5:
                        examples["party_weird_chars"].append(nm)

    return c, examples

def extract_inst_bookpage(join_rec):
    inst = get_first(join_rec, ["inst_raw", "instrument_raw", "instrument", "inst", "instrument_number", "inst_num"])
    bookpage = get_first(join_rec, ["book_page_raw", "book_page", "bookpage", "book_page_ref", "book", "page"])
    recorded_at = get_first(join_rec, ["recorded_at", "recorded_at_raw", "recorded_datetime", "recorded_date"])
    return inst, bookpage, recorded_at

def inspect_join(join_path):
    c = Counter()
    examples = defaultdict(list)
    for rec in read_ndjson(join_path):
        c["total_join_records"] += 1
        inst, bookpage, recorded_at = extract_inst_bookpage(rec)
        if not inst:
            c["missing_inst"] += 1
        if not bookpage:
            c["missing_book_page"] += 1
        if not recorded_at:
            c["missing_recorded_at"] += 1

        # address + parties checks also on join, since this is “final”
        addr, addr_raw = extract_address(rec)
        addr_any = addr_raw or addr
        if not addr_any:
            c["join_missing_address"] += 1
        else:
            if RE_TRAILING_Y.search(addr_any):
                c["join_address_trailing_Y"] += 1
                if len(examples["join_address_trailing_Y"]) < 5:
                    examples["join_address_trailing_Y"].append(addr_any)

        parties = extract_parties(rec)
        if not parties:
            c["join_missing_parties"] += 1
        else:
            for nm in parties:
                if RE_TRAILING_Y.search(nm):
                    c["join_party_trailing_Y"] += 1
                    if len(examples["join_party_trailing_Y"]) < 5:
                        examples["join_party_trailing_Y"].append(nm)

    return c, examples

def summarize_join_qa(path):
    try:
        qa = read_json(path)
    except Exception:
        return {"qa_parse_error": True}
    out = {}
    # Try common counters
    for k in [
        "events_in", "rows_in", "events_out",
        "matched", "unmatched_events", "unmatched_rows",
        "glue_mismatch", "missing_rowctx", "missing_townblocks"
    ]:
        if k in qa:
            out[k] = qa[k]
    # If nested:
    if "counts" in qa and isinstance(qa["counts"], dict):
        for k, v in qa["counts"].items():
            out[f"counts.{k}"] = v
    return out

def summarize_stitch_qa(path):
    try:
        qa = read_json(path)
    except Exception:
        return {"qa_parse_error": True}
    out = {}
    for k in [
        "pages_seen", "candidate_pagebreaks", "stitched", "stitched_parties_only",
        "same_page_repair", "no_continuation_found", "missing_raw_lines_for_next_page"
    ]:
        if k in qa:
            out[k] = qa[k]
    # sometimes nested under "summary"
    if "summary" in qa and isinstance(qa["summary"], dict):
        for k, v in qa["summary"].items():
            out[f"summary.{k}"] = v
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work_root", required=True)
    args = ap.parse_args()

    work_root = args.work_root
    if not os.path.isdir(work_root):
        raise SystemExit(f"Not a directory: {work_root}")

    chunk_dirs = sorted([
        os.path.join(work_root, d) for d in os.listdir(work_root)
        if os.path.isdir(os.path.join(work_root, d)) and re.match(r"^p\d{5}_p\d{5}$", d)
    ])

    rollup = []
    grand = Counter()

    for cd in chunk_dirs:
        chunk = os.path.basename(cd)
        row = {"chunk": chunk}

        events_path, events_kind = pick_preferred_events_file(cd)
        if events_path:
            ev_c, ev_ex = inspect_events(events_path)
            row.update({f"ev.{k}": v for k, v in ev_c.items()})
            row["events_file"] = os.path.basename(events_path)
            row["events_kind"] = events_kind
            # only keep a few examples per chunk (optional)
            row["ev_examples"] = {k: v for k, v in ev_ex.items() if v}

            grand.update(ev_c)
        else:
            row["events_missing"] = True

        join_path = find_join_file(cd)
        if join_path:
            j_c, j_ex = inspect_join(join_path)
            row.update({f"join.{k}": v for k, v in j_c.items()})
            row["join_file"] = os.path.basename(join_path)
            row["join_examples"] = {k: v for k, v in j_ex.items() if v}
            grand.update(j_c)
        else:
            row["join_missing"] = True

        join_qa = find_join_qa_file(cd)
        if join_qa:
            row["join_qa"] = summarize_join_qa(join_qa)

        stitch_qas = find_stitch_qa_files(cd)
        if stitch_qas.get("stitch_v1"):
            row["stitch_qa"] = summarize_stitch_qa(stitch_qas["stitch_v1"])
        if stitch_qas.get("crosschunk_patch"):
            row["crosschunk_qa"] = summarize_stitch_qa(stitch_qas["crosschunk_patch"])

        rollup.append(row)

    # Print human-readable summary
    print("\n=== CHUNK ROLLUP (key issues) ===")
    for r in rollup:
        print(f"\n[{r['chunk']}]")
        if r.get("events_missing"):
            print("  - events: MISSING")
        else:
            print(f"  - events: {r.get('events_file')} ({r.get('events_kind')}) total={r.get('ev.total_events',0)} "
                  f"missing_addr={r.get('ev.missing_address',0)} addr_Y={r.get('ev.address_trailing_Y',0)} "
                  f"missing_parties={r.get('ev.missing_parties',0)} party_Y={r.get('ev.party_trailing_Y',0)} "
                  f"missing_descr={r.get('ev.missing_descr',0)}")
        if r.get("join_missing"):
            print("  - join: MISSING")
        else:
            print(f"  - join: {r.get('join_file')} total={r.get('join.total_join_records',0)} "
                  f"missing_inst={r.get('join.missing_inst',0)} missing_bookpage={r.get('join.missing_book_page',0)} "
                  f"join_missing_addr={r.get('join.join_missing_address',0)} join_addr_Y={r.get('join.join_address_trailing_Y',0)} "
                  f"join_missing_parties={r.get('join.join_missing_parties',0)} join_party_Y={r.get('join.join_party_trailing_Y',0)}")

        if "stitch_qa" in r:
            sq = r["stitch_qa"]
            m = sq.get("missing_raw_lines_for_next_page") or sq.get("summary.missing_raw_lines_for_next_page")
            if m is not None:
                print(f"  - stitchQA missing_next_page_raw_lines={m} candidates={sq.get('candidate_pagebreaks') or sq.get('summary.candidate_pagebreaks')} stitched={sq.get('stitched') or sq.get('summary.stitched')}")
        if "crosschunk_qa" in r:
            cq = r["crosschunk_qa"]
            print(f"  - crosschunkQA: {cq}")

    print("\n=== GRAND TOTALS ===")
    # Only show key totals
    keys = [
        "total_events", "missing_address", "address_trailing_Y", "address_no_digit",
        "missing_parties", "party_trailing_Y", "party_weird_chars",
        "missing_descr", "missing_town",
        "total_join_records", "missing_inst", "missing_book_page", "missing_recorded_at",
        "join_missing_address", "join_address_trailing_Y", "join_missing_parties", "join_party_trailing_Y"
    ]
    for k in keys:
        if k in grand:
            print(f"{k}: {grand[k]}")

    # Optionally dump full JSON rollup next to work_root
    out_path = os.path.join(work_root, "INSPECTION__ROLLUP.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(rollup, f, indent=2)
    print(f"\n[done] wrote {out_path}")

if __name__ == "__main__":
    main()