import argparse, json, os, re
from collections import Counter, defaultdict

VERIFY_TOKEN_RE = re.compile(r"\\s+Y\\s*$")
ADDR_SUFFIX_RE = re.compile(r"\\|ADDR\\|\\d+$")

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line), None
            except Exception as e:
                yield None, str(e)

def get_in(d, path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    totals = 0
    bad_json = 0

    missing_addr = 0
    addr_trailing_y = 0
    addr_has_verify_token_anywhere = 0

    missing_book_or_page = 0
    by_office_doctype = Counter()
    by_office_missing_bp = Counter()

    addr_suffix_count = 0
    inst_groups = defaultdict(set)  # (office, inst, dt) -> set(event_id)

    sample_trailing_y = []
    sample_missing_bp = []

    for evt, err in iter_ndjson(args.events):
        totals += 1
        if evt is None:
            bad_json += 1
            continue

        office = evt.get("registry_office") or "UNKNOWN_OFFICE"
        dt = evt.get("doc_type_code") or evt.get("event_type") or "UNKNOWN_DT"
        inst = get_in(evt, ["recording","instrument_number_raw"], None) or ""
        book = get_in(evt, ["recording","book"], None)
        page = get_in(evt, ["recording","page"], None)
        addr = get_in(evt, ["property_ref","address_raw"], None)
        eid = evt.get("event_id","")

        by_office_doctype[(office, dt, dt)] += 1

        # address presence + trailing Y detection
        if addr is None or str(addr).strip() == "":
            missing_addr += 1
        else:
            s = str(addr)
            if VERIFY_TOKEN_RE.search(s):
                addr_trailing_y += 1
                if len(sample_trailing_y) < 15:
                    sample_trailing_y.append({"event_id": eid, "address_raw": s})
            if " Y" in s or s.endswith("Y"):
                addr_has_verify_token_anywhere += 1

        # missing book/page
        if (book is None or str(book).strip()=="") or (page is None or str(page).strip()==""):
            missing_book_or_page += 1
            by_office_missing_bp[(office, dt, dt)] += 1
            if len(sample_missing_bp) < 20:
                sample_missing_bp.append({
                    "event_id": eid,
                    "registry_office": office,
                    "doc_type_code": dt,
                    "instrument_number_raw": inst,
                    "book": book,
                    "page": page,
                    "address_raw": addr
                })

        # multi-addr suffix sanity
        if ADDR_SUFFIX_RE.search(eid):
            addr_suffix_count += 1

        if inst:
            inst_groups[(office, inst, dt)].add(eid)

    # how many inst groups have >1 events? (multi-property behavior)
    multi_inst = 0
    max_inst_size = 0
    top_multi = []
    for k, s in inst_groups.items():
        n = len(s)
        if n > 1:
            multi_inst += 1
            if n > max_inst_size:
                max_inst_size = n
            if len(top_multi) < 25:
                top_multi.append({"office": k[0], "instrument": k[1], "doc_type": k[2], "count_events": n})

    report = {
        "events_path": os.path.abspath(args.events),
        "rows_total": totals,
        "bad_json_rows": bad_json,
        "missing_address_raw": missing_addr,
        "address_trailing_verify_Y": addr_trailing_y,
        "address_contains_Y_token_anywhere_loose": addr_has_verify_token_anywhere,
        "missing_book_or_page": missing_book_or_page,
        "missing_book_or_page_top_buckets": [
            {"registry_office": k[0], "doc_type_code": k[1], "bucket": k, "count": c}
            for k, c in by_office_missing_bp.most_common(15)
        ],
        "doc_type_volume_top_buckets": [
            {"registry_office": k[0], "doc_type_code": k[1], "bucket": k, "count": c}
            for k, c in by_office_doctype.most_common(15)
        ],
        "event_id_addr_suffix_count": addr_suffix_count,
        "multi_instrument_groups_count": multi_inst,
        "multi_instrument_max_events_in_group": max_inst_size,
        "multi_instrument_samples": top_multi,
        "samples": {
            "trailing_y": sample_trailing_y,
            "missing_book_or_page": sample_missing_bp
        }
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("[ok] QA report ->", args.out)
    print("rows_total", totals)
    print("missing_book_or_page", missing_book_or_page)
    print("address_trailing_verify_Y", addr_trailing_y)
    print("event_id_addr_suffix_count", addr_suffix_count)
    print("multi_instrument_groups_count", multi_inst)

if __name__ == "__main__":
    main()
