import json, csv, os, re, sys
from collections import defaultdict, Counter

def get(d, path, default=None):
    cur = d
    for key in path:
        if cur is None:
            return default
        if isinstance(key, int):
            if isinstance(cur, list) and 0 <= key < len(cur):
                cur = cur[key]
            else:
                return default
        else:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                return default
    return cur if cur is not None else default

def norm_str(s):
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def looks_like_town(s):
    s = norm_str(s).upper()
    if not s:
        return False
    # Town usually letters/spaces; digits strongly suggest not-a-town
    return not re.search(r"\d", s)

def has_digit(s):
    return bool(re.search(r"\d", norm_str(s)))

def take_first_property_ref(obj):
    # Try common locations:
    # 1) top-level property_refs
    # 2) event.property_refs
    refs = get(obj, ["property_refs"]) or get(obj, ["event", "property_refs"]) or []
    if isinstance(refs, list) and refs:
        r0 = refs[0] if isinstance(refs[0], dict) else {}
        return (norm_str(r0.get("town")), norm_str(r0.get("address_raw") or r0.get("address")))
    return ("", "")

def extract_core(obj):
    # These paths are intentionally defensive; your schema may vary.
    page_index   = get(obj, ["page_index"])
    record_index = get(obj, ["record_index"])

    inst = (
        get(obj, ["inst_raw"]) or
        get(obj, ["recording", "inst_raw"]) or
        get(obj, ["recording", "instrument_raw"]) or
        get(obj, ["rowctx", "inst_raw"]) or
        ""
    )
    book_page = (
        get(obj, ["book_page_raw"]) or
        get(obj, ["recording", "book_page_raw"]) or
        get(obj, ["rowctx", "book_page_raw"]) or
        ""
    )
    ref_bp = (
        get(obj, ["ref_book_page_raw"]) or
        get(obj, ["recording", "ref_book_page_raw"]) or
        get(obj, ["rowctx", "ref_book_page_raw"]) or
        ""
    )
    recorded_at = (
        get(obj, ["recorded_at_raw"]) or
        get(obj, ["recording", "recorded_at_raw"]) or
        get(obj, ["rowctx", "recorded_at_raw"]) or
        ""
    )

    # consideration
    cons = (
        get(obj, ["consideration", "amount_raw"]) or
        get(obj, ["consideration_raw"]) or
        get(obj, ["amount_raw"]) or
        ""
    )

    # descr
    descr = (
        get(obj, ["descr_loc_raw"]) or
        get(obj, ["descr_raw"]) or
        get(obj, ["document", "descr_loc_raw"]) or
        ""
    )

    town, addr = take_first_property_ref(obj)

    # parties count
    parties = (
        get(obj, ["parties", "parties_raw"]) or
        get(obj, ["event", "parties", "parties_raw"]) or
        []
    )
    parties_n = len(parties) if isinstance(parties, list) else 0

    return {
        "page_index": page_index,
        "record_index": record_index,
        "inst_raw": norm_str(inst),
        "book_page_raw": norm_str(book_page),
        "ref_book_page_raw": norm_str(ref_bp),
        "recorded_at_raw": norm_str(recorded_at),
        "town_primary": norm_str(town).upper(),
        "address_primary": norm_str(addr).upper(),
        "consideration_raw": norm_str(cons),
        "descr_raw": norm_str(descr),
        "parties_n": parties_n,
    }

def main():
    if len(sys.argv) < 3:
        print("Usage: python inspect_joined_ndjson_v1.py <joined.ndjson> <out_dir>")
        sys.exit(2)

    in_path = sys.argv[1]
    out_dir = sys.argv[2]
    os.makedirs(out_dir, exist_ok=True)

    rows = []
    bad_json = 0

    with open(in_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                bad_json += 1
                continue
            rows.append(extract_core(obj))

    print(f"[ok] loaded rows: {len(rows)}  bad_json_lines: {bad_json}")

    # Write full table
    full_csv = os.path.join(out_dir, "joined_table.csv")
    with open(full_csv, "w", newline="", encoding="utf-8") as fo:
        w = csv.DictWriter(fo, fieldnames=list(rows[0].keys()) if rows else [])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print("[ok] wrote", full_csv)

    # --- Diagnostics ---
    # 1) inst duplicates with conflicting town/address/consideration/book_page
    by_inst = defaultdict(list)
    for r in rows:
        inst = r["inst_raw"]
        if inst:
            by_inst[inst].append(r)

    conflicts = []
    for inst, items in by_inst.items():
        towns = {x["town_primary"] for x in items if x["town_primary"]}
        addrs = {x["address_primary"] for x in items if x["address_primary"]}
        cons  = {x["consideration_raw"] for x in items if x["consideration_raw"]}
        bps   = {x["book_page_raw"] for x in items if x["book_page_raw"]}

        # conflict if multiple distinct values show up
        if len(towns) > 1 or len(addrs) > 1 or len(cons) > 1 or len(bps) > 1:
            conflicts.append((inst, len(items), len(towns), len(addrs), len(cons), len(bps)))

    conflicts.sort(key=lambda t: (t[1], t[2]+t[3]+t[4]+t[5]), reverse=True)

    conflicts_csv = os.path.join(out_dir, "inst_conflicts.csv")
    with open(conflicts_csv, "w", newline="", encoding="utf-8") as fo:
        w = csv.writer(fo)
        w.writerow(["inst_raw","n_rows","towns","addrs","considerations","book_pages"])
        for inst, n, nt, na, nc, nb in conflicts:
            w.writerow([inst, n, nt, na, nc, nb])
    print("[ok] wrote", conflicts_csv)

    # 2) swapped-looking town/address
    swapped = []
    for r in rows:
        t = r["town_primary"]
        a = r["address_primary"]
        if t and a:
            # bad if town has digits OR address has no digit
            if has_digit(t) or (not has_digit(a)):
                swapped.append(r)

    swapped_csv = os.path.join(out_dir, "swap_suspects.csv")
    with open(swapped_csv, "w", newline="", encoding="utf-8") as fo:
        w = csv.DictWriter(fo, fieldnames=list(rows[0].keys()) if rows else [])
        w.writeheader()
        for r in swapped:
            w.writerow(r)
    print("[ok] wrote", swapped_csv)

    # 3) record_index direction anomalies (per page)
    # If record_index is descending on many pages, your join might be flipped.
    per_page = defaultdict(list)
    for r in rows:
        if r["page_index"] is None or r["record_index"] is None:
            continue
        per_page[r["page_index"]].append(r["record_index"])

    direction = []
    for p, ris in per_page.items():
        if len(ris) < 2:
            continue
        # count how often it increases vs decreases in the observed order in file
        inc = sum(1 for i in range(1, len(ris)) if ris[i] >= ris[i-1])
        dec = sum(1 for i in range(1, len(ris)) if ris[i] <  ris[i-1])
        direction.append((p, len(ris), inc, dec))
    direction.sort(key=lambda t: t[3], reverse=True)

    direction_csv = os.path.join(out_dir, "record_index_direction_by_page.csv")
    with open(direction_csv, "w", newline="", encoding="utf-8") as fo:
        w = csv.writer(fo)
        w.writerow(["page_index","n","inc_steps","dec_steps"])
        for p,n,inc,dec in direction:
            w.writerow([p,n,inc,dec])
    print("[ok] wrote", direction_csv)

    # 4) Quick focus list (your named suspects)
    focus_insts = {"19407","19437"}
    focus_rows = [r for r in rows if r["inst_raw"] in focus_insts]
    focus_csv = os.path.join(out_dir, "focus_19407_19437.csv")
    with open(focus_csv, "w", newline="", encoding="utf-8") as fo:
        if rows:
            w = csv.DictWriter(fo, fieldnames=list(rows[0].keys()))
            w.writeheader()
            for r in focus_rows:
                w.writerow(r)
    print("[ok] wrote", focus_csv)

    print("\n[finish] Done. Open the CSVs in Excel and we’ll know exactly where the join is going wrong.")

if __name__ == "__main__":
    main()