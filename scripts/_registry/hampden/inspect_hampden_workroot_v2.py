import argparse, os, re, json, glob
from collections import Counter, defaultdict
from typing import Any, Iterable, Tuple, List

RE_TRAILING_Y = re.compile(r"(?:\s+Y|\s+Y\.)\s*$", re.IGNORECASE)

# --- recursive path utilities -------------------------------------------------
def walk_paths(obj: Any, prefix: str = "") -> Iterable[Tuple[str, Any]]:
    """Yield (path, value) for all leaf-ish values and also container nodes."""
    yield prefix, obj
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            yield from walk_paths(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:50]):  # cap traversal per list
            p = f"{prefix}[{i}]"
            yield from walk_paths(v, p)

def first_value_by_path_regex(obj: Any, patterns: List[re.Pattern]) -> Any:
    for path, val in walk_paths(obj):
        if val in (None, "", [], {}):
            continue
        pl = path.lower()
        if any(p.search(pl) for p in patterns):
            return val
    return None

def find_all_values_by_path_regex(obj: Any, patterns: List[re.Pattern], limit=50) -> List[Any]:
    out = []
    for path, val in walk_paths(obj):
        if val in (None, "", [], {}):
            continue
        pl = path.lower()
        if any(p.search(pl) for p in patterns):
            out.append(val)
            if len(out) >= limit:
                break
    return out

# --- field extractors ---------------------------------------------------------
def extract_address_any(obj: Any) -> str | None:
    # Prefer address_raw-ish, then address-ish
    v = first_value_by_path_regex(obj, [
        re.compile(r"address_raw\b"),
        re.compile(r"addr_raw\b"),
        re.compile(r"\braw_address\b"),
    ])
    if isinstance(v, str) and v.strip():
        return v.strip()
    v = first_value_by_path_regex(obj, [
        re.compile(r"\baddress\b"),
        re.compile(r"\baddr\b"),
    ])
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None

def extract_descr_any(obj: Any) -> str | None:
    v = first_value_by_path_regex(obj, [
        re.compile(r"\bdescr(_loc)?\b"),
        re.compile(r"\blegal\b"),
        re.compile(r"\blocation\b"),
        re.compile(r"\bdescription\b"),
    ])
    return v.strip() if isinstance(v, str) and v.strip() else None

def extract_town_any(obj: Any) -> str | None:
    v = first_value_by_path_regex(obj, [
        re.compile(r"\btown\b"),
        re.compile(r"\bmunicip"),
        re.compile(r"\bcity\b"),
    ])
    return v.strip() if isinstance(v, str) and v.strip() else None

def extract_parties_any(obj: Any) -> List[str]:
    parties: List[str] = []

    # 1) Common: lists under any path containing party/grantor/grantee
    candidates = find_all_values_by_path_regex(obj, [
        re.compile(r"\bpart(y|ies)\b"),
        re.compile(r"\bgrantor"),
        re.compile(r"\bgrantee"),
    ], limit=200)

    def add_party(x):
        if isinstance(x, str):
            s = x.strip()
            if s:
                parties.append(s)
        elif isinstance(x, dict):
            for k in ("name","party_name","raw","text","value"):
                v = x.get(k)
                if isinstance(v, str) and v.strip():
                    parties.append(v.strip())
                    return

    for cand in candidates:
        if isinstance(cand, list):
            for item in cand:
                add_party(item)
        else:
            add_party(cand)

    # de-dupe while preserving order
    seen = set()
    out = []
    for p in parties:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

def extract_inst_bookpage_recorded_any(obj: Any) -> Tuple[str|None, str|None, str|None]:
    inst = first_value_by_path_regex(obj, [
        re.compile(r"\binst(_raw)?\b"),
        re.compile(r"\binstrument(_number|_raw)?\b"),
        re.compile(r"\bdocument(_number)?\b"),
    ])
    bookpage = first_value_by_path_regex(obj, [
        re.compile(r"\bbook[_-]?page(_raw)?\b"),
        re.compile(r"\bbookpage\b"),
        re.compile(r"\bbook\b.*\bpage\b"),
    ])
    recorded = first_value_by_path_regex(obj, [
        re.compile(r"\brecorded(_at|_date|_datetime|_time)?\b"),
        re.compile(r"\brecording(_date|_time)?\b"),
    ])

    def norm(v):
        return v.strip() if isinstance(v, str) and v.strip() else None

    return norm(inst), norm(bookpage), norm(recorded)

def has_digit(s: str) -> bool:
    return any(ch.isdigit() for ch in s)

# --- file readers -------------------------------------------------------------
def read_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def pick_preferred_events_file(chunk_dir):
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

# --- inspections --------------------------------------------------------------
def inspect_records_ndjson(path, mode: str):
    c = Counter()
    examples = defaultdict(list)

    for rec in read_ndjson(path):
        c["total"] += 1

        addr = extract_address_any(rec)
        if not addr:
            c["missing_address"] += 1
        else:
            if RE_TRAILING_Y.search(addr):
                c["address_trailing_Y"] += 1
                if len(examples["address_trailing_Y"]) < 5:
                    examples["address_trailing_Y"].append(addr)
            if addr.strip() and not has_digit(addr):
                c["address_no_digit"] += 1
                if len(examples["address_no_digit"]) < 5:
                    examples["address_no_digit"].append(addr)

        parties = extract_parties_any(rec)
        if not parties:
            c["missing_parties"] += 1
        else:
            for p in parties:
                if RE_TRAILING_Y.search(p):
                    c["party_trailing_Y"] += 1
                    if len(examples["party_trailing_Y"]) < 5:
                        examples["party_trailing_Y"].append(p)
                if re.search(r"[|•■�]", p):
                    c["party_weird_chars"] += 1
                    if len(examples["party_weird_chars"]) < 5:
                        examples["party_weird_chars"].append(p)

        if mode == "events":
            descr = extract_descr_any(rec)
            town = extract_town_any(rec)
            if not descr:
                c["missing_descr"] += 1
            if not town:
                c["missing_town"] += 1

        if mode == "join":
            inst, bookpage, recorded = extract_inst_bookpage_recorded_any(rec)
            if not inst:
                c["missing_inst"] += 1
            if not bookpage:
                c["missing_book_page"] += 1
            if not recorded:
                c["missing_recorded_at"] += 1

    return c, examples

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
    grand_events = Counter()
    grand_join = Counter()

    for cd in chunk_dirs:
        chunk = os.path.basename(cd)
        row = {"chunk": chunk}

        ev_path, ev_kind = pick_preferred_events_file(cd)
        if ev_path:
            ev_c, ev_ex = inspect_records_ndjson(ev_path, mode="events")
            row["events_file"] = os.path.basename(ev_path)
            row["events_kind"] = ev_kind
            row["events_counts"] = dict(ev_c)
            row["events_examples"] = {k:v for k,v in ev_ex.items() if v}
            grand_events.update(ev_c)
        else:
            row["events_missing"] = True

        join_path = find_join_file(cd)
        if join_path:
            j_c, j_ex = inspect_records_ndjson(join_path, mode="join")
            row["join_file"] = os.path.basename(join_path)
            row["join_counts"] = dict(j_c)
            row["join_examples"] = {k:v for k,v in j_ex.items() if v}
            grand_join.update(j_c)
        else:
            row["join_missing"] = True

        rollup.append(row)

    print("\n=== GRAND TOTALS (EVENTS) ===")
    for k in ["total","missing_address","address_trailing_Y","address_no_digit","missing_parties","party_trailing_Y","party_weird_chars","missing_descr","missing_town"]:
        if k in grand_events:
            print(f"{k}: {grand_events[k]}")

    print("\n=== GRAND TOTALS (JOIN) ===")
    for k in ["total","missing_inst","missing_book_page","missing_recorded_at","missing_address","address_trailing_Y","missing_parties","party_trailing_Y"]:
        if k in grand_join:
            print(f"{k}: {grand_join[k]}")

    out_path = os.path.join(work_root, "INSPECTION__ROLLUP_v2.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(rollup, f, indent=2)
    print(f"\n[done] wrote {out_path}")

if __name__ == "__main__":
    main()