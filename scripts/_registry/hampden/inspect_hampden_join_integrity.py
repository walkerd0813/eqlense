import argparse
import csv
import glob
import json
import os
import random
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

TRAILING_Y_RE = re.compile(r"\s+Y\s*$", re.IGNORECASE)


def iter_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                # Skip bad lines but keep going
                continue


def safe_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x)


def get_meta(ev: Dict[str, Any]) -> Dict[str, Any]:
    return ev.get("meta") or {}


def get_recording(ev: Dict[str, Any]) -> Dict[str, Any]:
    return ev.get("recording") or {}


def get_parties_list(ev: Dict[str, Any]) -> List[Any]:
    parties = ev.get("parties")
    if isinstance(parties, dict):
        pr = parties.get("parties_raw") or []
        if isinstance(pr, list):
            return pr
    return []


def get_refs(ev: Dict[str, Any]) -> List[Dict[str, Any]]:
    refs = ev.get("property_refs") or []
    return refs if isinstance(refs, list) else []


def ref_to_str(ref: Dict[str, Any]) -> str:
    town = safe_str(ref.get("town")).strip()
    addr = safe_str(ref.get("address_raw")).strip()
    if town and addr:
        return f"{town} | {addr}"
    if town:
        return town
    return addr


def primary_str(ev: Dict[str, Any]) -> str:
    refs = get_refs(ev)
    if refs:
        return ref_to_str(refs[0])
    return ""


def additional_refs_preview(ev: Dict[str, Any], max_items: int = 2) -> str:
    refs = get_refs(ev)
    if len(refs) <= 1:
        return ""
    tail = refs[1 : 1 + max_items]
    preview = " ; ".join(ref_to_str(r) for r in tail if isinstance(r, dict))
    if len(refs) > 1 + max_items:
        preview += f" ; (+{len(refs) - (1 + max_items)} more)"
    return preview


def parties_preview(ev: Dict[str, Any], max_items: int = 2) -> str:
    parties = get_parties_list(ev)
    if not parties:
        return ""
    out = []
    for p in parties[:max_items]:
        if isinstance(p, dict):
            name = safe_str(p.get("name_raw")).strip()
            side = safe_str(p.get("side_code_raw")).strip()
            et = safe_str(p.get("entity_type_raw")).strip()
            s = " ".join(x for x in [side, et, name] if x)
            out.append(s)
        else:
            out.append(safe_str(p).strip())
    if len(parties) > max_items:
        out.append(f"(+{len(parties) - max_items} more)")
    return " | ".join([x for x in out if x])


def has_trailing_y_anywhere(ev: Dict[str, Any]) -> bool:
    for r in get_refs(ev):
        if not isinstance(r, dict):
            continue
        addr = safe_str(r.get("address_raw"))
        if TRAILING_Y_RE.search(addr or ""):
            return True
    return False


def flags_for(ev: Dict[str, Any]) -> List[str]:
    f = []
    refs = get_refs(ev)
    rec = get_recording(ev)

    if not refs:
        f.append("NO_REFS")
    if not get_parties_list(ev):
        f.append("NO_PARTIES")
    if not safe_str(ev.get("descr_loc_raw")).strip():
        f.append("NO_DESCR")
    cons = ev.get("consideration") or {}
    if not safe_str((cons or {}).get("amount_raw")).strip():
        f.append("NO_CONS")
    if not safe_str(rec.get("inst_raw")).strip():
        f.append("NO_INST")
    if not safe_str(rec.get("book_page_raw")).strip():
        f.append("NO_BOOK_PAGE")

    if has_trailing_y_anywhere(ev):
        f.append("TRAILING_Y")

    return f


def discover_join_files(work_root: str) -> List[str]:
    # search recursively for join__DEED__*.ndjson
    pat = os.path.join(work_root, "**", "join__DEED__*.ndjson")
    files = glob.glob(pat, recursive=True)
    # remove QA jsons if any accidentally match
    files = [p for p in files if p.lower().endswith(".ndjson")]
    return sorted(files)


def chunk_name_from_path(path: str) -> str:
    # chunk folder is typically ...\p00050_p00099\join__DEED__...
    parts = os.path.normpath(path).split(os.sep)
    for i in range(len(parts) - 1, -1, -1):
        if parts[i].startswith("p") and "_p" in parts[i]:
            return parts[i]
        if re.fullmatch(r"p\d{5}_p\d{5}", parts[i] or ""):
            return parts[i]
    # fallback
    return os.path.basename(os.path.dirname(path))


def load_all_events(join_files: List[str], max_events: Optional[int] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for jf in join_files:
        for ev in iter_ndjson(jf):
            ev["_join_path"] = jf
            ev["_chunk"] = chunk_name_from_path(jf)
            out.append(ev)
            if max_events and len(out) >= max_events:
                return out
    return out


def summarize_by_chunk(events: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    sums: Dict[str, Dict[str, int]] = {}
    for ev in events:
        ck = ev.get("_chunk") or "UNKNOWN"
        s = sums.setdefault(ck, {
            "total": 0,
            "no_refs": 0,
            "no_parties": 0,
            "no_descr": 0,
            "no_cons": 0,
            "no_inst": 0,
            "no_book_page": 0,
            "trailing_y": 0,
        })
        s["total"] += 1
        fl = flags_for(ev)
        if "NO_REFS" in fl: s["no_refs"] += 1
        if "NO_PARTIES" in fl: s["no_parties"] += 1
        if "NO_DESCR" in fl: s["no_descr"] += 1
        if "NO_CONS" in fl: s["no_cons"] += 1
        if "NO_INST" in fl: s["no_inst"] += 1
        if "NO_BOOK_PAGE" in fl: s["no_book_page"] += 1
        if "TRAILING_Y" in fl: s["trailing_y"] += 1
    return sums


def pick_samples(events: List[Dict[str, Any]], n: int, mode: str, seed: int) -> List[Dict[str, Any]]:
    """
    mode:
      - good: prefer rows with minimal flags
      - bad: prefer rows with flags
      - mixed: half good, half bad
      - first: first N by (page, record)
      - random: random N
    """
    random.seed(seed)

    def sort_key(ev: Dict[str, Any]) -> Tuple[int, int]:
        m = get_meta(ev)
        return (int(m.get("page_index") or 0), int(m.get("record_index") or 0))

    evs = sorted(events, key=sort_key)

    if mode == "first":
        return evs[:n]

    if mode == "random":
        if len(evs) <= n:
            return evs
        return random.sample(evs, n)

    scored: List[Tuple[int, Dict[str, Any]]] = []
    for ev in evs:
        fl = flags_for(ev)
        scored.append((len(fl), ev))

    good = [ev for sc, ev in scored if sc == 0]
    meh = [ev for sc, ev in scored if sc == 1]
    bad = [ev for sc, ev in scored if sc >= 2]

    if mode == "good":
        pool = good + meh + bad
        return pool[:n]

    if mode == "bad":
        pool = bad + meh + good
        return pool[:n]

    # mixed default
    half = n // 2
    pool = (good + meh)[:half] + bad[:(n - half)]
    # If not enough, fill from remainder
    if len(pool) < n:
        rest = [ev for ev in evs if ev not in pool]
        pool += rest[: (n - len(pool))]
    return pool[:n]


def print_table(rows: List[Dict[str, Any]], max_width: int = 220) -> None:
    # basic console table (no external deps)
    headers = [
        "chunk", "page", "ri", "inst", "book_page",
        "n_refs", "primary", "addl_refs",
        "n_parties", "parties_preview",
        "descr", "cons", "flags",
    ]

    table: List[List[str]] = []
    for ev in rows:
        m = get_meta(ev)
        rec = get_recording(ev)
        cons = ev.get("consideration") or {}
        refs = get_refs(ev)
        parties = get_parties_list(ev)

        row = [
            safe_str(ev.get("_chunk")),
            safe_str(m.get("page_index")),
            safe_str(m.get("record_index")),
            safe_str(rec.get("inst_raw")),
            safe_str(rec.get("book_page_raw") or rec.get("ref_book_page_raw")),
            safe_str(len(refs)),
            primary_str(ev),
            additional_refs_preview(ev, max_items=2),
            safe_str(len(parties)),
            parties_preview(ev, max_items=2),
            safe_str(ev.get("descr_loc_raw")),
            safe_str((cons or {}).get("amount_raw")),
            ",".join(flags_for(ev)),
        ]
        table.append(row)

    # compute col widths
    widths = [len(h) for h in headers]
    for r in table:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    # cap some big columns so it doesn't explode
    cap = {
        "primary": 50,
        "addl_refs": 40,
        "parties_preview": 45,
        "descr": 24,
        "flags": 32,
    }
    for i, h in enumerate(headers):
        if h in cap:
            widths[i] = min(widths[i], cap[h])

    def fmt_cell(i: int, s: str) -> str:
        w = widths[i]
        if len(s) > w:
            s = s[: max(0, w - 1)] + "…"
        return s.ljust(w)

    # print
    line = " | ".join(fmt_cell(i, h) for i, h in enumerate(headers))
    print(line)
    print("-" * min(max_width, len(line)))

    for r in table:
        out = " | ".join(fmt_cell(i, r[i]) for i in range(len(headers)))
        if len(out) > max_width:
            out = out[: max_width - 1] + "…"
        print(out)


def write_csv(rows: List[Dict[str, Any]], out_csv: str) -> None:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    headers = [
        "chunk",
        "join_path",
        "page_index",
        "record_index",
        "inst_raw",
        "book_page_raw",
        "primary",
        "n_property_refs",
        "property_refs_all",
        "n_parties",
        "parties_all",
        "descr_loc_raw",
        "consideration_raw",
        "flags",
    ]

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()

        for ev in rows:
            m = get_meta(ev)
            rec = get_recording(ev)
            cons = ev.get("consideration") or {}

            refs = [ref_to_str(r) for r in get_refs(ev) if isinstance(r, dict)]
            parties = []
            for p in get_parties_list(ev):
                if isinstance(p, dict):
                    parties.append(" ".join(x for x in [
                        safe_str(p.get("side_code_raw")),
                        safe_str(p.get("entity_type_raw")),
                        safe_str(p.get("name_raw")),
                    ] if x).strip())
                else:
                    parties.append(safe_str(p))

            w.writerow({
                "chunk": safe_str(ev.get("_chunk")),
                "join_path": safe_str(ev.get("_join_path")),
                "page_index": safe_str(m.get("page_index")),
                "record_index": safe_str(m.get("record_index")),
                "inst_raw": safe_str(rec.get("inst_raw")),
                "book_page_raw": safe_str(rec.get("book_page_raw") or rec.get("ref_book_page_raw")),
                "primary": primary_str(ev),
                "n_property_refs": len(refs),
                "property_refs_all": " || ".join(refs),
                "n_parties": len(parties),
                "parties_all": " || ".join(parties),
                "descr_loc_raw": safe_str(ev.get("descr_loc_raw")),
                "consideration_raw": safe_str((cons or {}).get("amount_raw")),
                "flags": ",".join(flags_for(ev)),
            })


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work_root", required=True, help="...\\_work\\PIPELINE_ALL_DEED_YYYYMMDDThhmmssZ")
    ap.add_argument("--sample", type=int, default=60, help="How many rows to print/export")
    ap.add_argument("--mode", default="mixed", choices=["mixed", "good", "bad", "first", "random"])
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out_csv", default="", help="Optional CSV output path")
    ap.add_argument("--max_width", type=int, default=220, help="Console width cap")
    args = ap.parse_args()

    work_root = args.work_root
    join_files = discover_join_files(work_root)

    if not join_files:
        raise SystemExit(f"[ERR] no join__DEED__*.ndjson found under: {work_root}")

    events = load_all_events(join_files)

    # Summary
    sums = summarize_by_chunk(events)
    total = {
        "total": 0, "no_refs": 0, "no_parties": 0, "no_descr": 0, "no_cons": 0,
        "no_inst": 0, "no_book_page": 0, "trailing_y": 0
    }
    print("\n=== Integrity Summary by chunk ===")
    for ck in sorted(sums.keys()):
        s = sums[ck]
        for k in total:
            total[k] += s.get(k, 0)
        print(
            f"{ck}: total={s['total']} no_refs={s['no_refs']} no_parties={s['no_parties']} "
            f"no_descr={s['no_descr']} no_cons={s['no_cons']} no_inst={s['no_inst']} "
            f"no_book_page={s['no_book_page']} trailing_y={s['trailing_y']}"
        )

    print("\n=== Overall ===")
    print(
        f"total={total['total']} no_refs={total['no_refs']} no_parties={total['no_parties']} "
        f"no_descr={total['no_descr']} no_cons={total['no_cons']} no_inst={total['no_inst']} "
        f"no_book_page={total['no_book_page']} trailing_y={total['trailing_y']}"
    )

    # Samples table
    sample_rows = pick_samples(events, n=args.sample, mode=args.mode, seed=args.seed)

    print(f"\n=== Sample rows (mode={args.mode}, n={len(sample_rows)}) ===")
    print_table(sample_rows, max_width=args.max_width)

    # CSV
    if args.out_csv:
        write_csv(sample_rows, args.out_csv)
        print(f"\n[done] wrote CSV: {args.out_csv}")


if __name__ == "__main__":
    main()a