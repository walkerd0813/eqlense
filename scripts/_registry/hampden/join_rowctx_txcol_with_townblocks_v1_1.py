#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

RE_TIME = re.compile(r"\b(\d{1,2}):(\d{2}):(\d{2})([ap])\b", re.IGNORECASE)
RE_DATE = re.compile(r"\b(\d{1,2})-(\d{1,2})-(\d{2,4})\b")

def read_ndjson(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for raw in f:
            if not raw:
                continue
            s = raw.strip()
            if not s:
                continue
            if "\\n{" in s:
                s = s.replace("\\n", "\n")
            if not s.startswith("{"):
                brace = s.find("{")
                if brace >= 0:
                    s = s[brace:]
                else:
                    continue
            for part in s.splitlines():
                part = part.strip()
                if not part:
                    continue
                if not part.startswith("{"):
                    b = part.find("{")
                    if b >= 0:
                        part = part[b:]
                    else:
                        continue
                try:
                    yield json.loads(part)
                except json.JSONDecodeError:
                    continue

def write_ndjson(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def _norm_year(y: str) -> str:
    if len(y) == 2:
        return "20" + y
    return y

def norm_recorded_at(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = " ".join(str(s).strip().split())
    dm = RE_DATE.search(s)
    tm = RE_TIME.search(s)
    if not dm or not tm:
        return None
    mm, dd, yy = dm.group(1), dm.group(2), _norm_year(dm.group(3))
    hh, mi, ss, ap = tm.group(1), tm.group(2), tm.group(3), tm.group(4).lower()
    mm = mm.zfill(2); dd = dd.zfill(2); hh = hh.zfill(2)
    return f"{mm}-{dd}-{yy} {hh}:{mi}:{ss}{ap}"

def get_page_index(evt: Dict[str, Any]) -> Optional[int]:
    page = evt.get("page_index")
    if page is None:
        page = evt.get("meta", {}).get("page_index")
    try:
        return int(page) if page is not None else None
    except Exception:
        return None

def get_record_index(evt: Dict[str, Any]) -> Optional[int]:
    rec_idx = evt.get("record_index")
    if rec_idx is None:
        rec_idx = evt.get("meta", {}).get("record_index")
    try:
        return int(rec_idx) if rec_idx is not None else None
    except Exception:
        return None

def get_recorded_at_raw(evt: Dict[str, Any]) -> Optional[str]:
    rec_raw = None
    if isinstance(evt.get("recording"), dict):
        rec_raw = evt["recording"].get("recorded_at_raw")
    if not rec_raw:
        rec_raw = evt.get("recorded_at_raw")
    return rec_raw

def attach_rowctx(evt: Dict[str, Any], row: Dict[str, Any], prefer_overwrite: bool) -> None:
    evt.setdefault("recording", {})
    if not isinstance(evt["recording"], dict):
        evt["recording"] = {}

    mapping = {
        "recorded_at_raw": "recorded_at_raw",
        "book_page_raw": "book_page_raw",
        "inst_raw": "inst_raw",
        "grp_seq_raw": "grp_seq_raw",
        "ref_book_page_raw": "ref_book_page_raw",
    }
    for src_k, dst_k in mapping.items():
        if src_k not in row:
            continue
        v = row.get(src_k)
        if v is None or v == "":
            continue
        if prefer_overwrite or (evt["recording"].get(dst_k) in (None, "")):
            evt["recording"][dst_k] = v

    # consideration mapping (rowctx -> evt.consideration.amount_raw)
    cons = row.get("consideration_raw")
    if cons not in (None, ""):
        evt.setdefault("consideration", {})
        if not isinstance(evt["consideration"], dict):
            evt["consideration"] = {}
        if prefer_overwrite or (evt["consideration"].get("amount_raw") in (None, "")):
            evt["consideration"]["amount_raw"] = cons

    # keep rowctx QA breadcrumb
    evt.setdefault("rowctx", {})
    if not isinstance(evt["rowctx"], dict):
        evt["rowctx"] = {}
    evt["rowctx"].update({
        "source": (row.get("qa", {}) or {}).get("status") or row.get("engine") or "rowctx_txcol",
        "page_index": row.get("page_index"),
        "record_index": row.get("record_index"),
        "x_center": (row.get("qa", {}) or {}).get("x_center"),
    })

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--townblocks", required=True)
    ap.add_argument("--rowctx", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--qa", required=True)
    ap.add_argument("--prefer_overwrite", action="store_true")
    ap.add_argument("--flip_record_index", action="store_true",
                    help="Flip townblocks record_index per page: (N - rec + 1). Use when townblocks counts top-down and rowctx counts bottom-up.")
    args = ap.parse_args()

    # Load rowctx into indexes
    idx_by_page_rec: Dict[Tuple[int, str], Dict[str, Any]] = {}
    idx_by_page_recindex: Dict[Tuple[int, int], Dict[str, Any]] = {}
    rows_by_page: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

    row_seen = 0
    for row in read_ndjson(args.rowctx):
        row_seen += 1
        try:
            page_i = int(row.get("page_index"))
        except Exception:
            continue

        rec_norm = norm_recorded_at(row.get("recorded_at_raw"))
        if rec_norm:
            idx_by_page_rec[(page_i, rec_norm)] = row

        if row.get("record_index") is not None:
            try:
                ridx = int(row.get("record_index"))
                idx_by_page_recindex[(page_i, ridx)] = row
            except Exception:
                pass

        rows_by_page[page_i].append(row)

    # Stable ordinal ordering within each page
    rows_by_page_ord: Dict[int, List[Dict[str, Any]]] = {}
    for page_i, rows in rows_by_page.items():
        def sort_key(r: Dict[str, Any]):
            rn = norm_recorded_at(r.get("recorded_at_raw"))
            return (0, rn) if rn else (1, "")
        rows_by_page_ord[page_i] = sorted(rows, key=sort_key)

    # PASS 1: count townblocks rows per page (for flip)
    tb_count_by_page: Dict[int, int] = defaultdict(int)
    for evt in read_ndjson(args.townblocks):
        p = get_page_index(evt)
        if p is not None:
            tb_count_by_page[p] += 1

    # PASS 2: join
    out_rows: List[Dict[str, Any]] = []
    qa = {
        "engine": "join_rowctx_txcol_with_townblocks_v1_1",
        "inputs": {"townblocks": args.townblocks, "rowctx": args.rowctx},
        "options": {"flip_record_index": bool(args.flip_record_index), "prefer_overwrite": bool(args.prefer_overwrite)},
        "counts": {
            "townblocks_seen": 0,
            "rowctx_seen": row_seen,
            "matched_by_recorded_at": 0,
            "matched_by_record_index": 0,
            "matched_by_ordinal": 0,
            "unmatched": 0,
        },
        "unmatched_samples": [],
    }

    ordinal_counter: Dict[int, int] = defaultdict(int)

    for evt in read_ndjson(args.townblocks):
        qa["counts"]["townblocks_seen"] += 1

        page_i = get_page_index(evt)
        rec_norm = norm_recorded_at(get_recorded_at_raw(evt))
        rec_i = get_record_index(evt)

        # flip if requested
        if args.flip_record_index and (page_i is not None) and (rec_i is not None):
            n = tb_count_by_page.get(page_i) or 0
            if n > 0:
                rec_i = (n - rec_i + 1)

        matched_row = None
        matched_how = None

        if page_i is not None and rec_norm is not None:
            matched_row = idx_by_page_rec.get((page_i, rec_norm))
            if matched_row:
                matched_how = "recorded_at"

        if matched_row is None and page_i is not None and rec_i is not None:
            matched_row = idx_by_page_recindex.get((page_i, rec_i))
            if matched_row:
                matched_how = "record_index"

        if matched_row is None and page_i is not None:
            ord_i = ordinal_counter[page_i]
            ordinal_counter[page_i] += 1
            page_rows = rows_by_page_ord.get(page_i) or []
            if 0 <= ord_i < len(page_rows):
                matched_row = page_rows[ord_i]
                matched_how = "ordinal"

        if matched_row is not None:
            attach_rowctx(evt, matched_row, prefer_overwrite=bool(args.prefer_overwrite))
            if matched_how == "recorded_at":
                qa["counts"]["matched_by_recorded_at"] += 1
            elif matched_how == "record_index":
                qa["counts"]["matched_by_record_index"] += 1
            else:
                qa["counts"]["matched_by_ordinal"] += 1
        else:
            qa["counts"]["unmatched"] += 1
            if len(qa["unmatched_samples"]) < 25:
                qa["unmatched_samples"].append({
                    "page_index": page_i,
                    "recorded_at_raw": get_recorded_at_raw(evt),
                    "addr": evt.get("addr") or (evt.get("property_refs")[0].get("address_raw") if isinstance(evt.get("property_refs"), list) and evt.get("property_refs") else None),
                })

        out_rows.append(evt)

    write_ndjson(args.out, out_rows)
    os.makedirs(os.path.dirname(args.qa), exist_ok=True)
    with open(args.qa, "w", encoding="utf-8") as f:
        json.dump(qa, f, ensure_ascii=False, indent=2)

    print(f"[done] events_out={len(out_rows)} out={args.out} qa={args.qa}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())