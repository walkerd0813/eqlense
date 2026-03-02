#!/usr/bin/env python3
"""
consideration_extract_v1_3.py

Goal
- Populate transaction_semantics.consideration_amount and fee_amount reliably.
- Set transaction_semantics.price_amount = consideration_amount (NEVER fee).

Key fix vs v1_1
- v1_1 mistakenly pulled the *Fee* column (often 12) as "price".
- v1_3 can pull the source text from a RAW INDEX file (e.g. deed_index_raw_*.ndjson)
  keyed by event_id, then parse the Cons column/label.

Usage
  python scripts/phase5/consideration_extract_v1_3.py \
    --infile <attached_or_flattened_events.ndjson> \
    --rawindex <deed_index_raw.ndjson> \
    --out <out.ndjson> \
    --audit <audit.json>

Notes
- Reads infile twice: pass1 collects event_ids; pass2 writes enriched rows.
- Scans rawindex once and only stores matches for the event_ids we need.
"""

import argparse, json, os, re
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

RE_CONS_LABEL = re.compile(r"(?i)\bcons(?:ideration)?\b\D{0,40}\$?\s*([0-9][0-9,]*)")
RE_FEE_LABEL  = re.compile(r"(?i)\bfee\b\D{0,40}\$?\s*([0-9][0-9,]*)")

# Common Hampden index row pattern: Date Ctrl Doc Fee Cons Book Page
RE_INDEX_ROW = re.compile(
    r"(?P<date>\d{1,2}-\d{1,2}-\d{4})\s+"  # date
    r"(?P<ctrl>\S+)\s+"                    # ctrl
    r"(?P<doc>\S+)\s+"                     # doc
    r"(?P<fee>\d{1,6})\s+"                 # fee
    r"(?P<cons>\d{1,12})\s+"               # cons
    r"(?P<book>\S+)\s+"                    # book
    r"(?P<page>\S+)"                       # page
)

RE_NOMINAL_10 = re.compile(r"(?i)\bten\s+dollars?\b|\b\$?10\b")
RE_NOMINAL_1  = re.compile(r"(?i)\bone\s+dollar\b|\b\$?1\b")


def to_text(raw_lines: Any) -> str:
    if raw_lines is None:
        return ""
    if isinstance(raw_lines, str):
        return raw_lines.replace("\r", "")
    if isinstance(raw_lines, list):
        return "\n".join([str(x) for x in raw_lines]).replace("\r", "")
    return str(raw_lines).replace("\r", "")


def parse_int(s: str) -> Optional[int]:
    try:
        return int(s.replace(",", "").strip())
    except Exception:
        return None


def extract_fee_cons(text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Return (fee, cons, snippet_source)."""
    if not text:
        return None, None, None

    # 1) Prefer explicit labels if present
    m_cons = RE_CONS_LABEL.search(text)
    m_fee = RE_FEE_LABEL.search(text)
    if m_cons or m_fee:
        fee = parse_int(m_fee.group(1)) if m_fee else None
        cons = parse_int(m_cons.group(1)) if m_cons else None
        if m_cons:
            start = max(0, m_cons.start() - 50)
            end = min(len(text), m_cons.end() + 80)
            snip = text[start:end]
        else:
            snip = None
        return fee, cons, "label"

    # 2) Try index-row positional parse
    m = RE_INDEX_ROW.search(text)
    if m:
        fee = parse_int(m.group("fee"))
        cons = parse_int(m.group("cons"))
        snip = m.group(0)
        return fee, cons, "index_row"

    return None, None, None


def extract_nominal(text: str) -> Tuple[Optional[int], bool, Optional[str]]:
    if not text:
        return None, False, None
    if RE_NOMINAL_10.search(text):
        return 10, True, "nominal:10"
    if RE_NOMINAL_1.search(text):
        return 1, True, "nominal:1"
    return None, False, None


def read_event_ids(infile: str) -> set:
    ids = set()
    with open(infile, "r", encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            eid = r.get("event_id")
            if eid:
                ids.add(eid)
    return ids


def build_rawindex_lookup(rawindex: str, want_ids: set) -> Dict[str, str]:
    """Scan rawindex and return event_id -> combined raw text for the subset we need."""
    lookup: Dict[str, str] = {}
    with open(rawindex, "r", encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            eid = r.get("event_id")
            if not eid or eid not in want_ids:
                continue
            # prefer raw_lines in raw index
            text = to_text(r.get("raw_lines"))
            if not text:
                # last resort: some raws store block under meta/source
                text = to_text((r.get("meta") or {}).get("raw_lines"))
            if not text:
                text = to_text((r.get("source") or {}).get("raw_lines"))
            lookup[eid] = text
            # micro-optimization: stop early if we found all
            if len(lookup) >= len(want_ids):
                break
    return lookup


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--rawindex", required=True, help="deed_index_raw_*.ndjson containing raw_lines w/ Fee/Cons")
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    want_ids = read_event_ids(args.infile)
    raw_lookup = build_rawindex_lookup(args.rawindex, want_ids)

    total = 0
    src_found = 0
    src_missing = 0

    filled_fee = 0
    filled_cons = 0
    filled_price = 0
    nominal_rows = 0
    still_missing_price = 0

    reasons: Dict[str, int] = {}

    def bump(k: str, n: int = 1):
        reasons[k] = reasons.get(k, 0) + n

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            total += 1
            r = json.loads(line)
            eid = r.get("event_id")

            ts = r.get("transaction_semantics") or {}

            # Source text priority: rawindex lookup -> row.raw_lines -> row.source.raw_lines
            text = raw_lookup.get(eid) if eid else None
            if text:
                src_found += 1
            else:
                src_missing += 1
                text = to_text(r.get("raw_lines")) or to_text((r.get("source") or {}).get("raw_lines"))

            fee_amt, cons_amt, mode = extract_fee_cons(text or "")
            nom_amt, nom_flag, nom_tag = extract_nominal(text or "")

            # Fee
            if fee_amt is not None and ts.get("fee_amount") is None:
                ts["fee_amount"] = fee_amt
                filled_fee += 1

            # Consideration
            if cons_amt is not None:
                if ts.get("consideration_amount") is None:
                    ts["consideration_amount"] = cons_amt
                    filled_cons += 1
                # keep/augment human readable consideration field
                cons_obj = r.get("consideration") or {}
                if cons_obj.get("amount") is None:
                    cons_obj["amount"] = cons_amt
                if cons_obj.get("text_raw") is None:
                    cons_obj["text_raw"] = (text[:220] if mode is None else mode)
                cons_obj["nominal_flag"] = False
                r["consideration"] = cons_obj

            elif nom_flag:
                nominal_rows += 1
                cons_obj = r.get("consideration") or {}
                cons_obj["amount"] = nom_amt
                cons_obj["text_raw"] = nom_tag
                cons_obj["nominal_flag"] = True
                r["consideration"] = cons_obj
                if ts.get("consideration_amount") is None:
                    ts["consideration_amount"] = nom_amt

            # Price = consideration (never fee)
            price = ts.get("consideration_amount")
            if price is not None:
                ts["price_amount"] = price
                ts["price_source"] = "cons" if cons_amt is not None else ("nominal" if nom_flag else "other")
                filled_price += 1
            else:
                ts["price_amount"] = None
                ts["price_source"] = "missing"
                still_missing_price += 1
                bump("price:missing")

            # If we only found a fee and no cons, record for follow-up
            if fee_amt is not None and cons_amt is None and not nom_flag:
                bump("cons:missing_fee_present")

            # suspicious: fee equals price
            if ts.get("fee_amount") is not None and ts.get("price_amount") is not None and ts["fee_amount"] == ts["price_amount"]:
                bump("warn:fee_equals_price")

            ts["_price_enrich"] = {
                "tool": "consideration_extract_v1_3",
                "rawindex_used": os.path.basename(args.rawindex),
                "rawindex_hit": bool(text) and (eid in raw_lookup if eid else False),
            }

            r["transaction_semantics"] = ts
            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    audit = {
        "tool": "consideration_extract_v1_3",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "infile": args.infile,
        "rawindex": args.rawindex,
        "out": args.out,
        "total": total,
        "rawindex_event_ids": len(want_ids),
        "rawindex_found": src_found,
        "rawindex_missing": src_missing,
        "filled_fee_amount": filled_fee,
        "filled_consideration_amount": filled_cons,
        "filled_price_amount": filled_price,
        "nominal_rows": nominal_rows,
        "still_missing_price": still_missing_price,
        "reasons": reasons,
    }

    with open(args.audit, "w", encoding="utf-8") as af:
        json.dump(audit, af, indent=2)

    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
