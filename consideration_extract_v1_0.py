#!/usr/bin/env python3
"""Phase 5 — Consideration extractor (index-block parsing)

Goal
  - Populate a defensible numeric consideration/price signal when it exists in the deed index block
  - Do NOT guess when it isn't present
  - Work county-agnostic: it scans ALL string fields for patterns like "Cons" / "Consideration".

Input
  NDJSON rows (one JSON per line). Expected typical keys:
    event_id, recording, document, meta, source, property_ref, etc.

Output
  Same NDJSON rows with a new field:
    transaction_semantics.price_amount (float)
    transaction_semantics.price_currency ("USD")
    transaction_semantics.price_source ("parsed_index_text")
    transaction_semantics.price_evidence { match, field_path, confidence }

Notes
  - This does NOT change attachment outcomes.
  - It is safe to run on any county as long as the raw index block lives somewhere in the row.
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Any, Dict, List, Tuple


RE_CONS_STRICT = re.compile(
    r"\b(?:cons|consideration)\b\s*[:=]?\s*(\$?\s*[0-9][0-9,]*\.?[0-9]{0,2})",
    re.IGNORECASE,
)

# Some index blocks abbreviate as "Cons" and place Fee + Cons on one line.
# Example: "Fee: 105.00 Cons: 330000.00 Bk/Pg: 25711/575"
RE_FEE_CONS = re.compile(
    r"\bcons\b\s*[:=]?\s*(\$?\s*[0-9][0-9,]*\.?[0-9]{0,2})",
    re.IGNORECASE,
)


def iter_strings(obj: Any, path: str = "") -> List[Tuple[str, str]]:
    """Return list of (path, string) for all strings inside obj."""
    out: List[Tuple[str, str]] = []
    if isinstance(obj, str):
        out.append((path, obj))
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            out.extend(iter_strings(v, p))
        return out
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{path}[{i}]"
            out.extend(iter_strings(v, p))
        return out
    return out


def parse_money(s: str) -> float | None:
    s2 = s.strip().replace("$", "").replace(",", "")
    try:
        return float(s2)
    except Exception:
        return None


def extract_price(row: Dict[str, Any]) -> Dict[str, Any] | None:
    """Return evidence dict or None."""
    strings = iter_strings(row)
    best: Dict[str, Any] | None = None

    for field_path, text in strings:
        if not text or len(text) < 3:
            continue

        m = RE_CONS_STRICT.search(text)
        if m:
            amt = parse_money(m.group(1))
            if amt is None or amt <= 0:
                continue
            cand = {
                "amount": amt,
                "currency": "USD",
                "source": "parsed_index_text",
                "field_path": field_path,
                "match": m.group(0)[:200],
                "confidence": "A",
            }
            # Prefer strict cons/consideration matches; keep the first A.
            return cand

        m2 = RE_FEE_CONS.search(text)
        if m2:
            amt = parse_money(m2.group(1))
            if amt is None or amt <= 0:
                continue
            cand2 = {
                "amount": amt,
                "currency": "USD",
                "source": "parsed_index_text",
                "field_path": field_path,
                "match": m2.group(0)[:200],
                "confidence": "B",
            }
            # Keep highest B amount (rare but safe)
            if best is None or (best.get("confidence") == "B" and amt > float(best.get("amount", 0))):
                best = cand2

    return best


def ensure_obj(d: Dict[str, Any], key: str) -> Dict[str, Any]:
    v = d.get(key)
    if isinstance(v, dict):
        return v
    v = {}
    d[key] = v
    return v


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    total = 0
    filled = 0
    already = 0
    missing = 0
    reasons = {}

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            total += 1
            row = json.loads(line)

            ts = ensure_obj(row, "transaction_semantics")
            if ts.get("price_amount") is not None:
                already += 1
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                continue

            ev = extract_price(row)
            if ev is None:
                missing += 1
                reasons["price:missing"] = reasons.get("price:missing", 0) + 1
            else:
                ts["price_amount"] = ev["amount"]
                ts["price_currency"] = ev["currency"]
                ts["price_source"] = ev["source"]
                ts["price_confidence"] = ev["confidence"]
                ts["price_evidence"] = {
                    "field_path": ev["field_path"],
                    "match": ev["match"],
                }
                filled += 1

            fout.write(json.dumps(row, ensure_ascii=False) + "\n")

    audit = {
        "tool": "consideration_extract_v1_0",
        "created_at_utc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "infile": args.infile,
        "out": args.out,
        "total": total,
        "filled_price": filled,
        "already_had_price": already,
        "still_missing_price": missing,
        "reasons": reasons,
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
