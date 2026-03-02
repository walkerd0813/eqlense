#!/usr/bin/env python3
import argparse, json, re
from datetime import datetime, timezone

MONEY_RE = re.compile(r'(?<!\d)(\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{2})?(?!\d)')

def to_int_amount(s: str):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        v = int(round(float(s)))
        return v if v > 0 else None
    if not isinstance(s, str):
        return None
    ss = s.strip()
    if ss == "":
        return None
    # strip $ and spaces
    ss = ss.replace("$", " ")
    # prefer numbers with commas
    m = MONEY_RE.search(ss)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None

def first_non_null(*vals):
    for v in vals:
        if v is not None and v != "":
            return v
    return None

def extract_from_consideration_obj(cons):
    """
    cons may be dict, str, number.
    Try common keys.
    """
    if cons is None:
        return None, None
    if isinstance(cons, (int, float, str)):
        amt = to_int_amount(cons)
        return (amt, "consideration:scalar") if amt else (None, "consideration:unparseable")
    if isinstance(cons, dict):
        # common keys we have seen across variants
        keys = [
            "amount", "amount_raw", "consideration", "consideration_raw",
            "cons", "cons_raw", "price", "price_raw", "value", "value_raw"
        ]
        for k in keys:
            if k in cons:
                amt = to_int_amount(cons.get(k))
                if amt:
                    return amt, f"consideration:{k}"
        # sometimes nested
        for k in ("parsed", "raw", "index", "block"):
            if k in cons:
                amt = to_int_amount(cons.get(k))
                if amt:
                    return amt, f"consideration:{k}"
        # last resort: scan dict string
        amt = to_int_amount(json.dumps(cons, ensure_ascii=False))
        return (amt, "consideration:scan_dict") if amt else (None, "consideration:missing_keys")
    return None, "consideration:unsupported_type"

def extract_from_text_fields(r: dict):
    """
    Scan a few known text containers if present.
    """
    candidates = []

    # property_ref.address_candidates[].context has been used in earlier versions
    pr = r.get("property_ref") or {}
    ac = pr.get("address_candidates") or []
    if isinstance(ac, list):
        for c in ac:
            if isinstance(c, dict):
                candidates.append(c.get("context"))
                candidates.append(c.get("raw_context"))
                candidates.append(c.get("block_text"))
                candidates.append(c.get("text"))

    # other possible fields
    rec = r.get("recording") or {}
    doc = r.get("document") or {}
    candidates += [
        rec.get("recorded_at_raw"),
        rec.get("recording_date_raw"),
        doc.get("description_raw"),
        doc.get("remarks_raw"),
    ]

    for s in candidates:
        amt = to_int_amount(s) if isinstance(s, str) else None
        if amt:
            return amt, "text:scan"
    return None, "text:no_hit"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    total = 0
    filled = 0
    already = 0
    still_missing = 0
    reasons = {}

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            total += 1
            r = json.loads(line)

            ts = r.get("transaction_semantics")
            if not isinstance(ts, dict):
                ts = {}
                r["transaction_semantics"] = ts

            if ts.get("price_amount") is not None:
                already += 1
                fout.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            amt = None
            why = None

            # 1) structured consideration object
            amt, why = extract_from_consideration_obj(r.get("consideration"))
            if not amt:
                # 2) scan text fields
                amt2, why2 = extract_from_text_fields(r)
                if amt2:
                    amt, why = amt2, why2

            if amt:
                ts["price_amount"] = amt
                ts["price_source"] = why
                filled += 1
            else:
                still_missing += 1
                reasons[why or "price:missing"] = reasons.get(why or "price:missing", 0) + 1

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    audit = {
        "tool": "consideration_extract_v1_1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "infile": args.infile,
        "out": args.out,
        "total": total,
        "filled_price": filled,
        "already_had_price": already,
        "still_missing_price": still_missing,
        "reasons": reasons,
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2, ensure_ascii=False)

    print(json.dumps(audit, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
