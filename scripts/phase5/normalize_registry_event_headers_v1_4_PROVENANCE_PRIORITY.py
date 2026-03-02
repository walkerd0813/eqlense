import argparse, json, re, time

RE_MONEY_DEC = re.compile(r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2}))")
RE_DEED_LINE = re.compile(r"\bDEED\b", re.I)

def to_int_amount(x):
    if x is None: return None
    try:
        return int(float(str(x).replace(",","")))
    except:
        return None

def find_deed_line_money(raw_block: str):
    if not raw_block:
        return None, None
    for ln in raw_block.splitlines():
        if RE_DEED_LINE.search(ln):
            ms = RE_MONEY_DEC.findall(ln)
            if ms:
                raw = ms[-1]
                amt = to_int_amount(raw)
                return amt, raw
            return None, None
    return None, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--mirror_to_transaction_semantics", action="store_true")
    args = ap.parse_args()

    total=0; wrote=0
    filled_from_ts=0
    kept_existing_cons=0
    filled_from_deed_line=0
    missing=0
    nominal=0
    mirrored=0

    t0=time.time()

    with open(args.infile,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line: continue
            total += 1
            ev = json.loads(line)

            doc = ev.get("document") or {}
            raw_block = doc.get("raw_block") or ""

            c = ev.get("consideration")
            if not isinstance(c, dict): c = {}
            flags = c.get("flags") or []
            raw_text = c.get("raw_text")
            src = c.get("source")

            ts = ev.get("transaction_semantics")
            if not isinstance(ts, dict): ts = {}

            # PRIORITY 1: transaction_semantics.price_amount
            amt_int = to_int_amount(ts.get("price_amount"))
            if amt_int is not None:
                filled_from_ts += 1
                parse_status = "PARSED"
                src = ts.get("price_source") or "transaction_semantics.price_amount"
            else:
                # PRIORITY 2: existing consideration.amount
                amt_int = to_int_amount(c.get("amount"))
                if amt_int is not None:
                    kept_existing_cons += 1
                    parse_status = c.get("parse_status") or "PARSED"
                    src = src or "existing_consideration.amount"
                else:
                    # PRIORITY 3: deed-line parse
                    amt2, raw2 = find_deed_line_money(raw_block)
                    if amt2 is not None:
                        amt_int = amt2
                        raw_text = raw2
                        parse_status = "PARSED"
                        src = "document.raw_block.deed_line"
                        filled_from_deed_line += 1
                    else:
                        parse_status = "MISSING"
                        src = "missing"
                        missing += 1

            if amt_int is not None:
                if amt_int < 1000 or amt_int in (0,1,10,100):
                    if "ZERO_OR_NOMINAL" not in flags:
                        flags.append("ZERO_OR_NOMINAL")
                    nominal += 1

            ev["consideration"] = {
                "raw_text": raw_text,
                "amount": amt_int,
                "currency": c.get("currency") or "USD",
                "parse_status": parse_status,
                "flags": flags,
                "source": src
            }

            # optional compatibility mirror (only when ts missing)
            if args.mirror_to_transaction_semantics and amt_int is not None:
                if ts.get("price_amount") is None:
                    ts["price_amount"] = amt_int
                    ts["price_source"] = src
                    mirrored += 1
                ev["transaction_semantics"] = ts

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
            wrote += 1

    audit = {
        "script": "normalize_registry_event_headers_v1_4_PROVENANCE_PRIORITY",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "infile": args.infile,
        "out": args.out,
        "total": total,
        "written": wrote,
        "filled_from_ts": filled_from_ts,
        "kept_existing_consideration": kept_existing_cons,
        "filled_from_deed_line": filled_from_deed_line,
        "missing_after_parse": missing,
        "nominal_detected": nominal,
        "mirrored_to_transaction_semantics": mirrored,
        "elapsed_s": round(time.time()-t0,3)
    }
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows:", wrote, "filled_from_ts:", filled_from_ts, "kept_existing:", kept_existing_cons,
          "filled_from_deed_line:", filled_from_deed_line, "missing:", missing, "nominal:", nominal, "mirrored:", mirrored)

if __name__=="__main__":
    main()
