import argparse, json, re, time

# Money with decimals (matches 1.00, 5,000.00, 325000.00)
RE_MONEY_DEC = re.compile(r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2}))")
RE_DEED_LINE = re.compile(r"\bDEED\b", re.I)

def to_int_amount(s):
    if s is None: return None
    try:
        return int(float(str(s).replace(",","")))
    except:
        return None

def find_deed_line_money(raw_block: str):
    if not raw_block:
        return None, None
    for ln in raw_block.splitlines():
        if RE_DEED_LINE.search(ln):
            # take the LAST money-with-decimals token on the deed line
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
    args = ap.parse_args()

    total=0; wrote=0
    filled_from_ts=0
    filled_from_block=0
    missing=0
    nominal=0

    t0=time.time()

    with open(args.infile,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line: continue
            total += 1
            ev = json.loads(line)

            doc = ev.get("document") or {}
            raw_block = doc.get("raw_block") or ""

            # canonical consideration object
            c = ev.get("consideration")
            if not isinstance(c, dict): c = {}
            flags = c.get("flags") or []

            ts = ev.get("transaction_semantics") or {}
            amt = c.get("amount")
            if amt is None:
                amt = ts.get("price_amount")
                if amt is not None:
                    filled_from_ts += 1

            # If still missing, try DEED line parse (safe)
            raw_text = c.get("raw_text")
            if amt is None:
                amt2, raw2 = find_deed_line_money(raw_block)
                if amt2 is not None:
                    amt = amt2
                    raw_text = raw2
                    filled_from_block += 1

            # normalize
            amt_int = None
            if amt is not None:
                amt_int = to_int_amount(amt)

            if amt_int is not None:
                parse_status = "PARSED"
                if amt_int < 1000 or amt_int in (0,1,10,100):
                    if "ZERO_OR_NOMINAL" not in flags:
                        flags.append("ZERO_OR_NOMINAL")
                    nominal += 1
            else:
                parse_status = "MISSING"
                missing += 1

            ev["consideration"] = {
                "raw_text": raw_text,
                "amount": amt_int,
                "currency": "USD",
                "parse_status": parse_status,
                "flags": flags,
                "source": "transaction_semantics.price_amount" if ts.get("price_amount") is not None else ("document.raw_block.deed_line" if amt_int is not None else "missing")
            }

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
            wrote += 1

    audit = {
        "script": "normalize_registry_event_headers_v1_2_PARSEBLOCK_DEEDLINE",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "infile": args.infile,
        "out": args.out,
        "total": total,
        "written": wrote,
        "filled_from_ts": filled_from_ts,
        "filled_from_deed_line": filled_from_block,
        "missing_after_parse": missing,
        "nominal_detected": nominal,
        "elapsed_s": round(time.time()-t0,3)
    }
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows:", wrote, "filled_from_ts:", filled_from_ts, "filled_from_deed_line:", filled_from_block, "missing:", missing, "nominal:", nominal)

if __name__=="__main__":
    main()
