import argparse, json, re, time

RE_MONEY = re.compile(r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)")
RE_GIFT = re.compile(r"\bLOVE AND AFFECTION\b|\bGIFT\b", re.I)
RE_NOT_STATED = re.compile(r"\bNOT STATED\b|\bNO CONSIDERATION\b|\bCONSIDERATION NOT STATED\b", re.I)

def to_int_amount(x):
    if x is None:
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    try:
        s = str(x).strip().replace(",", "")
        if s == "":
            return None
        return int(float(s))
    except:
        return None

def extract_money_from_block(block: str):
    if not block:
        return None, None
    # Find first money-like token (e.g., 5,000.00) in the raw block
    m = RE_MONEY.search(block)
    if not m:
        return None, None
    raw = m.group(1)
    amt = to_int_amount(raw)
    return amt, raw

def extract_party_lines_from_block(block: str):
    if not block:
        return []
    party_lines = []
    for line in block.splitlines():
        line2 = line.strip()
        # Hampden pattern: "1 P LASTNAME FIRST ..."
        if re.search(r"^\d+\s+P\s+[A-Z]", line2):
            party_lines.append(line2)
    return party_lines

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    total = 0
    wrote = 0
    filled_cons = 0
    filled_doc = 0

    t0 = time.time()

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            ev = json.loads(line)

            doc = ev.get("document") or {}
            raw_block = doc.get("raw_block") or ""

            # ---- doc_type_code/doc_type_desc ----
            doc_type = doc.get("doc_type")
            if doc_type:
                ev["doc_type_code"] = doc_type
                ev["doc_type_desc"] = doc_type
                filled_doc += 1
            else:
                # fallback: if block contains DEED
                if "DEED" in raw_block.upper():
                    ev["doc_type_code"] = "DEED"
                    ev["doc_type_desc"] = "DEED"
                    filled_doc += 1

            # ---- consideration canonicalize ----
            c = ev.get("consideration")
            if not isinstance(c, dict):
                c = {}

            ts = ev.get("transaction_semantics") or {}

            amt = to_int_amount(c.get("amount"))
            if amt is None:
                amt = to_int_amount(ts.get("price_amount"))

            raw_text = c.get("raw_text")
            if raw_text is None:
                amt2, raw2 = extract_money_from_block(raw_block)
                if amt is None and amt2 is not None:
                    amt = amt2
                if raw2 is not None:
                    raw_text = raw2

            flags = c.get("flags") or []
            parse_status = "PARSED" if isinstance(amt, int) else "MISSING"

            # text-based flags from raw_block
            if raw_block:
                if RE_GIFT.search(raw_block):
                    if "GIFT" not in flags:
                        flags.append("GIFT")
                    if "LOVE_AND_AFFECTION" not in flags and "LOVE AND AFFECTION" in raw_block.upper():
                        flags.append("LOVE_AND_AFFECTION")
                if RE_NOT_STATED.search(raw_block):
                    if "CONSIDERATION_NOT_STATED" not in flags:
                        flags.append("CONSIDERATION_NOT_STATED")

            ev["consideration"] = {
                "raw_text": raw_text,
                "amount": amt,
                "currency": "USD",
                "parse_status": parse_status,
                "flags": flags,
                "source": "transaction_semantics.price_amount" if ts.get("price_amount") is not None else "document.raw_block"
            }

            if ev["consideration"]["amount"] is not None:
                filled_cons += 1

            # ---- attach normalize into attach.status/property_id/method ----
            a = ev.get("attach")
            if not isinstance(a, dict):
                a = {}
            a["status"] = ev.get("attach_status") or a.get("status") or "UNKNOWN"
            a["property_id"] = ev.get("property_id") or a.get("property_id")
            a["method"] = ev.get("attach_method") or a.get("method")
            ev["attach"] = a

            # ---- parties (store raw party lines so we don't lose signal) ----
            party_lines = extract_party_lines_from_block(raw_block)
            p = ev.get("parties")
            if not isinstance(p, dict):
                p = {}
            if party_lines and not p.get("party_lines_raw"):
                p["party_lines_raw"] = party_lines
            ev["parties"] = p

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
            wrote += 1

    audit = {
        "script": "normalize_registry_event_headers_v1_0",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "infile": args.infile,
        "out": args.out,
        "total": total,
        "written": wrote,
        "filled_consideration_amount": filled_cons,
        "filled_doc_type": filled_doc,
        "elapsed_s": round(time.time() - t0, 3)
    }

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("rows:", wrote, "cons_amount_filled:", filled_cons, "doc_type_filled:", filled_doc)

if __name__ == "__main__":
    main()
