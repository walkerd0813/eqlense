# scripts/phase5/arms_length_classify_v1_2.py
# Arms-length classification (heuristic, auditable). v1_2
# Key fix vs v1_1: read price from transaction_semantics.price_amount (and fallbacks),
# and treat tiny values (e.g., fees like 12) as invalid consideration.
import argparse, json, re
from datetime import datetime, timezone
from collections import Counter

RULES_VERSION = "v1_2"

NON_ARMS_INSTRUMENT_HINTS = [
    "QUITCLAIM", "QCD", "QUIT CLAIM",
    "DEED IN LIEU",
    "FORECLOSURE", "SHERIFF", "EXECUTION", "REO",
    "TRUSTEE", "TRUST", "ESTATE",
    "GIFT",
    "DIVORCE", "FAMILY", "SPOUSE",
    "TAX",
    "BANKRUPT",
]

ARM_INSTRUMENT_HINTS = [
    "WARRANTY", "WARRANTEE",
]

NOMINAL_AMOUNTS = set([0, 1, 10, 100])

def _get_text(r: dict) -> str:
    parts=[]
    doc=r.get("document") or {}
    rec=r.get("recording") or {}
    for k in ("instrument_type","description_raw","doc_type"):
        v=doc.get(k)
        if v: parts.append(str(v))
    for k in ("document_type_raw","doc_type_raw"):
        v=rec.get(k)
        if v: parts.append(str(v))
    return " | ".join(parts).upper()

def _get_price(r: dict):
    # priority: transaction_semantics.price_amount
    ts = r.get("transaction_semantics") or {}
    for k in ("price_amount","consideration_amount","amount"):
        v = ts.get(k)
        if isinstance(v,(int,float)):
            return v, f"transaction_semantics.{k}"
    # fallback: consideration.amount
    cons = r.get("consideration") or {}
    v = cons.get("amount")
    if isinstance(v,(int,float)):
        return v, "consideration.amount"
    # fallback: attach.consideration_amount if present
    att = r.get("attach") or {}
    v = att.get("consideration_amount")
    if isinstance(v,(int,float)):
        return v, "attach.consideration_amount"
    return None, None

def _classify_row(r: dict, min_price: int):
    reasons=[]
    price, price_src = _get_price(r)
    if price is None:
        return ("AL_UNKNOWN","C",["price:missing"], {"price_src": None})

    # normalize numeric
    try:
        price_val = float(price)
    except Exception:
        return ("AL_UNKNOWN","C",["price:unreadable"], {"price_src": price_src, "price_raw": price})

    if price_val in NOMINAL_AMOUNTS:
        return ("NON_ARMS","B",[f"price:nominal:{int(price_val)}"], {"price_src": price_src, "price": price_val})

    if price_val < min_price:
        # This catches the "12" fee bleed-through and other non-consideration artifacts.
        return ("AL_UNKNOWN","C",[f"price:too_small:<{min_price}"], {"price_src": price_src, "price": price_val})

    txt = _get_text(r)
    # instrument hints
    for h in NON_ARMS_INSTRUMENT_HINTS:
        if h in txt:
            return ("NON_ARMS","B",[f"instrument:hint:{h}"], {"price_src": price_src, "price": price_val, "instrument_text": txt})

    for h in ARM_INSTRUMENT_HINTS:
        if h in txt:
            # still could be non-arms, but better than unknown if price is real
            return ("ARMS","B",[f"instrument:hint:{h}"], {"price_src": price_src, "price": price_val, "instrument_text": txt})

    # attached vs unknown can raise confidence slightly
    attach_status = r.get("attach_status") or (r.get("attach") or {}).get("attach_status")
    if attach_status in ("ATTACHED_A","ATTACHED_B"):
        return ("ARMS","B",["price:present","attach:attached"], {"price_src": price_src, "price": price_val})

    return ("ARMS","C",["price:present"], {"price_src": price_src, "price": price_val})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--min_price", type=int, default=1000)
    args = ap.parse_args()

    class_counts=Counter()
    conf_counts=Counter()
    reason_counts=Counter()

    total=0
    with open(args.infile,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            if not line.strip(): 
                continue
            total += 1
            r = json.loads(line)

            cls, conf, reasons, debug = _classify_row(r, args.min_price)
            # write under deterministic key
            r["arms_length"] = {
                "rules_version": RULES_VERSION,
                "class": cls,               # ARMS | NON_ARMS | AL_UNKNOWN
                "confidence": conf,         # A | B | C (heuristic)
                "reasons": reasons[:8],     # keep short, audit-friendly
                "debug": debug,
            }

            class_counts[cls]+=1
            conf_counts[conf]+=1
            for rsn in reasons:
                reason_counts[rsn]+=1

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

    audit = {
        "rules_version": RULES_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "infile": args.infile,
        "out": args.out,
        "min_price": args.min_price,
        "total": total,
        "class_counts": dict(class_counts),
        "confidence_counts": dict(conf_counts),
        "top_reasons": reason_counts.most_common(25),
    }

    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
