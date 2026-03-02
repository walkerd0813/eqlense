#!/usr/bin/env python3
import argparse, json, re
from datetime import datetime, timezone

RULES_VERSION = "v1_1"

NON_ARMS_INSTRUMENT_PATTERNS = [
    (r"\bFORECLOS", "foreclosure"),
    (r"\bSHERIFF\b", "sheriff_deed"),
    (r"\bDEED\s+IN\s+LIEU\b", "deed_in_lieu"),
    (r"\bBANKRUPT", "bankruptcy"),
    (r"\bTRUSTEE\b", "trustee"),
    (r"\bTAX\b", "tax_related"),
    (r"\b(LIS\s+PENDENS|LISPENDENS)\b", "lis_pendens"),
    (r"\b(QUIT\s*CLAIM|QUITCLAIM)\b", "quitclaim_possible_non_arm"),
    (r"\bCONFIRM", "confirmation_deed"),
    (r"\bEXECUTOR\b", "executor_estate"),
    (r"\bADMINISTRATOR\b", "administrator_estate"),
    (r"\bGUARDIAN\b", "guardian"),
    (r"\bCONSERVATOR\b", "conservator"),
    (r"\bCOURT\b", "court_order"),
    (r"\bDIVORCE\b", "divorce"),
    (r"\bFAMILY\b", "family_transfer_hint"),
    (r"\bGIFT\b", "gift"),
    (r"\bNOMINAL\b", "nominal_consideration"),
]

ARMS_INSTRUMENT_PATTERNS = [
    (r"\bWARRANTY\b", "warranty_deed"),
    (r"\b(INDIVIDUAL|GENERAL)\s+WARRANTY\b", "warranty_deed"),
    (r"\bBARGAIN\b", "bargain_sale_deed"),
    (r"\bGRANT\b", "grant_deed"),
]

def _get(d, path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur

def normalize_text(s):
    if s is None:
        return ""
    return str(s).strip().upper()

def parse_amount(x):
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        t = x.strip()
        if not t:
            return None
        t = re.sub(r"[\$,]", "", t)
        try:
            return float(t)
        except:
            return None
    return None

def extract_consideration_amount(row):
    cons = row.get("consideration")
    if isinstance(cons, dict):
        for k in ("amount", "consideration", "value", "amount_raw", "consideration_raw"):
            if k in cons:
                amt = parse_amount(cons.get(k))
                if amt is not None:
                    return amt
    for k in ("consideration_amount", "price", "sale_price"):
        if k in row:
            amt = parse_amount(row.get(k))
            if amt is not None:
                return amt
    return None

def instrument_type(row):
    return normalize_text(_get(row, ["document","instrument_type"]) or _get(row, ["document","document_type"]) or row.get("instrument_type"))

def semantic_flags(row):
    ts = row.get("transaction_semantics")
    if not isinstance(ts, dict):
        return {}
    flags = {}
    for k,v in ts.items():
        if isinstance(v, bool):
            flags[k] = v
        elif isinstance(v, (int,float)) and v in (0,1):
            flags[k] = bool(v)
        elif isinstance(v, str) and v.strip().lower() in ("true","false","0","1"):
            flags[k] = v.strip().lower() in ("true","1")
    return flags

def classify_row(row):
    reasons=[]
    inst = instrument_type(row)
    flags = semantic_flags(row)
    amt = extract_consideration_amount(row)

    for fk in ("is_foreclosure","foreclosure","is_bank","is_trustee","is_estate","is_divorce","is_gift","is_nominal"):
        if flags.get(fk) is True:
            reasons.append(f"semantic:{fk}")

    for pat, tag in NON_ARMS_INSTRUMENT_PATTERNS:
        if re.search(pat, inst):
            reasons.append(f"instrument:{tag}")

    if any(r.startswith("semantic:") for r in reasons) or any("instrument:foreclosure" == r or "instrument:sheriff_deed" == r or "instrument:deed_in_lieu" == r for r in reasons):
        return ("AL_FALSE", "A", reasons or ["non_arms_signal"])

    quitclaim = ("instrument:quitclaim_possible_non_arm" in reasons)

    arms_hits=[]
    for pat, tag in ARMS_INSTRUMENT_PATTERNS:
        if re.search(pat, inst):
            arms_hits.append(f"instrument:{tag}")

    if amt is not None:
        if amt <= 100 or (amt < 10000 and quitclaim):
            return ("AL_FALSE", "B", reasons + (["price:nominal_or_low"] if amt <= 100 else ["price:low_with_quitclaim"]))
        if arms_hits and not quitclaim:
            return ("AL_TRUE", "B", reasons + arms_hits + ["price:present"])
        return ("AL_UNKNOWN", "B", reasons + (arms_hits if arms_hits else []) + ["price:present_ambiguous"])
    else:
        if any(r in ("instrument:gift","instrument:divorce","instrument:executor_estate","instrument:administrator_estate","instrument:trustee") for r in reasons):
            return ("AL_FALSE", "B", reasons + ["price:missing"])
        base = reasons + (arms_hits if arms_hits else []) + ["price:missing"]
        return ("AL_UNKNOWN", "C", base)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    class_counts={}
    conf_counts={}
    reason_counts={}
    total=0

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            total += 1
            row = json.loads(line)
            al_class, conf, reasons = classify_row(row)
            row["arms_length"] = {
                "class": al_class,
                "confidence": conf,
                "reasons": reasons[:12],
                "rules_version": RULES_VERSION
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            class_counts[al_class] = class_counts.get(al_class, 0) + 1
            conf_counts[conf] = conf_counts.get(conf, 0) + 1
            for r in reasons:
                reason_counts[r] = reason_counts.get(r, 0) + 1

    top_reasons = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:30]
    audit = {
        "rules_version": RULES_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "infile": args.infile,
        "out": args.out,
        "total": total,
        "class_counts": class_counts,
        "confidence_counts": conf_counts,
        "top_reasons": top_reasons,
    }
    with open(args.audit, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, indent=2, ensure_ascii=False)

    print(json.dumps(audit, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
