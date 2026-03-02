import argparse, json, re
from collections import Counter

NON_ARMS_KW = [
  "GIFT","LOVE","AFFECTION","FAMILY","NOMINAL","TRUSTEE","ESTATE","HEIR",
  "DIVORCE","FORECLOSURE","SHERIFF","EXECUTOR","ADMINISTRATOR","DEED IN LIEU",
  "TAX TAKING","CONSERVATOR","GUARDIAN"
]

def classify(ev):
    cons = ev.get("consideration") or {}
    amt = cons.get("amount")
    txt = (cons.get("text_raw") or "") + " " + ((ev.get("document") or {}).get("description_raw") or "") + " " + ((ev.get("document") or {}).get("raw_block") or "")
    up = txt.upper()

    if cons.get("nominal_flag") is True:
        return "NON_ARMS_LENGTH", ["NOMINAL_FLAG"]

    hits = [k for k in NON_ARMS_KW if k in up]
    if hits:
        return "NON_ARMS_LENGTH", ["KW:"+",".join(hits[:5])]

    if isinstance(amt, (int,float)):
        if amt <= 100:
            return "NON_ARMS_LENGTH", ["LOW_AMOUNT<=100"]
        if amt >= 1000:
            return "ARMS_LENGTH", ["AMOUNT>=1000"]
        return "UNKNOWN", ["MID_AMOUNT"]
    return "UNKNOWN", ["NO_AMOUNT"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--audit", dest="audit", required=True)
    args = ap.parse_args()

    c = Counter()
    with open(args.out, "w", encoding="utf-8") as out:
        with open(args.inp, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                ev = json.loads(line)
                cls, reasons = classify(ev)
                ev["arms_length"] = {"class": cls, "reasons": reasons}
                c[cls] += 1
                out.write(json.dumps(ev, ensure_ascii=False) + "\n")

    with open(args.audit, "w", encoding="utf-8") as a:
        json.dump({"in": args.inp, "out": args.out, "counts": dict(c)}, a, indent=2)

    print("[done] arms_length counts:", dict(c))
    print("[done] audit:", args.audit)

if __name__ == "__main__":
    main()
