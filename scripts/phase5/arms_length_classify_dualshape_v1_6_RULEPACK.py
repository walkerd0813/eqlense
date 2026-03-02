import argparse, json, re, time
from collections import Counter

RE_CONFIRM = re.compile(r"\bCONFIRM(ATORY|ING)?\b", re.I)
RE_CORRECTION = re.compile(r"\bCORRECTION\b|\bSCRIVENER\b|\bRE[- ]?RECORD\b", re.I)
RE_GIFT = re.compile(r"\bLOVE AND AFFECTION\b|\bGIFT\b", re.I)
RE_NOT_STATED = re.compile(r"\bCONSIDERATION NOT STATED\b|\bNOT STATED\b|\bNO CONSIDERATION\b", re.I)

RE_FORECLOSURE = re.compile(r"\bFORECLOS(URE|ING)\b|\bREO\b|\bSHERIFF\b|\bDEED IN LIEU\b|\bNOTICE OF SALE\b", re.I)
RE_TAX = re.compile(r"\bTAX TAKING\b|\bTREASURER\b|\bCOLLECTOR OF TAXES\b", re.I)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--min_amount", type=int, default=10000)
    ap.add_argument("--nominal_max", type=int, default=1000)
    args = ap.parse_args()

    total=0; wrote=0
    class_counts = Counter()
    reason_counts = Counter()
    conf_counts = Counter()

    t0=time.time()

    with open(args.infile,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line: continue
            total += 1
            ev = json.loads(line)

            # baseline inputs
            c = ev.get("consideration") or {}
            amt = c.get("amount")
            flags = c.get("flags") or []
            parse_status = c.get("parse_status")

            doc = ev.get("document") or {}
            raw_block = doc.get("raw_block") or ""

            attach_status = ev.get("attach_status") or (ev.get("attach") or {}).get("status") or "UNKNOWN"

            reasons = []
            cls = "UNKNOWN"
            conf = "LOW"

            # 1) Text-based HIGH confidence NON_ARMS
            if raw_block:
                if RE_CONFIRM.search(raw_block):
                    cls = "NON_ARMS_LENGTH"; conf="HIGH"; reasons.append("CONFIRMATORY_DOC_TYPE")
                elif RE_CORRECTION.search(raw_block):
                    cls = "NON_ARMS_LENGTH"; conf="HIGH"; reasons.append("CORRECTION_OR_SCRIVENER_DOC")
                elif RE_GIFT.search(raw_block):
                    cls = "NON_ARMS_LENGTH"; conf="HIGH"; reasons.append("GIFT_LOVE_AND_AFFECTION")
                else:
                    # per your rule: "not stated" should not force NON_ARMS by default
                    if RE_NOT_STATED.search(raw_block):
                        reasons.append("CONSIDERATION_NOT_STATED_TEXT")

                # forced-sale style cues
                if cls == "UNKNOWN" and (RE_FORECLOSURE.search(raw_block) or RE_TAX.search(raw_block)):
                    cls = "NON_ARMS_LENGTH"; conf="HIGH"; reasons.append("FORECLOSURE_OR_FORCED_SALE_CUE")

            # 2) Nominal consideration rule (HIGH)
            if cls == "UNKNOWN":
                if "ZERO_OR_NOMINAL" in flags:
                    cls = "NON_ARMS_LENGTH"; conf="HIGH"; reasons.append("NOMINAL_OR_ZERO_CONSIDERATION")

            # 3) Price threshold rule
            if cls == "UNKNOWN":
                if isinstance(amt, int):
                    if amt >= args.min_amount:
                        cls = "ARMS_LENGTH"
                        # confidence: better if attached
                        conf = "MED" if attach_status == "ATTACHED_A" else "LOW"
                        reasons.append("CONSIDERATION_GE_THRESHOLD")
                        if attach_status == "ATTACHED_A":
                            reasons.append("ATTACH_ATTACHED_A")
                    else:
                        # below threshold but non-nominal: keep UNKNOWN (not automatically non-arms)
                        reasons.append("CONSIDERATION_PRESENT_BELOW_THRESHOLD")
                else:
                    if parse_status == "MISSING":
                        reasons.append("MISSING_CONSIDERATION")
                    else:
                        reasons.append("CONSIDERATION_UNPARSED")

            ev["arms_length"] = {
                "class": cls,
                "confidence": conf,
                "reasons": reasons,
                "thresholds_used": {
                    "min_amount": args.min_amount,
                    "nominal_max": args.nominal_max
                },
                "inputs_summary": {
                    "amount_present": isinstance(amt, int),
                    "amount": amt,
                    "parse_status": parse_status,
                    "flags": flags,
                    "attach_status": attach_status,
                    "has_raw_block": bool(raw_block)
                }
            }

            class_counts[f"class:{cls}"] += 1
            conf_counts[conf] += 1
            for r in reasons:
                reason_counts[r] += 1

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
            wrote += 1

    audit = {
        "script": "arms_length_classify_dualshape_v1_6_RULEPACK",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "infile": args.infile,
        "out": args.out,
        "min_amount": args.min_amount,
        "total": total,
        "written": wrote,
        "class_counts": dict(class_counts),
        "confidence_counts": dict(conf_counts),
        "top_reasons": reason_counts.most_common(20),
        "elapsed_s": round(time.time()-t0,3)
    }

    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("classified:", wrote)
    print("class_counts:", dict(class_counts))

if __name__=="__main__":
    main()
