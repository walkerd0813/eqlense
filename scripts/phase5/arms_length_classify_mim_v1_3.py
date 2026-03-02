import argparse, json, re, time
from collections import Counter, defaultdict

NONARMS_DOC_HINTS = [
    r"\bCONFIRM", r"\bCONFIRMATORY\b",
    r"\bCORRECTION\b", r"\bSCRIVENER\b",
    r"\bRE[- ]?RECORD", r"\bRECORD(ED)?\b.*\bAGAIN\b"
]

GOV_HINTS = [r"\bCITY OF\b", r"\bTOWN OF\b", r"\bCOMMONWEALTH\b", r"\bSTATE OF\b", r"\bUNITED STATES\b"]

def norm_str(x):
    return "" if x is None else str(x)

def text_has_any(s, patterns):
    s = s or ""
    for p in patterns:
        if re.search(p, s, flags=re.I):
            return True
    return False

def get_doc_text(ev):
    # registry truth fields (preferred)
    return " | ".join([
        norm_str(ev.get("doc_type_code")),
        norm_str(ev.get("doc_type_desc"))
    ]).strip()

def get_cons(ev):
    c = ev.get("consideration") or {}
    amt = c.get("amount")
    if isinstance(amt, float):
        amt = int(amt)
    if isinstance(amt, int):
        amount = amt
    else:
        amount = None
    parse_status = c.get("parse_status") or "MISSING"
    flags = c.get("flags") or []
    raw_text = c.get("raw_text")
    return amount, parse_status, flags, raw_text

def get_attach(ev):
    a = ev.get("attach") or {}
    return (a.get("status") or "UNKNOWN",
            a.get("property_id"),
            a.get("confidence"))

def party_stats(ev):
    parties = ev.get("parties") or []
    roles = defaultdict(int)
    has_gov = False
    has_grantor = False
    has_grantee = False

    # We trust normalized roles if present; otherwise best-effort
    for p in parties:
        role = (p.get("role") or "UNKNOWN").upper()
        roles[role] += 1
        et = (p.get("entity_type") or "UNKNOWN").upper()
        if et == "GOV":
            has_gov = True
        if role == "GRANTOR":
            has_grantor = True
        if role == "GRANTEE":
            has_grantee = True

        # fallback: detect gov by text if entity_type missing
        nm = (p.get("name_norm") or p.get("name_raw") or "")
        if not has_gov and text_has_any(nm, GOV_HINTS):
            has_gov = True

    parse_quality = "LOW"
    if len(parties) >= 2:
        parse_quality = "MED"
    if has_grantor and has_grantee and len(parties) >= 2:
        parse_quality = "HIGH"

    return parse_quality, has_gov, has_grantor, has_grantee, dict(roles)

def classify_one(ev, min_amount):
    amount, parse_status, flags, raw_text = get_cons(ev)
    attach_status, property_id, attach_conf = get_attach(ev)
    doc_text = get_doc_text(ev)
    pq, has_gov, has_grantor, has_grantee, roles = party_stats(ev)

    reasons = []
    cls = "UNKNOWN"
    conf = "LOW"

    # ---- hard NON_ARMS triggers (highest priority) ----
    if isinstance(flags, list) and "ZERO_OR_NOMINAL" in flags:
        cls = "NON_ARMS_LENGTH"
        reasons.append("NOMINAL_OR_ZERO_CONSIDERATION")
        conf = "HIGH"
    elif amount is not None and (amount in (0, 1, 10, 100) or amount < min_amount):
        cls = "NON_ARMS_LENGTH"
        reasons.append("NOMINAL_OR_ZERO_CONSIDERATION")
        conf = "HIGH"

    if cls == "UNKNOWN":
        if isinstance(flags, list) and any(f in flags for f in ["LOVE_AND_AFFECTION", "GIFT"]):
            cls = "NON_ARMS_LENGTH"
            reasons.append("GIFT_LOVE_AND_AFFECTION")
            conf = "HIGH"
        elif isinstance(flags, list) and "FAMILY_TRANSFER" in flags:
            cls = "NON_ARMS_LENGTH"
            reasons.append("FAMILY_TRANSFER_INDICATOR")
            conf = "HIGH"

    if cls == "UNKNOWN":
        if text_has_any(doc_text, NONARMS_DOC_HINTS):
            cls = "NON_ARMS_LENGTH"
            reasons.append("CONFIRMATORY_DOC_TYPE")
            conf = "HIGH"

    # ---- missing/ambiguous consideration => UNKNOWN (never default NON_ARMS) ----
    if cls == "UNKNOWN":
        if parse_status in ("MISSING", "AMBIGUOUS", "NON_NUMERIC") or amount is None:
            reasons.append("MISSING_CONSIDERATION" if parse_status == "MISSING" else "AMBIGUOUS_CONSIDERATION_TEXT")
            # confidence remains LOW unless doc type gives a stronger hint
            conf = "LOW"

    # ---- ARMS trigger (only if we have market-like price and no hard non-arms triggers) ----
    if cls == "UNKNOWN":
        if amount is not None and amount >= min_amount:
            cls = "ARMS_LENGTH"
            reasons.append("CONSIDERATION_GE_THRESHOLD")

            # confidence grading
            if attach_status in ("ATTACHED_A", "ATTACHED_B") and property_id:
                reasons.append("ATTACH_ATTACHED")
                if has_grantor and has_grantee:
                    reasons.append("PARTIES_PRESENT")
                    conf = "HIGH"
                else:
                    conf = "MED"
            else:
                reasons.append("ATTACH_NOT_ATTACHED")
                conf = "MED" if pq in ("MED", "HIGH") else "LOW"

    # ---- gov transfer weak signal (does not override market-like price) ----
    if has_gov:
        reasons.append("GOV_PARTY_PRESENT")
        if cls == "UNKNOWN" and amount is not None and amount < min_amount:
            cls = "NON_ARMS_LENGTH"
            reasons.append("GOV_TRANSFER")
            conf = "MED"

    # ---- final inputs summary ----
    inputs_summary = {
        "amount_present": amount is not None,
        "amount": amount,
        "parse_status": parse_status,
        "flags": flags,
        "attach_status": attach_status,
        "party_parse_quality": pq,
        "has_grantor": has_grantor,
        "has_grantee": has_grantee,
        "doc_type_desc": ev.get("doc_type_desc"),
        "doc_type_code": ev.get("doc_type_code")
    }

    out = ev
    out["arms_length"] = {
        "class": cls,
        "confidence": conf,
        "reasons": sorted(set(reasons)),
        "thresholds_used": {"min_amount": min_amount},
        "inputs_summary": inputs_summary
    }
    return out, cls, conf, reasons

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--min_amount", type=int, default=10000)
    args = ap.parse_args()

    t0 = time.time()
    stats = Counter()
    top_reasons = Counter()

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)

            # Only classify deed-like events here; others can be left without arms_length or handled later.
            et = (ev.get("event_type") or "").upper()
            if et not in ("DEED", "FORECLOSURE_DEED"):
                # passthrough
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                stats["total_passthrough_non_deed"] += 1
                continue

            ev2, cls, conf, reasons = classify_one(ev, args.min_amount)
            fout.write(json.dumps(ev2, ensure_ascii=False) + "\n")

            stats["total_classified"] += 1
            stats[f"class:{cls}"] += 1
            stats[f"conf:{conf}"] += 1
            for r in set(reasons):
                top_reasons[r] += 1

    audit = {
        "script": "arms_length_classify_mim_v1_3",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "infile": args.infile,
        "out": args.out,
        "min_amount": args.min_amount,
        "stats": dict(stats),
        "top_reasons": top_reasons.most_common(30),
        "elapsed_s": round(time.time() - t0, 3)
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:", args.out)
    print("[done] audit:", args.audit)
    print("classified:", stats.get("total_classified", 0), "passthrough_non_deed:", stats.get("total_passthrough_non_deed", 0))
    print("class_counts:", {k:v for k,v in stats.items() if k.startswith("class:")})

if __name__ == "__main__":
    main()
