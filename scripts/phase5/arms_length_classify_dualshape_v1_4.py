import argparse, json, re, time
from collections import Counter, defaultdict

GOV_HINTS = [r"\bCITY OF\b", r"\bTOWN OF\b", r"\bCOMMONWEALTH\b", r"\bSTATE OF\b", r"\bUNITED STATES\b"]

CONFIRM_CORR_HINTS = [
    r"\bCONFIRM(ATORY)?\b",
    r"\bCORRECTION\b",
    r"\bSCRIVENER\b",
    r"\bRE[- ]?RECORD\b",
    r"\bCONFIRMING\b"
]

def norm_str(x):
    return "" if x is None else str(x)

def text_has_any(s, patterns):
    s = s or ""
    for p in patterns:
        if re.search(p, s, flags=re.I):
            return True
    return False

def to_int(x):
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

def get_doc_text(ev):
    # Prefer normalized doc_type fields if present
    dt = " | ".join([norm_str(ev.get("doc_type_code")), norm_str(ev.get("doc_type_desc"))]).strip()
    if dt.strip("| ").strip():
        return dt

    # Legacy fallback: document fields
    doc = ev.get("document") or {}
    legacy = " | ".join([
        norm_str(doc.get("index_doc_type")),
        norm_str(doc.get("instrument_type")),
        norm_str(doc.get("document_type")),
        norm_str(doc.get("doc_type")),
        norm_str(doc.get("raw_block"))[:2000],
    ]).strip()
    return legacy

def get_cons(ev):
    # Prefer canonical consideration.*
    c = ev.get("consideration") or {}
    amt = to_int(c.get("amount"))
    parse_status = c.get("parse_status") or ("PARSED" if amt is not None else "MISSING")
    flags = c.get("flags") or []
    raw_text = c.get("raw_text")

    # Fallback
    if amt is None:
        ts = ev.get("transaction_semantics") or {}
        amt = to_int(ts.get("price_amount"))
        if amt is not None:
            parse_status = "PARSED"

    return amt, parse_status, flags, raw_text

def get_attach(ev):
    a = ev.get("attach") or {}
    status = a.get("status")
    pid = a.get("property_id")

    if not status:
        status = ev.get("attach_status") or a.get("attach_status") or "UNKNOWN"
    if not pid:
        pid = ev.get("property_id") or a.get("property_id")

    return status, pid

def party_stats(ev):
    parties = ev.get("parties")
    roles = defaultdict(int)
    has_gov = False
    has_grantor = False
    has_grantee = False

    # MIM-like list of rows
    if isinstance(parties, list):
        for p in parties:
            if not isinstance(p, dict):
                continue
            role = (p.get("role") or "UNKNOWN").upper()
            roles[role] += 1
            if role == "GRANTOR":
                has_grantor = True
            if role == "GRANTEE":
                has_grantee = True
            et = (p.get("entity_type") or "UNKNOWN").upper()
            if et == "GOV":
                has_gov = True
            nm = (p.get("name_norm") or p.get("name_raw") or "")
            if not has_gov and text_has_any(nm, GOV_HINTS):
                has_gov = True

    # Legacy dict style
    if not isinstance(parties, list):
        p = parties if isinstance(parties, dict) else {}
        for nm in (p.get("grantor") or []):
            roles["GRANTOR"] += 1
            has_grantor = True
            if not has_gov and text_has_any(str(nm or ""), GOV_HINTS):
                has_gov = True
        for nm in (p.get("grantee") or []):
            roles["GRANTEE"] += 1
            has_grantee = True
            if not has_gov and text_has_any(str(nm or ""), GOV_HINTS):
                has_gov = True

        # if we stored party_lines_raw during header normalization
        for ln in (p.get("party_lines_raw") or []):
            roles["UNKNOWN"] += 1
            if not has_gov and text_has_any(str(ln or ""), GOV_HINTS):
                has_gov = True

    total_parties = sum(roles.values())
    pq = "LOW"
    if total_parties >= 2:
        pq = "MED"
    if has_grantor and has_grantee and total_parties >= 2:
        pq = "HIGH"

    return pq, has_gov, has_grantor, has_grantee, dict(roles)

def classify_one(ev, min_amount, nominal_max, low_band_min):
    amount, parse_status, flags, raw_text = get_cons(ev)
    attach_status, property_id = get_attach(ev)
    doc_text = get_doc_text(ev)
    pq, has_gov, has_grantor, has_grantee, roles = party_stats(ev)

    reasons = []
    cls = "UNKNOWN"
    conf = "LOW"

    # ---------- STRUCTURAL NON-ARMS ----------
    # 1) explicit consideration flags
    if isinstance(flags, list):
        if any(f in flags for f in ["GIFT", "LOVE_AND_AFFECTION"]):
            cls = "NON_ARMS_LENGTH"
            reasons.append("GIFT_LOVE_AND_AFFECTION")
            conf = "HIGH"
        if "CONSIDERATION_NOT_STATED" in flags:
            cls = "NON_ARMS_LENGTH"
            reasons.append("CONSIDERATION_NOT_STATED")
            conf = "HIGH"

    # 2) confirmatory/correction doc types
    if cls == "UNKNOWN" and text_has_any(doc_text, CONFIRM_CORR_HINTS):
        cls = "NON_ARMS_LENGTH"
        reasons.append("CONFIRMATORY_DOC_TYPE")
        conf = "HIGH"

    # 3) nominal/very small amounts
    if cls == "UNKNOWN" and amount is not None:
        if amount in (0, 1, 10, 100) or amount < nominal_max:
            cls = "NON_ARMS_LENGTH"
            reasons.append("NOMINAL_OR_ZERO_CONSIDERATION")
            conf = "HIGH"

    # ---------- LOW-BAND AMOUNTS ----------
    # If it's not nominal, but still below min_amount, keep UNKNOWN (do NOT call it non-arms)
    if cls == "UNKNOWN" and amount is not None:
        if low_band_min <= amount < min_amount:
            cls = "UNKNOWN"
            reasons.append("LOW_CONSIDERATION_BELOW_THRESHOLD")
            conf = "LOW"

    # ---------- MISSING CONSIDERATION ----------
    if cls == "UNKNOWN":
        if parse_status in ("MISSING", "AMBIGUOUS", "NON_NUMERIC") or amount is None:
            reasons.append("MISSING_CONSIDERATION" if parse_status == "MISSING" else "AMBIGUOUS_CONSIDERATION_TEXT")
            conf = "LOW"

    # ---------- ARMS ----------
    if cls == "UNKNOWN" and amount is not None and amount >= min_amount:
        cls = "ARMS_LENGTH"
        reasons.append("CONSIDERATION_GE_THRESHOLD")
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

    # gov signal (tag only; don't override unless already NON_ARMS by other reasons)
    if has_gov:
        reasons.append("GOV_PARTY_PRESENT")

    ev["arms_length"] = {
        "class": cls,
        "confidence": conf,
        "reasons": sorted(set(reasons)),
        "thresholds_used": {"min_amount": min_amount, "nominal_max": nominal_max, "low_band_min": low_band_min},
        "inputs_summary": {
            "amount_present": amount is not None,
            "amount": amount,
            "parse_status": parse_status,
            "flags": flags,
            "attach_status": attach_status,
            "party_parse_quality": pq,
            "has_grantor": has_grantor,
            "has_grantee": has_grantee,
        }
    }
    return ev, cls, conf, reasons

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--min_amount", type=int, default=10000)
    ap.add_argument("--nominal_max", type=int, default=1000)
    ap.add_argument("--low_band_min", type=int, default=1000)
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

            et = (ev.get("event_type") or "").upper()
            if et not in ("DEED", "FORECLOSURE_DEED"):
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                stats["total_passthrough_non_deed"] += 1
                continue

            ev2, cls, conf, reasons = classify_one(ev, args.min_amount, args.nominal_max, args.low_band_min)
            fout.write(json.dumps(ev2, ensure_ascii=False) + "\n")

            stats["total_classified"] += 1
            stats[f"class:{cls}"] += 1
            stats[f"conf:{conf}"] += 1
            for r in set(reasons):
                top_reasons[r] += 1

    audit = {
        "script": "arms_length_classify_dualshape_v1_4",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "infile": args.infile,
        "out": args.out,
        "min_amount": args.min_amount,
        "nominal_max": args.nominal_max,
        "low_band_min": args.low_band_min,
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
