#!/usr/bin/env python3
"""Arms-length classifier (conservative, auditable).

Input: NDJSON of registry events already attached to property spine.
Output: NDJSON with added fields:
  transaction_semantics.arms_length = {
      "class": "ARMS_LENGTH"|"NON_ARMS_LENGTH"|"UNKNOWN",
      "confidence": "A"|"B"|"C",
      "reasons": [..],
      "rules_version": "v1_0"
  }
Also writes a JSON audit with counts and top reasons.

Design goals:
- Deterministic & rerunnable
- Conservative (avoid false ARMS_LENGTH)
- Evidence-first: reasons + rule ids

NOTE: This v1_0 uses only fields present in the NDJSON (instrument_type, parties names, consideration if present).
When consideration becomes available, extend with price/ratio rules as v1_1.
"""

import argparse
import json
import re
from collections import Counter, defaultdict

RULES_VERSION = "v1_0"

RE_SPACE = re.compile(r"\s+")

# --- helpers ---

def s_norm(x):
    if x is None:
        return ""
    if isinstance(x, (int, float)):
        return str(x)
    if isinstance(x, dict):
        # sometimes callers pass entire objects accidentally
        return json.dumps(x, ensure_ascii=False, sort_keys=True)
    return RE_SPACE.sub(" ", str(x)).strip().upper()


def get_instrument(r):
    # try a few likely paths
    doc = r.get("document") or {}
    inst = doc.get("instrument_type") or doc.get("instrument") or doc.get("instrument_type_raw")
    return s_norm(inst)


def extract_parties(r):
    # expects something like r['parties'] with grantor/grantee arrays or dicts
    parties = r.get("parties")
    names = []
    if isinstance(parties, dict):
        for k in ("grantor", "grantee", "grantors", "grantees", "seller", "buyer"):
            v = parties.get(k)
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, str):
                        names.append(it)
                    elif isinstance(it, dict):
                        nm = it.get("name") or it.get("raw") or it.get("party")
                        if nm:
                            names.append(nm)
            elif isinstance(v, str):
                names.append(v)
    elif isinstance(parties, list):
        for it in parties:
            if isinstance(it, str):
                names.append(it)
            elif isinstance(it, dict):
                nm = it.get("name") or it.get("raw")
                if nm:
                    names.append(nm)
    names = [s_norm(n) for n in names if n]
    return names


def get_consideration_amount(r):
    cons = r.get("consideration")
    if isinstance(cons, dict):
        # common keys
        for k in ("amount", "amount_num", "consideration", "value"):
            v = cons.get(k)
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, str):
                vv = re.sub(r"[^0-9.]", "", v)
                try:
                    return float(vv) if vv else None
                except Exception:
                    return None
    if isinstance(cons, (int, float)):
        return float(cons)
    if isinstance(cons, str):
        vv = re.sub(r"[^0-9.]", "", cons)
        try:
            return float(vv) if vv else None
        except Exception:
            return None
    return None


# --- rule engine ---

# strong NON-arms-length instrument keywords
NON_AL_INSTRUMENT_PATTERNS = [
    ("R_NAL_FORECLOSURE", re.compile(r"\b(FORECLOSURE|SHERIFF|SHERIFF'S|REO)\b")),
    ("R_NAL_TAX", re.compile(r"\b(TAX|TAX\s+FORECLOSURE|TAX\s+TAKING|TAX\s+COLLECTOR)\b")),
    ("R_NAL_ESTATE", re.compile(r"\b(ESTATE|EXECUTOR|EXECUTRIX|ADMINISTRATOR|ADMINISTRATRIX|PROBATE)\b")),
    ("R_NAL_DIVORCE", re.compile(r"\b(DIVORCE|SEPARATION|MARITAL|DOMESTIC)\b")),
    ("R_NAL_GIFT", re.compile(r"\b(GIFT|LOVE\s+AND\s+AFFECTION)\b")),
    ("R_NAL_FAMILY", re.compile(r"\b(HEIR|HEIRS|FAMILY)\b")),
    ("R_NAL_CONFIRM", re.compile(r"\b(CONFIRMATION|CONFIRMATORY|CORRECTIVE)\b")),
    ("R_NAL_TRUST_DISTRIBUTION", re.compile(r"\b(TRUSTEE|TRUST)\b.*\b(DISTRIBUTION|ASSIGNMENT|TRANSFER)\b")),
]

# weak signals (do not force NON_AL; reduce confidence)
WEAK_SIGNALS = [
    ("R_WEAK_QUITCLAIM", re.compile(r"\bQUIT\s*CLAIM\b")),
    ("R_WEAK_TRUST", re.compile(r"\bTRUST\b")),
    ("R_WEAK_DEED", re.compile(r"\bDEED\b")),
]

# party-name patterns that often indicate NON_AL (still conservative)
PARTY_NONAL_PATTERNS = [
    ("R_NAL_BANK", re.compile(r"\b(BANK|N\.A\.|NATIONAL\s+ASSOCIATION|MORTGAGE|FEDERAL\s+NATIONAL\s+MORTGAGE|FANNIE\s+MAE|FREDDIE\s+MAC|HUD|FNMA|FHLMC)\b")),
    ("R_NAL_CITY_STATE", re.compile(r"\b(CITY\s+OF|TOWN\s+OF|COMMONWEALTH\s+OF|STATE\s+OF|UNITED\s+STATES)\b")),
]


def classify_row(r):
    inst = get_instrument(r)
    parties = extract_parties(r)
    cons_amt = get_consideration_amount(r)

    reasons = []
    nonal_hits = 0

    # 1) consideration-based (if present) — very strong
    if cons_amt is not None:
        if cons_amt <= 100:
            reasons.append("R_NAL_NOMINAL_CONSIDERATION")
            nonal_hits += 2
        elif cons_amt <= 1000:
            reasons.append("R_WEAK_LOW_CONSIDERATION")

    # 2) instrument patterns
    for rid, pat in NON_AL_INSTRUMENT_PATTERNS:
        if inst and pat.search(inst):
            reasons.append(rid)
            nonal_hits += 2

    # 3) parties patterns
    joined = " | ".join(parties)
    for rid, pat in PARTY_NONAL_PATTERNS:
        if joined and pat.search(joined):
            reasons.append(rid)
            nonal_hits += 1

    # 4) weak signals
    weak = []
    for rid, pat in WEAK_SIGNALS:
        if inst and pat.search(inst):
            weak.append(rid)

    # Decision (conservative):
    # - If strong non-al signals => NON_ARMS_LENGTH (A/B)
    # - Else if no strong signals and we have some normal-looking deed => ARMS_LENGTH (C) ONLY if we have consideration >= 10k
    # - Otherwise UNKNOWN

    if nonal_hits >= 2:
        cls = "NON_ARMS_LENGTH"
        conf = "A" if nonal_hits >= 4 else "B"
    else:
        # only allow ARMS_LENGTH if we have a real dollar amount and it's not nominal
        if cons_amt is not None and cons_amt >= 10000 and not reasons:
            cls = "ARMS_LENGTH"
            conf = "B" if not weak else "C"
            reasons.extend(weak)
        else:
            cls = "UNKNOWN"
            conf = "C"
            reasons.extend(weak)

    # de-dupe reasons but keep order
    seen = set()
    reasons2 = []
    for x in reasons:
        if x not in seen:
            reasons2.append(x)
            seen.add(x)

    return {
        "class": cls,
        "confidence": conf,
        "reasons": reasons2,
        "rules_version": RULES_VERSION,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="Input NDJSON")
    ap.add_argument("--out", required=True, help="Output NDJSON")
    ap.add_argument("--audit", required=True, help="Output audit JSON")
    args = ap.parse_args()

    counts = Counter()
    reason_counts = Counter()
    conf_counts = Counter()

    total = 0

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            al = classify_row(r)

            ts = r.get("transaction_semantics")
            if not isinstance(ts, dict):
                ts = {}
            ts["arms_length"] = al
            r["transaction_semantics"] = ts

            fout.write(json.dumps(r, ensure_ascii=False) + "\n")

            total += 1
            counts[al["class"]] += 1
            conf_counts[al["confidence"]] += 1
            for rr in al.get("reasons", []):
                reason_counts[rr] += 1

    audit = {
        "rules_version": RULES_VERSION,
        "infile": args.infile,
        "out": args.out,
        "total": total,
        "class_counts": dict(counts),
        "confidence_counts": dict(conf_counts),
        "top_reasons": reason_counts.most_common(50),
    }

    with open(args.audit, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, ensure_ascii=False, indent=2)

    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
