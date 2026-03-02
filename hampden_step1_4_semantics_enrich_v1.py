#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

NOMINAL_THRESH = 100.00
RE_NUM = re.compile(r'(?<!\d)(\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d+(?:\.\d{2})?)(?!\d)')
TRUST_TOKENS = re.compile(r'\b(TRUST|TRS|TRUSTEE|REVOCABLE|IRREVOCABLE|LIVING TRUST)\b', re.I)
ESTATE_TOKENS = re.compile(r'\b(ESTATE|EXECUTOR|ADMINISTRATOR|PERSONAL REPRESENTATIVE|PR)\b', re.I)
BANK_TOKENS = re.compile(r'\b(BANK|N\.A\.|N A|MORTGAGE|FINANCIAL|CREDIT UNION|FUND|TRUST COMPANY)\b', re.I)
GOV_TOKENS = re.compile(r'\b(CITY OF|TOWN OF|COMMONWEALTH|STATE OF|UNITED STATES|COUNTY OF|HOUSING AUTHORITY)\b', re.I)
LLC_TOKENS = re.compile(r'\b(LLC|L\.L\.C\.|INC|CORP|CO\.|LP|L\.P\.|LLP|L\.L\.P\.|LIMITED PARTNERSHIP)\b', re.I)

def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def parse_amount(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    m = RE_NUM.search(s.replace("$",""))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",",""))
    except Exception:
        return None

def party_name_tokens(parties: List[Dict[str, Any]]) -> str:
    parts=[]
    for p in parties or []:
        name = (p.get("name") or p.get("party_name") or "").strip()
        if name:
            parts.append(name)
    return " | ".join(parts)

def classify_deed(e: Dict[str, Any]) -> Tuple[str, float, List[str]]:
    rules=[]
    cons = e.get("consideration") or {}
    amt = parse_amount(cons.get("amount")) or parse_amount(cons.get("text_raw")) or parse_amount(e.get("consideration_amount")) or parse_amount(e.get("consideration"))
    nominal = False
    if amt is not None and amt <= NOMINAL_THRESH:
        nominal = True
        rules.append("nominal_consideration_lte_100")

    parties = e.get("parties") or []
    tokens = party_name_tokens(parties)
    trust = bool(TRUST_TOKENS.search(tokens))
    estate = bool(ESTATE_TOKENS.search(tokens))
    bank = bool(BANK_TOKENS.search(tokens))
    gov = bool(GOV_TOKENS.search(tokens))
    llc = bool(LLC_TOKENS.search(tokens))

    if estate: rules.append("estate_tokens_in_parties")
    if trust: rules.append("trust_tokens_in_parties")
    if bank: rules.append("bank_tokens_in_parties")
    if gov: rules.append("gov_tokens_in_parties")
    if llc: rules.append("entity_tokens_in_parties")

    if nominal and (trust or estate or gov):
        return ("related_party_transfer", 0.80, rules)
    if nominal:
        return ("related_party_transfer", 0.65, rules)

    if trust or estate:
        if amt is None:
            return ("internal_restructure", 0.70, rules)
        else:
            return ("internal_restructure", 0.60, rules)

    if amt is not None:
        rules.append("consideration_present_non_nominal")
        return ("arms_length_sale", 0.70, rules)

    return ("unknown", 0.40, rules)

def enrich_event(e: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    doc_type = ((e.get("document") or {}).get("document_type") or e.get("doc_type") or "").upper()
    if "document" not in e or not isinstance(e.get("document"), dict):
        e["document"] = {}
    if not e["document"].get("document_type") and doc_type:
        e["document"]["document_type"] = doc_type

    if "consideration" not in e or not isinstance(e.get("consideration"), dict):
        legacy_amt = e.get("consideration_amount")
        legacy_txt = e.get("consideration_text_raw") or e.get("consideration")
        e["consideration"] = {"text_raw": legacy_txt, "amount": legacy_amt}

    amt = parse_amount(e["consideration"].get("amount")) or parse_amount(e["consideration"].get("text_raw"))
    if amt is not None:
        e["consideration"]["amount"] = amt
        e["consideration"].setdefault("currency","USD")
    e["consideration"]["nominal_flag"] = bool(amt is not None and amt <= NOMINAL_THRESH)

    if table_name == "foreclosure_events":
        tx_class, conf, rules = ("distress_transfer", 0.95, ["table_foreclosure_events"])
    elif table_name in ("lien_events","lis_pendens_events"):
        tx_class, conf, rules = ("distress_transfer", 0.85, [f"table_{table_name}"])
    elif table_name in ("mortgage_events","assignment_events","release_events"):
        tx_class, conf, rules = ("unknown", 0.50, [f"table_{table_name}"])
    else:
        tx_class, conf, rules = classify_deed(e)

    e["transaction_semantics"] = {
        "tx_class": tx_class,
        "confidence_score": float(conf),
        "rules_fired": rules
    }

    e.setdefault("audit", {})
    if isinstance(e["audit"], dict):
        e["audit"].setdefault("semantics_version", "hampden_step1_4_semantics_v1")
        e["audit"].setdefault("semantics_as_of_utc", utc_now())
    return e

def ndjson_iter(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
            except Exception:
                yield {"_raw_line": line, "_parse_error": True}

def write_ndjson(path: str, rows: List[Dict[str, Any]]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inDir", required=True)
    ap.add_argument("--outDir", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    tables = [
        "deed_events.ndjson",
        "mortgage_events.ndjson",
        "assignment_events.ndjson",
        "lien_events.ndjson",
        "release_events.ndjson",
        "lis_pendens_events.ndjson",
        "foreclosure_events.ndjson",
    ]

    counts = {}
    tx_counts = {}
    for fname in tables:
        in_path = os.path.join(args.inDir, fname)
        if not os.path.exists(in_path):
            continue
        table_name = fname.replace(".ndjson","")
        out_path = os.path.join(args.outDir, fname)
        out_rows=[]
        for e in ndjson_iter(in_path):
            out_rows.append(enrich_event(e, table_name))
        write_ndjson(out_path, out_rows)
        counts[table_name] = len(out_rows)
        if table_name == "deed_events":
            for r in out_rows:
                c = (r.get("transaction_semantics") or {}).get("tx_class","unknown")
                tx_counts[c]=tx_counts.get(c,0)+1

    audit = {
        "created_at_utc": utc_now(),
        "inDir": args.inDir,
        "outDir": args.outDir,
        "tables_written": counts,
        "deed_tx_class_counts": tx_counts
    }
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote tables:", counts)
    if tx_counts:
        print("[done] deed_tx_class_counts:", tx_counts)
    print("[done] audit:", args.audit)

if __name__ == "__main__":
    main()
