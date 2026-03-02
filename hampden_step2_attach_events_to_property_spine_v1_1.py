import argparse, json, os, re
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, Tuple, Optional, List

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def read_first_char(path: str) -> str:
    with open(path, "rb") as f:
        b = f.read(1)
    return b.decode("utf-8", errors="ignore")

def iter_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def load_json_any(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

STREET_SUFFIX = {
  "STREET":"ST","ST":"ST","AVE":"AVE","AVENUE":"AVE","ROAD":"RD","RD":"RD",
  "DRIVE":"DR","DR":"DR","LANE":"LN","LN":"LN","COURT":"CT","CT":"CT",
  "BOULEVARD":"BLVD","BLVD":"BLVD","PARKWAY":"PKWY","PKWY":"PKWY","HIGHWAY":"HWY","HWY":"HWY",
  "PLACE":"PL","PL":"PL","TERRACE":"TER","TER":"TER","CIRCLE":"CIR","CIR":"CIR"
}

def norm_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def normalize_town(s: Optional[str]) -> str:
    if not s:
        return ""
    s = norm_whitespace(str(s)).upper()
    s = re.sub(r"[^A-Z0-9 \-]", "", s)
    return s

def normalize_address(s: Optional[str]) -> str:
    if not s:
        return ""
    s = norm_whitespace(str(s)).upper()
    s = s.replace(",", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^(ADDR|ADDRESS|LOCATION)\s*:\s*", "", s).strip()
    s = re.sub(r"\b(APT|UNIT|#)\s*[A-Z0-9\-]+$", "", s).strip()
    parts = s.split(" ")
    if parts:
        last = parts[-1]
        if last in STREET_SUFFIX:
            parts[-1] = STREET_SUFFIX[last]
    s = " ".join(parts)
    s = re.sub(r"[^A-Z0-9 \-]", "", s)
    return s

def parse_consideration(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    t = str(text)
    m = re.search(r"(\$?\s*[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]{2})?)", t)
    if not m:
        m = re.search(r"(\$?\s*[0-9]+(?:\.[0-9]{2})?)", t)
    if not m:
        return None
    num = m.group(1).replace("$","").replace(",","").strip()
    try:
        return int(float(num))
    except Exception:
        return None

def classify_deed(parties_text: str, consideration: Optional[int], doc_type: str) -> Tuple[str,float,List[str]]:
    reasons=[]
    dt = (doc_type or "").upper()
    p = (parties_text or "").upper()

    if "FORECLOS" in dt or "FORECLOS" in p:
        return ("distress_transfer", 0.9, ["foreclosure_marker"])
    if consideration is not None and consideration <= 100:
        return ("related_party_transfer", 0.75, ["nominal_consideration"])
    if re.search(r"\b(TRUST|ESTATE|EXECUTOR|ADMINISTRATOR)\b", p):
        return ("internal_restructure", 0.65, ["trust_or_estate_marker"])
    if consideration is not None and consideration >= 10000:
        return ("arms_length_sale", 0.6, ["consideration_high"])
    return ("unknown", 0.25, ["insufficient_signals"])

def extract_event_locator(ev: Dict[str,Any]) -> Tuple[str,str,Optional[int],str]:
    doc = ev.get("document") or {}
    loc = ev.get("property_locator") or {}
    raw = ev.get("raw_cells") or {}

    doc_type = (doc.get("document_type") or ev.get("event_type") or raw.get("doc_type") or "").upper()
    town = loc.get("town") or raw.get("town") or ev.get("town") or ""
    addr = loc.get("address_raw") or raw.get("address_text") or ev.get("address_raw") or ""
    consideration_text = (ev.get("transaction") or {}).get("consideration_text_raw") or raw.get("consideration_text") or ev.get("consideration_text")

    consideration = parse_consideration(consideration_text)
    return (normalize_town(town), normalize_address(addr), consideration, doc_type)

def spine_iter(path: str) -> Iterable[Dict[str,Any]]:
    first = read_first_char(path)
    if first == "[":
        data = load_json_any(path)
        if isinstance(data, list):
            for x in data:
                if isinstance(x, dict):
                    yield x
        elif isinstance(data, dict) and isinstance(data.get("records"), list):
            for x in data["records"]:
                if isinstance(x, dict):
                    yield x
        return
    for obj in iter_ndjson(path):
        yield obj

def spine_keys(rec: Dict[str,Any]) -> Tuple[Optional[str],str,str]:
    pid = rec.get("property_id") or rec.get("id") or rec.get("propertyId")
    town = rec.get("town") or rec.get("city") or (rec.get("address") or {}).get("city") or (rec.get("address") or {}).get("town") or ""
    addr = rec.get("address_raw") or rec.get("full_address") or rec.get("site_address") or rec.get("address") or ""
    if isinstance(addr, dict):
        addr = addr.get("line1") or addr.get("street") or addr.get("full") or ""
    return (pid, normalize_town(town), normalize_address(addr))

def build_spine_index(spine_path: str) -> Dict[Tuple[str,str], str]:
    idx: Dict[Tuple[str,str], str] = {}
    for rec in spine_iter(spine_path):
        pid, town, addr = spine_keys(rec)
        if not pid or not town or not addr:
            continue
        key = (town, addr)
        if key not in idx:
            idx[key] = pid
    return idx

def iter_events_dir(events_dir: str) -> Iterable[Dict[str,Any]]:
    for name in os.listdir(events_dir):
        if not name.endswith(".ndjson"):
            continue
        p = os.path.join(events_dir, name)
        for ev in iter_ndjson(p):
            yield ev

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eventsDir", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    audit = {
        "created_at": utc_now(),
        "events_dir": args.eventsDir,
        "spine_path": args.spine,
        "counts": {"total": 0, "ATTACHED_A": 0, "UNKNOWN": 0},
        "sample_unknown": [],
        "notes": [
            "v1.1 attaches by exact normalized (town,address) only. Conservative by design.",
            "If ATTACHED_A is 0, the spine likely lacks Hampden records or uses different address fields."
        ]
    }

    print("[start] Hampden STEP 2 v1.1 attach (conservative town+address exact)")
    spine_idx = build_spine_index(args.spine)
    print(f"[info] spine_index_keys: {len(spine_idx)}")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as out:
        for ev in iter_events_dir(args.eventsDir):
            audit["counts"]["total"] += 1
            et = (ev.get("event_type") or "UNKNOWN").upper()
            town, addr, consideration, doc_type = extract_event_locator(ev)

            attach = {
                "attach_status": "UNKNOWN",
                "attach_method": "none",
                "attach_confidence": "UNKNOWN",
                "attach_score": 0.0,
                "property_id": None,
                "town_norm": town,
                "address_norm": addr
            }

            if town and addr:
                pid = spine_idx.get((town, addr))
                if pid:
                    attach.update({
                        "attach_status": "ATTACHED",
                        "attach_method": "town_address_exact",
                        "attach_confidence": "A",
                        "attach_score": 1.0,
                        "property_id": pid
                    })
                    audit["counts"]["ATTACHED_A"] += 1
                else:
                    audit["counts"]["UNKNOWN"] += 1
                    if len(audit["sample_unknown"]) < 25:
                        audit["sample_unknown"].append({
                            "event_id": ev.get("event_id"),
                            "event_type": et,
                            "town_norm": town,
                            "address_norm": addr,
                            "consideration_numeric": consideration
                        })
            else:
                audit["counts"]["UNKNOWN"] += 1

            if et == "DEED":
                raw = ev.get("raw_cells") or {}
                parties_text = " ".join([str(raw.get("grantor_block","")), str(raw.get("grantee_block",""))]).strip()
                tx_class, conf, reasons = classify_deed(parties_text, consideration, doc_type)
                ev.setdefault("transaction", {})
                if isinstance(ev.get("transaction"), dict):
                    ev["transaction"]["consideration_numeric"] = consideration
                    ev["transaction"]["transaction_class"] = tx_class
                    ev["transaction"]["confidence_score"] = conf
                    ev["transaction"]["reasons"] = reasons

            ev["attachment"] = attach
            out.write(json.dumps(ev, ensure_ascii=False) + "\n")

    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(f"[done] out: {args.out}")
    print(f"[done] audit: {args.audit}")
    print(f"[done] attach_status_counts: {{'ATTACHED_A': {audit['counts']['ATTACHED_A']}, 'UNKNOWN': {audit['counts']['UNKNOWN']}}}")

if __name__ == "__main__":
    main()
