#!/usr/bin/env python3
import argparse, json, re
from datetime import datetime

RE_TOWN_ADDR = re.compile(r"Town:\s*(?P<town>[^\r\n]+?)\s+Addr:\s*(?P<addr>[^\r\n]+)", re.IGNORECASE)
RE_MONEY = re.compile(r"(?P<amt>\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+(?:\.\d{2}))")
RE_RECORDED_LINE = re.compile(r"(?P<mdy>\d{2}-\d{2}-\d{4})\s+(?P<hms>\d{1,2}:\d{2}:\d{2}[ap])", re.IGNORECASE)
RE_PARTY = re.compile(r"^\s*(?P<side>[12])\s+(?P<kind>[PC])\s+(?P<name>.+?)\s*$")

NUMWORD = {"ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5","SIX":"6","SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10"}

def norm_town(t):
    if not t: return None
    return re.sub(r"\s+"," ", t.strip()).upper()

def norm_addr(a):
    if not a: return None
    s = re.sub(r"\s+"," ", a.strip()).upper()
    parts = s.split(" ")
    if parts and parts[0] in NUMWORD:
        parts[0] = NUMWORD[parts[0]]
    return " ".join(parts)

def parse_recorded(raw_block):
    m = RE_RECORDED_LINE.search(raw_block or "")
    if not m:
        return None, None, None
    raw = m.group(0)
    mm, dd, yyyy = m.group("mdy").split("-")
    date = f"{yyyy}-{mm}-{dd}"
    time = m.group("hms")
    return raw, date, time

def detect_layout(raw_block):
    txt = (raw_block or "").upper()
    if "LAND COURT" in txt or "LAN CORT" in txt or "LAND REG" in txt:
        return "LAND_COURT"
    if "DATE/TIME" in txt and "RECORDED" in txt and "BOOK-PAGE" in txt:
        return "RECORDED_LAND"
    return "UNKNOWN"

def parse_consideration_from_block(raw_block):
    if not raw_block: return None
    if "DEED" not in raw_block.upper(): return None
    amts = [m.group("amt") for m in RE_MONEY.finditer(raw_block)]
    if not amts: return None
    amts_sorted = sorted(amts, key=lambda x: (len(re.sub(r"[^\d]","", x)), x), reverse=True)
    return amts_sorted[0]

def money_to_float(s):
    if not s: return None
    try:
        return float(s.replace(",",""))
    except Exception:
        return None

def ensure(d, k, default):
    if k not in d: d[k] = default
    return d[k]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--events_version", default="DEED_ONLY_v1")
    ap.add_argument("--events_hash", default=None)
    args = ap.parse_args()

    n_in=n_out=n_cons=n_multi=0

    with open(args.inp,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line: continue
            n_in += 1
            try:
                ev = json.loads(line)
            except Exception:
                continue

            raw_block = None
            if isinstance(ev.get("document"), dict):
                raw_block = ev["document"].get("raw_block") or ev["document"].get("raw_text")
            raw_block = raw_block or ev.get("raw_block")

            ev["schema"] = {"name":"equitylens.registry_event","version":"phase5_deed_min_v1_1"}
            ev["event_type"] = "DEED"

            layout_kind = detect_layout(raw_block or "")
            ev["index_layout"] = {"kind": layout_kind, "header_fingerprint": None}

            pr = ensure(ev,"property_ref",{})
            if not isinstance(pr, dict):
                pr = {}
                ev["property_ref"] = pr

            pr.setdefault("town_raw", pr.get("town_raw"))
            pr.setdefault("address_raw", pr.get("address_raw"))
            pr["town_norm"] = norm_town(pr.get("town_raw"))
            pr["address_norm"] = norm_addr(pr.get("address_raw"))
            pr.setdefault("state","MA")
            pr.setdefault("zip5", None)
            pr.setdefault("primary_is_multi", False)
            pr.setdefault("multi_address", [])

            if raw_block:
                pairs = [(m.group("town").strip(), m.group("addr").strip()) for m in RE_TOWN_ADDR.finditer(raw_block)]
                if pairs:
                    if not pr.get("town_raw"): pr["town_raw"] = pairs[0][0]
                    if not pr.get("address_raw"): pr["address_raw"] = pairs[0][1]
                    pr["town_norm"] = norm_town(pr.get("town_raw"))
                    pr["address_norm"] = norm_addr(pr.get("address_raw"))
                    if len(pairs) > 1:
                        pr["primary_is_multi"] = True
                        pr["multi_address"] = []
                        for t,a in pairs[1:]:
                            pr["multi_address"].append({"town_raw":t,"address_raw":a,"town_norm":norm_town(t),"address_norm":norm_addr(a)})
                        n_multi += 1

            rec = ensure(ev,"recording",{})
            if not isinstance(rec, dict):
                rec = {}
                ev["recording"] = rec
            raw_dt, iso_date, rec_time = parse_recorded(raw_block or "")
            rec.setdefault("recorded_at_raw", raw_dt)
            rec.setdefault("recording_date", iso_date)
            rec.setdefault("recording_time", rec_time)
            for k in ["book","page","doc_number","instrument_number","group_seq","reference_book","reference_page","fee","verify_flag","status_flag"]:
                rec.setdefault(k, None)

            doc = ensure(ev,"document",{})
            if not isinstance(doc, dict):
                doc = {}
                ev["document"] = doc
            doc.setdefault("document_type","DEED")
            doc.setdefault("instrument_type", None)
            doc.setdefault("index_doc_type", doc.get("document_type") or "DEED")
            doc.setdefault("descr_loc_delivered", None)
            doc.setdefault("raw_block", raw_block)
            doc.setdefault("pages_ref", None)

            cons = ensure(ev,"consideration",{})
            if not isinstance(cons, dict):
                cons = {}
                ev["consideration"] = cons
            raw_cons = cons.get("raw_text") or (parse_consideration_from_block(raw_block) if raw_block else None)
            cons["raw_text"] = raw_cons
            amt = money_to_float(raw_cons)
            cons["amount"] = amt
            cons["currency"] = cons.get("currency") or "USD"
            cons["source"] = cons.get("source") or "INDEX"
            if raw_cons and amt is not None:
                cons["is_present"] = True
                cons["confidence"] = cons.get("confidence") or "B"
                cons["reason_missing"] = None
                n_cons += 1
            else:
                cons["is_present"] = False if not raw_cons else True
                cons["confidence"] = cons.get("confidence") or "UNKNOWN"
                cons["reason_missing"] = cons.get("reason_missing") or ("NOT_IN_INDEX" if not raw_cons else "PARSE_FAIL")

            parties = ensure(ev,"parties",{})
            if not isinstance(parties, dict):
                parties = {}
                ev["parties"] = parties
            parties.setdefault("grantor", [])
            parties.setdefault("grantee", [])
            parties.setdefault("parser_status", "UNPARSED")
            parties.setdefault("raw_lines", [])
            if raw_block:
                raw_lines=[]
                for ln in raw_block.splitlines():
                    if RE_PARTY.match(ln):
                        raw_lines.append(re.sub(r"\s+"," ", ln.strip()))
                if raw_lines:
                    parties["raw_lines"] = raw_lines

            attach = ensure(ev,"attach",{})
            if not isinstance(attach, dict):
                attach = {}
                ev["attach"] = attach
            attach.setdefault("attach_scope","SINGLE")
            attach.setdefault("attach_status","UNKNOWN")
            attach.setdefault("property_id", None)
            attach.setdefault("match_method", None)
            attach.setdefault("match_key", None)
            attach.setdefault("attachments", [])
            attach.setdefault("evidence", {
                "join_method":"town+address_norm",
                "join_basis":"deterministic_only",
                "spine_version": None,
                "spine_dataset_hash": None,
                "events_version": args.events_version,
                "events_dataset_hash": args.events_hash
            })

            src = ensure(ev,"source",{})
            if not isinstance(src, dict):
                src = {}
                ev["source"] = src
            src.setdefault("kind","registry_index_pdf")
            src.setdefault("name","hampden_index_pdf")
            src.setdefault("as_of_date", None)
            src.setdefault("dataset_hash", args.events_hash)
            src.setdefault("uri", None)
            src["layout_kind"] = layout_kind

            meta = ensure(ev,"meta",{})
            if not isinstance(meta, dict):
                meta = {}
                ev["meta"] = meta
            meta.setdefault("created_at", datetime.utcnow().isoformat()+"Z")
            meta.setdefault("pipeline","phase5_registry_events")
            meta.setdefault("run_id", meta.get("run_id") or "contract_enforcer_v1")
            meta.setdefault("qa_flags", [])

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
            n_out += 1

    print(json.dumps({"in_rows":n_in,"out_rows":n_out,"consideration_present":n_cons,"multi_property_events":n_multi}, indent=2))

if __name__ == "__main__":
    main()
