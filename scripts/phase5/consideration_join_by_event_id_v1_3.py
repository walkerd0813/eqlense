import argparse, json, re, time
from collections import Counter

# Only currency-like tokens with two decimals (e.g., 350000.00, 1,234.00)
MONEY_DEC_RE = re.compile(r"(?<!\d)(\d{1,3}(?:,\d{3})+|\d+)\.(\d{2})(?!\d)")

def _to_int_from_money_token(num_part, dec_part):
    s = (num_part or "").replace(",","")
    if not s.isdigit():
        return None
    try:
        return int(s)  # ignore cents; registry usually uses .00
    except:
        return None

def extract_cons_from_raw_lines_decimal_only(raw_lines, min_reasonable):
    if raw_lines is None:
        return None, None

    lines = []
    if isinstance(raw_lines, list):
        lines = [x for x in raw_lines if isinstance(x, str) and x.strip()]
    elif isinstance(raw_lines, str) and raw_lines.strip():
        lines = [raw_lines]
    else:
        return None, None

    vals = []
    for ln in lines:
        for m in MONEY_DEC_RE.finditer(ln):
            v = _to_int_from_money_token(m.group(1), m.group(2))
            if v is None:
                continue
            vals.append(v)

    if not vals:
        return None, "raw_lines:no_decimal_money_tokens"

    vmax = max(vals)

    # refuse fee-like values
    if vmax < min_reasonable:
        return None, "raw_lines:decimal_money_too_small"

    return vmax, "raw_lines:decimal_money:max_ge_min"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events_in", required=True)
    ap.add_argument("--raw_index_in", required=True)
    ap.add_argument("--events_out", required=True)
    ap.add_argument("--audit_out", required=True)
    ap.add_argument("--min_reasonable", type=int, default=1000)
    args = ap.parse_args()

    t0 = time.time()
    cons_by_event = {}
    src_by_event = {}
    idx = Counter()
    miss_eid = 0

    with open(args.raw_index_in, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                row=json.loads(line)
            except:
                idx["raw_index_bad_json"] += 1
                continue
            eid=row.get("event_id")
            if not isinstance(eid,str) or not eid.strip():
                miss_eid += 1
                continue

            cons, src = extract_cons_from_raw_lines_decimal_only(row.get("raw_lines"), args.min_reasonable)
            if cons is None:
                idx["no_cons_found"] += 1
                if src: idx[src] += 1
                continue

            cons_by_event[eid] = cons
            src_by_event[eid] = src or "unknown"
            idx["cons_mapped"] += 1

    out = Counter()
    with open(args.events_in, "r", encoding="utf-8") as fin, open(args.events_out, "w", encoding="utf-8") as fout:
        for line in fin:
            line=line.strip()
            if not line:
                continue
            try:
                ev=json.loads(line)
            except:
                out["events_bad_json"] += 1
                continue

            eid=ev.get("event_id")
            if not isinstance(eid,str) or not eid.strip():
                out["events_missing_event_id"] += 1
                continue

            ts=ev.get("transaction_semantics")
            if not isinstance(ts,dict):
                ts={}
                ev["transaction_semantics"]=ts

            cons=cons_by_event.get(eid)
            if cons is None:
                out["no_index_match_or_no_cons"] += 1
            else:
                ts["price_amount"]=int(cons)
                ts["price_source"]="raw_index_raw_lines_decimal_only"
                ts["price_parse_source"]=src_by_event.get(eid,"unknown")
                out["priced_written"] += 1

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
            out["events_written"] += 1

    audit = {
        "script": "consideration_join_by_event_id_v1_3_3_decimal_only",
        "events_in": args.events_in,
        "raw_index_in": args.raw_index_in,
        "events_out": args.events_out,
        "min_reasonable": args.min_reasonable,
        "stats_index": dict(idx),
        "stats_events": dict(out),
        "missing_event_id_in_index": miss_eid,
        "cons_map_size": len(cons_by_event),
        "elapsed_s": round(time.time()-t0, 3),
    }
    with open(args.audit_out, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:", args.events_out)
    print("[done] audit:", args.audit_out)
    print("events_written:", out.get("events_written",0))
    print("priced_written:", out.get("priced_written",0))
    print("no_cons_or_no_match:", out.get("no_index_match_or_no_cons",0))
    print("index_cons_map_size:", len(cons_by_event))

if __name__ == "__main__":
    main()
