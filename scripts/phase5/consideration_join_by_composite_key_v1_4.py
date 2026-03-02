import argparse, json, re, time
from collections import Counter, defaultdict

MONEY_DEC_RE = re.compile(r"(?<!\d)(\d{1,3}(?:,\d{3})+|\d+)\.(\d{2})(?!\d)")

def norm_str(x):
    if x is None:
        return ""
    s = str(x).strip()
    return s

def norm_intish(x):
    # normalize things like "025711" -> "25711" when possible
    s = norm_str(x)
    if not s:
        return ""
    s2 = re.sub(r"[^\d]", "", s)
    if not s2:
        return s
    try:
        return str(int(s2))
    except:
        return s2

def money_from_raw_lines_decimal_only(raw_lines, min_reasonable):
    if raw_lines is None:
        return None
    lines = []
    if isinstance(raw_lines, list):
        lines = [x for x in raw_lines if isinstance(x, str) and x.strip()]
    elif isinstance(raw_lines, str) and raw_lines.strip():
        lines = [raw_lines]
    else:
        return None

    vals = []
    for ln in lines:
        for m in MONEY_DEC_RE.finditer(ln):
            num = m.group(1).replace(",", "")
            if not num.isdigit():
                continue
            v = int(num)
            vals.append(v)
    if not vals:
        return None
    vmax = max(vals)
    if vmax < min_reasonable:
        return None
    return vmax

def build_key_from_recording(county, rec):
    """
    Deterministic key from recording identifiers.
    We include seq if present; otherwise empty.
    """
    if not isinstance(rec, dict):
        rec = {}

    date = norm_str(rec.get("recording_date") or rec.get("recording_date_raw") or rec.get("date") or "")
    book = norm_intish(rec.get("book"))
    page = norm_intish(rec.get("page"))
    doc  = norm_intish(rec.get("document_number") or rec.get("document_number_raw") or rec.get("doc_number") or rec.get("doc") or "")
    seq  = norm_intish(rec.get("seq") or rec.get("sequence") or "")

    # Key priority: date|book|page|doc|seq
    return "|".join([norm_str(county), date, book, page, doc, seq])

def build_key_without_seq(county, rec):
    if not isinstance(rec, dict):
        rec = {}
    date = norm_str(rec.get("recording_date") or rec.get("recording_date_raw") or rec.get("date") or "")
    book = norm_intish(rec.get("book"))
    page = norm_intish(rec.get("page"))
    doc  = norm_intish(rec.get("document_number") or rec.get("document_number_raw") or rec.get("doc_number") or rec.get("doc") or "")
    return "|".join([norm_str(county), date, book, page, doc, ""])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events_in", required=True)
    ap.add_argument("--raw_index_in", required=True)
    ap.add_argument("--events_out", required=True)
    ap.add_argument("--audit_out", required=True)
    ap.add_argument("--min_reasonable", type=int, default=1000)
    args = ap.parse_args()

    t0 = time.time()

    idx = Counter()

    # Build lookup maps from raw index
    # We keep both: strict key (with seq) and loose key (without seq)
    map_strict = {}
    map_loose  = {}
    dup_strict = defaultdict(int)
    dup_loose  = defaultdict(int)

    with open(args.raw_index_in, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except:
                idx["raw_index_bad_json"] += 1
                continue

            county = row.get("county") or (row.get("recording") or {}).get("county") or ""
            rec = row.get("recording") or {}
            price = money_from_raw_lines_decimal_only(row.get("raw_lines"), args.min_reasonable)

            if price is None:
                idx["raw_no_price_decimal_ge_min"] += 1
                continue

            k_strict = build_key_from_recording(county, rec)
            k_loose  = build_key_without_seq(county, rec)

            if k_strict in map_strict:
                dup_strict[k_strict] += 1
                # keep the larger price (safer)
                if price > map_strict[k_strict]["price_amount"]:
                    map_strict[k_strict] = {"price_amount": price, "price_source": "raw_index_raw_lines_decimal_only", "join_key": k_strict}
                    idx["raw_strict_upgraded_price"] += 1
                else:
                    idx["raw_strict_dupe_kept_prev"] += 1
            else:
                map_strict[k_strict] = {"price_amount": price, "price_source": "raw_index_raw_lines_decimal_only", "join_key": k_strict}
                idx["raw_strict_added"] += 1

            if k_loose in map_loose:
                dup_loose[k_loose] += 1
                if price > map_loose[k_loose]["price_amount"]:
                    map_loose[k_loose] = {"price_amount": price, "price_source": "raw_index_raw_lines_decimal_only", "join_key": k_loose}
                    idx["raw_loose_upgraded_price"] += 1
                else:
                    idx["raw_loose_dupe_kept_prev"] += 1
            else:
                map_loose[k_loose] = {"price_amount": price, "price_source": "raw_index_raw_lines_decimal_only", "join_key": k_loose}
                idx["raw_loose_added"] += 1

    out = Counter()

    with open(args.events_in, "r", encoding="utf-8") as fin, open(args.events_out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except:
                out["events_bad_json"] += 1
                continue

            county = ev.get("county") or ""
            rec = ev.get("recording") or {}

            k_strict = build_key_from_recording(county, rec)
            k_loose  = build_key_without_seq(county, rec)

            hit = map_strict.get(k_strict)
            join_mode = None

            if hit:
                join_mode = "strict"
            else:
                hit = map_loose.get(k_loose)
                if hit:
                    join_mode = "loose"

            ts = ev.get("transaction_semantics")
            if not isinstance(ts, dict):
                ts = {}
                ev["transaction_semantics"] = ts

            if hit:
                ts["price_amount"] = int(hit["price_amount"])
                ts["price_source"] = hit["price_source"]
                ts["price_join_mode"] = join_mode
                ts["price_join_key"] = hit["join_key"]
                out["priced_written"] += 1
            else:
                out["no_match_on_composite_key"] += 1

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
            out["events_written"] += 1

    audit = {
        "script": "consideration_join_by_composite_key_v1_4",
        "events_in": args.events_in,
        "raw_index_in": args.raw_index_in,
        "events_out": args.events_out,
        "min_reasonable": args.min_reasonable,
        "raw_maps": {
            "strict_size": len(map_strict),
            "loose_size": len(map_loose),
            "strict_dup_keys": len(dup_strict),
            "loose_dup_keys": len(dup_loose),
        },
        "stats_index": dict(idx),
        "stats_events": dict(out),
        "elapsed_s": round(time.time() - t0, 3),
    }

    with open(args.audit_out, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] wrote:", args.events_out)
    print("[done] audit:", args.audit_out)
    print("events_written:", out.get("events_written", 0))
    print("priced_written:", out.get("priced_written", 0))
    print("no_match_on_composite_key:", out.get("no_match_on_composite_key", 0))
    print("raw_map_strict:", len(map_strict), "raw_map_loose:", len(map_loose))

if __name__ == "__main__":
    main()
