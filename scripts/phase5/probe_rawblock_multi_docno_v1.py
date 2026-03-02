import json, re

PATH = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k__reattached_axis2_v1_5.ndjson"

# Hampden header pattern: "MM-DD-YYYY  9:57:38a  26120  359  56029"
pat = re.compile(r"\b(\d{2}-\d{2}-\d{4})\s+\d{1,2}:\d{2}:\d{2}[ap]\s+\d+\s+\d+\s+(\d+)\b")

multi = 0
total = 0
examples = []

with open(PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        ev = json.loads(line)
        total += 1

        rb = (((ev.get("document") or {}).get("raw_block")) or "")
        docs = [m.group(2) for m in pat.finditer(rb)]
        uniq = sorted(set(docs))

        if len(uniq) >= 2:
            multi += 1
            if len(examples) < 8:
                examples.append({
                    "event_id": ev.get("event_id"),
                    "docno_raw": (ev.get("recording") or {}).get("document_number_raw"),
                    "docnos_in_raw_block": uniq[:15],
                    "count_docnos": len(uniq)
                })

print({"rows": total, "raw_block_multi_docno": multi, "rate": (multi/total if total else None)})
print("--- examples (first 8) ---")
for ex in examples:
    print(ex)
