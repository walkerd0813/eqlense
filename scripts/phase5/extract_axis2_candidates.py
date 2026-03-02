import json

IN = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/events_attached_DEED_ONLY_v1_8_1_MULTI.ndjson"
OUT = r"publicData/registry/hampden/_attached_DEED_ONLY_v1_8_1_MULTI/axis2_candidates_ge_10k.ndjson"

MIN_AMOUNT = 10000
VALID_STATUS = {"UNKNOWN", "PARTIAL_MULTI"}

count_in = 0
count_out = 0

with open(IN, "r", encoding="utf-8") as fin, open(OUT, "w", encoding="utf-8") as fout:
    for line in fin:
        count_in += 1
        row = json.loads(line)

        amt = row.get("consideration", {}).get("amount")
        status = row.get("attach", {}).get("attach_status")

        if amt is None or amt < MIN_AMOUNT:
            continue
        if status not in VALID_STATUS:
            continue

        fout.write(json.dumps(row, ensure_ascii=False) + "\n")
        count_out += 1

print({
    "rows_scanned": count_in,
    "rows_written": count_out,
    "min_consideration": MIN_AMOUNT,
    "statuses": sorted(VALID_STATUS),
    "out": OUT
})

