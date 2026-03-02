import json, collections
p = r"C:\seller-app\backend\publicData\registry\hampden\_work\OTR_EXTRACT_ALLDOCS_v1\events__HAMPDEN__OTR__RAW__20260119_023304.ndjson"
c = collections.Counter()
t = 0
with open(p, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        t += 1
        src = r.get("source") or {}
        pdf = (
            src.get("pdf_name")
            or src.get("pdf")
            or src.get("filename")
            or src.get("path")
            or "UNKNOWN_PDF"
        )
        c[pdf] += 1

print("total_events", t)
print("top_pdfs:")
for pdf, n in c.most_common(40):
    print(n, pdf)
