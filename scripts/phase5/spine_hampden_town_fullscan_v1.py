import json
from collections import Counter, defaultdict

CUR = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
OUT = r"publicData/_audit/registry/spine_hampden_town_fullscan_v1.json"

TOWNS = [
  "SPRINGFIELD","HOLYOKE","CHICOPEE","WEST SPRINGFIELD","AGAWAM","LONGMEADOW",
  "EAST LONGMEADOW","LUDLOW","WILBRAHAM","PALMER","WESTFIELD","HAMPDEN","RUSSELL"
]
TSET = set(TOWNS)

def iter_ndjson(path):
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            yield json.loads(line)

def main():
    ptr = json.load(open(CUR,"r",encoding="utf-8"))
    spine_path = ptr.get("properties_ndjson")
    if not spine_path:
        raise RuntimeError("CURRENT pointer missing properties_ndjson")

    counts = Counter()
    samples = defaultdict(list)
    rows = 0

    for row in iter_ndjson(spine_path):
        rows += 1
        town = str(row.get("town","")).upper().strip()
        if town in TSET:
            counts[town] += 1
            if len(samples[town]) < 3:
                samples[town].append({
                    "property_id": row.get("property_id"),
                    "parcel_id": row.get("parcel_id"),
                    "town": row.get("town"),
                    "full_address": row.get("full_address"),
                    "street_no": row.get("street_no"),
                    "street_name": row.get("street_name"),
                    "unit": row.get("unit"),
                    "zip": row.get("zip"),
                })

    report = {
      "spine_path": spine_path,
      "rows_scanned": rows,
      "counts": dict(counts),
      "samples_first_3_each": dict(samples)
    }
    with open(OUT,"w",encoding="utf-8") as f:
        json.dump(report,f,ensure_ascii=False,indent=2)

    print("=== SPINE FULLSCAN (Hampden towns) ===")
    print(json.dumps({"rows_scanned": rows, "counts": dict(counts)}, ensure_ascii=False))
    print(f"[ok] wrote: {OUT}")

if __name__ == "__main__":
    main()
