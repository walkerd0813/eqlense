import json, collections

CUR = r"publicData/properties/_attached/CURRENT/CURRENT_PROPERTIES_PHASE4_ASSESSOR_CANONICAL_V2.json"
OUT = r"publicData/_audit/registry/spine_schema_and_town_coverage_v1.json"

TOWNS = [
  "SPRINGFIELD","HOLYOKE","CHICOPEE","WEST SPRINGFIELD","AGAWAM","LONGMEADOW",
  "EAST LONGMEADOW","LUDLOW","WILBRAHAM","PALMER","WESTFIELD","HAMPDEN","RUSSELL"
]

def iter_ndjson(path):
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            yield json.loads(line)

def main():
    ptr = json.load(open(CUR,"r",encoding="utf-8"))
    spine_path = ptr.get("properties_ndjson")
    if not spine_path:
        raise RuntimeError("CURRENT pointer missing properties_ndjson")

    key_counts = collections.Counter()
    sample_rows = []
    town_counts = collections.Counter()
    addr_presence = collections.Counter()

    n = 0
    for row in iter_ndjson(spine_path):
        n += 1
        # keys
        for k in row.keys():
            key_counts[k] += 1

        # sample a few rows
        if len(sample_rows) < 5:
            sample_rows.append(row)

        # town extraction attempts
        town = (row.get("town_norm") or row.get("town") or row.get("municipality") or row.get("city") or "")
        if isinstance(town, dict):
            town = town.get("value") or town.get("name") or ""
        town = str(town).upper().strip()
        if town:
            town_counts[town] += 1

        # address presence attempts
        addr = (row.get("address_norm") or row.get("address") or row.get("address_line1") or row.get("full_address") or "")
        if isinstance(addr, dict):
            addr = addr.get("address_norm") or addr.get("line1") or addr.get("value") or addr.get("text") or ""
        addr = str(addr).strip()
        addr_presence["has_addr_any"] += 1 if addr else 0

        if n >= 20000:
            break

    wanted = {t: town_counts.get(t,0) for t in TOWNS}
    top_towns = dict(town_counts.most_common(25))

    report = {
      "spine_path": spine_path,
      "rows_scanned": n,
      "top_level_keys_top30": dict(key_counts.most_common(30)),
      "town_counts_for_target_towns": wanted,
      "top_25_towns_seen": top_towns,
      "address_presence": dict(addr_presence),
      "sample_rows_first_2": sample_rows[:2]
    }

    with open(OUT,"w",encoding="utf-8") as f:
        json.dump(report,f,ensure_ascii=False,indent=2)

    print("=== SPINE SCHEMA + TOWN COVERAGE (first 20k rows) ===")
    print(json.dumps({
      "rows_scanned": n,
      "address_presence": report["address_presence"],
      "town_counts_for_target_towns": report["town_counts_for_target_towns"],
      "top_10_towns_seen": list(top_towns.items())[:10],
      "top_level_keys_top10": list(report["top_level_keys_top30"].items())[:10],
    }, ensure_ascii=False))
    print(f"[ok] wrote: {OUT}")

if __name__ == "__main__":
    main()
