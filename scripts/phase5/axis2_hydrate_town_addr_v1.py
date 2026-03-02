import argparse, json, re, sys
from typing import Optional, Tuple

TOWN_RE = re.compile(r'\b(TOWN|CITY)\b[: ]+\s*([A-Z][A-Z \-]+)\b', re.I)
# common MA patterns where town is included in a single string e.g. "SPRINGFIELD - 151-153 CATHARINE ST"
TOWN_DASH_ADDR_RE = re.compile(r'^\s*([A-Z][A-Z \-]+)\s*-\s*(.+?)\s*$')

def get_nested(d, path):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def first_str(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def infer_town_addr(row: dict) -> Tuple[Optional[str], Optional[str]]:
    # 1) already present
    town = row.get("town")
    addr  = row.get("addr")
    if isinstance(town, str) and town.strip() and isinstance(addr, str) and addr.strip():
        return town.strip(), addr.strip()

    # 2) common nested spots (adjust-only-if-present, no assumptions)
    # Many of your axis2 rows carry original “property reference” text somewhere in here:
    candidates = [
        first_str(
            get_nested(row, ["property_ref","town"]),
            get_nested(row, ["property_ref","city"]),
            get_nested(row, ["property_ref","municipality"]),
        ),
        first_str(
            get_nested(row, ["property_ref","addr"]),
            get_nested(row, ["property_ref","address"]),
            get_nested(row, ["property_ref","address_raw"]),
            get_nested(row, ["property_ref","address_line"]),
        ),
        first_str(
            get_nested(row, ["document","property_address"]),
            get_nested(row, ["document","address"]),
            get_nested(row, ["document","address_raw"]),
        ),
        first_str(
            get_nested(row, ["source","address"]),
            get_nested(row, ["source","address_raw"]),
            get_nested(row, ["meta","address_raw"]),
        ),
        first_str(
            get_nested(row, ["recording","town"]),
            get_nested(row, ["recording","city"]),
        ),
    ]

    # Try to pull town/addr from combined strings if needed
    # Look for any single string field that may carry "TOWN - ADDRESS"
    blob_candidates = []
    for path in [
        ["property_ref","raw"],
        ["property_ref","text"],
        ["document","raw"],
        ["document","text"],
        ["source","raw"],
        ["source","text"],
        ["meta","raw"],
        ["meta","text"],
    ]:
        v = get_nested(row, path)
        if isinstance(v, str) and v.strip():
            blob_candidates.append(v.strip())

    # If we already found town or addr separately above, keep them
    town2 = candidates[0] if isinstance(candidates[0], str) else None
    addr2 = candidates[1] if isinstance(candidates[1], str) else None

    # If missing, attempt parse from blobs
    if not town2 or not addr2:
        for blob in blob_candidates:
            m = TOWN_DASH_ADDR_RE.match(blob.upper())
            if m and not town2 and not addr2:
                town2 = m.group(1).strip()
                addr2 = m.group(2).strip()
                break
            m2 = TOWN_RE.search(blob.upper())
            if m2 and not town2:
                town2 = m2.group(2).strip()

    # If addr is still missing, try fallback to any “address-like” field we touched
    if not addr2:
        for v in [
            get_nested(row, ["property_ref","addr"]),
            get_nested(row, ["property_ref","address"]),
            get_nested(row, ["property_ref","address_raw"]),
            get_nested(row, ["document","property_address"]),
            get_nested(row, ["document","address"]),
            get_nested(row, ["document","address_raw"]),
            get_nested(row, ["source","address"]),
            get_nested(row, ["source","address_raw"]),
        ]:
            if isinstance(v, str) and v.strip():
                addr2 = v.strip()
                break

    return town2, addr2

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    total = 0
    filled_town = 0
    filled_addr = 0
    still_missing_town = 0
    still_missing_addr = 0

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            total += 1

            had_town = ("town" in row) and isinstance(row.get("town"), str) and row["town"].strip()
            had_addr = ("addr" in row) and isinstance(row.get("addr"), str) and row["addr"].strip()

            town, addr = infer_town_addr(row)

            if not had_town and isinstance(town, str) and town.strip():
                row["town"] = town.strip().upper()
                filled_town += 1
            if not had_addr and isinstance(addr, str) and addr.strip():
                row["addr"] = addr.strip().upper()
                filled_addr += 1

            if not (("town" in row) and isinstance(row.get("town"), str) and row["town"].strip()):
                still_missing_town += 1
            if not (("addr" in row) and isinstance(row.get("addr"), str) and row["addr"].strip()):
                still_missing_addr += 1

            fout.write(json.dumps(row, ensure_ascii=False) + "\n")

    audit = {
        "total": total,
        "filled_town": filled_town,
        "filled_addr": filled_addr,
        "still_missing_town": still_missing_town,
        "still_missing_addr": still_missing_addr,
    }
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()
