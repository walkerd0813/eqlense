#!/usr/bin/env python3
import argparse, json, re
from collections import defaultdict

STREET_SUFFIX_MAP = {
    "STREET":"ST","ST":"ST","ST.":"ST",
    "AVENUE":"AVE","AVE":"AVE","AVE.":"AVE",
    "ROAD":"RD","RD":"RD","RD.":"RD",
    "DRIVE":"DR","DR":"DR","DR.":"DR",
    "LANE":"LN","LN":"LN","LN.":"LN",
    "COURT":"CT","CT":"CT","CT.":"CT",
    "BOULEVARD":"BLVD","BLVD":"BLVD","BLVD.":"BLVD",
    "PLACE":"PL","PL":"PL","PL.":"PL",
    "TERRACE":"TER","TER":"TER","TER.":"TER",
    "CIRCLE":"CIR","CIR":"CIR","CIR.":"CIR",
    "PARKWAY":"PKWY","PKWY":"PKWY","PKWY.":"PKWY",
    "HIGHWAY":"HWY","HWY":"HWY","HWY.":"HWY",
}

def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip())

def norm_town(s: str) -> str:
    if not s: return ""
    s = norm_ws(str(s)).upper()
    s = re.sub(r"[^A-Z0-9 \-]", "", s)
    s = s.replace("TOWN OF ", "").replace("CITY OF ", "")
    return norm_ws(s)

def strip_unit(addr: str) -> str:
    a = addr.upper()
    a = re.sub(r"\s+(UNIT|APT|APARTMENT|STE|SUITE|#)\s*[A-Z0-9\-]+.*$", "", a).strip()
    return a

def norm_street_tokens(tokens):
    out=[]
    for t in tokens:
        t=str(t).strip().upper().strip(".")
        if not t:
            continue
        out.append(STREET_SUFFIX_MAP.get(t, t))
    return out

def parse_addr(addr_raw: str):
    if not addr_raw:
        return [], ""
    a = norm_ws(str(addr_raw)).upper()
    a = strip_unit(a)
    a = re.sub(r"[\,]", " ", a)
    a = norm_ws(a)

    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s+(.*)$", a)
    if m:
        n1 = int(m.group(1)); n2 = int(m.group(2))
        street = m.group(3).strip()
        tokens = norm_street_tokens(street.split())
        return [n1, n2], " ".join(tokens)

    m = re.match(r"^\s*(\d+)\s+(.*)$", a)
    if m:
        n1 = int(m.group(1))
        street = m.group(2).strip()
        tokens = norm_street_tokens(street.split())
        return [n1], " ".join(tokens)

    tokens = norm_street_tokens(a.split())
    return [], " ".join(tokens)

def get_first(d, keys):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, "", []):
            return d[k]
    return None

def extract_axis2_town_addr(row: dict):
    if row.get("town") and row.get("addr"):
        return row.get("town"), row.get("addr")

    pr = row.get("property_ref") if isinstance(row.get("property_ref"), dict) else {}
    doc = row.get("document") if isinstance(row.get("document"), dict) else {}
    rec = row.get("recording") if isinstance(row.get("recording"), dict) else {}
    src = row.get("source") if isinstance(row.get("source"), dict) else {}

    town = (
        get_first(row, ["town","city"]) or
        get_first(pr, ["town","city","municipality"]) or
        get_first(doc, ["town","city","municipality"]) or
        get_first(rec, ["town","city"]) or
        get_first(src, ["town","city"])
    )

    addr = (
        get_first(row, ["addr","address","address_raw"]) or
        get_first(pr, ["addr","address","address_raw","address_line1","site_address","location"]) or
        get_first(doc, ["addr","address","address_raw","address_line1","site_address","description","location"]) or
        get_first(src, ["addr","address","address_raw","location"])
    )

    if not addr and isinstance(pr, dict):
        a1 = get_first(pr, ["street","street_name","st_name"])
        n1 = get_first(pr, ["house_number","street_number","number"])
        if n1 and a1:
            addr = f"{n1} {a1}"

    if not (town and addr):
        blob = None
        for v in [get_first(doc, ["description","location"]), get_first(pr, ["location"]), get_first(src, ["raw_text","text","context"])]:
            if isinstance(v, str) and len(v) >= 10:
                blob = v
                break
        if blob and not addr:
            m = re.search(r"\b(\d+\s*-\s*\d+|\d+)\s+[A-Z0-9\.\- ]{3,40}\b", blob.upper())
            if m:
                addr = norm_ws(m.group(0))
        if blob and not town:
            m = re.findall(r"\b[A-Z][A-Z \-]{2,}\b", blob.upper())
            if m:
                town = norm_ws(m[-1])

    return town, addr

def extract_spine_town_addr(row: dict):
    town = get_first(row, ["town","city","municipality","jurisdiction_name"])
    addr = None

    if isinstance(row.get("address"), dict):
        ad = row["address"]
        town = town or get_first(ad, ["town","city","municipality"])
        line1 = get_first(ad, ["line1","address_line1","street_address","full"])
        if line1:
            addr = line1
        else:
            n = get_first(ad, ["house_number","number","street_number"])
            st = get_first(ad, ["street","street_name","st_name"])
            if n and st:
                addr = f"{n} {st}"
    else:
        addr = get_first(row, ["address","addr","site_address","address_line1","full_address","address_full","street_address"])

    if not addr:
        n = get_first(row, ["house_number","street_number","number"])
        st = get_first(row, ["street","street_name","st_name"])
        if n and st:
            addr = f"{n} {st}"

    return town, addr

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--only_unknown", action="store_true")
    args = ap.parse_args()

    idx = defaultdict(list)
    spine_rows = 0
    spine_keyed = 0

    with open(args.spine, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            spine_rows += 1
            pid = r.get("property_id") or r.get("id")
            if not pid:
                continue
            town_raw, addr_raw = extract_spine_town_addr(r)
            town = norm_town(town_raw)
            nums, street = parse_addr(addr_raw or "")
            if not town or not nums or not street:
                continue
            key = (town, nums[0], street)
            idx[key].append(pid)
            spine_keyed += 1

    total = promoted = collisions = no_town = no_addr = no_num = no_match = 0

    with open(args.infile, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            total += 1

            if args.only_unknown and row.get("attach_status") != "UNKNOWN":
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                continue

            if row.get("attach_status") != "UNKNOWN":
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                continue

            town_raw, addr_raw = extract_axis2_town_addr(row)
            if not town_raw:
                no_town += 1
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                continue
            if not addr_raw:
                no_addr += 1
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                continue

            town = norm_town(town_raw)
            nums, street = parse_addr(addr_raw)

            if not nums:
                no_num += 1
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                continue

            candidates = set()
            for n in nums:
                key = (town, n, street)
                for pid in idx.get(key, []):
                    candidates.add(pid)

            if len(candidates) == 1:
                pid = next(iter(candidates))
                row["property_id"] = pid
                row["attach_status"] = "ATTACHED_B"
                row["why"] = "NONE"
                row["match_method"] = "axis2_rescue_unique_promote_v1"
                mm = row.get("match_meta") if isinstance(row.get("match_meta"), dict) else {}
                mm["rescue"] = {
                    "town_raw": town_raw,
                    "addr_raw": addr_raw,
                    "town_norm": town,
                    "street_norm": street,
                    "house_nums_tried": nums,
                    "candidates_n": 1,
                    "method": "town+house+street unique",
                }
                row["match_meta"] = mm
                promoted += 1
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                continue

            if len(candidates) > 1:
                collisions += 1
                mm = row.get("match_meta") if isinstance(row.get("match_meta"), dict) else {}
                mm["rescue_collision"] = {
                    "town_raw": town_raw,
                    "addr_raw": addr_raw,
                    "town_norm": town,
                    "street_norm": street,
                    "house_nums_tried": nums,
                    "candidate_property_ids_n": len(candidates),
                }
                row["match_meta"] = mm
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                continue

            no_match += 1
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")

    audit = {
        "infile": args.infile,
        "spine": args.spine,
        "out": args.out,
        "spine_rows": spine_rows,
        "spine_keyed": spine_keyed,
        "total_rows": total,
        "promoted_to_ATTACHED_B": promoted,
        "unknown_no_town": no_town,
        "unknown_no_addr": no_addr,
        "unknown_no_num": no_num,
        "unknown_no_match": no_match,
        "rescue_collisions": collisions,
    }
    audit_path = args.out.replace(".ndjson", "__audit_rescue_promote_v1.json")
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] axis2_rescue_promote_unique_v1")
    print(" total:", total)
    print(" promoted_to_ATTACHED_B:", promoted)
    print(" unknown_no_town:", no_town, " unknown_no_addr:", no_addr, " unknown_no_num:", no_num)
    print(" unknown_no_match:", no_match, " collisions:", collisions)
    print("[ok] OUT  ", args.out)
    print("[ok] AUDIT", audit_path)

if __name__ == "__main__":
    main()
