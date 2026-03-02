import argparse, json, re, datetime
from collections import defaultdict

def nowz():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

SUF = [
  (" STREET"," ST"),(" AVENUE"," AVE"),(" ROAD"," RD"),(" BOULEVARD"," BLVD"),
  (" DRIVE"," DR"),(" COURT"," CT"),(" PLACE"," PL"),(" TERRACE"," TERR"),
  (" LANE"," LN"),(" CIRCLE"," CIR"),(" PARKWAY"," PKWY"),(" HIGHWAY"," HWY"),
  (" MOUNT "," MT ")
]
DIR = [(" NORTH"," N"),(" SOUTH"," S"),(" EAST"," E"),(" WEST"," W")]

def norm_street(s: str) -> str:
    if not s: return ""
    s = s.upper().strip()
    s = re.sub(r"\s+"," ",s)
    s = s.replace(".","")
    for a,b in DIR:
        s = s.replace(a,b)
    for a,b in SUF:
        if s.endswith(a): s = s[:-len(a)] + b
        s = s.replace(a,b)
    s = re.sub(r"\s+"," ",s).strip()
    return s

def parse_match_key(mk: str):
    # mk: TOWN|366 W SECOND STREET|UNIT|3  OR  TOWN|23 ROSEBERY ROAD
    if not mk or "|" not in mk: return None
    parts = mk.split("|")
    town = (parts[0] or "").strip().upper()
    if not town: return None
    addr = (parts[1] or "").strip()
    if not addr: return None
    addr = re.sub(r"\s+\b(UNIT|APT|APARTMENT|STE|SUITE|PH|PENTHOUSE)\b.*$","",addr,flags=re.I).strip()
    m = re.match(r"^\s*(\d+)\s+(.*)$", addr)
    if not m: return None
    street_no = m.group(1).strip()
    street_name = m.group(2).strip()
    if not street_no or not street_name: return None
    return town, street_no, street_name

def build_site_index(spine_path):
    # key: STREETNAME_NORM|TOWN  -> set(property_id)
    idx = defaultdict(set)
    scanned = 0
    with open(spine_path,"r",encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            scanned += 1
            try: r = json.loads(line)
            except Exception: continue
            sk = (r.get("site_key") or "").strip()
            if not sk or "|" not in sk: continue
            sp = sk.split("|")
            if len(sp) < 2: continue
            street = norm_street(sp[0])
            town = (sp[1] or "").strip().upper()
            pid = r.get("property_id") or r.get("property_uid") or r.get("building_group_id")
            if not street or not town or not pid: continue
            idx[f"{street}|{town}"].add(pid)
    return idx, scanned, sum(len(v) for v in idx.values())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--spine", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--engine_id", required=True)
    args = ap.parse_args()

    site_idx, spine_scanned, site_pairs = build_site_index(args.spine)

    rows_scanned = 0
    rows_attached_b = 0
    no_parse = 0
    multi = 0
    samples = []

    with open(args.infile,"r",encoding="utf-8") as fin, open(args.out,"w",encoding="utf-8") as fout:
        for line in fin:
            if not line.strip(): continue
            rows_scanned += 1
            try: ev = json.loads(line)
            except Exception:
                fout.write(line); continue

            a = ev.get("attach") or {}
            st = a.get("attach_status") or ev.get("attach_status")
            mk = (a.get("match_key") or ev.get("match_key") or "").strip()

            if st != "UNKNOWN" or not mk:
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            parsed = parse_match_key(mk)
            if not parsed:
                no_parse += 1
                fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
                continue

            town, street_no, street_name = parsed
            key = f"{norm_street(street_name)}|{town}"
            cands = list(site_idx.get(key, []))

            if len(cands) == 1:
                pid = cands[0]
                a2 = dict(a)
                a2["attach_status"] = "ATTACHED_B"
                a2["match_method"] = (a.get("match_method") or ev.get("match_method") or "") + "|sitekey_B"
                a2["match_key_site"] = key
                a2["property_id"] = pid
                a2["attached_at"] = nowz()
                a2["engine_id"] = args.engine_id
                ev["attach"] = a2
                ev["property_id"] = pid
                rows_attached_b += 1
                if len(samples) < 25:
                    samples.append({"match_key": mk, "site_key": key, "property_id": pid})
            elif len(cands) > 1:
                multi += 1

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
      "engine_id": args.engine_id,
      "infile": args.infile,
      "spine": args.spine,
      "out": args.out,
      "rows_scanned": rows_scanned,
      "rows_attached_b": rows_attached_b,
      "no_parse": no_parse,
      "multi_sitekey": multi,
      "spine_rows_scanned": spine_scanned,
      "sitekey_pairs": site_pairs,
      "samples": samples,
      "finished_at": nowz()
    }
    with open(args.audit,"w",encoding="utf-8") as f:
        json.dump(audit,f,indent=2)

    print(json.dumps({"done": True, "rows_scanned": rows_scanned, "rows_attached_b": rows_attached_b}, indent=2))

if __name__ == "__main__":
    main()