import argparse, json, re, datetime, os, hashlib

def nowz():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

UNITLIKE = re.compile(r'^[A-Z0-9]{1,8}$')  # simple, deterministic
TRAIL_NUM = re.compile(r'^(.*?)(?:\s+)([0-9]{1,5}[A-Z]?)$')

def norm(s):
    return (s or "").strip()

def build_site_key(street_name, town, zip5):
    street_name = norm(street_name)
    town = norm(town).upper()
    zip5 = norm(zip5)
    return f"{street_name}|{town}|{zip5}"

def build_address_key(tier, street_no, street_name, town, zip5):
    tier = norm(tier) or "A"
    return f"{tier}|{norm(street_no)}|{norm(street_name)}|{norm(town).upper()}|{norm(zip5)}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    rows_scanned = 0
    rows_changed = 0
    changes = {"fixed_address_key":0, "fixed_site_key":0, "fixed_street_name_from_keys":0}

    with open(args.infile, "r", encoding="utf-8") as fi, open(args.out, "w", encoding="utf-8") as fo:
        for line in fi:
            rows_scanned += 1
            r = json.loads(line)

            street_no = norm(r.get("street_no"))
            street_name = norm(r.get("street_name"))
            unit = norm(r.get("unit"))
            town = norm(r.get("town") or r.get("city") or "").upper()
            zip5 = norm(r.get("zip"))

            ak = norm(r.get("address_key"))
            sk = norm(r.get("site_key"))

            changed = False

            # If street_name itself has trailing unit-like numeric and unit is empty, we DO NOT touch here.
            # (That was handled by UNIT_FROM_STREET_SUFFIX_NUMERIC already.)
            #
            # Here we only fix: keys that incorrectly include the unit in the street-name slot.

            if unit and UNITLIKE.match(unit):
                # Fix site_key if it ends with " {unit}"
                if sk:
                    want_sk = build_site_key(street_name, town, zip5)
                    if sk != want_sk and (sk.endswith(" "+unit) or sk.startswith(street_name+" "+unit)):
                        r["site_key"] = want_sk
                        changes["fixed_site_key"] += 1
                        changed = True

                # Fix address_key similarly
                if ak:
                    tier = (ak.split("|",1)[0] if "|" in ak else "A")
                    want_ak = build_address_key(tier, street_no, street_name, town, zip5)
                    if ak != want_ak and (f"|{street_name} {unit}|" in ak or ak.endswith("|"+zip5) and f"|{street_name} {unit}|" in ak):
                        r["address_key"] = want_ak
                        changes["fixed_address_key"] += 1
                        changed = True

                # Optional: if street_name is clean but keys are polluted, we are done.
                # If street_name itself is polluted AND unit matches trailing token, strip it (safe).
                m = TRAIL_NUM.match(street_name.upper())
                if m and unit and m.group(2) == unit:
                    r["street_name"] = m.group(1).strip()
                    street_name = r["street_name"]
                    changes["fixed_street_name_from_keys"] += 1
                    # rebuild keys again with cleaned name
                    r["site_key"] = build_site_key(street_name, town, zip5)
                    tier = (ak.split("|",1)[0] if "|" in ak else "A")
                    r["address_key"] = build_address_key(tier, street_no, street_name, town, zip5)
                    changed = True

            if changed:
                rows_changed += 1

            fo.write(json.dumps(r, ensure_ascii=False) + "\n")

    audit = {
        "engine_id": "address_authority.rebuild_keys_strip_unit_from_street_v1",
        "started_at": nowz(),
        "done": True,
        "rows_scanned": rows_scanned,
        "rows_changed": rows_changed,
        "changes": changes,
        "finished_at": nowz(),
        "infile": os.path.abspath(args.infile),
        "out": os.path.abspath(args.out),
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps({"done": True, "rows_scanned": rows_scanned, "rows_changed": rows_changed, "changes": changes}, indent=2))

if __name__ == "__main__":
    main()
