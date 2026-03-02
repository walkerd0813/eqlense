from __future__ import annotations

import argparse, json, time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()

    t0 = time.time()
    rows = 0
    promoted = 0
    left_building_only = 0
    json_err = 0

    with open(args.out, "w", encoding="utf-8") as fo:
        with open(args.inp, "r", encoding="utf-8") as f:
            for ln in f:
                if not ln.strip():
                    continue
                try:
                    r = json.loads(ln)
                except Exception:
                    json_err += 1
                    continue

                rows += 1
                a = r.get("attach") or {}
                st = (a.get("status") or a.get("attach_status") or "").upper()

                # promote ONLY those that are clearly building-attached
                if st == "ATTACHED_BUILDING":
                    a2 = dict(a)

                    a2["status"] = "ATTACHED_A"
                    a2["attach_status"] = "ATTACHED_A"
                    a2["attach_scope"] = "BUILDING"
                    a2["attach_precision"] = "BUILDING"
                    # keep method as building_scope_authority; do NOT rename it

                    flags = list(a2.get("flags") or [])
                    for flg in ("ATTACHED_A_BUILDING_SCOPE", "NOT_PARCEL_PRECISE"):
                        if flg not in flags:
                            flags.append(flg)
                    a2["flags"] = flags

                    r["attach"] = a2
                    promoted += 1

                elif st == "BUILDING_ONLY":
                    left_building_only += 1

                fo.write(json.dumps(r, ensure_ascii=False) + "\n")

    audit = {
        "engine": "promote_building_scope_to_attached_a_v1",
        "inputs": {"in": args.inp},
        "counts": {
            "rows_seen": rows,
            "promoted_attached_building_to_attached_a": promoted,
            "left_building_only": left_building_only,
            "json_errors_skipped": json_err,
        },
        "seconds": round(time.time() - t0, 2),
        "out": args.out,
    }

    with open(args.audit, "w", encoding="utf-8") as fa:
        json.dump(audit, fa, ensure_ascii=False, indent=2)

    print("[ok]", json.dumps(audit, ensure_ascii=False))


if __name__ == "__main__":
    main()
