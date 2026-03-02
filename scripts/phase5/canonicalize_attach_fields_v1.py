import json, sys

TOP_KEYS = ("attach_scope","attach_status","why","match_method","property_id","match_meta")

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    args = ap.parse_args()

    n = 0
    moved = 0
    removed = 0
    conflicts_fixed = 0

    with open(args.inp, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            e = json.loads(line)
            n += 1

            a = e.get("attach")
            if not isinstance(a, dict):
                a = {}
                e["attach"] = a

            top_present = any(k in e for k in TOP_KEYS)
            if top_present:
                top_status = e.get("attach_status")
                nested_status = a.get("attach_status")

                # If top says ATTACHED_A and nested says UNKNOWN (or missing), fix it
                if top_status == "ATTACHED_A" and (nested_status in (None, "UNKNOWN")):
                    conflicts_fixed += 1

                # Move top-level -> nested (canonical)
                for k in TOP_KEYS:
                    if k in e:
                        a[k] = e.get(k)
                        moved += 1

                # Remove top-level duplicates
                for k in TOP_KEYS:
                    if k in e:
                        del e[k]
                        removed += 1

            fout.write(json.dumps(e, ensure_ascii=False) + "\n")

    print({
        "rows": n,
        "top_fields_moved": moved,
        "top_fields_removed": removed,
        "conflicts_fixed_top_attached_nested_unknown": conflicts_fixed
    })

if __name__ == "__main__":
    main()
