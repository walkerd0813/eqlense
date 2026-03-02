import argparse, json, re
from collections import Counter, defaultdict

RANGE_RE = re.compile(r"^\d+\s*[-–]\s*\d+")
UNIT_RE = re.compile(r"\b(UNIT|APT|APARTMENT|#)\b", re.I)
LOT_RE = re.compile(r"\b(LOT|PAR\b|PARCEL)\b", re.I)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    args = ap.parse_args()

    c = Counter()
    samples = defaultdict(list)

    with open(args.inp, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            ev = json.loads(line)
            st = (ev.get("attach") or {}).get("attach_status") or ""
            if st.startswith("ATTACHED"):
                continue

            pr = ev.get("property_ref") or {}
            town = pr.get("town_raw") or pr.get("town") or ""
            addr = pr.get("address_raw") or pr.get("address") or ""
            raw_block = (ev.get("document") or {}).get("raw_block") or ""

            reason = "OTHER_KEY_MISMATCH"
            if raw_block.count("Town:") > 1:
                reason = "MULTI_ADDRESS_IN_ONE_EVENT"
            elif RANGE_RE.match(addr):
                reason = "ADDRESS_RANGE_STYLE"
            elif UNIT_RE.search(addr) or re.search(r"\s+[A-Z]{1,2}-\d+$", addr):
                reason = "UNIT_APT_SUFFIX_PRESENT"
            elif LOT_RE.search(addr):
                reason = "LEGAL_DESC_LOT_PARCEL_STYLE"
            elif addr.upper().split()[-1] in ("LA","HGY","TERR"):
                reason = "SUFFIX_ALIAS_LA_HGY_TERR"

            c[reason] += 1
            if len(samples[reason]) < 20:
                doc = (ev.get("recording") or {}).get("document_number") or (ev.get("recording") or {}).get("document_number_raw")
                samples[reason].append({"doc": doc, "town_raw": town, "addr_raw": addr})

    with open(args.out, "w", encoding="utf-8") as out:
        out.write("TOP_UNKNOWN_REASONS: " + str(c.most_common()) + "\n")
        for reason in [r for r,_ in c.most_common()]:
            out.write("\n--- " + reason + " ---\n")
            for s in samples.get(reason, []):
                out.write(str(s) + "\n")

    print("[done] wrote:", args.out)

if __name__ == "__main__":
    main()
