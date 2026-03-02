import json, re, argparse
from collections import Counter

def it(p):
    with open(p,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if line:
                yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    args = ap.parse_args()

    suffix_last = Counter()
    towns = Counter()
    kinds = Counter()

    for ev in it(args.inp):
        a = ev.get("attach",{}) or {}
        if (a.get("attach_status") or "").upper() != "UNKNOWN":
            continue
        pr = ev.get("property_ref",{}) or {}
        addr = (pr.get("address_norm") or pr.get("address_raw") or "").upper().strip()
        town = (pr.get("town_norm") or pr.get("town_raw") or "").upper().strip()
        towns[town]+=1
        toks = addr.split()
        if toks:
            suffix_last[toks[-1]] += 1

        if re.match(r"^\d+\s*-\s*\d+\s+", addr): kinds["range_like"]+=1
        elif re.match(r"^\d+[A-Z]\b", addr): kinds["alpha_suffix_num (2B/10A)"]+=1
        elif re.match(r"^\d+\s+", addr): kinds["simple_num"]+=1
        else: kinds["no_num"]+=1

    print("IN:", args.inp)
    print("UNKNOWN towns top10:", towns.most_common(10))
    print("UNKNOWN last-token top20:", suffix_last.most_common(20))
    print("UNKNOWN kind:", dict(kinds))

if __name__ == "__main__":
    main()
