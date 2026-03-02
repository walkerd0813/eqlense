#!/usr/bin/env python3
# hampden_axis2_nonum_rescue_v1_35_1.py
# Conservative NO_NUM rescue while preserving all fields.

import argparse, json, re
from collections import Counter, defaultdict

def iter_ndjson(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def write_ndjson(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

_SUFFIX_MAP = {
    "STREET":"ST","ST":"ST",
    "AVENUE":"AVE","AVE":"AVE",
    "ROAD":"RD","RD":"RD",
    "DRIVE":"DR","DR":"DR",
    "LANE":"LN","LN":"LN",
    "COURT":"CT","CT":"CT",
    "PLACE":"PL","PL":"PL",
    "BOULEVARD":"BLVD","BLVD":"BLVD",
    "PARKWAY":"PKWY","PKWY":"PKWY",
    "CIRCLE":"CIR","CIR":"CIR",
    "TERRACE":"TER","TER":"TER",
    "HIGHWAY":"HWY","HWY":"HWY",
}

def norm_tokens(s: str):
    if not s or not isinstance(s, str):
        return []
    s = s.upper().strip()
    s = re.sub(r"[#,]", " ", s)
    s = re.sub(r"\\s+", " ", s).strip()
    toks = s.split(" ")
    out = []
    for t in toks:
        out.append(_SUFFIX_MAP.get(t, t))
    return out

def extract_unit(tokens):
    # Conservative: UNIT/APT <X>
    if not tokens:
        return tokens, None
    unit_markers = {"UNIT","APT","APARTMENT","#"}
    out = []
    unit = None
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t in unit_markers and i + 1 < len(tokens):
            cand = tokens[i+1]
            if re.fullmatch(r"[A-Z0-9\\-]{1,8}", cand):
                unit = cand
                i += 2
                continue
        out.append(t)
        i += 1
    return out, unit

def build_spine_index(spine_path):
    '''
    Build conservative index:
      key: (town_norm, street_no, street_norm) -> list of property_id candidates
    Only index rows with clear town + addr and leading numeric street number.
    '''
    idx = defaultdict(list)
    for r in iter_ndjson(spine_path):
        town = (r.get("town") or r.get("municipality") or r.get("city") or "")
        town = town.strip().upper() if isinstance(town, str) else ""
        addr = (r.get("addr") or r.get("address") or r.get("site_addr") or r.get("address_full") or "")
        if not town or not addr or not isinstance(addr, str):
            continue
        toks = norm_tokens(addr)
        if not toks:
            continue
        if not re.fullmatch(r"\\d+", toks[0]):
            continue
        no = toks[0]
        rest = toks[1:]
        rest, _unit = extract_unit(rest)
        street = " ".join(rest).strip()
        if not street:
            continue
        pid = r.get("property_id") or r.get("id") or r.get("parcel_id")
        if pid:
            idx[(town, no, street)].append(pid)
    # prune empties
    for k in list(idx.keys()):
        idx[k] = [p for p in idx[k] if p]
        if not idx[k]:
            del idx[k]
    return idx

def try_rescue_no_num(row, spine_idx):
    '''
    Intentionally conservative: currently no automatic rescues.
    We keep this function as a placeholder for future *gated* rescues.
    '''
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--spine", dest="spine", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    ap.add_argument("--audit", dest="auditp", required=True)
    args = ap.parse_args()

    _spine_idx = build_spine_index(args.spine)

    counts = Counter()
    out_rows = []

    for row in iter_ndjson(args.inp):
        st = row.get("attach_status")
        why = row.get("why")
        if st != "UNKNOWN":
            out_rows.append(row)
            counts["pass_through"] += 1
            continue

        rescued = None
        if str(why).lower() == "no_num":
            rescued = try_rescue_no_num(row, _spine_idx)

        if rescued is None:
            out_rows.append(row)
            if str(why).lower() == "no_num":
                counts["still_unknown_no_num"] += 1
            else:
                counts["still_unknown_other"] += 1
            continue

        new_row = dict(row)
        new_row.update(rescued)
        out_rows.append(new_row)
        counts["rescued"] += 1

    write_ndjson(args.outp, out_rows)

    audit = {
        "version": "v1_35_1",
        "in": args.inp,
        "spine": args.spine,
        "out": args.outp,
        "counts": dict(counts),
        "notes": [
            "v1_35_1 preserves full rows; does not drop town/addr on UNKNOWN rows.",
            "Rescue logic remains intentionally conservative (no automatic NO_NUM rescues performed).",
        ],
    }
    with open(args.auditp, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("[done] v1_35_1 NONUM rescue (preserve fields)")
    for k, v in counts.items():
        print(f"  {k}: {v}")
    print(f"[ok] OUT   {args.outp}")
    print(f"[ok] AUDIT {args.auditp}")

if __name__ == "__main__":
    main()
