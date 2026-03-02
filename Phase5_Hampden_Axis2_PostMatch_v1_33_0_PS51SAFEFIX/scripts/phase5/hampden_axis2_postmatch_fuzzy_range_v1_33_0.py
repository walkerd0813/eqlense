import argparse
import json
import os
import re
from collections import Counter, defaultdict

# ----------------------------
# Helpers
# ----------------------------

def ndjson_iter(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                # skip bad line, but count later
                yield {"__parse_error__": True, "__raw__": line}


def ndjson_write(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def norm_town(s: str) -> str:
    return (s or "").strip().upper()


def norm_addr_basic(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().upper()
    s = s.replace("\t", " ")
    s = re.sub(r"\s+", " ", s)
    # normalize common punctuation
    s = s.replace(",", " ")
    s = s.replace(".", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# Very small suffix alias map (we keep this conservative)
SUFFIX_ALIAS = {
    "LA": "LN",
    "LANE": "LN",
    "LN": "LN",
    "AV": "AVE",
    "AVENUE": "AVE",
    "STREET": "ST",
    "ROAD": "RD",
    "DRIVE": "DR",
    "TERR": "TER",
    "TERRACE": "TER",
    "PKY": "PKWY",
}

UNIT_TOKENS = {"UNIT", "APT", "APARTMENT", "#", "STE", "SUITE"}


def normalize_suffix_tokens(tokens):
    if not tokens:
        return tokens
    last = tokens[-1]
    repl = SUFFIX_ALIAS.get(last)
    if repl:
        tokens = tokens[:-1] + [repl]
    return tokens


def split_unit(tokens):
    """Split tokens into (street_tokens, unit_tokens) where unit starts at UNIT/APT/#/STE."""
    for i, t in enumerate(tokens):
        if t in UNIT_TOKENS:
            return tokens[:i], tokens[i:]
    return tokens, []


def parse_no_num_rescue(addr_raw: str):
    """Return list of candidate normalized addresses to try (in order) or []."""
    addr = norm_addr_basic(addr_raw)
    if not addr:
        return []

    toks = addr.split(" ")

    out = []

    # Case 1: trailing unit -> reorder: "<street...> UNIT <X>" => "<X> <street...>"
    street_toks, unit_toks = split_unit(toks)
    if unit_toks:
        # we only use the first token after UNIT/APT/# as the unit/leading number candidate
        if len(unit_toks) >= 2:
            unit_val = unit_toks[1]
            # only rescue if unit_val looks like a house number (digits or digits+letter)
            if re.fullmatch(r"\d+[A-Z]?", unit_val):
                st = normalize_suffix_tokens(street_toks)
                cand = " ".join([unit_val] + st)
                out.append(cand)

    # Case 2: leading "2B" / "10V" house numbers
    m = re.match(r"^(\d+)([A-Z])\b", addr)
    if m:
        num = m.group(1)
        letter = m.group(2)
        rest = addr[len(m.group(0)):].strip()
        rest_toks = rest.split(" ") if rest else []
        rest_toks, unit_toks2 = split_unit(rest_toks)
        rest_toks = normalize_suffix_tokens(rest_toks)

        # try "2B <street>"
        out.append(" ".join([f"{num}{letter}"] + rest_toks + unit_toks2))
        # try "2 B <street>" (sometimes stored split)
        out.append(" ".join([num, letter] + rest_toks + unit_toks2))
        # try digits only (fallback)
        out.append(" ".join([num] + rest_toks + unit_toks2))

    # De-dupe while preserving order
    seen = set()
    ordered = []
    for c in out:
        c = norm_addr_basic(c)
        if c and c not in seen:
            seen.add(c)
            ordered.append(c)
    return ordered


def spine_get_fields(row: dict):
    # Heuristics for town/address fields in the spine
    town = row.get("town_norm") or row.get("town") or row.get("city") or row.get("municipality")
    addr = row.get("address_norm") or row.get("address") or row.get("site_address") or row.get("full_address")
    pid = row.get("property_id") or row.get("parcel_id") or row.get("id")
    return town, addr, pid


def event_get_fields(row: dict):
    town = row.get("town") or row.get("town_norm")
    addr = row.get("addr") or row.get("address") or row.get("address_norm")
    return town, addr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--spine", dest="spine", required=True)
    ap.add_argument("--out", dest="out", required=True)
    args = ap.parse_args()

    counters = Counter()

    # Build full-address unique index: town|addr_norm -> [property_id,...]
    idx_full = defaultdict(list)

    spine_rows = 0
    for r in ndjson_iter(args.spine):
        if r.get("__parse_error__"):
            continue
        town, addr, pid = spine_get_fields(r)
        if not town or not addr or not pid:
            continue
        t = norm_town(town)
        a = norm_addr_basic(addr)
        if not t or not a:
            continue
        idx_full[(t, a)].append(pid)
        spine_rows += 1

    # Stream input and apply conservative rescues
    out_rows = []
    in_rows = 0

    for r in ndjson_iter(args.inp):
        in_rows += 1
        if r.get("__parse_error__"):
            counters["no_parse"] += 1
            out_rows.append(r)
            continue

        # Default passthrough
        counters["pass_through"] += 1

        st = (r.get("attach_status") or "").upper()
        why = (r.get("why") or "").lower()
        mm = (r.get("match_method") or "").lower()

        # Only attempt to change UNKNOWN + no_num
        if st == "UNKNOWN" and (why == "no_num" or (mm == "no_match" and why == "no_num")):
            town, addr = event_get_fields(r)
            t = norm_town(town)
            cands = parse_no_num_rescue(addr)

            attached = False
            for cand in cands:
                pids = idx_full.get((t, cand), [])
                if len(pids) == 1:
                    r["attach_status"] = "ATTACHED_B"
                    r["property_id"] = pids[0]
                    r["match_method"] = "axis2_no_num_rescue_unique_exact"
                    r["why"] = None
                    counters["attach_no_num_rescue_unique_exact"] += 1
                    attached = True
                    break

            if not attached:
                counters["no_num_no_rescue"] += 1

        out_rows.append(r)

    # Write output
    ndjson_write(args.out, out_rows)

    audit = {
        "version": "v1_33_0",
        "in": args.inp,
        "spine": args.spine,
        "out": args.out,
        "spine_rows_indexed": spine_rows,
        "rows_in": in_rows,
        "counts": dict(counters),
    }
    audit_path = os.path.splitext(args.out)[0] + "__audit_v1_33_0.json"
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=2)

    # Minimal console summary
    print("[done] v1_33_0 postmatch")
    for k, v in counters.most_common():
        print(f"  {k}: {v}")
    print(f"[ok] OUT   {args.out}")
    print(f"[ok] AUDIT {audit_path}")


if __name__ == "__main__":
    main()
