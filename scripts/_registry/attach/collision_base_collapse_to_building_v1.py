#!/usr/bin/env python3
import argparse, json, os, re, hashlib
from collections import defaultdict, Counter

# Minimal suffix canonicalization for base-key matching
# (event match_keys often say STREET/ROAD/AVENUE; spine uses ST/RD/AVE)
SUFFIX_MAP = {
  "STREET":"ST","ST":"ST",
  "ROAD":"RD","RD":"RD",
  "AVENUE":"AVE","AVE":"AVE",
  "BOULEVARD":"BLVD","BLVD":"BLVD",
  "PLACE":"PL","PL":"PL",
  "COURT":"CT","CT":"CT",
  "LANE":"LN","LN":"LN",
  "DRIVE":"DR","DR":"DR",
  "TERRACE":"TER","TER":"TER",
  "CIRCLE":"CIR","CIR":"CIR",
  "SQUARE":"SQ","SQ":"SQ",
  "PARKWAY":"PKWY","PKWY":"PKWY",
  "HIGHWAY":"HWY","HWY":"HWY",
  "WAY":"WAY","WY":"WAY",
  "ROW":"ROW",
  "EXTENSION":"EXT","EXT":"EXT",
}

DIR_MAP = {"NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W","N":"N","S":"S","E":"E","W":"W","NE":"NE","NW":"NW","SE":"SE","SW":"SW"}

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def norm_town(s: str) -> str:
    return norm_space(s).upper()

def norm_addr_base(addr: str) -> str:
    """
    Normalize to something comparable between:
      event match_key: '580 WASHINGTON STREET'
      spine core:      '580 WASHINGTON ST'
    We do NOT attempt unit parsing here.
    """
    s = norm_space(addr).upper()
    # strip punctuation that sometimes appears
    s = re.sub(r"[.,#]", " ", s)
    s = norm_space(s)

    # convert common directionals to canonical tokens
    toks = s.split(" ")
    toks2 = []
    for t in toks:
        toks2.append(DIR_MAP.get(t, t))
    s = " ".join(toks2)

    # last token suffix canonicalization (only if it's a known suffix word)
    toks = s.split(" ")
    if len(toks) >= 2:
        last = toks[-1]
        if last in SUFFIX_MAP:
            toks[-1] = SUFFIX_MAP[last]
    s = " ".join(toks)

    # handle "ST" spelled as "STREET" or vice versa already via map
    return s

def build_spine_base_index(spine_path: str, prefer_building_group=True):
    idx = defaultdict(set)   # town|base -> set(anchor_ids)
    stats = Counter()
    with open(spine_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            town = norm_town(r.get("town"))
            sno = norm_space(r.get("street_no"))
            sname = norm_space(r.get("street_name"))
            if not town or not sno or not sname:
                stats["spine_rows_missing_core"] += 1
                continue

            base = norm_addr_base(f"{sno} {sname}")
            key = f"{town}|{base}"

            anchor = None
            if prefer_building_group:
                anchor = r.get("building_group_id") or r.get("property_id") or r.get("parcel_id")
            else:
                anchor = r.get("property_id") or r.get("building_group_id") or r.get("parcel_id")

            if anchor:
                idx[key].add(anchor)
                stats["spine_rows_indexed"] += 1
            else:
                stats["spine_rows_missing_anchor"] += 1
    return idx, stats

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_events", required=True, help="events output ndjson (already attached) to post-fix")
    ap.add_argument("--spine", required=True, help="spine ndjson (v44_1 unitfix or later)")
    ap.add_argument("--out", required=True, help="output ndjson (postfixed)")
    ap.add_argument("--audit", required=True, help="audit json")
    ap.add_argument("--anchor", default="building_group_id", choices=["building_group_id","property_id"], help="prefer which anchor field (default building_group_id)")
    args = ap.parse_args()

    prefer_bg = (args.anchor == "building_group_id")
    spine_idx, spine_stats = build_spine_base_index(args.spine, prefer_building_group=prefer_bg)

    counts = Counter()
    resolved = 0
    scanned = 0

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.audit), exist_ok=True)

    with open(args.in_events, "r", encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            scanned += 1
            ev = json.loads(line)
            a = ev.get("attach") or {}

            status = a.get("attach_status")
            mm = a.get("match_method")
            mk = a.get("match_key") or ""

            counts[f"in_status::{status}"] += 1
            counts[f"in_method::{mm}"] += 1

            # Only attempt: collision_base + no unit key
            if status == "UNKNOWN" and mm == "collision_base" and not a.get("match_key_unit"):
                # mk format: TOWN|<addr>
                if "|" in mk:
                    town, addr = mk.split("|", 1)
                    key = f"{norm_town(town)}|{norm_addr_base(addr)}"
                    anchors = spine_idx.get(key) or set()
                    if len(anchors) == 1:
                        anchor_id = next(iter(anchors))
                        # deterministically attach to building anchor
                        a["attach_status"] = "ATTACHED_A"
                        a["property_id"] = anchor_id
                        a["match_method"] = "base|collision_collapsed"
                        a["attach_scope"] = "SINGLE"
                        a["match_tag"] = "building_collapse"
                        a["candidate_anchor_count"] = 1
                        resolved += 1
                        counts["resolved_collision_base_to_building"] += 1
                    else:
                        # keep UNKNOWN; record why
                        a["candidate_anchor_count"] = len(anchors)
                        if len(anchors) == 0:
                            counts["collision_base_no_spine_key"] += 1
                        else:
                            counts["collision_base_multi_anchor_still_ambiguous"] += 1

                ev["attach"] = a

            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")

    audit = {
        "done": True,
        "rows_scanned": scanned,
        "rows_resolved": resolved,
        "spine_index_stats": dict(spine_stats),
        "counters": dict(counts),
        "notes": {
            "rule": "If event is UNKNOWN+collision_base and has no unit key, collapse to unique building anchor if all spine candidates share exactly 1 building_group_id/property_id.",
            "safety": "Never guesses unit; only attaches when building anchor is unique."
        }
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print(json.dumps({"done": True, "rows_scanned": scanned, "rows_resolved": resolved}, indent=2))

if __name__ == "__main__":
    main()