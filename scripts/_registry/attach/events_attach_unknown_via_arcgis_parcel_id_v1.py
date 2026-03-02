#!/usr/bin/env python
import argparse, json, re, time
import urllib.parse, urllib.request
from collections import Counter

BASE = "https://services1.arcgis.com/hGdibHYSPO59RG1h/arcgis/rest/services/L3_TAXPAR_POLY_ASSESS_gdb/FeatureServer/0/query"

UNIT_PATTERNS = [
    re.compile(r"\b(UNIT|APT|APARTMENT|STE|SUITE|FL|FLOOR|NO)\s*([A-Z0-9\-]+)\b", re.I),
    re.compile(r"\#\s*([A-Z0-9\-]+)\b", re.I),
]

NUM_RE = re.compile(r"^(\d+[A-Z]?(?:-\d+[A-Z]?)?)\s+(.*)$", re.I)

def clean(s: str) -> str:
    return " ".join((s or "").strip().split())

def up(s: str) -> str:
    return clean(s).upper()

def strip_unit(addr: str) -> str:
    a = addr or ""
    for p in UNIT_PATTERNS:
        a = p.sub("", a)
    return clean(a)

def parse_num_and_core(addr_base: str):
    addr = up(addr_base)
    m = NUM_RE.match(addr)
    if not m:
        return None, None
    num = m.group(1)
    core = clean(m.group(2))
    return num, core

def esc(s: str) -> str:
    return urllib.parse.quote(s, safe="")

def req_json(url: str):
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))

def arc_query(where: str, out_fields="MAP_PAR_ID,LOC_ID,PROP_ID,ADDR_NUM,FULL_STR,SITE_ADDR,CITY"):
    url = (
        BASE
        + f"?where={esc(where)}"
        + f"&outFields={esc(out_fields)}"
        + "&returnGeometry=false"
        + "&resultRecordCount=5"
        + "&f=json"
    )
    return req_json(url)

def load_spine_index(path: str):
    mp = {}
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            r = json.loads(ln)
            k = str(r.get("map_par_id") or "").strip()
            pid = r.get("property_id")
            if k and pid and k not in mp:
                mp[k] = pid
    return mp

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--spine_index", required=True)
    ap.add_argument("--town", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--limit_unknown", type=int, default=20000)
    ap.add_argument("--sleep", type=float, default=0.05)
    args = ap.parse_args()

    town = up(args.town)
    spine = load_spine_index(args.spine_index)

    c = Counter()
    with open(args.events, "r", encoding="utf-8") as fi, open(args.out, "w", encoding="utf-8") as fo:
        for ln in fi:
            ln = ln.strip()
            if not ln:
                continue

            r = json.loads(ln)
            a = r.get("attach") or {}
            st = up(a.get("status") or "")

            if st != "UNKNOWN":
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            pr = r.get("property_ref") or {}
            t = up(pr.get("town_raw") or "")
            if t != town:
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            c["unknown_seen_in_town"] += 1
            if c["unknown_seen_in_town"] > args.limit_unknown:
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            addr_raw = pr.get("address_raw") or ""
            addr_base = strip_unit(addr_raw)
            num, core = parse_num_and_core(addr_base)
            if not num or not core:
                c["no_num_or_core"] += 1
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                continue

            where1 = f"CITY = '{town}' AND ADDR_NUM = '{num}' AND FULL_STR LIKE '%{core}%'"
            data = arc_query(where1)
            feats = data.get("features") or []
            c["arc_queries"] += 1

            if not feats:
                where2 = f"CITY = '{town}' AND SITE_ADDR LIKE '%{num} {core}%'"
                data = arc_query(where2)
                feats = data.get("features") or []
                c["arc_queries"] += 1
                c["fallback_site_addr"] += 1

            if not feats:
                c["arc_no_hit"] += 1
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                time.sleep(args.sleep)
                continue

            attrs = (feats[0].get("attributes") or {})
            map_par = str(attrs.get("MAP_PAR_ID") or "").strip()
            if not map_par:
                c["arc_hit_missing_map_par_id"] += 1
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                time.sleep(args.sleep)
                continue

            c["arc_hit"] += 1
            pid = spine.get(map_par)

            a["evidence"] = a.get("evidence") or {}
            a["evidence"]["arcgis"] = {
                "MAP_PAR_ID": map_par,
                "LOC_ID": attrs.get("LOC_ID"),
                "PROP_ID": attrs.get("PROP_ID"),
                "ADDR_NUM": attrs.get("ADDR_NUM"),
                "FULL_STR": attrs.get("FULL_STR"),
                "SITE_ADDR": attrs.get("SITE_ADDR"),
                "CITY": attrs.get("CITY"),
            }

            if not pid:
                c["arc_hit_no_spine_match"] += 1
                a["flags"] = list(set((a.get("flags") or []) + ["ARC_HIT_NO_SPINE_PARCEL_ID"]))
                r["attach"] = a
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                time.sleep(args.sleep)
                continue

            a["property_id"] = pid
            a["status"] = "ATTACHED_A"
            a["method"] = "parcel_id_lookup_arcgis"
            a["confidence"] = "A"
            a["flags"] = list(set((a.get("flags") or []) + ["ATTACHED_VIA_ARCGIS_PARCEL_ID"]))
            r["attach"] = a
            c["attached_via_arcgis"] += 1
            fo.write(json.dumps(r, ensure_ascii=False) + "\n")
            time.sleep(args.sleep)

    audit = {
        "counts": dict(c),
        "town": town,
        "limit_unknown": args.limit_unknown,
        "events_in": args.events,
        "spine_index": args.spine_index,
        "out": args.out,
    }
    with open(args.audit, "w", encoding="utf-8") as fo:
        json.dump(audit, fo, indent=2)
    print(json.dumps(audit, indent=2))

if __name__ == "__main__":
    main()