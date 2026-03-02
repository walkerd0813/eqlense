#!/usr/bin/env python
import argparse, json, re, sys, time
from collections import defaultdict

UNIT_RE = re.compile(r"\b(UNIT|APT|APARTMENT|#|FL|FLOOR|RM|ROOM|STE|SUITE)\b.*$", re.IGNORECASE)

def norm_town(t: str) -> str:
    return (t or "").strip().upper()

def norm_num(n: str) -> str:
    return (n or "").strip().upper()

def street_core_from_key_part(s: str) -> str:
    s = (s or "").strip().upper()
    # Remove trailing unit-ish junk
    s = UNIT_RE.sub("", s).strip()
    # Collapse spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_match_key(mk: str):
    # Expected: A|NUM|STREET|TOWN|
    if not mk or "|" not in mk:
        return None
    parts = mk.split("|")
    if len(parts) < 5:
        return None
    tag = parts[0].strip().upper()
    num = parts[1].strip()
    street = parts[2].strip()
    town = parts[3].strip()
    return tag, num, street, town

def build_key(tag: str, num: str, street: str, town: str) -> str:
    return f"{tag}|{num}|{street}|{town}|"

RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)([A-Z]?)\s*$", re.IGNORECASE)
RANGE_ALPHA_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)([A-Z])\s*$", re.IGNORECASE)

def candidate_nums_from_range(num_raw: str):
    n = norm_num(num_raw)
    # 79-81
    m = RANGE_RE.match(n)
    if m:
        a = m.group(1)
        b = m.group(2)
        suf = (m.group(3) or "").upper()
        # candidate endpoints
        out = []
        out.append(a)
        out.append(b + suf if suf else b)
        # also try stripping alpha if present on right endpoint (71A -> 71)
        if suf:
            out.append(b)
        # dedupe preserving order
        seen = set()
        ded = []
        for x in out:
            if x not in seen:
                ded.append(x)
                seen.add(x)
        return ded
    # 71-71A where the right side is "71A" but range regex already handles it via group3 on right
    # If something else: fall back none
    return []

def load_address_authority_from_canon(canon_path: str, max_rows: int = 0):
    """
    Builds a map: canonical_key -> set(property_id)
    canonical_key is based on attach.evidence.match_keys_used[0] but with street_core cleaned.
    Only uses ATTACHED_A rows.
    """
    m = defaultdict(set)
    rows = 0
    used = 0

    with open(canon_path, "r", encoding="utf-8") as f:
        for ln in f:
            if not ln.strip():
                continue
            rows += 1
            if max_rows and rows > max_rows:
                break
            try:
                r = json.loads(ln)
            except Exception:
                continue

            a = r.get("attach") or {}
            st = (a.get("status") or a.get("attach_status") or "").upper()
            if st != "ATTACHED_A":
                continue

            pid = a.get("property_id") or r.get("property_id")
            if not pid:
                continue

            ev = a.get("evidence") or {}
            mks = ev.get("match_keys_used") or []
            if not mks:
                continue
            mk0 = mks[0]
            parsed = parse_match_key(mk0)
            if not parsed:
                continue
            tag, num, street, town = parsed
            street_core = street_core_from_key_part(street)
            num2 = norm_num(num)
            town2 = norm_town(town)
            key = build_key(tag, num2, street_core, town2)
            m[key].add(pid)
            used += 1

    return m, {"canon_rows_seen": rows, "authority_pairs_loaded": used, "unique_keys": len(m)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--canon", required=True, help="Canonical events NDJSON (used as address authority from ATTACHED_A rows)")
    ap.add_argument("--events", required=True, help="UNKNOWN-only (or mixed) events NDJSON to upgrade by range endpoints")
    ap.add_argument("--out", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--town", default="", help="Optional: restrict upgrades to this town (e.g., BOSTON). Empty = all towns.")
    ap.add_argument("--limit", type=int, default=0, help="Optional cap on events rows processed (0=all).")
    ap.add_argument("--canon_max_rows", type=int, default=0, help="Optional cap when building authority map (0=all).")
    args = ap.parse_args()

    started = time.time()
    town_filter = norm_town(args.town)

    authority, auth_meta = load_address_authority_from_canon(args.canon, max_rows=args.canon_max_rows)

    counts = defaultdict(int)
    counts["authority_unique_keys"] = auth_meta["unique_keys"]
    counts["authority_pairs_loaded"] = auth_meta["authority_pairs_loaded"]
    counts["canon_rows_seen"] = auth_meta["canon_rows_seen"]

    with open(args.out, "w", encoding="utf-8") as fo:
        with open(args.events, "r", encoding="utf-8") as f:
            for ln in f:
                if not ln.strip():
                    continue
                counts["events_rows_seen"] += 1
                if args.limit and counts["events_rows_seen"] > args.limit:
                    break

                try:
                    r = json.loads(ln)
                except Exception:
                    counts["events_json_errors_skipped"] += 1
                    continue

                a = r.get("attach") or {}
                st = (a.get("status") or a.get("attach_status") or "").upper()
                if st != "UNKNOWN":
                    # we still write it through unchanged
                    fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                    counts["events_passthrough_non_unknown"] += 1
                    continue

                pref = r.get("property_ref") or {}
                town = norm_town(pref.get("town_raw") or "")
                if town_filter and town != town_filter:
                    fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                    counts["events_passthrough_wrong_town"] += 1
                    continue

                ev = a.get("evidence") or {}
                mks = ev.get("match_keys_used") or []
                if not mks:
                    fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                    counts["unknown_no_match_key"] += 1
                    continue

                parsed = parse_match_key(mks[0])
                if not parsed:
                    fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                    counts["unknown_bad_match_key_format"] += 1
                    continue

                tag, num_raw, street_raw, town_raw = parsed
                street_core = street_core_from_key_part(street_raw)
                town0 = norm_town(town_raw)
                num0 = norm_num(num_raw)

                cands = candidate_nums_from_range(num0)
                if not cands:
                    fo.write(json.dumps(r, ensure_ascii=False) + "\n")
                    counts["unknown_not_a_range"] += 1
                    continue

                # Build candidate keys and resolve to property_ids
                hit_pids = set()
                hit_keys = []
                for cn in cands:
                    key = build_key(tag, norm_num(cn), street_core, town0)
                    if key in authority:
                        # If key maps to multiple property_ids, add all
                        for pid in authority[key]:
                            hit_pids.add(pid)
                        hit_keys.append(key)

                if len(hit_pids) == 1:
                    pid = next(iter(hit_pids))
                    # Upgrade attach
                    a2 = dict(a)
                    a2["status"] = "ATTACHED_A"
                    a2["property_id"] = pid
                    a2["method"] = "range_endpoints_authority"
                    a2["confidence"] = "A"
                    # Evidence
                    ev2 = dict(ev)
                    ev2["range_num_raw"] = num0
                    ev2["range_candidates"] = cands
                    ev2["street_core_used"] = street_core
                    ev2["authority_hit_keys"] = hit_keys
                    a2["evidence"] = ev2
                    # Flags
                    flags = list(a2.get("flags") or [])
                    if "ATTACHED_VIA_RANGE_ENDPOINTS" not in flags:
                        flags.append("ATTACHED_VIA_RANGE_ENDPOINTS")
                    a2["flags"] = flags

                    r["attach"] = a2
                    counts["upgraded_attached_a"] += 1
                else:
                    # Keep UNKNOWN but flag why
                    a2 = dict(a)
                    flags = list(a2.get("flags") or [])
                    if len(hit_pids) == 0:
                        if "RANGE_NO_MATCH" not in flags:
                            flags.append("RANGE_NO_MATCH")
                        counts["range_no_match"] += 1
                    else:
                        if "RANGE_AMBIGUOUS_MULTI_MATCH" not in flags:
                            flags.append("RANGE_AMBIGUOUS_MULTI_MATCH")
                        counts["range_ambiguous_multi"] += 1
                    a2["flags"] = flags
                    # keep evidence (but add candidates for auditability)
                    ev2 = dict(ev)
                    ev2["range_num_raw"] = num0
                    ev2["range_candidates"] = cands
                    ev2["street_core_used"] = street_core
                    a2["evidence"] = ev2
                    r["attach"] = a2

                fo.write(json.dumps(r, ensure_ascii=False) + "\n")

    elapsed = round(time.time() - started, 2)
    audit = {
        "engine": "events_attach_unknown_range_endpoints_v1",
        "inputs": {"canon": args.canon, "events": args.events},
        "params": {"town": town_filter or "(all)", "limit": args.limit, "canon_max_rows": args.canon_max_rows},
        "counts": dict(counts),
        "seconds": elapsed,
    }
    with open(args.audit, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2, ensure_ascii=False)
    print("[ok]", audit)

if __name__ == "__main__":
    main()