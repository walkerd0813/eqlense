import json, re, sys
from collections import Counter, defaultdict

IN_NDJSON = sys.argv[1]
OUT_UNKNOWN = sys.argv[2]
OUT_REPORT = sys.argv[3]

RE_TRAIL_Y = re.compile(r"\s+Y$", re.I)
RE_RANGE = re.compile(r"\b\d+\s*-\s*\d+\b")
RE_UNIT  = re.compile(r"\b(UNIT|APT|APARTMENT|#)\b", re.I)
RE_LOT   = re.compile(r"\bLOT\b", re.I)
RE_HGY   = re.compile(r"\bHGY\b", re.I)
RE_HWY   = re.compile(r"\bHWY\b|\bHIGHWAY\b", re.I)
RE_CDR   = re.compile(r"\bCDR\b", re.I)

def bump(c, k, n=1): c[k] += n

def get_status(ev):
    a = ev.get("attach") or {}
    return a.get("attach_status") or "UNKNOWN"

def is_multi(ev):
    a = ev.get("attach") or {}
    return (a.get("attach_scope") == "MULTI") or bool((ev.get("property_ref") or {}).get("primary_is_multi"))

def addr_text(ev):
    pr = ev.get("property_ref") or {}
    # best-effort; contract events should have these
    raw = pr.get("address_raw") or ""
    norm = pr.get("address_norm") or ""
    return f"{raw} || {norm}"

unknown_rows = 0
written = 0

status_counts = Counter()
why_counts = Counter()
pattern_counts = Counter()
multi_breakdown = Counter()

top_examples = defaultdict(list)

with open(IN_NDJSON, "r", encoding="utf-8") as fin, open(OUT_UNKNOWN, "w", encoding="utf-8") as fout:
    for line in fin:
        line=line.strip()
        if not line: continue
        ev=json.loads(line)

        st = get_status(ev)
        bump(status_counts, st)

        # triage set = UNKNOWN + PARTIAL_MULTI + MISSING_TOWN_OR_ADDRESS
        if st not in ("UNKNOWN", "PARTIAL_MULTI", "MISSING_TOWN_OR_ADDRESS"):
            continue

        unknown_rows += 1
        fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
        written += 1

        a = ev.get("attach") or {}
        # reason
        reason = None
        if st == "PARTIAL_MULTI":
            # count how many attached inside
            attached = 0
            total = 0
            for att in (a.get("attachments") or []):
                total += 1
                if att.get("attach_status") == "ATTACHED_A":
                    attached += 1
            reason = f"PARTIAL_MULTI_{attached}_of_{total}"
            bump(multi_breakdown, reason)
        else:
            mm = a.get("match_method")
            if mm == "collision": reason = "collision"
            elif mm == "no_match": reason = "no_match"
            else: reason = (mm or "unknown_reason")

        bump(why_counts, reason)

        # patterns
        txt = addr_text(ev).upper()
        if RE_TRAIL_Y.search(txt): bump(pattern_counts, "trail_Y")
        if RE_RANGE.search(txt):   bump(pattern_counts, "range")
        if RE_UNIT.search(txt):    bump(pattern_counts, "unit_token")
        if RE_LOT.search(txt):     bump(pattern_counts, "lot_token")
        if RE_HGY.search(txt):     bump(pattern_counts, "HGY")
        if RE_HWY.search(txt):     bump(pattern_counts, "HWY/HIGHWAY")
        if RE_CDR.search(txt):     bump(pattern_counts, "CDR")

        # capture examples (cap)
        k = reason
        if len(top_examples[k]) < 10:
            top_examples[k].append({
                "event_id": ev.get("event_id"),
                "town": (ev.get("property_ref") or {}).get("town_norm") or (ev.get("property_ref") or {}).get("town_raw"),
                "addr_raw": (ev.get("property_ref") or {}).get("address_raw"),
                "addr_norm": (ev.get("property_ref") or {}).get("address_norm"),
                "match_key": (a.get("match_key")),
                "scope": a.get("attach_scope"),
            })

report = {
    "in_file": IN_NDJSON,
    "unknowns_out_file": OUT_UNKNOWN,
    "rows_written": written,
    "status_counts": dict(status_counts),
    "why_counts": dict(why_counts),
    "pattern_counts": dict(pattern_counts),
    "multi_breakdown": dict(multi_breakdown),
    "examples_by_reason": dict(top_examples),
}

with open(OUT_REPORT, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

print(json.dumps({
    "rows_written": written,
    "top_why": why_counts.most_common(10),
    "top_patterns": pattern_counts.most_common(10)
}, indent=2))
