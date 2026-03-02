import json, re, collections

PATH = r"C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_v1_7_8\events_attached_DEED_ONLY_v1_7_8.ndjson"

UNIT_RE = re.compile(r"\b(UNIT|APT|APARTMENT|STE|SUITE|#)\b", re.I)
RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s+")
LOT_RE   = re.compile(r"\b(LOT|PL/W/CTF|CTF|PAR)\b", re.I)
POBOX_RE = re.compile(r"\bP\.?\s*O\.?\s*BOX\b", re.I)
ROUTE_RE = re.compile(r"\bRTE\b|\bROUTE\b", re.I)

def classify(town_raw, addr_raw, candidates_considered):
    t = (town_raw or "").strip()
    a = (addr_raw or "").strip()

    if not t or not a:
        return "MISSING_TOWN_OR_ADDRESS"

    # legal-description-ish (won't match postal spine)
    if LOT_RE.search(a):
        return "LEGAL_DESC_LOT_PARCEL_STYLE"

    if POBOX_RE.search(a):
        return "PO_BOX"

    # multiple addresses tend to appear as commas or repeated Addr lines (already expanded)
    if "," in a:
        return "COMMA_MULTI_FRAGMENT"

    if RANGE_RE.match(a):
        return "RANGE_NOT_EXPANDED_OR_TOO_WIDE"

    if UNIT_RE.search(a):
        return "UNIT_NOT_STRIPPED_OR_ODD_FORMAT"

    if candidates_considered == 0:
        return "NO_CANDIDATES_GENERATED"

    return "OTHER_KEY_MISMATCH"

reasons = collections.Counter()
samples = collections.defaultdict(list)

unknown_total = 0
missing_total = 0

with open(PATH, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip(): 
            continue
        ev = json.loads(line)
        if ev.get("event_type") != "DEED":
            continue

        st = ((ev.get("attach") or {}).get("status") or "")
        pr = ev.get("property_ref") or {}
        town_raw = pr.get("town_raw") or pr.get("town") or ""
        addr_raw = pr.get("address_raw") or pr.get("address") or ""
        cand = (ev.get("attach") or {}).get("candidates_considered", 0)

        if st == "UNKNOWN":
            unknown_total += 1
            r = classify(town_raw, addr_raw, cand)
            reasons[r] += 1
            if len(samples[r]) < 12:
                doc = (ev.get("recording") or {}).get("document_number") or (ev.get("recording") or {}).get("document_number_raw")
                samples[r].append({"town_raw": town_raw, "addr_raw": addr_raw, "doc": doc, "cand": cand})
        elif st == "MISSING_TOWN_OR_ADDRESS":
            missing_total += 1

print("UNKNOWN_TOTAL", unknown_total)
print("MISSING_TOWN_OR_ADDRESS_TOTAL", missing_total)
print("TOP_REASONS", reasons.most_common(12))
for r, arr in samples.items():
    print("\n---", r, "---")
    for s in arr[:8]:
        print(s)
