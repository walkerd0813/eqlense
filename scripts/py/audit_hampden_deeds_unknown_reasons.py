import json, re
from collections import Counter, defaultdict

ATTACHED = r"C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_v1\events_attached_DEED_ONLY_v1.ndjson"

def norm_ws(s): return re.sub(r"\s+", " ", (s or "").strip())

def has_unit(addr):
    a = (addr or "").upper()
    return any(tok in a for tok in [" APT ", " UNIT ", "#", " FLOOR ", " FL ", " STE ", " SUITE "])

def has_range(addr):
    a = (addr or "")
    return bool(re.search(r"\b\d+\s*-\s*\d+\b", a))

def has_multi_addr_raw(raw):
    if not raw: return False
    t = raw.upper()
    # common in your PDF: repeated "Town: ... Addr:" lines
    return t.count("ADDR:") >= 2

def looks_like_bad_town(town):
    t = (town or "").upper()
    return ("ADDR" in t) or t.endswith(" Y") or ("  Y" in t)

def looks_like_bad_addr(addr):
    a = (addr or "")
    if not a.strip(): return True
    if a.upper().endswith(" Y"): return True
    if "                                       " in a: return True
    return False

reasons = Counter()
samples = defaultdict(list)

total_unknown = 0
total_missing = 0

with open(ATTACHED, "r", encoding="utf-8") as f:
    for line in f:
        line=line.strip()
        if not line: continue
        ev = json.loads(line)

        st = ((ev.get("attach") or {}).get("status") 
              or (ev.get("attach_status")) 
              or (ev.get("attach_status_counts")))  # tolerate schema drift

        # your attach script writes attach_status inside event; most likely ev["attach"]["status"]
        if isinstance(st, dict):
            continue

        # robust: check known fields
        status = None
        if isinstance(ev.get("attach"), dict):
            status = ev["attach"].get("status")
        if not status:
            status = ev.get("attach_status")

        if status == "MISSING_TOWN_OR_ADDRESS":
            total_missing += 1
            continue
        if status != "UNKNOWN":
            continue

        total_unknown += 1

        pr = ev.get("property_ref") or {}
        town_raw = pr.get("town_raw") or pr.get("town") or ""
        addr_raw = pr.get("address_raw") or pr.get("address") or ""
        raw_lines = pr.get("raw_lines") or ev.get("raw_lines") or None
        raw_block = pr.get("raw_block") or ev.get("raw_block") or ev.get("document", {}).get("raw_block") or ""

        town = norm_ws(town_raw)
        addr = norm_ws(addr_raw)

        picked = False

        if has_multi_addr_raw(raw_block):
            reasons["MULTI_ADDRESS_IN_ONE_EVENT"] += 1
            if len(samples["MULTI_ADDRESS_IN_ONE_EVENT"]) < 8:
                samples["MULTI_ADDRESS_IN_ONE_EVENT"].append({"town_raw":town_raw,"addr_raw":addr_raw,"doc":(ev.get("recording") or {}).get("document_number")})
            picked = True

        if has_range(addr):
            reasons["ADDRESS_RANGE_133-137_STYLE"] += 1
            if len(samples["ADDRESS_RANGE_133-137_STYLE"]) < 8:
                samples["ADDRESS_RANGE_133-137_STYLE"].append({"town_raw":town_raw,"addr_raw":addr_raw,"doc":(ev.get("recording") or {}).get("document_number")})
            picked = True

        if has_unit(addr):
            reasons["UNIT_APT_SUFFIX_PRESENT"] += 1
            if len(samples["UNIT_APT_SUFFIX_PRESENT"]) < 8:
                samples["UNIT_APT_SUFFIX_PRESENT"].append({"town_raw":town_raw,"addr_raw":addr_raw,"doc":(ev.get("recording") or {}).get("document_number")})
            picked = True

        if looks_like_bad_town(town_raw):
            reasons["TOWN_HAS_ADDR_OR_TRAILING_Y"] += 1
            if len(samples["TOWN_HAS_ADDR_OR_TRAILING_Y"]) < 8:
                samples["TOWN_HAS_ADDR_OR_TRAILING_Y"].append({"town_raw":town_raw,"addr_raw":addr_raw,"doc":(ev.get("recording") or {}).get("document_number")})
            picked = True

        if looks_like_bad_addr(addr_raw):
            reasons["ADDR_HAS_TRAILING_Y_OR_SPACING"] += 1
            if len(samples["ADDR_HAS_TRAILING_Y_OR_SPACING"]) < 8:
                samples["ADDR_HAS_TRAILING_Y_OR_SPACING"].append({"town_raw":town_raw,"addr_raw":addr_raw,"doc":(ev.get("recording") or {}).get("document_number")})
            picked = True

        if not picked:
            reasons["OTHER_KEY_MISMATCH"] += 1
            if len(samples["OTHER_KEY_MISMATCH"]) < 8:
                samples["OTHER_KEY_MISMATCH"].append({"town_raw":town_raw,"addr_raw":addr_raw,"doc":(ev.get("recording") or {}).get("document_number")})

print("UNKNOWN_TOTAL", total_unknown)
print("MISSING_TOWN_OR_ADDRESS_TOTAL", total_missing)
print("TOP_REASONS", reasons.most_common(20))
for k,v in samples.items():
    print("\n---",k,"---")
    for s in v:
        print(s)
