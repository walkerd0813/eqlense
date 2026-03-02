import json, re, collections
from pathlib import Path

IN_PATH = r"C:\seller-app\backend\publicData\registry\hampden\_attached_DEED_ONLY_v1_7_9\events_attached_DEED_ONLY_v1_7_9.ndjson"
OUT_PATH = r"C:\seller-app\backend\publicData\_audit\registry\hampden_deeds_unknown_bucket_report_v1_7_9.txt"

# --- helpers ---
def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def is_range_addr(a: str) -> bool:
    return bool(re.match(r"^\d+\s*-\s*\d+\s+", a or ""))

def has_unit(a: str) -> bool:
    return bool(re.search(r"\b(UNIT|APT|APARTMENT|#)\b", a or ""))

def lot_par_style(a: str) -> bool:
    return bool(re.search(r"\b(LOT|PAR|PARCEL)\b", a or "")) and not bool(re.match(r"^\d+\s", a or ""))

def odd_suffix(a: str) -> bool:
    # Hampden registry quirks seen: "LA", "HGY", "TERR"
    return bool(re.search(r"\b(LA|HGY|TERR)\b", a or ""))

def multi_addr_hint(ev: dict) -> bool:
    blk = ((ev.get("document") or {}).get("raw_block")) or ((ev.get("property_ref") or {}).get("legal_desc_raw")) or ""
    if not blk: 
        return False
    # multiple "Town:" lines or "Addr:" occurrences
    return len(re.findall(r"\bTown:\b", blk)) >= 2 or len(re.findall(r"\bAddr:\b", blk)) >= 2

def missing_locator(ev: dict) -> bool:
    pr = ev.get("property_ref") or {}
    t = norm_ws(pr.get("town_raw") or pr.get("town") or "")
    a = norm_ws(pr.get("address_raw") or pr.get("address") or "")
    return (not t) or (not a)

def get_doc(ev: dict) -> str:
    rec = ev.get("recording") or {}
    return str(rec.get("document_number") or rec.get("document_number_raw") or "").strip()

def get_town_addr(ev: dict):
    pr = ev.get("property_ref") or {}
    town = norm_ws(pr.get("town_raw") or pr.get("town") or "")
    addr = norm_ws(pr.get("address_raw") or pr.get("address") or "")
    return town, addr

def get_status(ev: dict) -> str:
    at = ev.get("attach") or {}
    return (at.get("attach_status") or "").strip() or "NO_ATTACH_FIELD"

# --- scan ---
c = collections.Counter()
samples = collections.defaultdict(list)

total = 0
unknown_total = 0

with open(IN_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        total += 1
        ev = json.loads(line)
        st = get_status(ev)

        if st != "UNKNOWN":
            continue

        unknown_total += 1
        town, addr = get_town_addr(ev)
        doc = get_doc(ev)

        # bucket logic (ordered)
        if missing_locator(ev):
            reason = "MISSING_TOWN_OR_ADDRESS"
        elif multi_addr_hint(ev):
            reason = "MULTI_ADDRESS_IN_ONE_EVENT"
        elif is_range_addr(addr):
            reason = "ADDRESS_RANGE_123-125_STYLE"
        elif has_unit(addr):
            reason = "UNIT_APT_SUFFIX_PRESENT"
        elif lot_par_style(addr):
            reason = "LEGAL_DESC_LOT_PARCEL_STYLE"
        elif odd_suffix(addr):
            reason = "SUFFIX_ALIAS_LA_HGY_TERR"
        else:
            reason = "OTHER_KEY_MISMATCH"

        c[reason] += 1
        if len(samples[reason]) < 12:
            samples[reason].append({"doc": doc, "town_raw": town, "addr_raw": addr})

report_lines = []
report_lines.append(f"IN: {IN_PATH}")
report_lines.append(f"TOTAL_EVENTS: {total}")
report_lines.append(f"UNKNOWN_TOTAL: {unknown_total}")
report_lines.append(f"TOP_UNKNOWN_REASONS: {c.most_common()}")
report_lines.append("")

for reason, _ in c.most_common():
    report_lines.append(f"--- {reason} ---")
    for s in samples[reason]:
        report_lines.append(str(s))
    report_lines.append("")

Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
Path(OUT_PATH).write_text("\n".join(report_lines), encoding="utf-8")

print("[done] wrote:", OUT_PATH)
print("TOP_UNKNOWN_REASONS:", c.most_common())
