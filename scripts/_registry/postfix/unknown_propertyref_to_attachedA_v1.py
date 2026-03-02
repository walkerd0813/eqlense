import argparse, json, re, datetime
from collections import defaultdict

def nowz():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def up(s):
    return (s or "").strip().upper()

def norm_ws(s):
    return re.sub(r"\\s+", " ", (s or "").strip())

def fix_saint_prefix(street):
    s = norm_ws(street).upper()
    if s.startswith("ST "):
        return "ST " + s[3:]
    return s

def norm_street(s):
    s = norm_ws(s)
    s = s.replace(".", "")
    s = s.replace(" AVE", " AVENUE").replace(" RD", " ROAD").replace(" ST", " STREET").replace(" DR", " DRIVE").replace(" TER", " TERRACE")
    s = fix_saint_prefix(s)
    return up(s)

def parse_addr(addr_norm):
    a = norm_ws(addr_norm).upper()
    a = a.replace(",", " ")
    a = norm_ws(a)
    unit = None
    m = re.search(r"\buNIT\s+[A-Z0-9\-]+\b", a)
    if m:
        m = re.search(r"\buNIT\s+([A-Z0-9\-]+)\b", a)
        unit = m.group(1)
        a = norm_ws(re.sub(r"\buNIT\s+[A-Z0-9\-]+\b", "", a))
    a = a.replace("â€“","-")
    m = re.match(r"^(\d+)\s*-\s*(\d+)\s+(.*)$", a)
    if m:
        return (f"{m.group(1)}-{m.group(2)}", m.group(3), unit, True)
    m = re.match(r"^(\d+)\s+(\d+)\s+(.*)$", a)
    if m and abs(int(m.group(2))-int(m.group(1)))<=20:
        return (f"{m.group(1)}-{m.group(2)}", m.group(3), unit, True)
    m = re.match(r"^(\d+)\s+(.*)$", a)
    if not m:
        return (None, None, unit, False)
    return (m.group(1), m.group(2), unit, False)

def build_spine_indices(spine_path):
    base = defaultdict(list)
    unit = defaultdict(list)
    stats = {"rows":0, "base_keys":0, "unit_keys":0}
    with open(spine_path,"r",encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            stats["rows"] += 1
            try:
                r = json.loads(line)
            except:
                continue
            town = up(r.get("town") or (r.get("property_ref") or {}).get("town_code") or "")
            ak = (r.get("address_key") or "").strip()
            if ak.startswith("A|"):
                parts = ak.split("|")
                if len(parts)>=5:
                    sn = parts[1].strip()
                    st = norm_street(parts[2])
                    key = f"{town}|{sn}|{st}"
                    base[key].append(r)
                    stats["base_keys"] += 1
            uval = (r.get("unit") or "").strip()
            if uval and ak.startswith("A|"):
                parts = ak.split("|")
                if len(parts)>=5:
                    sn = parts[1].strip()
                    st = norm_street(parts[2])
                    ukey = f"{town}|{sn}|{st}|UNIT|{up(uval)}"
                    unit[ukey].append(r)
                    stats["unit_keys"] += 1
    return base, unit, stats

def apply_attach(ev, spine_row, status, method, match_key_used):
    a = ev.get("attach") or {}
    a["attach_status"] = status
    a["property_id"] = spine_row.get("property_id") or spine_row.get("property_uid")
    a["match_method"] = method
    a["match_key"] = a.get("match_key") or match_key_used
    a["match_key_used"] = match_key_used
    a["attached_at"] = nowz()
    ev["attach"] = a:
    return ev



