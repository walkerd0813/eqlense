import re

# Anything like ST/RD/AVE etc must NEVER be treated as doc type
STREET_TOKENS = {
    "ST","RD","AVE","AV","DR","LN","CT","PL","BLVD","WAY","PATH","SQ","TER","TERR","CIR",
    "N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST"
}

# Doc type tokens are usually 2–8 chars, mostly letters, sometimes mixed (e.g., DM, MTL, ESMT, LIEN, DEED)
DOC_TYPE_RE = re.compile(r"^[A-Z][A-Z0-9\-]{1,11}$")

# A small “protect list” you already see a lot
KNOWN_GOOD = {
    "DEED","DM","ASN","REL","LIEN","MTL","ESMT","MORT","MSDD","FTL","LIS","DIS"
}

def parse_doc_type_from_record_line(line: str):
    """
    Given a main record line like:
      '235,725   1 MTL   H C   3:37  MASSACHUSETTS TAX LIEN ...'
    return 'MTL' (or None if not parseable).
    """
    s = (line or "").strip().upper()
    if not s:
        return None

    # Must start with an instrument number (often comma-separated) and a seq integer
    # Example: 235,725   1 ...
    m = re.match(r"^\s*\d[\d,]*\s+(\d+)\s+([A-Z0-9\-]+)\b", s)
    if not m:
        return None

    dt = m.group(2).strip().upper()
    if dt in STREET_TOKENS:
        return None
    if not DOC_TYPE_RE.match(dt):
        return None

    # If it's a 2-char token that looks like a street suffix, reject
    if dt in STREET_TOKENS:
        return None

    # Accept if it’s known good, otherwise accept but you can later tighten if needed
    return dt
