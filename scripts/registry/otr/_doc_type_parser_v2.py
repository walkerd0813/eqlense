import re

SUSPICIOUS = {
  "ST","RD","AVE","DR","LA","CIR","TERR","NORTH","SOUTH","EAST","WEST","PL","BLVD","WAY","CT","RUN","PATH","SQ","TER","LN",
  "MAIN","HILL","PINE","PARK","MAPLE","OAK","ELM","BAY","STATE","BOSTON","UNION","MILL","HIGH","STONY"
}

def _clean(tok: str) -> str:
  tok = (tok or "").strip().upper()
  tok = re.sub(r"[^A-Z0-9\-\_]", "", tok)
  return tok[:32]

def parse_doc_type_from_record_line_v2(line: str):
  """
  Hampden OTR index lines are fixed-width-ish. The doc-type is the short token just after SEQ.
  We slice a conservative window where doc-type lives, then validate.
  Returns: (doc_type_code, qa_meta)
  """
  raw = line.rstrip("\n")
  qa = {"method": None, "candidate": None}

  # Heuristic: find the "SEQ" column by pattern: <number><spaces><seq><spaces><TYPE>
  # Many lines begin with "  235,725   1 MTL   H C   3:37  ..."
  m = re.match(r"^\s*[\d,]+\s+(\d+)\s+([A-Z0-9]{1,10})\s+", raw)
  if m:
    cand = _clean(m.group(2))
    qa["method"] = "regex_front"
    qa["candidate"] = cand
    return cand, qa

  # Fixed-width fallback: doc type typically appears around cols 16-26 in these prints
  # (varies, so we allow a wider window and take first token)
  window = raw[14:34] if len(raw) >= 34 else raw
  tokens = re.findall(r"[A-Z0-9]{1,10}", window.upper())
  if tokens:
    cand = _clean(tokens[0])
    qa["method"] = "slice_window"
    qa["candidate"] = cand
    return cand, qa

  qa["method"] = "none"
  qa["candidate"] = None
  return None, qa

def is_suspicious_doc_type(tok: str) -> bool:
  t = _clean(tok)
  if not t:
    return True
  if t in SUSPICIOUS:
    return True
  # Also treat pure directions and common street suffixes as suspicious
  if re.fullmatch(r"(N|S|E|W|NE|NW|SE|SW)", t):
    return True
  return False