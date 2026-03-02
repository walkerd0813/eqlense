import json, re
from collections import defaultdict, Counter
from datetime import datetime, timezone

def norm_ws(s): return re.sub(r"\s+", " ", (s or "").strip())
def up(s): return norm_ws(s).upper()

DIR_TOKENS = {"N","S","E","W","NE","NW","SE","SW","NORTH","SOUTH","EAST","WEST"}

# IMPORTANT: THIS MAP OUTPUTS SPINE DIALECT (short suffixes)
SUF_TO_SPINE = {
  # core
  "ST":"ST","STREET":"ST",
  "RD":"RD","ROAD":"RD",
  "DR":"DR","DRIVE":"DR",
  "AVE":"AV","AV":"AV","AVENUE":"AV",
  "BLVD":"BLVD","BOULEVARD":"BLVD",

  # circles
  "CIR":"CR","CIRCLE":"CR","CR":"CR",

  # terrace (spine dialect: TE)
  "TER":"TE","TERR":"TE","TERRACE":"TE","TE":"TE",

  # misc
  "CT":"CT","COURT":"CT",
  "LN":"LN","LANE":"LN",
  "PL":"PL","PLACE":"PL",
  "PKWY":"PKWY","PARKWAY":"PKWY",
  "WAY":"WAY",

  # Hampden-ish
  "HL":"HL","HILL":"HL",
}
def collapse_runs(s):
  # MELWOOOD -> MELWOOD
  return re.sub(r"([A-Z])\1{2,}", r"\1\1", s)

def split_tokens(s):
  return [t for t in re.split(r"[^A-Z0-9#]+", up(s)) if t]

def normalize_unit_from_tokens(toks):
  """
  Broad unit handling:
  - 'UNIT 305' / 'APT 3' / 'STE 2A'
  - '#305'
  - trailing single token sometimes used as unit (rare; we won't guess that)
  """
  unit=None
  out=[]
  i=0
  while i<len(toks):
    t=toks[i]
    if t in ("UNIT","APT","APARTMENT","STE","SUITE","#"):
      if i+1<len(toks):
        unit=toks[i+1].lstrip("#")
        i+=2
        continue
    if t.startswith("#") and len(t)>1:
      unit=t[1:]
      i+=1
      continue
    out.append(t)
    i+=1
  return out, unit

def norm_suffix(tok: str) -> str:
  tok = (tok or "").strip().upper().replace(".", "")
  return SUF_TO_SPINE.get(tok, tok)
