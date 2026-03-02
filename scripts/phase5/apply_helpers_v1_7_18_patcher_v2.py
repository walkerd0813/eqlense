import os, sys, shutil, datetime, re

PY = r"""C:\seller-app\backend\Phase5_Hampden_Step2_DeedsOnly_Attach_v1_7_12_BULLETPROOF\hampden_step2_attach_events_to_property_spine_v1_7_12.py"""

NEW_BLOCK = r"""
# ------------------------ Address Variant Helpers (v1.7.18) ------------------------
# Purpose: Generate safe, deterministic address variants to improve exact key matching
# without using fuzzy/nearest logic (keeps pipeline defensible).
#
# IMPORTANT: This does NOT "best guess" an address. It only tries alternate normal forms
# that are logically equivalent (unit/lot/extn removal, directional + street-type aliases,
# optional trailing street-type stripping).

import re

_STREET_TYPES = {
    "ST","STREET","RD","ROAD","AVE","AVENUE","BLVD","BOULEVARD","DR","DRIVE","LN","LANE","CT","COURT",
    "PL","PLACE","TER","TERRACE","HWY","HIGHWAY","WAY","PKWY","PARKWAY","CIR","CIRCLE","SQ","SQUARE",
    "PT","POINT","TRL","TRAIL","RTE","ROUTE"
}

_DIR_TO_ABBR = {
    "NORTH":"N","SOUTH":"S","EAST":"E","WEST":"W",
    "NORTHEAST":"NE","NORTHWEST":"NW","SOUTHEAST":"SE","SOUTHWEST":"SW",
}
_ABBR_TO_DIR = {v:k for k,v in _DIR_TO_ABBR.items()}

_TYPE_ALIASES = {
    "AVENUE":"AVE","AVE":"AVE",
    "BOULEVARD":"BLVD","BLVD":"BLVD",
    "STREET":"ST","ST":"ST",
    "ROAD":"RD","RD":"RD",
    "DRIVE":"DR","DR":"DR",
    "LANE":"LN","LN":"LN",
    "COURT":"CT","CT":"CT",
    "PLACE":"PL","PL":"PL",
    "TERRACE":"TER","TER":"TER",
    "CIRCLE":"CIR","CIR":"CIR",
    "PARKWAY":"PKWY","PKWY":"PKWY",
    "HIGHWAY":"HWY","HWY":"HWY",
    "ROUTE":"RTE","RTE":"RTE",
    # MA quirks seen in assessor strings
    "LA":"LN",
    "PKY":"PKWY"
}

_RE_UNIT = re.compile(r"\b(UNIT|APT|APARTMENT|#)\b.*$", re.I)
_RE_LOT  = re.compile(r"\b(LOT|PAR|PARCEL)\b.*$", re.I)
_RE_REAR = re.compile(r"^\s*REAR(\s+OF)?\s+", re.I)
_RE_EXT  = re.compile(r"\b(EXTN|EXT)\b.*$", re.I)

def _collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).strip()

def _strip_trailing_street_type(s: str) -> str:
    parts = _collapse_spaces(s).split(" ")
    if len(parts) >= 2 and parts[-1].upper().rstrip(".") in _STREET_TYPES:
        return " ".join(parts[:-1]).strip()
    return s

def _apply_dir_aliases(s: str):
    parts = _collapse_spaces(s).split(" ")
    outs = set()
    outs.add(" ".join([_DIR_TO_ABBR.get(p.upper(), p) for p in parts]).strip())
    outs.add(" ".join([_ABBR_TO_DIR.get(p.upper(), p) for p in parts]).strip())
    return [o for o in outs if o]

def _apply_type_aliases(s: str):
    parts = _collapse_spaces(s).split(" ")
    if not parts:
        return []
    outs = set()
    last = parts[-1].upper().rstrip(".")
    if last in _TYPE_ALIASES:
        outs.add(" ".join(parts[:-1] + [_TYPE_ALIASES[last]]).strip())
    outs.add(" ".join([_TYPE_ALIASES.get(p.upper().rstrip("."), p) for p in parts]).strip())
    return [o for o in outs if o]

def _addr_variants(addr_norm: str):
    base = _collapse_spaces((addr_norm or "").upper())
    if not base:
        return
    seen = set()
    def y(v, suf):
        v = _collapse_spaces(v.upper())
        if not v or v in seen:
            return
        seen.add(v)
        yield (v, suf)

    yield from y(base, "")

    v = _RE_REAR.sub("", base)
    if v != base: yield from y(v, "strip_rear")

    v = _RE_UNIT.sub("", base)
    if v != base: yield from y(v, "strip_unit")

    v = _RE_LOT.sub("", base)
    if v != base: yield from y(v, "strip_lot")

    v = _RE_EXT.sub("", base)
    if v != base: yield from y(v, "strip_ext")

    for v in _apply_type_aliases(base):
        if v != base: yield from y(v, "street_type_alias")

    for v in _apply_dir_aliases(base):
        if v != base: yield from y(v, "dir_alias")

    v = _strip_trailing_street_type(base)
    if v != base: yield from y(v, "strip_street_type")

    for seed, seed_suf in [(_RE_UNIT.sub("", base),"strip_unit"), (_RE_LOT.sub("", base),"strip_lot"), (_RE_EXT.sub("", base),"strip_ext")]:
        seed = _collapse_spaces(seed)
        if not seed or seed == base:
            continue
        v2 = _strip_trailing_street_type(seed)
        if v2 != seed: yield from y(v2, seed_suf + "+strip_street_type")
        for v3 in _apply_type_aliases(seed):
            if v3 != seed: yield from y(v3, seed_suf + "+street_type_alias")
        for v3 in _apply_dir_aliases(seed):
            if v3 != seed: yield from y(v3, seed_suf + "+dir_alias")

# ---------------------- End Address Variant Helpers (v1.7.18) ----------------------
"""

def main():
    if not os.path.exists(PY):
        print("[fail] python file not found:", PY)
        sys.exit(1)

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    bak = PY + ".bak_helpers_v1_7_18_v2_" + ts
    shutil.copy2(PY, bak)
    print("[backup]", bak)

    src = open(PY, "r", encoding="utf-8").read()

    # 1) Preferred: replace existing helper region = from "def _addr_variants" to just before "def attach_one"
    m1 = re.search(r"(?m)^def\s+_addr_variants\s*\(", src)
    m2 = re.search(r"(?m)^def\s+attach_one\s*\(", src)

    if m1 and m2 and m2.start() > m1.start():
        before = src[:m1.start()]
        after  = src[m2.start():]
        patched = before + NEW_BLOCK + "\n" + after
        open(PY, "w", encoding="utf-8").write(patched)
        print("[ok] replaced existing _addr_variants..attach_one helper region")
        print("[done] v1.7.18 helpers patch applied (v2)")
        return

    # 2) If _addr_variants missing but attach_one exists: insert NEW_BLOCK right above attach_one
    if (not m1) and m2:
        patched = src[:m2.start()] + NEW_BLOCK + "\n" + src[m2.start():]
        open(PY, "w", encoding="utf-8").write(patched)
        print("[ok] inserted helpers above attach_one (no previous _addr_variants found)")
        print("[done] v1.7.18 helpers patch applied (v2)")
        return

    print("[fail] could not locate attach_one() anchor; file structure unexpected")
    sys.exit(2)

if __name__ == "__main__":
    main()

