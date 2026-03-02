import re, datetime

p = r"scripts\phase5\hampden_axis2_reattach_ge10k_v1_28.py"
src = open(p, "r", encoding="utf-8").read()

bak = p + ".bak_suffix_evidence_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
open(bak, "w", encoding="utf-8").write(src)

new = src

# ------------------------------------------------------------
# 1) Replace SUF_TO_SPINE dict (fix TERRACE typo + add common tokens)
# ------------------------------------------------------------
dict_pat = re.compile(r"(?ms)^\s*SUF_TO_SPINE\s*=\s*\{.*?\}\s*\n")
dict_repl = """SUF_TO_SPINE = {
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
"""
new2, n_dict = dict_pat.subn(dict_repl, new, count=1)
if n_dict != 1:
    raise SystemExit(f"PATCH_FAIL: expected 1 SUF_TO_SPINE block, replaced={n_dict}")

new = new2

# ------------------------------------------------------------
# 2) Force norm_suffix to use SUF_TO_SPINE
# ------------------------------------------------------------
norm_pat = re.compile(r"(?ms)def\s+norm_suffix\s*\(.*?\):\n(?:[ \t].*\n)+")
m = norm_pat.search(new)
if not m:
    raise SystemExit("PATCH_FAIL: norm_suffix block not found")

norm_block = """def norm_suffix(tok: str) -> str:
  tok = (tok or "").strip().upper().replace(".", "")
  return SUF_TO_SPINE.get(tok, tok)
"""
new = new[:m.start()] + norm_block + new[m.end():]

# ------------------------------------------------------------
# 3) Fix evidence.events_in / spine_path to reflect args
# ------------------------------------------------------------
# Replace any hardcoded evidence.events_in assignment lines if present
new = re.sub(r"(?m)('events_in'\s*:\s*)'[^']*'(\s*,?)", r"\1args.events\2", new)
new = re.sub(r"(?m)('spine_path'\s*:\s*)'[^']*'(\s*,?)", r"\1args.spine\2", new)

open(p, "w", encoding="utf-8").write(new)

print("[backup]", bak)
print("[ok] patched", p)
print("[done] SUF_TO_SPINE fixed; norm_suffix enforced; evidence paths now use args")
