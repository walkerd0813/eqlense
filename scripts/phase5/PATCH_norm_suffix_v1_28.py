import re, datetime

p = r"scripts\phase5\hampden_axis2_reattach_ge10k_v1_28.py"
src = open(p, "r", encoding="utf-8").read()

bak = p + ".bak_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
open(bak, "w", encoding="utf-8").write(src)

# Replace ONLY the norm_suffix function body (leave everything else as-is)
pat = re.compile(r"def\s+norm_suffix\s*\(.*?\):\n(?:[ \t].*\n)+", re.M)

replacement = (
"def norm_suffix(tok: str) -> str:\n"
"  tok = (tok or \"\").strip().upper().replace(\".\", \"\")\n"
"  return SUFFIX_TO_SPINE.get(tok, tok)\n\n"
)

new, n = pat.subn(replacement, src, count=1)

if n != 1:
    raise SystemExit(f"PATCH_FAIL: expected 1 norm_suffix block, replaced={n}. Backup at {bak}")

open(p, "w", encoding="utf-8").write(new)

print("[backup]", bak)
print("[ok] patched", p)
print("[done] norm_suffix is now dict-lookup only (no substring replace).")
