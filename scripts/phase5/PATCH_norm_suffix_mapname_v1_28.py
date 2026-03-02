import re, datetime

p = r"scripts\phase5\hampden_axis2_reattach_ge10k_v1_28.py"
src = open(p, "r", encoding="utf-8").read()

bak = p + ".bak_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
open(bak, "w", encoding="utf-8").write(src)

# Patch only the norm_suffix function block
pat = re.compile(r"(def\s+norm_suffix\s*\(.*?\):\n(?:[ \t].*\n)+)", re.M)
m = pat.search(src)
if not m:
    raise SystemExit("PATCH_FAIL: norm_suffix block not found")

block = m.group(1)

# Replace map name inside that block
block2 = block.replace("SUFFIX_TO_SPINE", "SUF_TO_SPINE")

if block2 == block:
    print("[warn] no SUFFIX_TO_SPINE reference found inside norm_suffix (nothing to change).")

new = src[:m.start(1)] + block2 + src[m.end(1):]
open(p, "w", encoding="utf-8").write(new)

print("[backup]", bak)
print("[ok] patched", p)
print("[done] norm_suffix now references SUF_TO_SPINE")
