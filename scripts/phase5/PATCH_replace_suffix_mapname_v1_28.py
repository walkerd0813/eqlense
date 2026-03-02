import datetime

p = r"scripts\phase5\hampden_axis2_reattach_ge10k_v1_28.py"
src = open(p, "r", encoding="utf-8").read()

bak = p + ".bak_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
open(bak, "w", encoding="utf-8").write(src)

if "SUFFIX_TO_SPINE" not in src:
    raise SystemExit("PATCH_ABORT: SUFFIX_TO_SPINE not found in file (nothing to replace)")

new = src.replace("SUFFIX_TO_SPINE", "SUF_TO_SPINE")
open(p, "w", encoding="utf-8").write(new)

print("[backup]", bak)
print("[ok] patched", p)
print("[done] replaced SUFFIX_TO_SPINE -> SUF_TO_SPINE, count=", src.count("SUFFIX_TO_SPINE"))
