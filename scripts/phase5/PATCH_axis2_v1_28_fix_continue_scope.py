import re, datetime

p = r"scripts\phase5\hampden_axis2_reattach_ge10k_v1_28.py"
src = open(p, "r", encoding="utf-8").read()

bak = p + ".bak_fix_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
open(bak, "w", encoding="utf-8").write(src)

new = src

# -------------------------------------------------
# A) Remove the previously-inserted (badly placed) town normalization block
# -------------------------------------------------
bad_block_pat = re.compile(
    r"\n[ \t]*town\s*=\s*\(r\.get\('town'\).*?\.upper\(\)\n"
    r"[ \t]*if\s+not\s+town:\n"
    r"[ \t]*continue\n",
    re.S
)
new, removed = bad_block_pat.subn("\n", new)

# -------------------------------------------------
# B) Re-insert town normalization INSIDE build_spine_indexes loop:
# Find within build_spine_indexes, the first 'r = json.loads(line)' and insert after it.
# We preserve indentation.
# -------------------------------------------------
func = re.search(r"(def\s+build_spine_indexes\s*\(.*?\):\n)(.*?)(\n\S)", new, re.S)
if not func:
    raise SystemExit("PATCH_FAIL: build_spine_indexes not found")

func_head, func_body, func_tail = func.group(1), func.group(2), func.group(3)

m = re.search(r"(^[ \t]*r\s*=\s*json\.loads\(line\)\s*$)", func_body, re.M)
if not m:
    raise SystemExit("PATCH_FAIL: 'r = json.loads(line)' not found inside build_spine_indexes")

indent = re.match(r"^([ \t]*)r\s*=", m.group(1)).group(1)

insert = (
    m.group(1) + "\n"
    f"{indent}town = (r.get('town') or r.get('jurisdiction_name') or r.get('jurisdiction') or '').strip().upper()\n"
    f"{indent}if not town:\n"
    f"{indent}  continue"
)

func_body2 = func_body[:m.start(1)] + insert + func_body[m.end(1):]

new2 = new[:func.start(2)] + func_body2 + new[func.end(2):]

open(p, "w", encoding="utf-8").write(new2)

print("[backup]", bak)
print("[ok] patched", p)
print("[done] removed_bad_blocks=", removed, "and reinserted town normalization after json.loads(line) inside build_spine_indexes loop")
