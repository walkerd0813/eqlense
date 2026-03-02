import re, datetime

p = r"scripts\phase5\hampden_axis2_reattach_ge10k_v1_28.py"
src = open(p, "r", encoding="utf-8").read()

bak = p + ".bak_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
open(bak, "w", encoding="utf-8").write(src)

patched = src
n_total = 0

# -----------------------------
# PATCH A: Force overwrite of incoming attach blob
# Insert right after: r = json.loads(line)
# -----------------------------
needle = r"r\s*=\s*json\.loads\(line\)\s*\n"
insert = (
    "r = json.loads(line)\n"
    "      # --- PATCH: do not inherit prior attachment state from hydrated input ---\n"
    "      if isinstance(r.get('attach'), dict):\n"
    "        r['attach_prev'] = r.get('attach')\n"
    "      for _k in ('attach','attach_scope','attach_status','attachments_n'):\n"
    "        if _k in r:\n"
    "          try:\n"
    "            del r[_k]\n"
    "          except Exception:\n"
    "            pass\n"
)
patched, n = re.subn(needle, insert, patched, count=1)
n_total += n
if n != 1:
    raise SystemExit(f"PATCH_FAIL A: expected 1 insertion after json.loads(line), replaced={n}")

# -----------------------------
# PATCH B: Normalize spine town to UPPER when building indexes
# We replace the FIRST 'town =' assignment inside build_spine_indexes with robust logic.
# -----------------------------
m = re.search(r"(def\s+build_spine_indexes\s*\(.*?\):)(.*?)(\n\S)", patched, flags=re.S)
if not m:
    raise SystemExit("PATCH_FAIL B: build_spine_indexes function not found")

head = m.group(1)
body = m.group(2)
tail = m.group(3)

# replace first occurrence of a town assignment in that function body
town_pat = re.compile(r"(^[ \t]+town\s*=\s*.*?$)", re.M)
town_repl = (
    "    town = (r.get('town') or r.get('jurisdiction_name') or r.get('jurisdiction') or '').strip().upper()\n"
    "    if not town:\n"
    "      continue\n"
)
body2, n2 = town_pat.subn(town_repl, body, count=1)
if n2 != 1:
    raise SystemExit(f"PATCH_FAIL B: could not find/replace a 'town =' assignment inside build_spine_indexes (replaced={n2})")

patched2 = patched[:m.start(2)] + body2 + patched[m.end(2):]
patched = patched2
n_total += 1

open(p, "w", encoding="utf-8").write(patched)

print("[backup]", bak)
print("[ok] patched", p)
print("[done] applied patches:", n_total, "(A overwrite incoming attach + B town upper in spine index)")
