import io, re, sys

TARGET = sys.argv[1]

def die(msg):
    raise SystemExit(msg)

with io.open(TARGET, "r", encoding="utf-8") as f:
    txt = f.read()

# helper: strip trailing verify token "Y"
if "def strip_trailing_verify_token" not in txt:
    anchor = re.search(r"\n\s*def\s+norm_ws\s*\(", txt)
    if not anchor:
        die("Could not find def norm_ws(...) anchor")
    helper = []
    helper.append("VERIFY_TOKEN_RE = re.compile(r'\\s+Y\\s*$')")
    helper.append("")
    helper.append("def strip_trailing_verify_token(s: str):")
    helper.append("    if s is None:")
    helper.append("        return None")
    helper.append("    s2 = s.strip()")
    helper.append("    s2 = VERIFY_TOKEN_RE.sub('', s2).strip()")
    helper.append("    return s2")
    i = anchor.start()
    txt = txt[:i] + "\n" + "\n".join(helper) + "\n\n" + txt[i:]

# patch common address setters to trim trailing Y
txt = re.sub(r'current\["address_raw"\]\s*=\s*addr\b', 'current["address_raw"] = strip_trailing_verify_token(addr)', txt)
txt = re.sub(r'current\["address_raw"\]\s*=\s*mm\.group\("addr"\)\.strip\(\)', 'current["address_raw"] = strip_trailing_verify_token(mm.group("addr"))', txt)
txt = re.sub(r'current\["description_raw"\]\s*=\s*norm_ws\(m\.group\("desc"\)\)', 'current["description_raw"] = strip_trailing_verify_token(norm_ws(m.group("desc")))', txt)

# Option A multi-address expansion in Template A flush: replace "events.append(evt)" once
pat = re.compile(r"\n\s*events\.append\(evt\)\n\s*current\s*=\s*None\n")
if pat.search(txt):
    block = []
    block.append('        prop_refs = current.get("property_refs") or []')
    block.append('        if prop_refs:')
    block.append('            for j, pr in enumerate(prop_refs, start=1):')
    block.append('                evt2 = dict(evt)')
    block.append('                evt2["recording"] = dict(evt["recording"])')
    block.append('                evt2["property_ref"] = dict(evt["property_ref"])')
    block.append('                if pr.get("town_raw"): evt2["property_ref"]["town_raw"] = pr.get("town_raw")')
    block.append('                if pr.get("address_raw"): evt2["property_ref"]["address_raw"] = pr.get("address_raw")')
    block.append('                if pr.get("unit_raw"): evt2["property_ref"]["unit_raw"] = pr.get("unit_raw")')
    block.append('                evt2["event_id"] = evt["event_id"] + f"|ADDR|{j}"')
    block.append('                events.append(evt2)')
    block.append('        else:')
    block.append('            events.append(evt)')
    block.append('        current = None')
    txt = pat.sub("\n" + "\n".join(block) + "\n", txt, count=1)

with io.open(TARGET, "w", encoding="utf-8", newline="\n") as f:
    f.write(txt)

print("[ok] patched", TARGET)
