$ErrorActionPreference="Stop"
Set-StrictMode -Version Latest

$py = "C:\seller-app\backend\scripts\registry\otr\otr_extract_hampden_v2.py"
if (-not (Test-Path $py)) { throw "Missing: $py" }

$orig = Get-Content -Raw -LiteralPath $py -Encoding UTF8
$txt  = $orig

# --- 1) Inject strip_trailing_verify_token helper (only if missing) ---
if ($txt -notmatch "def\s+strip_trailing_verify_token\s*\(") {
  $needle = "def norm_ws("
  $idx = $txt.IndexOf($needle)
  if ($idx -lt 0) { throw "Could not locate norm_ws() to anchor helper injection." }
  $inject = @()
  $inject += "def strip_trailing_verify_token(s: str) -> str:"
  $inject += "    if s is None:"
  $inject += "        return None"
  $inject += "    s = s.strip()"
  $inject += "    # Hampden OTR tables append VFY token 'Y' at end of addr/desc lines; remove ONLY a trailing standalone token."
  $inject += "    return re.sub(r'\\s+Y\\s*$', '', s)"
  $inject += ""
  $injectText = ($inject -join \"`n\")
  $txt = $txt.Insert($idx, $injectText + \"`n\")
}

# --- 2) Template A: Town/Addr line -> strip trailing Y and preserve multi-addrs ---
# You showed these exact lines exist:
#   addr = norm_ws(tm.group(\"addr\"))
#   current[\"address_raw\"] = addr
if ($txt -match "addr\s*=\s*norm_ws\(tm\.group\(\""addr\""\)\)") {
  $txt = $txt -replace "addr\s*=\s*norm_ws\(tm\.group\(\""addr\""\)\)", "addr = strip_trailing_verify_token(norm_ws(tm.group(\""addr\"")))"
}

# Ensure address_raw assignment also strips (belt + suspenders)
if ($txt -match "current\[\x22address_raw\x22\]\s*=\s*addr") {
  $txt = $txt -replace "current\[\x22address_raw\x22\]\s*=\s*addr", "current[\"address_raw\"] = strip_trailing_verify_token(addr)"
}

# Also strip trailing Y from description_raw wherever it feeds unit extraction
if ($txt -match "current\[\x22description_raw\x22\]\s*=\s*norm_ws\(") {
  $txt = $txt -replace "current\[\x22description_raw\x22\]\s*=\s*norm_ws\((?<x>[^)]+)\)", "current[\"description_raw\"] = strip_trailing_verify_token(norm_ws(${x}))"
}

# --- 3) Template B / money-at-end address line: current[\"address_raw\"] = mm.group(\"addr\").strip() ---
if ($txt -match "current\[\x22address_raw\x22\]\s*=\s*mm\.group\(\""addr\""\)\.strip\(\)") {
  $txt = $txt -replace "current\[\x22address_raw\x22\]\s*=\s*mm\.group\(\""addr\""\)\.strip\(\)", "current[\"address_raw\"] = strip_trailing_verify_token(mm.group(\""addr\""\).strip())"
}

# --- 4) Option A multi-property: emit one event per Town/Addr line (same inst, unique event_id) ---
# We replace the single append with a loop block that:
#  - if current has multi_addrs list -> emit N events with eid suffix |ADDR|i
#  - else emit the original single event
$appendNeedle = "events.append(evt)"
if ($txt -notmatch [regex]::Escape($appendNeedle)) { throw "Could not find events.append(evt) to patch multi-address emission." }

$loop = @()
$loop += "        # --- MULTI_ADDR_OPTION_A ---"
$loop += "        addrs = current.get(\"multi_addrs\") or []"
$loop += "        if addrs:"
$loop += "            for i, a in enumerate(addrs, start=1):"
$loop += "                evt2 = dict(evt)"
$loop += "                evt2[\"event_id\"] = evt[\"event_id\"] + f\"|ADDR|{i}\""
$loop += "                evt2[\"property_ref\"] = dict(evt[\"property_ref\"])"
$loop += "                evt2[\"property_ref\"][\"town_raw\"] = a.get(\"town_raw\")"
$loop += "                evt2[\"property_ref\"][\"address_raw\"] = a.get(\"address_raw\")"
$loop += "                evt2[\"property_ref\"][\"unit_raw\"] = a.get(\"unit_raw\")"
$loop += "                evt2.setdefault(\"meta\", {})[\"multi_addr_count\"] = len(addrs)"
$loop += "                evt2[\"meta\"][\"multi_addr_seq\"] = i"
$loop += "                events.append(evt2)"
$loop += "        else:"
$loop += "            events.append(evt)"
$loopText = ($loop -join \"`n\")

# Replace only the first occurrence per template block (safer)
$pos = $txt.IndexOf($appendNeedle)
$txt = $txt.Substring(0,$pos) + $loopText + $txt.Substring($pos + $appendNeedle.Length)

# --- 5) Populate current.multi_addrs when we see Town/Addr lines (without grabbing attorney header addresses) ---
# We hook right after: current[\"town_raw\"] = ... and current[\"address_raw\"] = ...
# Add: current.setdefault(\"multi_addrs\", []).append({town,address,unit})
$hookNeedle = "current[\"address_raw\"] = strip_trailing_verify_token(addr)"
if ($txt -notmatch [regex]::Escape($hookNeedle)) { throw "Could not locate patched address_raw assignment to hook multi_addrs append." }
$hookInsert = @()
$hookInsert += $hookNeedle
$hookInsert += "            u = extract_unit(current.get(\"address_raw\")) or extract_unit(current.get(\"description_raw\"))"
$hookInsert += "            current[\"unit_raw\"] = u if u else current.get(\"unit_raw\")"
$hookInsert += "            current.setdefault(\"multi_addrs\", []).append({\"town_raw\": current.get(\"town_raw\"), \"address_raw\": current.get(\"address_raw\"), \"unit_raw\": current.get(\"unit_raw\")})"
$hookInsertText = ($hookInsert -join \"`n\")
$txt = $txt -replace [regex]::Escape($hookNeedle), [System.Text.RegularExpressions.Regex]::Escape($hookInsertText).Replace("\\","\")

# --- write back ---
Set-Content -LiteralPath $py -Value $txt -Encoding UTF8

# Python syntax check (real parse check)
python -m py_compile $py
if ($LASTEXITCODE -ne 0) { throw "Python syntax check failed after patch." }

Write-Host "[ok] patched: $py"
Write-Host "[ok] py_compile OK"
