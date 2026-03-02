cd C:\seller-app\backend

$PY  = "C:\seller-app\backend\Phase5_Hampden_Step2_DeedsOnly_Attach_v1_7_12_BULLETPROOF\hampden_step2_attach_events_to_property_spine_v1_7_12.py"
$BAK = "C:\seller-app\backend\Phase5_Hampden_Step2_DeedsOnly_Attach_v1_7_12_BULLETPROOF\hampden_step2_attach_events_to_property_spine_v1_7_12.py.bak_v1_7_16d_20260102_143013"

if (-not (Test-Path $BAK)) {
  Write-Host "[fail] restore backup not found: $BAK"
  Write-Host "[next] paste the newest bak_ path you want to restore from."
  exit 1
}

# 1) restore last-good
Copy-Item $BAK $PY -Force
Write-Host "[ok] restored $PY from $BAK"

# 2) patch: add safe fallbacks after pid = spine_idx.get(key) (without replacing existing method loops)
$src = Get-Content $PY -Raw

# Insert helpers once (near top of file, right before def attach_one() is safest)
if ($src -notmatch "(?m)^def\s+_el_strip_trailing_street_type\s*\(") {

  $m = [regex]::Match($src, "(?m)^(def\s+attach_one\s*\()", [System.Text.RegularExpressions.RegexOptions]::Multiline)
  if (-not $m.Success) {
    Write-Host "[fail] could not find anchor: def attach_one("
    exit 1
  }

  $helpers = @"
# ------------------------ Extra Key Fallback Helpers ------------------------
_EL_STREET_TYPES = set([
    "ST","STREET","RD","ROAD","DR","DRIVE","AVE","AV","AVENUE","BLVD","BOULEVARD","LN","LANE",
    "CT","COURT","PL","PLACE","PKWY","PARKWAY","HWY","HIGHWAY","TER","TERRACE","CIR","CIRCLE",
    "WAY","TRL","TRAIL","EXT","EXTN"
])

def _el_strip_rear_prefix(a: str) -> str:
    a = (a or "").strip().upper()
    if a.startswith("REAR OF "):
        return a[len("REAR OF "):].strip()
    if a.startswith("REAR "):
        return a[len("REAR "):].strip()
    return a

def _el_strip_trailing_street_type(a: str) -> str:
    a = (a or "").strip().upper()
    if not a:
        return a
    parts = a.split()
    if len(parts) >= 3 and parts[-1] in _EL_STREET_TYPES:
        return " ".join(parts[:-1]).strip()
    return a
# ---------------------- End Extra Key Fallback Helpers ----------------------

"@

  $src = $src.Insert($m.Index, $helpers)
  Write-Host "[ok] inserted fallback helpers above def attach_one()"
} else {
  Write-Host "[ok] fallback helpers already present"
}

# Patch the FIRST occurrence of:
#   pid = spine_idx.get(key)
#   if pid:
#       return pid, "ATTACHED_A", method, town_norm, addr_norm, None
#
# We replace ONLY that mini-block, leaving the rest of attach_one unchanged.
$pattern = "(?m)^(?<ind>\s*)pid\s*=\s*spine_idx\.get\(key\)\s*\r?\n(?<ind2>\s*)if\s+pid:\s*\r?\n(?<ind3>\s*)return\s+pid,\s*""ATTACHED_A"",\s*method,\s*town_norm,\s*addr_norm,\s*None\s*$"

$rx = New-Object System.Text.RegularExpressions.Regex(
  $pattern,
  [System.Text.RegularExpressions.RegexOptions]::Multiline
)

if (-not $rx.IsMatch($src)) {
  Write-Host "[fail] could not find ATTACHED_A return lookup block to patch."
  Write-Host "[next] run:"
  Write-Host "Select-String -Path `"$PY`" -Pattern 'return pid, ""ATTACHED_A""' -Context 0,6"
  exit 1
}

$src2 = $rx.Replace($src, {
  param($m)
  $ind  = $m.Groups["ind"].Value
  $ind2 = $m.Groups["ind2"].Value
  $ind3 = $m.Groups["ind3"].Value

@"
${ind}pid = spine_idx.get(key)
${ind}if not pid:
${ind}    # Fallback 1: strip REAR/REAR OF prefix (exact key only)
${ind}    _a1 = _el_strip_rear_prefix(addr_norm)
${ind}    if _a1 and _a1 != addr_norm:
${ind}        _k1 = make_key(town_norm, _a1) if 'make_key' in globals() else f"{town_norm}|{_a1}"
${ind}        pid = spine_idx.get(_k1)
${ind}        if pid:
${ind}            addr_norm = _a1
${ind}            method = (method + "+strip_rear") if method else "direct+strip_rear"
${ind}            key = _k1
${ind}if not pid:
${ind}    # Fallback 2: strip trailing street-type token (DR/RD/AVE/etc.) (exact key only)
${ind}    _a2 = _el_strip_trailing_street_type(addr_norm)
${ind}    if _a2 and _a2 != addr_norm:
${ind}        _k2 = make_key(town_norm, _a2) if 'make_key' in globals() else f"{town_norm}|{_a2}"
${ind}        pid = spine_idx.get(_k2)
${ind}        if pid:
${ind}            addr_norm = _a2
${ind}            method = (method + "+strip_street_type") if method else "direct+strip_street_type"
${ind}            key = _k2
${ind2}if pid:
${ind3}return pid, "ATTACHED_A", method, town_norm, addr_norm, None
"@
}, 1)

Set-Content -Path $PY -Value $src2 -Encoding UTF8
Write-Host "[ok] patched ATTACHED_A key lookup with 2 safe fallbacks (preserves existing variant engine)"
Write-Host "[done] v1.7.17 patch applied"

Write-Host "`n[confirm] show lookup region:"
Select-String -Path $PY -Pattern "Fallback 1: strip REAR|Fallback 2: strip trailing street-type|return pid, ""ATTACHED_A""" -Context 0,2 | Select-Object -First 80
