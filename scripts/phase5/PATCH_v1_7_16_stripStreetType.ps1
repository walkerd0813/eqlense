cd C:\seller-app\backend

$PY = "C:\seller-app\backend\Phase5_Hampden_Step2_DeedsOnly_Attach_v1_7_12_BULLETPROOF\hampden_step2_attach_events_to_property_spine_v1_7_12.py"

$bak = "$PY.bak_v1_7_16_stripStreetType_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $PY $bak -Force
Write-Host "[backup] $bak"

$src = Get-Content $PY -Raw

# ---------- insert helpers (only if missing) ----------
$anchor = "def detect_unknown_bucket"
$ins = @"
STREET_TYPES = {
    `"ST`",`"STREET`",`"AVE`",`"AV`",`"AVENUE`",`"RD`",`"ROAD`",`"DR`",`"DRIVE`",`"LN`",`"LANE`",`"BLVD`",`"BOULEVARD`",
    `"CT`",`"COURT`",`"CIR`",`"CIRCLE`",`"PKWY`",`"PARKWAY`",`"PL`",`"PLACE`",`"TER`",`"TERRACE`",`"WAY`",`"HWY`",`"HIGHWAY`",
    `"EXT`",`"EXTN`",`"EXTENSION`"
}

def _addr_variants(addr_norm):
    out = []
    a = (addr_norm or `"").strip().upper()
    if not a:
        return out
    out.append((a, `""))
    if a.startswith(`"REAR OF `"):
        out.append((a[len(`"REAR OF `"):].strip(), `"strip_rear_of`"))
    toks = a.split()
    if len(toks) >= 2 and toks[-1] in STREET_TYPES:
        out.append((`" `".join(toks[:-1]).strip(), `"strip_street_type`"))
    seen = set()
    uniq = []
    for v,m in out:
        if v and v not in seen:
            seen.add(v)
            uniq.append((v,m))
    return uniq
"@

if ($src -notmatch "STREET_TYPES\s*=\s*\{") {
  if ($src -match [regex]::Escape($anchor)) {
    $src = [regex]::Replace(
      $src,
      "(def\s+detect_unknown_bucket\s*\(.*?\)\s*:\s*\n)",
      "`$1`n$ins`n",
      1,
      [System.Text.RegularExpressions.RegexOptions]::Singleline
    )
    Write-Host "[ok] inserted STREET_TYPES + _addr_variants()"
  } else {
    Write-Host "[fail] could not find anchor: def detect_unknown_bucket"
    exit 1
  }
} else {
  Write-Host "[ok] STREET_TYPES already present (skipping insert)"
}

# ---------- patch direct lookup ----------
$replaced = $false

# IMPORTANT: use single quotes for regex literals, and represent a single quote inside as ''
# This regex matches: key = f"{town_norm}|{addr_norm}" OR f'{town_norm}|{addr_norm}'
$patA = '(key\s*=\s*f["'']\{town_norm\}\|\{addr_norm\}["'']\s*\n\s*pid\s*=\s*spine_idx\.get\(key\))'
if ($src -match $patA) {
  $src = [regex]::Replace($src, $patA, @"
# try direct key, then safe variants
pid = None
key = None
for _av, _msuf in _addr_variants(addr_norm):
    key = f"{town_norm}|{_av}"
    pid = spine_idx.get(key)
    if pid:
        method = "direct" if not _msuf else f"direct+{_msuf}"
        addr_norm = _av
        break
"@, 1)
  $replaced = $true
  Write-Host "[ok] patched attach_one direct lookup (pattern A)"
}

$patB = '(pid\s*=\s*spine_idx\.get\(\s*f["'']\{town_norm\}\|\{addr_norm\}["'']\s*\))'
if (-not $replaced -and $src -match $patB) {
  $src = [regex]::Replace($src, $patB, @"
# try direct key, then safe variants
pid = None
for _av, _msuf in _addr_variants(addr_norm):
    pid = spine_idx.get(f"{town_norm}|{_av}")
    if pid:
        method = "direct" if not _msuf else f"direct+{_msuf}"
        addr_norm = _av
        break
"@, 1)
  $replaced = $true
  Write-Host "[ok] patched attach_one direct lookup (pattern B)"
}

if (-not $replaced) {
  Write-Host "[fail] could not find the direct spine_idx lookup pattern to patch."
  Write-Host "[next] run: Select-String -Path `"$PY`" -Pattern 'spine_idx.get' -Context 0,8"
  exit 1
}

Set-Content -Path $PY -Value $src -Encoding UTF8
Write-Host "[done] v1.7.16 strip-street-type patch applied"
