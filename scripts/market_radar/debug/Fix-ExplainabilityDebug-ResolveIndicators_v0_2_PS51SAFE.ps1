param(
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_2.py"
$ps = Join-Path $Root "scripts\market_radar\debug\Run-MarketRadar-Explainability-Debug_v0_2_PS51SAFE.ps1"

if (!(Test-Path $py)) { throw "[error] missing: $py" }
if (!(Test-Path $ps)) { throw "[error] missing: $ps" }

$src = Get-Content -Raw -Encoding UTF8 $py
$orig = $src

# -----------------------------
# 1) Ensure --state argument exists (default MASS)
# Anchor insertion on the first occurrence of add_argument(
# -----------------------------
if ($src -notmatch "add_argument\(\s*['""]--state['""]") {

  $m = [regex]::Match($src, "add_argument\(")
  if (!$m.Success) {
    throw "[error] could not find any add_argument( call in $py (unexpected; file may have changed)."
  }

  # Insert before the first add_argument(
  $insert = "parser.add_argument('--state', default='MASS', help='State code for indicators pointers (default MASS)')`n"
  $src = $src.Insert($m.Index, $insert)
}

# -----------------------------
# 2) Add helper resolver + use it (no assumptions about parser variable name)
# -----------------------------
if ($src -notmatch "def\s+_resolve_indicators_ndjson") {
  $helper = @"
def _resolve_indicators_ndjson(ind_ptrs: dict, state: str):
    try:
        st = (ind_ptrs or {}).get('states', {}).get(state, {})
        return st.get('ndjson')
    except Exception:
        return None

"@

  # Insert helper after imports block.
  # Find first blank line after initial imports region.
  $importsEnd = [regex]::Match($src, "(\r?\n)\r?\n").Index
  if ($importsEnd -lt 0) { $importsEnd = 0 }
  $src = $src.Insert($importsEnd + 2, $helper)
}

# -----------------------------
# 3) Replace the old "indicators_p01_zip_state = None" (or create it)
# We anchor on reading indicators pointers, not on any exact structure.
# -----------------------------
if ($src -match "indicators_p01_zip_state\s*=\s*None") {
  $src = [regex]::Replace(
    $src,
    "indicators_p01_zip_state\s*=\s*None",
    "indicators_p01_zip_state = _resolve_indicators_ndjson(indicators_ptrs, args.state)"
  )
} elseif ($src -notmatch "indicators_p01_zip_state") {
  # Insert after the first occurrence of "indicators_ptrs ="
  $m2 = [regex]::Match($src, "indicators_ptrs\s*=")
  if ($m2.Success) {
    $lineEnd = $src.IndexOf("`n", $m2.Index)
    if ($lineEnd -lt 0) { $lineEnd = $src.Length }
    $src = $src.Insert($lineEnd + 1, "indicators_p01_zip_state = _resolve_indicators_ndjson(indicators_ptrs, args.state)`n")
  }
}

# -----------------------------
# 4) Add state into meta if possible (optional)
# -----------------------------
if ($src -match "'zip'\s*:\s*args\.zip" -and $src -notmatch "'state'\s*:") {
  $src = [regex]::Replace($src, "('zip'\s*:\s*args\.zip\s*,?)", "`$1`n    'state': args.state,")
}

if ($src -eq $orig) {
  Write-Host "[warn] no changes applied (file may already be patched or patterns did not match)."
} else {
  $bakPy = $py + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item $py $bakPy -Force
  Set-Content -Path $py -Value $src -Encoding UTF8
  Write-Host "[ok] patched python debug resolver"
  Write-Host ("[backup] {0}" -f $bakPy)
}

# -----------------------------
# Patch PS runner: add -State param + pass --state
# -----------------------------
$psSrc = Get-Content -Raw -Encoding UTF8 $ps
$origPs = $psSrc

if ($psSrc -notmatch "\[string\]\s*\$State") {
  $psSrc = [regex]::Replace($psSrc, "param\(", "param(`n  [string]`$State = 'MASS',")
}

if ($psSrc -notmatch "--state") {
  # Inject right after the python script name token if present
  $psSrc = [regex]::Replace(
    $psSrc,
    "(market_radar_explainability_debug_v0_2\.py\s*)",
    "`$1--state `"$State`" "
  )
}

if ($psSrc -ne $origPs) {
  $bakPs = $ps + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item $ps $bakPs -Force
  Set-Content -Path $ps -Value $psSrc -Encoding UTF8
  Write-Host "[ok] patched PS runner to accept -State and pass --state"
  Write-Host ("[backup] {0}" -f $bakPs)
}

Write-Host "[done] debug indicators resolve patch complete."
