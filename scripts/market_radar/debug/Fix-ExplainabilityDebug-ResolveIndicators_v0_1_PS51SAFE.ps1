param(
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

$py = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_2.py"
$ps = Join-Path $Root "scripts\market_radar\debug\Run-MarketRadar-Explainability-Debug_v0_2_PS51SAFE.ps1"

if (!(Test-Path $py)) { throw "[error] missing: $py" }
if (!(Test-Path $ps)) { throw "[error] missing: $ps" }

# -----------------------------
# Patch PY
# -----------------------------
$src = Get-Content -Raw -Encoding UTF8 $py

# 1) Add --state arg (default MASS) right after parser creation
if ($src -notmatch "add_argument\(\s*['""]--state['""]") {
  $marker = "parser = argparse.ArgumentParser"
  $idx = $src.IndexOf($marker)
  if ($idx -lt 0) { throw "[error] could not find parser = argparse.ArgumentParser in $py" }

  # find end of that line
  $lineEnd = $src.IndexOf("`n", $idx)
  if ($lineEnd -lt 0) { $lineEnd = $src.Length }

  $insert = "`nparser.add_argument('--state', default='MASS', help='State code for indicators pointers (default MASS)')"
  $src = $src.Insert($lineEnd, $insert)
}

# 2) Add helper resolver function after `import json` (or after imports)
if ($src -notmatch "def\s+_resolve_indicators_ndjson") {
  $helper = @"
def _resolve_indicators_ndjson(ind_ptrs: dict, state: str):
    try:
        st = (ind_ptrs or {}).get('states', {}).get(state, {})
        return st.get('ndjson')
    except Exception:
        return None

"@

  $importJson = "import json"
  $ij = $src.IndexOf($importJson)
  if ($ij -ge 0) {
    $ijLineEnd = $src.IndexOf("`n", $ij)
    if ($ijLineEnd -lt 0) { $ijLineEnd = $src.Length }
    $src = $src.Insert($ijLineEnd + 1, $helper)
  } else {
    # fallback: prepend helper near top
    $src = $helper + $src
  }
}

# 3) Replace any existing assignment so indicators path resolves from pointers
# We handle a few possible patterns safely:
if ($src -match "indicators_p01_zip_state\s*=\s*None") {
  $src = [regex]::Replace($src, "indicators_p01_zip_state\s*=\s*None", "indicators_p01_zip_state = _resolve_indicators_ndjson(indicators_ptrs, args.state)")
} elseif ($src -match "indicators_p01_zip_state\s*=\s*null") {
  $src = [regex]::Replace($src, "indicators_p01_zip_state\s*=\s*null", "indicators_p01_zip_state = _resolve_indicators_ndjson(indicators_ptrs, args.state)")
} else {
  # If the variable doesn't exist, we still want to set it after indicators_ptrs load
  if ($src -notmatch "indicators_p01_zip_state") {
    $needle = "indicators_ptrs"
    $ni = $src.IndexOf($needle)
    if ($ni -ge 0) {
      $niLineEnd = $src.IndexOf("`n", $ni)
      if ($niLineEnd -lt 0) { $niLineEnd = $src.Length }
      $src = $src.Insert($niLineEnd + 1, "indicators_p01_zip_state = _resolve_indicators_ndjson(indicators_ptrs, args.state)`n")
    }
  }
}

# 4) Add state into meta output (non-fatal if not found)
if ($src -match "'zip'\s*:\s*args\.zip") {
  $src = [regex]::Replace($src, "('zip'\s*:\s*args\.zip\s*,?)", "`$1`n    'state': args.state,")
}

# write python backup + patched file
$bakPy = $py + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $py $bakPy -Force
Set-Content -Path $py -Value $src -Encoding UTF8
Write-Host "[ok] patched python debug resolver"
Write-Host ("[backup] {0}" -f $bakPy)

# -----------------------------
# Patch PS runner (add -State + pass --state)
# -----------------------------
$psSrc = Get-Content -Raw -Encoding UTF8 $ps

# add param if missing
if ($psSrc -notmatch "\[string\]\s*\$State") {
  $psSrc = [regex]::Replace(
    $psSrc,
    "param\(",
    "param(`n  [string]`$State = 'MASS',"
  )
}

# pass --state if missing
if ($psSrc -notmatch "--state") {
  $psSrc = $psSrc -replace "market_radar_explainability_debug_v0_2\.py""", "market_radar_explainability_debug_v0_2.py"" --state `"$State`""
}

$bakPs = $ps + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item $ps $bakPs -Force
Set-Content -Path $ps -Value $psSrc -Encoding UTF8
Write-Host "[ok] patched PS runner to accept -State and pass --state"
Write-Host ("[backup] {0}" -f $bakPs)

Write-Host "[done] debug indicators resolve patch complete."
