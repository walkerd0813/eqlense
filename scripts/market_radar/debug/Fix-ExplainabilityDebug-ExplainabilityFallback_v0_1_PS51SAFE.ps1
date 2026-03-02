param(
  [Parameter(Mandatory=$true)][string]$Root
)

$target = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_3.py"
if (-not (Test-Path $target)) { throw "[error] missing target: $target" }

$bak = "$target.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -Force $target $bak

$src = Get-Content -Raw -Encoding UTF8 $target

# We insert a fallback block just BEFORE the 'paths = {' dict is created.
$needle = "paths = {"
$idx = $src.IndexOf($needle)
if ($idx -lt 0) { throw "[error] could not find marker 'paths = {' in $target" }

$fallback = @"
# --- PS51SAFE PATCH: explainability CURRENT fallback ---
# If pointers don't expose explainability path (or resolver misses it),
# but the CURRENT explainability artifact exists, use it.
if explainability_zip is None:
    _fallback = os.path.join(root, "publicData", "marketRadar", "CURRENT", "CURRENT_MARKET_RADAR_EXPLAINABILITY_ZIP.ndjson")
    if os.path.exists(_fallback):
        explainability_zip = _fallback
# --- END PATCH ---
"@

# Ensure we only patch once
if ($src.Contains("explainability CURRENT fallback")) {
  Write-Host "[skip] fallback already present"
  Write-Host "[backup] $bak"
  exit 0
}

$src2 = $src.Substring(0, $idx) + $fallback + "`r`n" + $src.Substring($idx)

Set-Content -Path $target -Value $src2 -Encoding UTF8
Write-Host "[ok] patched explainability resolver fallback"
Write-Host "[backup] $bak"
Write-Host "[done] patch complete"
