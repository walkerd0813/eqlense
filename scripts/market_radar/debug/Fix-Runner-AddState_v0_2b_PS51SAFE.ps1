param(
  [string]$Root = "C:\seller-app\backend"
)

$ErrorActionPreference = "Stop"

$runner = Join-Path $Root "scripts\market_radar\debug\Run-MarketRadar-Explainability-Debug_v0_2_PS51SAFE.ps1"
if (!(Test-Path $runner)) { throw "[error] missing runner: $runner" }

$src = Get-Content -Raw -Encoding UTF8 $runner
$orig = $src

# 1) Ensure -State param exists
if ($src -notlike "*`$State*") {
  # Insert State param right after "param(" if possible
  $idx = $src.IndexOf("param(")
  if ($idx -lt 0) { throw "[error] could not find param( block in $runner" }

  $insert = "param(`r`n  [string]`$State = 'MASS',"
  $src = $src.Substring(0,$idx) + $insert + $src.Substring($idx + "param(".Length)
}

# 2) Ensure python call includes --state "$State"
if ($src -notlike "*--state*") {
  $needle = "market_radar_explainability_debug_v0_2.py"
  $i = $src.IndexOf($needle)
  if ($i -lt 0) { throw "[error] could not find python script name token in runner (expected $needle)" }

  $after = $i + $needle.Length
  $src = $src.Insert($after, " --state `"$State`"")
}

if ($src -eq $orig) {
  Write-Host "[warn] no runner changes applied (may already be patched)."
} else {
  $bak = $runner + ".bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
  Copy-Item $runner $bak -Force
  Set-Content -Path $runner -Value $src -Encoding UTF8
  Write-Host "[ok] patched runner (State param + --state passthrough)"
  Write-Host ("[backup] {0}" -f $bak)
}

Write-Host "[done] runner patch complete."
