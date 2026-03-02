param([Parameter(Mandatory=$true)][string]$Root)

$target = Join-Path $Root "scripts\market_radar\debug\market_radar_explainability_debug_v0_3.py"
if (-not (Test-Path $target)) { throw "[error] target python file not found: $target" }

$bak = "$target.bak_" + (Get-Date -Format "yyyyMMdd_HHmmss")
Copy-Item -Force $target $bak

$pyPath = Join-Path $Root "scripts\market_radar\debug\__v0_3_rewrite_payload__.txt"
if (-not (Test-Path $pyPath)) { throw "[error] payload missing: $pyPath" }

$py = Get-Content -Raw -Encoding UTF8 $pyPath
Set-Content -Path $target -Value $py -Encoding UTF8

Write-Host "[backup] $bak"
Write-Host "[ok] rewrote $target (known-good v0_3)"
Write-Host "[done] v0_3 python rewrite complete."
